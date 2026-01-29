# LogFlow System Design - Long Term Architecture

## The Problem with Current State

```
Current: Everything in memory, nothing persisted, no metrics, no learning
Future:  Persistent state, optimization feedback loops, enterprise-ready
```

---

## Core Subsystems

```
┌─────────────────────────────────────────────────────────────────────┐
│                         LOGFLOW SYSTEM                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │   CONFIG     │  │    STATE     │  │   METRICS    │              │
│  │   MANAGER    │  │    STORE     │  │   COLLECTOR  │              │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘              │
│         │                 │                 │                        │
│         └────────────┬────┴────────────────┘                        │
│                      │                                               │
│              ┌───────▼───────┐                                      │
│              │   CORE ENGINE  │                                      │
│              │  (Conversion)  │                                      │
│              └───────┬───────┘                                      │
│                      │                                               │
│    ┌─────────────────┼─────────────────┐                            │
│    │                 │                 │                             │
│    ▼                 ▼                 ▼                             │
│ ┌──────┐        ┌──────┐        ┌──────┐                           │
│ │ CLI  │        │ HTTP │        │ SDK  │                           │
│ │      │        │ API  │        │      │                           │
│ └──────┘        └──────┘        └──────┘                           │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 1. Configuration Management

### Hierarchy (lowest to highest priority)

```
1. Compiled Defaults     → Sensible out-of-box
2. System Config         → /etc/logflow/config.yaml
3. User Config           → ~/.logflow/config.yaml
4. Project Config        → .logflow.yaml (in project dir)
5. Environment Vars      → LOGFLOW_COMPRESSION=zstd
6. CLI Flags             → --compression zstd
7. API Request           → {"compression": "zstd"}
```

### Config Schema

```yaml
# ~/.logflow/config.yaml
version: 1

# Engine settings
engine:
  default: duckdb              # duckdb | arrow
  memory_limit: 4GB            # Max memory usage
  threads: 0                   # 0 = auto (num CPUs)
  temp_dir: /tmp/logflow       # Temp file location

# Default conversion settings
conversion:
  compression: snappy          # snappy | zstd | gzip | lz4 | none
  row_group_size: 128MB        # Parquet row group size
  batch_size: 8192             # Processing batch size

# Plugin defaults
plugins:
  quality:
    enabled: true              # Run automatically
    null_threshold: 0.5        # Warn if > 50% nulls

  process_mining:
    enabled: false             # Don't run automatically
    auto_detect: true          # Suggest when columns match

# Server settings
server:
  port: 8080
  host: localhost
  max_upload_size: 500MB
  cors_origins: ["*"]

# Storage settings
storage:
  database: ~/.logflow/logflow.db    # SQLite for state
  jobs_retention: 30d                 # Keep job history for 30 days
  cache_dir: ~/.logflow/cache         # Schema cache, etc.

# Telemetry (opt-in)
telemetry:
  enabled: false
  endpoint: ""                        # Self-hosted option

# Profiles (reusable configurations)
profiles:
  erp_sales:
    case_id: OrderID
    activity: Step
    timestamp: EventTime
    compression: zstd
```

---

## 2. State Store (SQLite)

### Why SQLite?
- Zero configuration
- Single file, easy backup
- Embedded, no server needed
- Fast for our use case (local state)

### Schema

```sql
-- Job history
CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,           -- pending, running, completed, failed
    input_path TEXT NOT NULL,
    input_format TEXT,
    input_size INTEGER,
    output_path TEXT,
    output_size INTEGER,
    row_count INTEGER,
    column_count INTEGER,
    compression_ratio REAL,
    duration_ms INTEGER,
    throughput REAL,                 -- rows/sec
    error TEXT,
    config JSON,                     -- Full config used
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

-- Schema cache (avoid re-inferring)
CREATE TABLE schema_cache (
    file_hash TEXT PRIMARY KEY,      -- SHA256 of first 1MB
    file_path TEXT,
    file_size INTEGER,
    schema JSON,                     -- Detected columns
    pm_suggestion JSON,              -- Auto-detected PM mapping
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Profiles
CREATE TABLE profiles (
    name TEXT PRIMARY KEY,
    config JSON NOT NULL,
    usage_count INTEGER DEFAULT 0,
    last_used_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Performance metrics (for optimization)
CREATE TABLE metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT REFERENCES jobs(id),
    metric_name TEXT NOT NULL,       -- parse_time, write_time, etc.
    metric_value REAL NOT NULL,
    context JSON,                    -- file_size, format, etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_created ON jobs(created_at);
CREATE INDEX idx_metrics_name ON metrics(metric_name);
```

### Use Cases

```go
// Recent jobs
SELECT * FROM jobs ORDER BY created_at DESC LIMIT 20;

// Average throughput by format
SELECT input_format, AVG(throughput) as avg_throughput
FROM jobs WHERE status = 'completed'
GROUP BY input_format;

// Find similar schemas (for suggestions)
SELECT * FROM schema_cache
WHERE json_extract(schema, '$.columns[0].name') = 'order_id';

// Most used profiles
SELECT name, usage_count FROM profiles ORDER BY usage_count DESC;
```

---

## 3. Metrics & Optimization

### Metrics We Collect

```go
type ConversionMetrics struct {
    // Timing
    TotalDuration    time.Duration
    ParseDuration    time.Duration
    WriteDuration    time.Duration
    AnalysisDuration time.Duration

    // Throughput
    RowsPerSecond    float64
    BytesPerSecond   float64

    // Memory
    PeakMemoryUsage  int64
    GCPauseTotal     time.Duration

    // I/O
    BytesRead        int64
    BytesWritten     int64
    DiskIOWait       time.Duration

    // Quality
    NullRowsSkipped  int64
    ParseErrors      int64
}
```

### Optimization Feedback Loop

```
┌─────────────────────────────────────────────────────────────┐
│                  OPTIMIZATION LOOP                           │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│   1. COLLECT                                                 │
│      └── Every conversion records metrics                   │
│                                                              │
│   2. ANALYZE                                                 │
│      └── "CSV files > 100MB are 3x faster with DuckDB"      │
│      └── "zstd compression 20% better for text-heavy data"  │
│                                                              │
│   3. RECOMMEND                                               │
│      └── Auto-select engine based on file size/type         │
│      └── Suggest optimal compression                         │
│                                                              │
│   4. LEARN (Future)                                          │
│      └── ML model for optimal settings prediction           │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Example: Smart Engine Selection

```go
func SelectEngine(fileSize int64, format string) Engine {
    // Query historical metrics
    stats := db.Query(`
        SELECT engine, AVG(throughput) as avg_throughput
        FROM jobs
        WHERE input_format = ? AND input_size BETWEEN ? AND ?
        GROUP BY engine
    `, format, fileSize*0.8, fileSize*1.2)

    // Pick best performing engine for similar files
    if stats.DuckDB.AvgThroughput > stats.Arrow.AvgThroughput * 1.2 {
        return EngineDuckDB
    }
    return EngineArrow
}
```

---

## 4. Packaging & Distribution

### Build Matrix

```yaml
# .github/workflows/release.yaml
platforms:
  - os: linux
    arch: [amd64, arm64]
  - os: darwin
    arch: [amd64, arm64]  # Intel + Apple Silicon
  - os: windows
    arch: [amd64]

artifacts:
  - logflow-{version}-{os}-{arch}.tar.gz
  - logflow-{version}-{os}-{arch}.zip  # Windows
  - checksums.txt
```

### Distribution Channels

```
1. GitHub Releases    → Primary, with checksums
2. Homebrew           → brew install logflow/tap/logflow
3. Docker             → docker run logflow/logflow convert ...
4. Go Install         → go install github.com/logflow/logflow@latest
5. Scoop (Windows)    → scoop install logflow
```

### Version Strategy

```
v1.0.0    Major.Minor.Patch

Major: Breaking changes (config format, API changes)
Minor: New features, new plugins
Patch: Bug fixes, performance improvements
```

### Migration System

```go
// For config/database schema changes
type Migration struct {
    Version     int
    Description string
    Up          func(db *sql.DB) error
    Down        func(db *sql.DB) error
}

var migrations = []Migration{
    {1, "Initial schema", migrateV1Up, migrateV1Down},
    {2, "Add metrics table", migrateV2Up, migrateV2Down},
    {3, "Add profile usage tracking", migrateV3Up, migrateV3Down},
}
```

---

## 5. Performance Optimization Roadmap

### Phase 1: Measure (Current)
- [ ] Add timing instrumentation to all stages
- [ ] Record metrics to SQLite
- [ ] Add `logflow benchmark` command

### Phase 2: Obvious Wins
- [ ] Schema caching (don't re-infer same file)
- [ ] Connection pooling for DuckDB
- [ ] Parallel column processing for wide tables
- [ ] Memory-mapped I/O for large files

### Phase 3: Smart Defaults
- [ ] Auto-select engine based on metrics history
- [ ] Auto-tune batch size based on available memory
- [ ] Predictive compression selection

### Phase 4: Advanced
- [ ] Incremental conversion (only process new rows)
- [ ] Columnar caching for repeated analysis
- [ ] Distributed processing for very large files

---

## 6. Directory Structure (Long Term)

```
~/.logflow/
├── config.yaml           # User configuration
├── logflow.db            # SQLite state database
├── cache/
│   ├── schemas/          # Cached schema inferences
│   └── parquet/          # Temporary conversion cache
├── profiles/
│   ├── erp_sales.yaml
│   └── web_logs.yaml
├── plugins/              # Custom plugins (future)
└── logs/
    └── logflow.log       # Debug logs (optional)
```

---

## 7. API Versioning

```
/api/v1/upload
/api/v1/convert
/api/v1/job/:id

# Future versions
/api/v2/...
```

### Deprecation Policy
- v1 supported for 12 months after v2 release
- Clear deprecation warnings in responses
- Migration guides for breaking changes

---

## 8. Security Considerations

### Local Mode (Default)
- Binds to localhost only
- No authentication needed
- Files stay on local disk

### Network Mode (Optional)
- Requires `--network` flag
- Token-based authentication
- HTTPS with auto-generated certs

### Data Handling
- Temp files cleaned up after conversion
- No data sent externally (unless telemetry enabled)
- Anonymization plugin for PII

---

## 9. Monitoring & Debugging

### Health Check
```
GET /api/health
{
    "status": "healthy",
    "version": "1.0.0",
    "uptime": "2h30m",
    "jobs_completed": 150,
    "jobs_failed": 2,
    "disk_usage": "1.2GB"
}
```

### Debug Mode
```bash
LOGFLOW_DEBUG=1 logflow convert -i file.csv -o file.parquet
# Outputs detailed timing, memory usage, query plans
```

### Profiling
```bash
logflow convert -i file.csv -o file.parquet --profile cpu.pprof
go tool pprof cpu.pprof
```

---

## 10. Implementation Priority

### Now (Foundation)
1. Config system with hierarchy
2. SQLite state store
3. Basic metrics collection
4. Schema caching

### Next (Optimization)
5. Smart engine selection
6. Performance benchmarking
7. Batch processing improvements

### Later (Scale)
8. Plugin system (external plugins)
9. Distributed mode
10. Cloud storage integration (S3, GCS)

---

## Summary

```
BEFORE                          AFTER
------                          -----
Stateless                   →   Persistent job history
No learning                 →   Metrics-driven optimization
Single config               →   Hierarchical configuration
Manual tuning               →   Auto-tuned defaults
No caching                  →   Schema & result caching
Hope it's fast              →   Measured & optimized
```
