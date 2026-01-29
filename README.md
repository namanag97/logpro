# LogFlow

High-performance CLI for converting process mining data to Apache Parquet.

```
CSV/XES/XLSX/JSON  ──>  Parquet  ──>  PM4Py / Celonis / ProM
```

## Features

- **Multi-format Input**: CSV, XES, XLSX, JSON, JSONL, access logs (+ gzip compressed)
- **High Performance**: 100K+ events/sec via DuckDB + Arrow columnar processing
- **Interactive Wizard**: TUI for guided column mapping
- **Data Contracts**: Generate `.contract.json` for schema validation
- **Watch Mode**: Auto-convert on file changes (real-time pipelines)
- **Semantic Diff**: Git-style comparison of process logs
- **Data Quality**: Inspection, sampling, anonymization, BI exports

## Quick Start

```bash
# Interactive wizard
logflow

# Direct conversion
logflow convert -i events.csv -o events.parquet

# With DuckDB engine (fastest)
logflow convert -i events.csv -o events.parquet --duckdb

# Excel file
logflow convert -i data.xlsx -o data.parquet

# Compressed XES
logflow convert -i log.xes.gz -o log.parquet
```

## Installation

```bash
go install github.com/logflow/logflow/cmd/logflow@latest
```

Or build from source:
```bash
git clone https://github.com/logflow/logflow
cd logflow
go build -o logflow ./cmd/logflow
```

## Commands

### Core Commands

| Command | Description |
|---------|-------------|
| `logflow` | Interactive wizard (default) |
| `logflow convert` | Convert file to Parquet |
| `logflow apply` | Apply saved profile |
| `logflow profiles` | List saved profiles |

### Analysis Commands

| Command | Description |
|---------|-------------|
| `logflow inspect -i file` | Data quality report |
| `logflow stats -i file` | Basic statistics |
| `logflow diff file1 file2` | Semantic comparison |
| `logflow schema -i file.csv` | Infer CSV schema |

### Data Operations

| Command | Description |
|---------|-------------|
| `logflow sample -i in -o out` | Random sampling |
| `logflow anonymize -i in -o out` | GDPR-compliant hashing |
| `logflow export -i in -o dir` | PowerBI/Tableau star schema |
| `logflow watch in out` | Auto-convert on changes |

## Supported Formats

| Format | Extension | Status |
|--------|-----------|--------|
| CSV | `.csv`, `.csv.gz` | Full |
| XES | `.xes`, `.xes.gz` | Full |
| Excel | `.xlsx` | Full |
| JSON | `.json` | Full |
| JSONL | `.jsonl`, `.ndjson` | Full |
| Access Log | `.log` | Full |
| Parquet | `.parquet` | Read (via DuckDB) |

## Convert Options

```bash
logflow convert -i input.csv -o output.parquet \
  --case-id "CaseID" \
  --activity "Activity" \
  --timestamp "Timestamp" \
  --resource "Resource" \
  --timestamp-format "2006-01-02 15:04:05" \
  --compression zstd \
  --contract \
  --duckdb
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--case-id` | `case:concept:name` | Case ID column |
| `--activity` | `concept:name` | Activity column |
| `--timestamp` | `time:timestamp` | Timestamp column |
| `--resource` | `org:resource` | Resource column |
| `--compression` | `snappy` | Compression (snappy/zstd/gzip/lz4/none) |
| `--duckdb` | false | Use DuckDB engine |
| `--contract` | false | Generate data contract |
| `--metadata` | - | Custom metadata (key=value) |

## Watch Mode (Real-Time Pipeline)

Monitor a file and auto-convert on changes:

```bash
logflow watch sales.csv sales.parquet
```

Output:
```
Watching sales.csv -> sales.parquet
Press Ctrl+C to stop

[14:32:01] Change detected, converting... done in 142ms
[14:35:22] Change detected, converting... done in 156ms
```

## Semantic Diff

Compare two process logs to detect drift:

```bash
logflow diff march_2024.csv april_2024.csv
```

Output:
```
=== Process Log Diff Report ===

Events:  125,432 -> 142,891 (+17,459)
Cases:   8,234 -> 9,102 (+868)

Time Range:
  Left:  2024-03-01 to 2024-03-31 (744h)
  Right: 2024-04-01 to 2024-04-30 (720h)

Avg Case Duration: 4h32m -> 5h15m (+15.8%)

New Activities: ["Escalate to Manager", "Auto-Retry"]
Removed Activities: []

Activity Changes (top 10):
  Activity                         Left      Right      Delta       Status
  --------                         ----      -----      -----       ------
  Payment Processing              12543     15234     +5.2%    increased
  Order Validation                 8234      7102     -3.1%    decreased
  ...
```

### Structural Diff (PMPT - Git for Processes)

Use the Process Merkle Patricia Tree for O(1) fingerprint comparison:

```bash
logflow diff --structural q1.csv q2.csv
```

Output:
```
Building Process Merkle Patricia Trees...

Fingerprint Check (O(1)):
  Left:  a1b2c3d4e5f6...
  Right: f6e5d4c3b2a1...

Result: PROCESSES DIFFER

=== Process Tree Diff ===

New Variants (+3 nodes):
  + Login -> Verify -> 2FA -> Pay (cases: 1,234)
  + Login -> Retry -> Verify (cases: 456)

Removed Variants (-1 nodes):
  - Login -> Direct Pay (was: 2,100 cases)

Frequency Shifts (top 5):
  1. Verify: 12.3% -> 18.7% (+6.4%)
  2. Payment: 45.2% -> 41.1% (-4.1%)

Tree Statistics:
                           Left        Right
  Total Cases              8234         9102
  Total Events           125432       142891
  Unique Nodes               47           52
  Process Variants           12           15
  Max Depth                   8            9
```

**Why PMPT?**
- **O(1) comparison**: Compare million-row logs by comparing 32-byte hashes
- **50-90% compression**: Shared process prefixes stored once
- **Sub-linear search**: Find variants without scanning every row

## Data Contracts

Generate trust metadata alongside Parquet files:

```bash
logflow convert -i data.csv -o data.parquet --contract
```

Creates `data.contract.json`:
```json
{
  "version": "1.0",
  "file": {
    "name": "data.parquet",
    "size_bytes": 2970624,
    "sha256": "a1b2c3d4...",
    "row_count": 156789
  },
  "schema": {
    "columns": [
      {"name": "case_id", "type": "string", "nullable": false, "role": "case_id"},
      {"name": "activity", "type": "string", "nullable": false, "role": "activity"},
      {"name": "timestamp", "type": "int64", "nullable": false, "role": "timestamp"},
      {"name": "resource", "type": "string", "nullable": true, "role": "resource"}
    ]
  },
  "slo": {
    "null_tolerance": 0.05,
    "timestamp_order": "asc",
    "required_columns": ["case_id", "activity", "timestamp"]
  }
}
```

## Profiles

Save column mappings for reuse:

```bash
# Create profile via wizard, then apply later
logflow apply --profile erp_sales data.csv
```

Profiles stored in `~/.logflow/` as YAML:
```yaml
name: erp_sales
case_id: OrderID
activity: Step
timestamp: EventTime
resource: Handler
compression: zstd
```

## Data Quality Inspection

```bash
logflow inspect -i events.csv
```

Output:
```
=== Data Quality Report ===

Completeness:
  case_id:   100.0% (0 nulls)
  activity:  100.0% (0 nulls)
  timestamp: 99.8% (312 nulls)
  resource:  87.2% (19,842 nulls)

Uniqueness:
  Cases: 8,234 unique
  Activities: 12 unique
  Resources: 45 unique

Distribution:
  Events per case: min=1, max=847, avg=19.2

Top Activities:
  1. Submit Order (45,231)
  2. Validate (38,102)
  3. Process Payment (32,891)
```

## Performance

| Dataset | Rows | CSV Size | Parquet Size | Time (Arrow) | Time (DuckDB) |
|---------|------|----------|--------------|--------------|---------------|
| BPI 2017 | 1.2M | 145 MB | 13.6 MB | 2.1s | 0.8s |
| Insurance | 156K | 32.5 MB | 2.97 MB | 0.4s | 0.2s |

Typical compression ratio: **10x**

## Architecture

```
Input File ─> Parser ─> Event Channel ─> Writer ─> Parquet
                │                           │
            (Go/Arrow)                  (DuckDB)
```

- **Hybrid Engine**: Go for I/O, DuckDB for vectorized processing
- **Zero-Copy**: Apache Arrow columnar format
- **Streaming**: Processes files larger than memory
- **Pooled Buffers**: Minimal GC pressure

## Examples

### PowerBI Export

```bash
logflow export -i events.parquet --format powerbi -o ./powerbi/
```

Creates star schema:
- `Fact_Events.parquet`
- `Dim_Cases.parquet`
- `Dim_Activities.parquet`
- `Dim_Resources.parquet`
- `Dim_Time.parquet`

### Sampling

```bash
# 10% sample
logflow sample -i large.csv -o sample.parquet --rate 0.1

# Exact count (reservoir sampling)
logflow sample -i large.csv -o sample.parquet --count 10000
```

### Anonymization

```bash
logflow anonymize -i data.csv -o anon.parquet \
  --columns "customer_name,email" \
  --salt "my-secret-salt"
```

### Stdin/Stdout

```bash
cat events.csv | logflow convert -i - -o events.parquet --format csv
```

## Enterprise Features

### Process Merkle Patricia Tree (PMPT)

Git-like version control for business processes:

```go
import "github.com/logflow/logflow/pkg/pmpt"

// Build tree from events
builder := pmpt.NewBuilder(pmpt.DefaultBuilderConfig())
for event := range events {
    builder.Add(event)
}
tree := builder.FlushAll()

// O(1) fingerprint comparison
if tree1.Equals(tree2) {
    fmt.Println("Processes are identical")
}

// Find process variants
variants := tree.GetVariants()
for _, v := range variants[:5] {
    fmt.Printf("%s (cases: %d)\n", v.String(), v.Count)
}
```

### Interval Tree (Time-Based Queries)

O(log N) bottleneck detection:

```go
import "github.com/logflow/logflow/pkg/pmpt"

interval := pmpt.NewIntervalTree()

// Add time-annotated events
interval.Add(node, startTime, endTime)

// Find concurrent activities (bottleneck detection)
overlaps := interval.FindOverlaps("Approve", "Review")

// Query time range
events := interval.QueryRange(startTimestamp, endTimestamp)

// Find bottlenecks
bottlenecks := interval.FindBottlenecks(start, end, 10)
```

### Fault Tolerance

Circuit breaker and poison pill handling:

```go
import "github.com/logflow/logflow/pkg/resilience"

// Circuit breaker prevents overload
cb := resilience.NewCircuitBreaker().
    WithMaxMemory(0.90).      // Trip at 90% memory
    WithMaxConcurrent(1000).  // Max concurrent ops
    WithCooldown(30 * time.Second)

if cb.Allow() {
    cb.Start()
    defer cb.End(true)
    // Process...
}

// Poison pill handler for malicious data
handler := resilience.NewPoisonPillHandler().
    WithMaxRowSize(10 * 1024 * 1024).  // 10MB max row
    WithParseTimeout(5 * time.Second)

handler.SafeProcess(ctx, rowNum, func() error {
    return parseRow(row)
})
```

### Checkpointing (Resumable Processing)

Resume processing after Lambda timeout or crash:

```go
checkpoint := resilience.NewCheckpoint(input, output, "checkpoint.json")

if checkpoint.ShouldResume() {
    offset := checkpoint.GetResumePoint()
    // Seek to offset and resume
}

// Update checkpoint periodically
checkpoint.Update(rowNum, byteOffset)
```

### Observability (OpenTelemetry)

Integrate with Grafana, Datadog, or AWS X-Ray:

```go
import "github.com/logflow/logflow/pkg/telemetry"

tracer := telemetry.NewTracer("logflow").
    WithExportEndpoint("http://localhost:4317")

ctx, span := tracer.StartSpan(ctx, "parse_csv")
span.SetAttribute("file_size", fileSize)
span.SetAttribute("format", "csv")

// ... processing ...

span.AddEvent("rows_processed", map[string]interface{}{
    "count": rowCount,
})
tracer.EndSpan(span)

// Metrics
metrics := telemetry.NewMetrics()
metrics.IncrementEvents(1000)
metrics.RecordLatency(elapsed)

summary := metrics.Summary()
fmt.Printf("P99 latency: %v\n", summary.P99Latency)
```

## Architecture

```
                    ┌──────────────────────────────────────────┐
                    │              LogFlow CLI                  │
                    └─────────────────┬────────────────────────┘
                                      │
          ┌───────────────────────────┼───────────────────────────┐
          │                           │                           │
          ▼                           ▼                           ▼
   ┌─────────────┐           ┌─────────────┐           ┌─────────────┐
   │   Parser    │           │   DuckDB    │           │   Writer    │
   │ CSV/XES/XLSX│           │   Engine    │           │  Parquet    │
   └──────┬──────┘           └──────┬──────┘           └──────┬──────┘
          │                         │                         │
          └─────────────────────────┼─────────────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          │                         │                         │
          ▼                         ▼                         ▼
   ┌─────────────┐           ┌─────────────┐           ┌─────────────┐
   │    PMPT     │           │  Interval   │           │  Contract   │
   │ Merkle Tree │           │    Tree     │           │   .json     │
   └─────────────┘           └─────────────┘           └─────────────┘

Enterprise Layer:
   ┌─────────────┐           ┌─────────────┐           ┌─────────────┐
   │  Telemetry  │           │  Resilience │           │ Checkpoint  │
   │   (OTLP)    │           │ CircuitBreak│           │  (Resume)   │
   └─────────────┘           └─────────────┘           └─────────────┘
```

## Package Structure

```
pkg/
├── parser/        # Format parsers (CSV, XES, XLSX, JSON)
├── writer/        # Parquet writers (Arrow, DuckDB)
├── pmpt/          # Process Merkle Patricia Tree
│   ├── tree.go    # Core PMPT implementation
│   ├── interval.go # Interval tree for time queries
│   ├── builder.go  # Event stream to tree builder
│   └── serialize.go # Serialization for Parquet metadata
├── diff/          # Semantic log comparison
├── contract/      # Data contract generation
├── watch/         # File watcher (real-time pipelines)
├── resilience/    # Fault tolerance (circuit breaker, poison pill)
├── telemetry/     # Observability (tracing, metrics)
└── tui/           # Terminal UI components
```

## Requirements

- Go 1.21+
- DuckDB (bundled via go-duckdb)

## License

MIT
