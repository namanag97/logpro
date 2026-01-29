# LogFlow Backend Architecture

## Design Philosophy

```
"Simple things should be simple, complex things should be possible."
                                                    - Alan Kay
```

**Core Principle:** Drop a file → Get Parquet. Everything else is optional.

---

## Feature Inventory

### CORE (Zero Config)
| Feature | What it does | User Input Required |
|---------|--------------|---------------------|
| Format Conversion | CSV/JSON/XLSX/XES → Parquet | None |
| Auto Schema | Detect column types automatically | None |
| Compression | Apply Snappy/Zstd/Gzip | None (default: snappy) |

### PLUGINS (Progressive Disclosure)

#### Tier 1: Automatic Discovery
These run automatically if applicable, shown in results.

| Plugin | What it does | Trigger |
|--------|--------------|---------|
| Quality Analysis | Null rates, uniqueness, distributions | Always (fast) |
| Schema Summary | Column types, row counts | Always |
| Size Analysis | Compression ratio, file sizes | Always |

#### Tier 2: Suggested (Auto-Detected)
System detects these might be useful, offers one-click enable.

| Plugin | What it does | Detection |
|--------|--------------|-----------|
| Process Mining | Cases, activities, variants | Detect columns matching patterns |
| Time Series | Trends, seasonality | Detect timestamp columns |
| Geospatial | Location analysis | Detect lat/lon columns |

#### Tier 3: User-Initiated
User explicitly requests these features.

| Plugin | What it does | Requires |
|--------|--------------|----------|
| Anonymization | Hash PII columns | Column selection |
| Sampling | Random subset | Rate or count |
| Star Schema Export | Fact + Dimension tables | Column mapping |
| Diff | Compare two files | Second file |
| Watch Mode | Auto-convert on change | Output path |
| Data Contract | Generate .contract.json | Output path |

---

## API Design

### Endpoints

```
POST /api/upload          → Upload file, get job_id + auto-schema
POST /api/convert         → Start conversion (minimal config)
GET  /api/job/:id         → Poll job status + progress
GET  /api/job/:id/stream  → SSE for real-time progress
GET  /api/download/:id    → Download output file

POST /api/analyze         → Run plugin on existing file
GET  /api/plugins         → List available plugins
GET  /api/schema?path=x   → Get schema for file
```

### Response Flow

```
1. UPLOAD
   Request:  POST /api/upload (multipart file)
   Response: {
     "job_id": "abc123",
     "file_name": "sales.csv",
     "file_size": 15000000,
     "schema": {
       "columns": [
         {"name": "order_id", "type": "VARCHAR"},
         {"name": "product", "type": "VARCHAR"},
         {"name": "timestamp", "type": "TIMESTAMP"},
         {"name": "amount", "type": "DOUBLE"}
       ],
       "row_count": 150000
     },
     "suggestions": {
       "process_mining": {
         "detected": true,
         "confidence": 0.85,
         "mapping": {
           "case_id": "order_id",
           "activity": "product",
           "timestamp": "timestamp"
         }
       }
     }
   }

2. CONVERT (Minimal)
   Request:  POST /api/convert
   Body:     { "job_id": "abc123" }  // That's it! Zero config.
   Response: { "status": "started" }

3. CONVERT (With Options)
   Request:  POST /api/convert
   Body:     {
     "job_id": "abc123",
     "compression": "zstd",
     "plugins": ["quality", "process_mining"],
     "pm_config": {
       "case_id": "order_id",
       "activity": "product",
       "timestamp": "timestamp"
     }
   }

4. POLL STATUS
   GET /api/job/abc123
   Response: {
     "id": "abc123",
     "status": "running",
     "progress": {
       "phase": "converting",
       "percent": 45,
       "rows_written": 67500,
       "throughput": 125000,
       "message": "Processing rows..."
     }
   }

5. COMPLETED
   GET /api/job/abc123
   Response: {
     "id": "abc123",
     "status": "completed",
     "result": {
       "input_path": "/tmp/sales.csv",
       "output_path": "/tmp/sales.parquet",
       "input_size": 15000000,
       "output_size": 1200000,
       "row_count": 150000,
       "compression": 12.5,
       "duration_ms": 1200,
       "throughput": 125000
     },
     "analysis": {
       "quality": {
         "overall_score": 94.5,
         "columns": [...]
       },
       "process_mining": {
         "total_cases": 5000,
         "total_events": 150000,
         "unique_activities": 12,
         "top_variants": [...]
       }
     }
   }
```

---

## Backend Module Structure

```
pkg/
├── core/                    # ZERO-CONFIG CORE
│   ├── convert.go          # Main conversion logic
│   ├── plugins.go          # Plugin interfaces
│   └── detect.go           # Auto-detection logic
│
├── plugins/                 # OPTIONAL PLUGINS
│   ├── quality/            # Data quality analysis
│   │   └── plugin.go
│   ├── processmining/      # PM-specific features
│   │   ├── plugin.go
│   │   ├── pmpt.go         # Merkle Patricia Tree
│   │   └── diff.go         # Process diff
│   ├── anonymize/          # PII hashing
│   │   └── plugin.go
│   ├── sample/             # Random sampling
│   │   └── plugin.go
│   ├── export/             # Star schema, BI exports
│   │   └── plugin.go
│   └── timeseries/         # Time-based analysis
│       └── plugin.go
│
├── server/                  # HTTP API
│   ├── server.go           # Main server
│   ├── handlers.go         # Route handlers
│   ├── sse.go              # Server-Sent Events
│   └── middleware.go       # CORS, logging, etc.
│
└── web/                     # EMBEDDED UI
    ├── index.html
    ├── app.js
    └── style.css
```

---

## User Journey Mapping

### Journey 1: "I just want Parquet" (80% of users)

```
User Action                    System Response
-----------                    ---------------
1. Open LogFlow               → Clean landing page
2. Drop CSV file              → "sales.csv uploaded (15MB, 150K rows)"
                              → Shows column preview
3. Click "Convert"            → Progress bar
                              → "✓ Complete: 15MB → 1.2MB (12.5x)"
4. Click "Download"           → Gets sales.parquet
```

**API calls:** upload → convert → poll → download (4 calls, zero config)

### Journey 2: "I want Process Mining insights"

```
User Action                    System Response
-----------                    ---------------
1-2. Same as above            → Schema detected, PM suggested
                              → "Looks like process data! Enable analysis?"
3. Click "Yes, analyze"       → Shows mapping (auto-filled)
                              → User can adjust if needed
4. Click "Convert & Analyze"  → Progress: Converting... Analyzing...
5. View Results               → Parquet + PM Report
                              → Cases, variants, bottlenecks
```

### Journey 3: "I need to anonymize before sharing"

```
User Action                    System Response
-----------                    ---------------
1-2. Same as above            → Schema detected
3. Click "More Options"       → Expands plugin panel
4. Enable "Anonymize"         → Shows column selector
5. Select "customer_email"    → Marked for hashing
6. Click "Convert"            → customer_email → SHA256 hashes
```

### Journey 4: "Compare two process logs"

```
User Action                    System Response
-----------                    ---------------
1. Upload march.csv           → Converted
2. Upload april.csv           → Converted
3. Click "Compare"            → Semantic diff report
                              → New activities, removed activities
                              → Frequency changes
                              → Duration changes
```

---

## Progress Streaming (SSE)

For real-time updates without polling:

```
GET /api/job/abc123/stream
Accept: text/event-stream

Response (streaming):
event: progress
data: {"phase":"converting","percent":10,"message":"Starting..."}

event: progress
data: {"phase":"converting","percent":45,"rows":67500,"throughput":125000}

event: progress
data: {"phase":"analyzing","percent":80,"message":"Running quality check..."}

event: complete
data: {"status":"completed","result":{...}}
```

---

## Plugin Registration

```go
// Each plugin self-registers
func init() {
    core.RegisterPlugin(&QualityPlugin{})
    core.RegisterPlugin(&ProcessMiningPlugin{})
    // etc.
}

// Plugin interface
type Plugin interface {
    Name() string
    Description() string

    // Auto-detection
    CanAutoRun(schema []Column) bool     // Run automatically?
    ShouldSuggest(schema []Column) bool  // Suggest to user?

    // Execution
    Analyze(ctx context.Context, result *ConversionResult) (interface{}, error)
}
```

---

## Configuration Hierarchy

```
1. Zero Config (default)     → Just works
2. Auto-Detected             → System suggests, user confirms
3. User-Specified            → Explicit configuration
4. Profile (saved)           → Reusable configurations
```

---

## Error Handling Strategy

```
Error Type          User Message                          Action
----------          ------------                          ------
File not found      "File not found: {path}"              Show upload again
Unsupported format  "Format not supported: {ext}"         List supported formats
Parse error         "Error at row {n}: {detail}"          Show row preview
Memory limit        "File too large for browser"          Suggest CLI
Network error       "Connection lost, retrying..."        Auto-retry
```

---

## Next Steps

1. **Backend:** Implement remaining plugins (anonymize, sample, export)
2. **API:** Add SSE endpoint for streaming progress
3. **UI:** You provide spec, I implement
4. **CLI:** Add `logflow serve` command to start web server

---

## Feature Matrix

| Feature | CLI | Web UI | API |
|---------|-----|--------|-----|
| Convert CSV/JSON/XLSX → Parquet | ✅ | ✅ | ✅ |
| Auto schema detection | ✅ | ✅ | ✅ |
| Quality analysis | ✅ | ✅ | ✅ |
| Process mining | ✅ | ✅ | ✅ |
| Anonymization | ✅ | ✅ | ✅ |
| Sampling | ✅ | ✅ | ✅ |
| Star schema export | ✅ | 🔜 | ✅ |
| Diff/Compare | ✅ | 🔜 | ✅ |
| Watch mode | ✅ | ❌ | ❌ |
| Data contracts | ✅ | ✅ | ✅ |
| PMPT (Merkle Tree) | ✅ | 🔜 | ✅ |
| Batch processing | ✅ | 🔜 | ✅ |

✅ = Available  🔜 = Planned  ❌ = Not Applicable
