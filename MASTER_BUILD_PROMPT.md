# LogFlow: Master Build Prompt for Agent

## Mission

Build a production-grade, high-performance data ingestion and analytics library in Go that can **stream any data format and output analysis-ready Parquet/Iceberg**, with pluggable interfaces for auth, catalog, storage, and scheduling.

---

## Context & Prior Work

### What Already Exists (in `/Users/namanagarwal/logpro/pkg/ingest/`)

```
pkg/ingest/
├── detect.go         (780 lines)  - Format/delimiter/encoding detection
├── heuristics.go     (707 lines)  - Source buckets, decision table, adaptive tuning
├── pipeline.go       (450 lines)  - Unified pipeline with concurrency control
├── fast_path.go      (355 lines)  - DuckDB native processing
├── robust_path.go    (830 lines)  - Go streaming with error recovery
├── quality.go        (612 lines)  - Checksum, entropy, cardinality (HyperLogLog)
├── ingest.go         (357 lines)  - Engine API
└── benchmark_test.go (600 lines)  - Tests and benchmarks
```

**Current Performance**: 933K rows/sec, 106 MB/sec, EXCELLENT grade

### Key Documents to Reference
- `PLATFORM_ARCHITECTURE.md` - Full multi-dimensional architecture
- `INGESTION_PLAN.md` - Detailed integration plan
- `INTEGRATION_PLAN.md` - Module-level refactoring plan

---

## Architecture Principles

### 1. Core vs Pluggable Separation

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         PLUGGABLE (Interfaces)                           │
│   Auth │ Catalog │ Storage │ Scheduler │ Alerting │ Metrics             │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │ plug into
┌────────────────────────────────▼────────────────────────────────────────┐
│                         CORE ENGINE (We Build)                           │
│                                                                          │
│   ANY FORMAT ──▶ DETECT ──▶ DECODE ──▶ VALIDATE ──▶ WRITE ──▶ QUERY    │
│                                                                          │
│   Inputs:  CSV, TSV, JSON, JSONL, XML, XES, Parquet, Avro, Excel, Logs  │
│   Outputs: Parquet files, Iceberg tables, Delta tables, Arrow streams   │
│   Query:   DuckDB-based SQL, Arrow Flight for streaming                 │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2. The Three Layers

| Layer | Responsibility | Key Interfaces |
|-------|----------------|----------------|
| **Ingest** | Stream any format → Arrow → Storage | `Source`, `Decoder`, `Sink` |
| **Storage** | Manage tables, partitions, versioning | `Catalog`, `Table`, `ObjectStorage` |
| **Query** | SQL analytics on stored data | `QueryEngine`, `ResultSet` |

### 3. Heuristic-Driven Processing

```
Source Classification (by processing cost):
├── Bucket 1 - Native (Parquet, ORC, Avro): ~500 MB/s, high concurrency
├── Bucket 2 - Cheap (CSV, TSV, JSONL): ~150 MB/s, medium concurrency
├── Bucket 3 - Medium (JSON, XML): ~50 MB/s, limited concurrency
└── Bucket 4 - Expensive (Excel, PDF): ~10 MB/s, low concurrency

Decision Flow:
File → Detect Format → Classify Bucket → Select Strategy → Configure Heuristics → Execute
```

---

## Complete Module Structure to Build

```
pkg/
│
├── ═══════════════════════════════════════════════════════════════════════
│   INTERFACES (Users implement these to extend the system)
├── ═══════════════════════════════════════════════════════════════════════
│
├── interfaces/
│   ├── auth.go                 # Authenticator, Authorizer, Identity
│   ├── catalog.go              # Catalog, Table, TableSpec
│   ├── storage.go              # ObjectStorage, ObjectInfo
│   ├── scheduler.go            # Scheduler, Job, JobSpec, JobStatus
│   ├── metrics.go              # MetricsExporter, Gauge, Counter, Histogram
│   ├── tracer.go               # Tracer, Span, SpanContext
│   └── alerter.go              # Alerter, Alert, AlertLevel
│
├── ═══════════════════════════════════════════════════════════════════════
│   CORE - Ingest Layer (Stream anything to Arrow)
├── ═══════════════════════════════════════════════════════════════════════
│
├── ingest/
│   │
│   ├── core/                   # Core abstractions
│   │   ├── source.go           # Source interface: Location(), Open(), Metadata()
│   │   ├── decoder.go          # Decoder interface: Decode() -> chan RecordBatch
│   │   ├── sink.go             # Sink interface: Write(RecordBatch), Close()
│   │   ├── batch.go            # RecordBatch wrapper with metadata
│   │   └── options.go          # IngestOptions, DecodeOptions, SinkOptions
│   │
│   ├── sources/                # Source implementations
│   │   ├── file.go             # Local file source
│   │   ├── glob.go             # Glob pattern source (multiple files)
│   │   ├── http.go             # HTTP/HTTPS URL source
│   │   ├── stream.go           # io.Reader wrapper
│   │   └── memory.go           # In-memory bytes source (for testing)
│   │
│   ├── detect/                 # Format detection (REFACTOR from detect.go)
│   │   ├── detector.go         # Main Detector struct
│   │   ├── format.go           # Format enum and detection
│   │   ├── encoding.go         # Character encoding detection
│   │   ├── delimiter.go        # CSV delimiter detection
│   │   ├── magic.go            # Magic byte detection
│   │   └── quality.go          # Data quality indicators
│   │
│   ├── decoders/               # Format decoders (all implement Decoder interface)
│   │   ├── registry.go         # Decoder registry with format routing
│   │   ├── duckdb.go           # DuckDB-based decoder (REFACTOR from fast_path.go)
│   │   ├── csv.go              # Streaming CSV decoder (REFACTOR from robust_path.go)
│   │   ├── json.go             # JSON/JSONL decoder
│   │   ├── xml.go              # XML decoder
│   │   ├── xes.go              # XES process mining decoder
│   │   ├── parquet.go          # Parquet pass-through decoder
│   │   ├── avro.go             # Avro decoder
│   │   ├── excel.go            # Excel decoder
│   │   └── custom.go           # Custom decoder registration
│   │
│   ├── sinks/                  # Output sinks (all implement Sink interface)
│   │   ├── parquet.go          # Parquet file sink
│   │   ├── iceberg.go          # Iceberg table sink
│   │   ├── delta.go            # Delta Lake sink
│   │   ├── arrow_ipc.go        # Arrow IPC stream sink
│   │   └── null.go             # Null sink (for benchmarking)
│   │
│   ├── schema/                 # Schema management
│   │   ├── inference.go        # Infer schema from data samples
│   │   ├── evolution.go        # Schema evolution (add/drop/rename columns)
│   │   ├── policy.go           # Schema policies (strict, merge, evolve)
│   │   ├── cache.go            # Schema caching (infer once, apply many)
│   │   └── mapping.go          # Type mapping between formats
│   │
│   ├── quality/                # Data quality (REFACTOR from quality.go)
│   │   ├── validator.go        # Streaming quality validator
│   │   ├── checksum.go         # FNV-1a/xxHash checksums
│   │   ├── entropy.go          # Shannon entropy calculation
│   │   ├── cardinality.go      # HyperLogLog cardinality estimation
│   │   ├── rules.go            # Validation rules (not null, range, pattern)
│   │   └── report.go           # Quality report generation
│   │
│   ├── errors/                 # Error handling
│   │   ├── policy.go           # Error policies (strict, permissive, quarantine)
│   │   ├── handler.go          # Error handler with callbacks
│   │   ├── quarantine.go       # Quarantine bad records to separate file
│   │   └── recovery.go         # Error recovery strategies
│   │
│   ├── heuristics/             # Decision engine (REFACTOR from heuristics.go)
│   │   ├── engine.go           # HeuristicEngine main struct
│   │   ├── buckets.go          # Source bucket classification
│   │   ├── decisions.go        # Decision table rules
│   │   ├── adaptive.go         # Adaptive tuning from metrics
│   │   └── config.go           # Heuristic configuration
│   │
│   ├── flow/                   # Flow control
│   │   ├── backpressure.go     # Bounded queue with backpressure
│   │   ├── concurrency.go      # Per-bucket concurrency limits
│   │   ├── rate_limiter.go     # Optional rate limiting
│   │   └── batch_accumulator.go # Accumulate records into batches
│   │
│   ├── pipeline.go             # Main Pipeline orchestrator (REFACTOR)
│   ├── engine.go               # High-level Engine API
│   └── config.go               # Unified configuration
│
├── ═══════════════════════════════════════════════════════════════════════
│   CORE - Storage Layer (Table management)
├── ═══════════════════════════════════════════════════════════════════════
│
├── storage/
│   │
│   ├── table/                  # Table abstraction
│   │   ├── table.go            # Table interface implementation
│   │   ├── schema.go           # Table schema operations
│   │   ├── partitioning.go     # Partition strategies
│   │   ├── statistics.go       # Table statistics
│   │   └── snapshot.go         # Point-in-time snapshots
│   │
│   ├── catalog/                # Catalog implementations
│   │   ├── interface.go        # Catalog interface (defined in interfaces/)
│   │   ├── file.go             # Simple file-based catalog (DEFAULT)
│   │   ├── memory.go           # In-memory catalog (for testing)
│   │   └── sql.go              # SQL-based catalog (SQLite/Postgres)
│   │
│   ├── object/                 # Object storage implementations
│   │   ├── interface.go        # ObjectStorage interface (in interfaces/)
│   │   ├── local.go            # Local filesystem (DEFAULT)
│   │   └── memory.go           # In-memory storage (for testing)
│   │
│   ├── layout/                 # File layout strategies
│   │   ├── partitioner.go      # Partition files by columns
│   │   ├── naming.go           # File naming conventions
│   │   └── sizing.go           # Target file sizing
│   │
│   └── maintenance/            # Storage maintenance
│       ├── compaction.go       # Compact small files
│       ├── vacuum.go           # Remove old snapshots
│       └── optimize.go         # Optimize layout (clustering)
│
├── ═══════════════════════════════════════════════════════════════════════
│   CORE - Query Layer (SQL analytics)
├── ═══════════════════════════════════════════════════════════════════════
│
├── query/
│   │
│   ├── engine/                 # Query execution
│   │   ├── engine.go           # QueryEngine interface and DuckDB impl
│   │   ├── context.go          # Query context (timeout, memory limit)
│   │   ├── result.go           # QueryResult, ResultSet interfaces
│   │   └── streaming.go        # Streaming result iteration
│   │
│   ├── sql/                    # SQL handling
│   │   ├── parser.go           # SQL parsing (use sqlparser library)
│   │   ├── validator.go        # SQL validation
│   │   └── rewriter.go         # Query rewriting (add filters, etc.)
│   │
│   ├── cache/                  # Query caching
│   │   ├── result_cache.go     # Cache query results
│   │   ├── metadata_cache.go   # Cache table metadata
│   │   └── invalidation.go     # Cache invalidation strategies
│   │
│   ├── flight/                 # Arrow Flight server
│   │   ├── server.go           # Flight server implementation
│   │   ├── handlers.go         # GetFlightInfo, DoGet, DoPut
│   │   └── auth.go             # Flight authentication
│   │
│   └── semantic/               # Semantic layer (optional)
│       ├── model.go            # Semantic model definition
│       ├── dimension.go        # Dimension definitions
│       ├── measure.go          # Measure definitions
│       └── query_builder.go    # Build SQL from semantic queries
│
├── ═══════════════════════════════════════════════════════════════════════
│   DEFAULTS - Simple implementations users can use out-of-box
├── ═══════════════════════════════════════════════════════════════════════
│
├── defaults/
│   ├── auth/
│   │   ├── noop.go             # No auth (for local/dev)
│   │   └── apikey.go           # Simple API key auth
│   │
│   ├── metrics/
│   │   ├── noop.go             # No metrics
│   │   └── log.go              # Log-based metrics
│   │
│   └── alerting/
│       ├── noop.go             # No alerting
│       └── log.go              # Log-based alerting
│
├── ═══════════════════════════════════════════════════════════════════════
│   API - External interfaces
├── ═══════════════════════════════════════════════════════════════════════
│
├── api/
│   ├── rest/                   # REST API
│   │   ├── server.go           # HTTP server setup
│   │   ├── routes.go           # Route definitions
│   │   ├── handlers/
│   │   │   ├── ingest.go       # POST /v1/ingest
│   │   │   ├── query.go        # POST /v1/query
│   │   │   ├── tables.go       # CRUD /v1/tables
│   │   │   └── health.go       # GET /health
│   │   └── middleware/
│   │       ├── auth.go         # Auth middleware (uses interfaces.Authenticator)
│   │       ├── logging.go      # Request logging
│   │       ├── recovery.go     # Panic recovery
│   │       └── cors.go         # CORS handling
│   │
│   └── grpc/                   # gRPC API (optional, for high-perf)
│       ├── proto/
│       │   ├── ingest.proto
│       │   └── query.proto
│       └── server.go
│
├── ═══════════════════════════════════════════════════════════════════════
│   CONFIG - Unified configuration
├── ═══════════════════════════════════════════════════════════════════════
│
├── config/
│   ├── config.go               # Main Config struct
│   ├── defaults.go             # Default values
│   ├── loader.go               # Load from file/env
│   └── validation.go           # Config validation
│
├── ═══════════════════════════════════════════════════════════════════════
│   TESTING - Test utilities
├── ═══════════════════════════════════════════════════════════════════════
│
├── testing/
│   ├── generators/
│   │   ├── csv.go              # Generate test CSV
│   │   ├── json.go             # Generate test JSON
│   │   ├── noisy.go            # Generate noisy/dirty data
│   │   └── xes.go              # Generate XES process mining data
│   │
│   ├── assertions/
│   │   ├── schema.go           # Schema assertions
│   │   ├── data.go             # Data assertions
│   │   └── parquet.go          # Parquet file assertions
│   │
│   └── roundtrip/
│       └── roundtrip.go        # Round-trip testing utilities
│
└── ═══════════════════════════════════════════════════════════════════════
    PUBLIC API - Main entry points
    ═══════════════════════════════════════════════════════════════════════

├── logflow.go                  # Main package entry point
└── doc.go                      # Package documentation
```

---

## Interface Definitions (Critical - Build These First)

### interfaces/auth.go
```go
package interfaces

import "context"

// Identity represents an authenticated user/service
type Identity interface {
    ID() string
    Type() string // "user", "service", "api_key"
    Tenant() string
    Roles() []string
    Attributes() map[string]string
}

// Authenticator validates credentials and returns identity
type Authenticator interface {
    // Authenticate validates the token and returns identity
    Authenticate(ctx context.Context, token string) (Identity, error)

    // Type returns the auth type (jwt, apikey, oauth, mtls)
    Type() string
}

// Authorizer checks if identity can perform action
type Authorizer interface {
    // Authorize checks if identity can perform action on resource
    Authorize(ctx context.Context, identity Identity, action string, resource string) (bool, error)

    // AuthorizeData checks row/column level access
    AuthorizeData(ctx context.Context, identity Identity, table string, columns []string, filter string) (allowed bool, appliedFilter string, err error)
}

// NoopAuthenticator allows all requests (for local/dev use)
type NoopAuthenticator struct{}

func (n *NoopAuthenticator) Authenticate(ctx context.Context, token string) (Identity, error) {
    return &AnonymousIdentity{}, nil
}

func (n *NoopAuthenticator) Type() string { return "noop" }
```

### interfaces/catalog.go
```go
package interfaces

import (
    "context"
    "github.com/apache/arrow/go/v14/arrow"
)

// Catalog manages table metadata
type Catalog interface {
    // Table operations
    CreateTable(ctx context.Context, spec TableSpec) error
    GetTable(ctx context.Context, database, table string) (Table, error)
    ListTables(ctx context.Context, database string) ([]TableInfo, error)
    DropTable(ctx context.Context, database, table string) error

    // Database operations
    CreateDatabase(ctx context.Context, name string) error
    ListDatabases(ctx context.Context) ([]string, error)
    DropDatabase(ctx context.Context, name string) error
}

// Table represents a logical table
type Table interface {
    // Metadata
    Name() string
    Database() string
    Schema() *arrow.Schema
    Location() string
    Format() TableFormat
    Partitioning() PartitionSpec
    Properties() map[string]string

    // Snapshots (for time travel)
    CurrentSnapshot() Snapshot
    Snapshots() []Snapshot
    AsOf(version int64) (Table, error)
    AsOfTime(timestamp int64) (Table, error)

    // Files
    DataFiles() []DataFile
    ManifestFiles() []ManifestFile
}

// TableSpec defines a new table
type TableSpec struct {
    Database     string
    Name         string
    Schema       *arrow.Schema
    Location     string
    Format       TableFormat
    Partitioning PartitionSpec
    Properties   map[string]string
}

// TableFormat is the underlying format
type TableFormat int

const (
    FormatParquet TableFormat = iota
    FormatIceberg
    FormatDelta
    FormatHudi
)
```

### interfaces/storage.go
```go
package interfaces

import (
    "context"
    "io"
)

// ObjectStorage provides object storage operations
type ObjectStorage interface {
    // Basic operations
    Put(ctx context.Context, path string, data io.Reader, opts PutOptions) error
    Get(ctx context.Context, path string) (io.ReadCloser, error)
    Delete(ctx context.Context, path string) error
    Exists(ctx context.Context, path string) (bool, error)

    // Listing
    List(ctx context.Context, prefix string, opts ListOptions) ([]ObjectInfo, error)
    ListPaginated(ctx context.Context, prefix string, opts ListOptions) ObjectIterator

    // Metadata
    Head(ctx context.Context, path string) (ObjectInfo, error)

    // Bulk operations
    DeleteMany(ctx context.Context, paths []string) error
    Copy(ctx context.Context, src, dst string) error
}

// ObjectInfo contains object metadata
type ObjectInfo struct {
    Path         string
    Size         int64
    LastModified int64
    ETag         string
    ContentType  string
    Metadata     map[string]string
}

// PutOptions for write operations
type PutOptions struct {
    ContentType string
    Metadata    map[string]string
}

// ListOptions for listing operations
type ListOptions struct {
    MaxKeys   int
    Delimiter string
    StartAfter string
}
```

### interfaces/scheduler.go
```go
package interfaces

import (
    "context"
    "time"
)

// Scheduler manages job scheduling and execution
type Scheduler interface {
    // Job lifecycle
    Submit(ctx context.Context, spec JobSpec) (JobID, error)
    Cancel(ctx context.Context, id JobID) error
    Get(ctx context.Context, id JobID) (Job, error)

    // Listing
    List(ctx context.Context, filter JobFilter) ([]Job, error)

    // Scheduling
    Schedule(ctx context.Context, spec JobSpec, schedule Schedule) (JobID, error)
    Unschedule(ctx context.Context, id JobID) error
}

// JobSpec defines a job to run
type JobSpec struct {
    Name        string
    Type        JobType
    Config      map[string]interface{}
    Priority    int
    Timeout     time.Duration
    Retries     int
    DependsOn   []JobID
}

// JobType categorizes jobs
type JobType string

const (
    JobTypeIngest     JobType = "ingest"
    JobTypeTransform  JobType = "transform"
    JobTypeExport     JobType = "export"
    JobTypeCompaction JobType = "compaction"
    JobTypeVacuum     JobType = "vacuum"
)

// Job represents a running or completed job
type Job interface {
    ID() JobID
    Spec() JobSpec
    Status() JobStatus
    Progress() float64
    StartedAt() time.Time
    CompletedAt() time.Time
    Error() error
    Metrics() JobMetrics
}

// JobStatus is the job state
type JobStatus string

const (
    JobStatusPending   JobStatus = "pending"
    JobStatusRunning   JobStatus = "running"
    JobStatusSucceeded JobStatus = "succeeded"
    JobStatusFailed    JobStatus = "failed"
    JobStatusCancelled JobStatus = "cancelled"
)

// Schedule defines when a job runs
type Schedule struct {
    Type       ScheduleType
    Cron       string        // For cron type
    Interval   time.Duration // For interval type
    StartTime  time.Time
    EndTime    time.Time
    Timezone   string
}

// NoopScheduler runs jobs immediately (no scheduling)
type NoopScheduler struct{}
```

### interfaces/metrics.go
```go
package interfaces

// MetricsExporter exports metrics to a backend
type MetricsExporter interface {
    // Counters
    Counter(name string, value int64, tags map[string]string)

    // Gauges
    Gauge(name string, value float64, tags map[string]string)

    // Histograms
    Histogram(name string, value float64, tags map[string]string)

    // Timers
    Timer(name string, duration int64, tags map[string]string)

    // Flush any buffered metrics
    Flush() error
}

// NoopMetrics discards all metrics
type NoopMetrics struct{}
```

---

## Core Abstractions (Build After Interfaces)

### ingest/core/source.go
```go
package core

import (
    "context"
    "io"
)

// Source represents a data source to ingest
type Source interface {
    // Identity
    ID() string
    Location() string

    // Metadata
    Format() Format
    Size() int64
    Metadata() map[string]string

    // Reading
    Open(ctx context.Context) (io.ReadCloser, error)
    OpenSeekable(ctx context.Context) (io.ReadSeekCloser, error)
}

// Format identifies the data format
type Format string

const (
    FormatUnknown Format = ""
    FormatCSV     Format = "csv"
    FormatTSV     Format = "tsv"
    FormatJSON    Format = "json"
    FormatJSONL   Format = "jsonl"
    FormatXML     Format = "xml"
    FormatXES     Format = "xes"
    FormatParquet Format = "parquet"
    FormatAvro    Format = "avro"
    FormatORC     Format = "orc"
    FormatExcel   Format = "xlsx"
)
```

### ingest/core/decoder.go
```go
package core

import (
    "context"
    "github.com/apache/arrow/go/v14/arrow"
)

// Decoder converts a source into Arrow RecordBatches
type Decoder interface {
    // Metadata
    ID() string
    SupportedFormats() []Format
    CostHint() int // 1=cheap, 2=medium, 3=expensive

    // Schema
    InferSchema(ctx context.Context, source Source, opts InferOptions) (*arrow.Schema, error)

    // Decoding
    Decode(ctx context.Context, source Source, schema *arrow.Schema, opts DecodeOptions) (<-chan DecodedBatch, error)
}

// DecodedBatch is a RecordBatch with metadata
type DecodedBatch struct {
    Batch     arrow.Record
    Index     int64      // Batch index
    Offset    int64      // Byte offset in source
    RowCount  int64      // Rows in this batch
    Errors    []RowError // Errors during decoding
    IsFinal   bool       // Last batch
}

// RowError represents an error for a specific row
type RowError struct {
    Row     int64
    Column  string
    Message string
    Raw     []byte
}

// DecodeOptions configures decoding
type DecodeOptions struct {
    BatchSize       int
    Schema          *arrow.Schema // If nil, infer
    ErrorPolicy     ErrorPolicy
    MaxErrors       int
    OnError         func(RowError)
    OnProgress      func(bytesRead, totalBytes int64)
}

// ErrorPolicy defines error handling
type ErrorPolicy int

const (
    ErrorPolicyStrict     ErrorPolicy = iota // Abort on first error
    ErrorPolicyPermissive                    // Skip bad rows
    ErrorPolicyQuarantine                    // Write bad rows to separate file
)
```

### ingest/core/sink.go
```go
package core

import (
    "context"
    "github.com/apache/arrow/go/v14/arrow"
)

// Sink writes Arrow RecordBatches to a destination
type Sink interface {
    // Lifecycle
    Open(ctx context.Context, schema *arrow.Schema, opts SinkOptions) error
    Write(ctx context.Context, batch arrow.Record) error
    Flush(ctx context.Context) error
    Close(ctx context.Context) (*SinkResult, error)
}

// SinkOptions configures the sink
type SinkOptions struct {
    // Output location
    OutputPath   string
    OutputFormat OutputFormat

    // Parquet settings
    Compression    string // snappy, zstd, gzip, lz4, none
    RowGroupSize   int
    PageSize       int
    UseDictionary  bool

    // Partitioning
    PartitionBy    []string
    PartitionStyle PartitionStyle // hive, directory

    // File sizing
    TargetFileSize int64 // Target size per file
    MaxRecordsPerFile int64
}

// SinkResult contains write statistics
type SinkResult struct {
    RowsWritten   int64
    BytesWritten  int64
    FilesWritten  int
    Paths         []string
    Duration      time.Duration
    Checksum      uint64
}

// OutputFormat for the sink
type OutputFormat string

const (
    OutputParquet OutputFormat = "parquet"
    OutputIceberg OutputFormat = "iceberg"
    OutputDelta   OutputFormat = "delta"
)
```

---

## Quality Requirements

### Performance Targets
- Clean CSV ingestion: **>900,000 rows/sec**
- Parquet output: **>100 MB/sec**
- Query latency (1M rows): **<100ms**
- Memory usage: **<2x input size** for streaming

### Reliability Targets
- Data integrity: **100%** (checksums match)
- Error recovery: **>99.9%** rows recovered in permissive mode
- No data loss in quarantine mode

### Code Quality
- Test coverage: **>80%**
- All public APIs documented
- No panics in production code (recover and return errors)
- Context cancellation respected everywhere
- Resources properly closed (use defer)

---

## Implementation Order

### Phase 1: Foundation (Week 1)
1. `interfaces/` - All interface definitions
2. `ingest/core/` - Source, Decoder, Sink abstractions
3. `defaults/` - Noop implementations

### Phase 2: Detection & Decoding (Week 2)
4. `ingest/detect/` - Format detection (refactor existing)
5. `ingest/decoders/registry.go` - Decoder registry
6. `ingest/decoders/csv.go` - CSV decoder
7. `ingest/decoders/duckdb.go` - DuckDB decoder (refactor existing)

### Phase 3: Quality & Flow (Week 3)
8. `ingest/quality/` - Quality validation (refactor existing)
9. `ingest/errors/` - Error handling
10. `ingest/flow/` - Backpressure and concurrency
11. `ingest/heuristics/` - Heuristic engine (refactor existing)

### Phase 4: Storage (Week 4)
12. `ingest/sinks/parquet.go` - Parquet sink
13. `storage/catalog/file.go` - File-based catalog
14. `storage/object/local.go` - Local filesystem
15. `storage/table/` - Table abstraction

### Phase 5: Query (Week 5)
16. `query/engine/` - DuckDB query engine
17. `query/sql/` - SQL parsing
18. `query/cache/` - Result caching

### Phase 6: API & Polish (Week 6)
19. `api/rest/` - REST API
20. `config/` - Configuration
21. `testing/` - Test utilities
22. Documentation and examples

---

## Testing Strategy

### Unit Tests
- Every public function has tests
- Edge cases covered (empty input, huge input, malformed data)
- Mock interfaces for isolation

### Integration Tests
- Full pipeline tests (ingest → storage → query)
- Multiple formats tested
- Error scenarios tested

### Benchmark Tests
- Performance regression tests
- Memory usage tests
- Comparison with baseline

### Fuzz Tests
- All decoders fuzz tested
- No panics on malformed input

---

## Example Usage (Target API)

```go
package main

import (
    "context"
    "log"

    "github.com/logflow/logflow"
    "github.com/logflow/logflow/interfaces"
)

func main() {
    ctx := context.Background()

    // Simplest: Just convert a file
    result, err := logflow.Convert(ctx, "data.csv", "data.parquet")
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Converted %d rows", result.RowsWritten)

    // With options
    result, err = logflow.Convert(ctx, "data.csv", "data.parquet",
        logflow.WithCompression("zstd"),
        logflow.WithQualityValidation(true),
        logflow.WithErrorPolicy(logflow.ErrorPolicyPermissive),
    )

    // With query
    engine := logflow.NewEngine()
    defer engine.Close()

    // Ingest
    engine.Ingest(ctx, "events.csv", "events")

    // Query
    results, err := engine.Query(ctx, `
        SELECT activity, COUNT(*) as count
        FROM events
        GROUP BY activity
        ORDER BY count DESC
    `)

    // With your own auth
    engine = logflow.NewEngine(
        logflow.WithAuthenticator(myAuthSystem),
        logflow.WithAuthorizer(myRBACSystem),
    )

    // With catalog (Iceberg tables)
    engine = logflow.NewEngine(
        logflow.WithCatalog(icebergCatalog),
        logflow.WithStorage(s3Storage),
    )

    // Stream to table
    engine.IngestToTable(ctx, "events.csv", "analytics.events",
        logflow.WithPartitionBy("date"),
    )
}
```

---

## Do NOT Do

1. **Don't build auth implementation** - only interfaces
2. **Don't build cloud storage** - only interfaces (local filesystem default)
3. **Don't build Airflow/Temporal integration** - only scheduler interface
4. **Don't build web UI** - focus on library/API
5. **Don't over-abstract** - keep it simple, add complexity only when needed
6. **Don't break existing functionality** - refactor incrementally

---

## Success Criteria

The library is complete when:

1. ✅ Can ingest CSV, JSON, JSONL, Parquet, XES with single function call
2. ✅ Outputs valid, queryable Parquet files
3. ✅ Maintains >900K rows/sec for clean CSV
4. ✅ Handles dirty data with configurable error policies
5. ✅ Query engine returns correct results
6. ✅ All interfaces allow extension
7. ✅ Tests pass with >80% coverage
8. ✅ Documentation complete

---

## Start Command

Begin by creating the `interfaces/` package with all interface definitions. This is the foundation everything else builds on.

```bash
mkdir -p pkg/interfaces
# Create auth.go, catalog.go, storage.go, scheduler.go, metrics.go
```

Then create `ingest/core/` with Source, Decoder, Sink.

Then refactor existing code into the new structure.

**Quality over speed. Get the abstractions right first.**
