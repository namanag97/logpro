# Production-Grade Ingestion Library - Deep Integration Plan

## Overview

This document provides a comprehensive plan to evolve the current ingestion package into a production-grade, extensible library. The plan avoids code duplication by refactoring existing components into clean abstractions.

---

## Current State Analysis

### What We Have (4,800+ lines)

| File | Lines | Purpose | Reusable? |
|------|-------|---------|-----------|
| `detect.go` | 780 | Format/encoding/quality detection | Yes, becomes part of FormatRouter |
| `heuristics.go` | 707 | Buckets, decision table, adaptive tuning | Yes, becomes HeuristicEngine |
| `pipeline.go` | 450 | Unified pipeline with concurrency | Refactor into orchestrator |
| `fast_path.go` | 355 | DuckDB native processing | Yes, becomes a Decoder plugin |
| `robust_path.go` | 830 | Go streaming with recovery | Yes, becomes a Decoder plugin |
| `quality.go` | 612 | Checksum, entropy, cardinality | Yes, becomes QualityValidator |
| `ingest.go` | 357 | Engine API | Refactor into Pipeline |

### What's Missing

1. **Core Abstractions**: IngestSource, Decoder, Sink interfaces
2. **Plugin Registry**: Extensible format routing
3. **Bad Data Policies**: Structured error handling
4. **Observability**: Metrics, tracing, progress callbacks
5. **Schema Management**: Inference, drift, policies
6. **Backpressure**: Flow control with bounded queues
7. **Compaction**: Small file merging
8. **Lakehouse**: Iceberg/Delta integration
9. **Configuration**: Unified config model
10. **Testing Framework**: Round-trip and fuzzing

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           INGESTION LIBRARY ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────────────┐   │
│  │                              CONFIG LAYER                                     │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │   │
│  │  │ IO Config   │  │ Parser Cfg  │  │ Writer Cfg  │  │ Policy Cfg  │          │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘          │   │
│  └──────────────────────────────────────────────────────────────────────────────┘   │
│                                         │                                            │
│  ┌──────────────────────────────────────▼───────────────────────────────────────┐   │
│  │                            INGEST SOURCE                                      │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │   │
│  │  │ LocalFile   │  │ S3/GCS/ADLS │  │ HTTP/URL    │  │ Stream      │          │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘          │   │
│  └──────────────────────────────────────┬───────────────────────────────────────┘   │
│                                         │                                            │
│  ┌──────────────────────────────────────▼───────────────────────────────────────┐   │
│  │                           FORMAT ROUTER                                       │   │
│  │                                                                               │   │
│  │   detect.go logic + Plugin Registry + Magic Bytes + Cost Hints               │   │
│  │                                                                               │   │
│  │   ┌───────────────────────────────────────────────────────────────────┐      │   │
│  │   │  DECODER PLUGINS (Arrow RecordBatch output)                       │      │   │
│  │   │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐     │      │   │
│  │   │  │DuckDB   │ │CSV/TSV  │ │JSON/L   │ │XML/XES  │ │Custom   │     │      │   │
│  │   │  │(fast)   │ │(robust) │ │(robust) │ │(stream) │ │(plugin) │     │      │   │
│  │   │  │cost=1   │ │cost=2   │ │cost=2   │ │cost=3   │ │cost=N   │     │      │   │
│  │   │  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘     │      │   │
│  │   └───────────────────────────────────────────────────────────────────┘      │   │
│  └──────────────────────────────────────┬───────────────────────────────────────┘   │
│                                         │                                            │
│  ┌──────────────────────────────────────▼───────────────────────────────────────┐   │
│  │                         ARROW STREAM LAYER                                    │   │
│  │                                                                               │   │
│  │   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                      │   │
│  │   │ RecordBatch │───▶│ Schema      │───▶│ Quality     │                      │   │
│  │   │ Stream      │    │ Policy      │    │ Validator   │                      │   │
│  │   └─────────────┘    └─────────────┘    └─────────────┘                      │   │
│  │          │                  │                  │                              │   │
│  │          ▼                  ▼                  ▼                              │   │
│  │   ┌─────────────────────────────────────────────────────────────────────┐    │   │
│  │   │                    BAD DATA HANDLER                                  │    │   │
│  │   │   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐            │    │   │
│  │   │   │ Strict   │  │Permissive│  │Quarantine│  │ Custom   │            │    │   │
│  │   │   └──────────┘  └──────────┘  └──────────┘  └──────────┘            │    │   │
│  │   └─────────────────────────────────────────────────────────────────────┘    │   │
│  └──────────────────────────────────────┬───────────────────────────────────────┘   │
│                                         │                                            │
│  ┌──────────────────────────────────────▼───────────────────────────────────────┐   │
│  │                         ORCHESTRATOR                                          │   │
│  │                                                                               │   │
│  │   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                      │   │
│  │   │ Heuristics  │    │ Backpressure│    │ Concurrency │                      │   │
│  │   │ Engine      │    │ Controller  │    │ Manager     │                      │   │
│  │   └─────────────┘    └─────────────┘    └─────────────┘                      │   │
│  │          │                  │                  │                              │   │
│  │          └──────────────────┼──────────────────┘                              │   │
│  │                             ▼                                                 │   │
│  │   ┌─────────────────────────────────────────────────────────────────────┐    │   │
│  │   │                    BOUNDED QUEUE                                     │    │   │
│  │   │         (RecordBatches with backpressure)                           │    │   │
│  │   └─────────────────────────────────────────────────────────────────────┘    │   │
│  └──────────────────────────────────────┬───────────────────────────────────────┘   │
│                                         │                                            │
│  ┌──────────────────────────────────────▼───────────────────────────────────────┐   │
│  │                              SINK LAYER                                       │   │
│  │                                                                               │   │
│  │   ┌─────────────────────────────────────────────────────────────────────┐    │   │
│  │   │  SINK PLUGINS                                                        │    │   │
│  │   │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐        │    │   │
│  │   │  │Parquet  │ │Iceberg  │ │Delta    │ │Arrow    │ │Custom   │        │    │   │
│  │   │  │Writer   │ │Table    │ │Lake     │ │IPC      │ │(plugin) │        │    │   │
│  │   │  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘        │    │   │
│  │   └─────────────────────────────────────────────────────────────────────┘    │   │
│  └──────────────────────────────────────┬───────────────────────────────────────┘   │
│                                         │                                            │
│  ┌──────────────────────────────────────▼───────────────────────────────────────┐   │
│  │                         OBSERVABILITY                                         │   │
│  │   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                      │   │
│  │   │ Metrics     │    │ Progress    │    │ Error       │                      │   │
│  │   │ Collector   │    │ Reporter    │    │ Channel     │                      │   │
│  │   └─────────────┘    └─────────────┘    └─────────────┘                      │   │
│  └──────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Module Structure (Proposed)

```
pkg/ingest/
│
├── core/                      # Core abstractions
│   ├── source.go              # IngestSource interface
│   ├── decoder.go             # Decoder interface (Arrow output)
│   ├── sink.go                # Sink interface
│   └── batch.go               # RecordBatch wrapper
│
├── registry/                  # Plugin system
│   ├── registry.go            # Format → Decoder mapping
│   ├── routing.go             # Format detection + cost hints
│   └── plugins.go             # Plugin lifecycle
│
├── decoders/                  # Built-in decoders
│   ├── duckdb.go              # ← Refactor from fast_path.go
│   ├── csv.go                 # ← Refactor from robust_path.go (CSV part)
│   ├── json.go                # ← Refactor from robust_path.go (JSON part)
│   ├── xes.go                 # ← Refactor from robust_path.go (XES part)
│   └── parquet.go             # Pass-through/rewrite
│
├── sinks/                     # Built-in sinks
│   ├── parquet.go             # Parquet writer with row groups
│   ├── iceberg.go             # Iceberg table commit
│   ├── delta.go               # Delta Lake commit
│   └── arrow_ipc.go           # Arrow IPC stream
│
├── schema/                    # Schema management
│   ├── inference.go           # Schema inference from samples
│   ├── evolution.go           # Schema drift/evolution
│   ├── policy.go              # Strict/merge/evolving policies
│   └── cache.go               # Schema caching
│
├── errors/                    # Error handling
│   ├── policy.go              # Strict/permissive/quarantine
│   ├── handler.go             # Error aggregation
│   └── quarantine.go          # Bad data isolation
│
├── flow/                      # Flow control
│   ├── backpressure.go        # Bounded queues
│   ├── concurrency.go         # Per-cost semaphores
│   └── rate_limiter.go        # Optional rate limiting
│
├── metrics/                   # Observability
│   ├── collector.go           # Metrics collection
│   ├── progress.go            # Progress callbacks
│   └── tracing.go             # Span/trace integration
│
├── heuristics/                # Decision engine
│   ├── engine.go              # ← Refactor from heuristics.go
│   ├── buckets.go             # Source classification
│   ├── decisions.go           # Decision table
│   └── adaptive.go            # Adaptive tuning
│
├── files/                     # File operations
│   ├── batcher.go             # Small file batching
│   ├── compaction.go          # Post-ingest compaction
│   └── layout.go              # Partitioning strategy
│
├── detect/                    # Detection logic
│   ├── format.go              # ← Refactor from detect.go
│   ├── encoding.go            # Character encoding
│   ├── delimiter.go           # CSV delimiter detection
│   └── quality.go             # Data quality indicators
│
├── quality/                   # Quality validation
│   ├── validator.go           # ← Refactor from quality.go
│   ├── checksum.go            # FNV/xxHash
│   ├── entropy.go             # Shannon entropy
│   └── cardinality.go         # HyperLogLog
│
├── config/                    # Configuration
│   ├── config.go              # Unified config struct
│   ├── defaults.go            # Sensible defaults
│   └── validation.go          # Config validation
│
├── testing/                   # Test utilities
│   ├── roundtrip.go           # Round-trip testing
│   ├── fuzzing.go             # Fuzz test helpers
│   └── generators.go          # Test data generators
│
├── pipeline.go                # Main orchestrator
├── api.go                     # Public API
└── doc.go                     # Package documentation
```

---

## Phase 1: Core Abstractions (No Breaking Changes)

### 1.1 IngestSource Interface

```go
// core/source.go - NEW FILE

// IngestSource represents a data source to ingest.
type IngestSource interface {
    // Location returns the source location (path, URL, etc.)
    Location() string

    // Format returns the detected or specified format
    Format() Format

    // Size returns the size in bytes (0 if unknown)
    Size() int64

    // Metadata returns source-specific metadata
    Metadata() map[string]string

    // Open returns a reader for the source
    Open(ctx context.Context) (io.ReadCloser, error)

    // OpenSeekable returns a seekable reader if supported
    OpenSeekable(ctx context.Context) (io.ReadSeekCloser, error)
}

// Implementations: LocalFile, S3Object, HTTPSource, StreamSource
```

### 1.2 Decoder Interface

```go
// core/decoder.go - NEW FILE

// Decoder converts bytes to Arrow RecordBatches.
type Decoder interface {
    // ID returns the decoder identifier
    ID() string

    // CostHint returns the processing cost (1=cheap, 2=medium, 3=expensive)
    CostHint() int

    // SupportedFormats returns formats this decoder handles
    SupportedFormats() []Format

    // InferSchema reads a sample and returns the inferred schema
    InferSchema(ctx context.Context, source IngestSource, sampleSize int) (*arrow.Schema, error)

    // Decode streams Arrow RecordBatches from the source
    Decode(ctx context.Context, source IngestSource, schema *arrow.Schema, opts DecodeOptions) (<-chan DecodedBatch, error)
}

// DecodedBatch wraps a RecordBatch with metadata
type DecodedBatch struct {
    Batch       arrow.Record
    BytesRead   int64
    RowsRead    int64
    Errors      []DecodeError
    IsLast      bool
}

// DecodeOptions configures decoding behavior
type DecodeOptions struct {
    BatchSize       int
    SkipErrors      bool
    MaxErrors       int
    OnError         func(DecodeError)
}
```

### 1.3 Sink Interface

```go
// core/sink.go - NEW FILE

// Sink writes Arrow RecordBatches to a destination.
type Sink interface {
    // ID returns the sink identifier
    ID() string

    // Open prepares the sink for writing
    Open(ctx context.Context, schema *arrow.Schema, opts SinkOptions) error

    // Write writes a RecordBatch
    Write(ctx context.Context, batch arrow.Record) error

    // Flush ensures all buffered data is written
    Flush(ctx context.Context) error

    // Close finalizes and closes the sink
    Close(ctx context.Context) (*SinkResult, error)
}

// SinkResult contains write statistics
type SinkResult struct {
    BytesWritten    int64
    RowsWritten     int64
    FilesWritten    int
    OutputPaths     []string
    Metadata        map[string]string
}

// SinkOptions configures sink behavior
type SinkOptions struct {
    OutputPath      string
    Compression     string
    RowGroupSize    int
    PartitionBy     []string
    FileSize        int64  // Target file size
}
```

### 1.4 Refactoring Map

| Current Code | Becomes | Changes Needed |
|--------------|---------|----------------|
| `detect.go` | `detect/format.go` + `detect/encoding.go` + `detect/delimiter.go` | Split into focused files |
| `fast_path.go` | `decoders/duckdb.go` | Implement Decoder interface |
| `robust_path.processCSV` | `decoders/csv.go` | Extract, implement Decoder |
| `robust_path.processJSON` | `decoders/json.go` | Extract, implement Decoder |
| `robust_path.processXES` | `decoders/xes.go` | Extract, implement Decoder |
| `heuristics.go` | `heuristics/engine.go` + `heuristics/buckets.go` | Split into focused files |
| `quality.go` | `quality/validator.go` + `quality/checksum.go` | Split into focused files |
| `pipeline.go` | `pipeline.go` (refactored) | Use new interfaces |

---

## Phase 2: Plugin Registry

### 2.1 Registry Implementation

```go
// registry/registry.go - NEW FILE

// Registry manages decoder and sink plugins.
type Registry struct {
    decoders map[string]Decoder
    sinks    map[string]Sink

    // Format routing rules
    formatRules []FormatRule

    mu sync.RWMutex
}

// FormatRule maps detection criteria to a decoder
type FormatRule struct {
    // Matching criteria (any match triggers)
    Extensions  []string    // e.g., [".csv", ".tsv"]
    MimeTypes   []string    // e.g., ["text/csv"]
    MagicBytes  []byte      // e.g., []byte("PK") for ZIP

    // Target
    DecoderID   string
    CostHint    int
    Priority    int         // Higher = checked first
}

// Global default registry
var DefaultRegistry = NewRegistry()

func init() {
    // Register built-in decoders
    DefaultRegistry.RegisterDecoder(NewDuckDBDecoder())
    DefaultRegistry.RegisterDecoder(NewCSVDecoder())
    DefaultRegistry.RegisterDecoder(NewJSONDecoder())
    DefaultRegistry.RegisterDecoder(NewXESDecoder())
    DefaultRegistry.RegisterDecoder(NewParquetDecoder())

    // Register built-in sinks
    DefaultRegistry.RegisterSink(NewParquetSink())
}

// Route selects the best decoder for a source
func (r *Registry) Route(source IngestSource) (Decoder, error) {
    r.mu.RLock()
    defer r.mu.RUnlock()

    // Try format rules in priority order
    for _, rule := range r.formatRules {
        if rule.Matches(source) {
            if decoder, ok := r.decoders[rule.DecoderID]; ok {
                return decoder, nil
            }
        }
    }

    return nil, ErrNoDecoderFound
}
```

### 2.2 Extensibility Hooks

```go
// registry/plugins.go - NEW FILE

// DecoderFactory creates decoder instances
type DecoderFactory func(config map[string]interface{}) (Decoder, error)

// RegisterCustomDecoder allows users to add custom decoders
func (r *Registry) RegisterCustomDecoder(
    id string,
    factory DecoderFactory,
    formats []Format,
    costHint int,
) error {
    decoder, err := factory(nil)
    if err != nil {
        return err
    }

    r.mu.Lock()
    defer r.mu.Unlock()

    r.decoders[id] = decoder

    // Add format rules
    for _, format := range formats {
        r.formatRules = append(r.formatRules, FormatRule{
            Extensions: []string{format.Extension()},
            DecoderID:  id,
            CostHint:   costHint,
        })
    }

    return nil
}
```

---

## Phase 3: Error Handling & Bad Data Policies

### 3.1 Error Policy System

```go
// errors/policy.go - NEW FILE

// ErrorPolicy defines how to handle bad data
type ErrorPolicy int

const (
    // PolicyStrict aborts on first error
    PolicyStrict ErrorPolicy = iota

    // PolicyPermissive skips bad records, continues
    PolicyPermissive

    // PolicyQuarantine copies bad records to separate output
    PolicyQuarantine
)

// ErrorHandler processes decoding/writing errors
type ErrorHandler struct {
    Policy          ErrorPolicy
    MaxErrors       int
    QuarantinePath  string

    // Callbacks
    OnInvalidRecord func(record InvalidRecord)
    OnFileSkipped   func(file string, err error)
    OnSchemaMismatch func(file string, old, new *arrow.Schema)

    // State
    errorCount      int64
    quarantineFile  *os.File
    mu              sync.Mutex
}

// InvalidRecord represents a record that failed parsing
type InvalidRecord struct {
    Source      string
    LineNumber  int64
    ByteOffset  int64
    RawData     []byte
    Error       error
    Timestamp   time.Time
}

// Handle processes an error according to policy
func (h *ErrorHandler) Handle(err InvalidRecord) error {
    h.mu.Lock()
    defer h.mu.Unlock()

    h.errorCount++

    switch h.Policy {
    case PolicyStrict:
        return fmt.Errorf("strict mode: %w", err.Error)

    case PolicyPermissive:
        if h.OnInvalidRecord != nil {
            h.OnInvalidRecord(err)
        }
        if h.MaxErrors > 0 && h.errorCount >= int64(h.MaxErrors) {
            return ErrTooManyErrors
        }
        return nil // Continue

    case PolicyQuarantine:
        return h.quarantine(err)
    }

    return nil
}
```

### 3.2 Structured Error Output

```go
// errors/handler.go - NEW FILE

// ErrorRecord is the schema for error output
type ErrorRecord struct {
    SourceFile   string    `parquet:"source_file"`
    LineNumber   int64     `parquet:"line_number"`
    ByteOffset   int64     `parquet:"byte_offset"`
    ErrorType    string    `parquet:"error_type"`
    ErrorMessage string    `parquet:"error_message"`
    RawData      string    `parquet:"raw_data"`
    Timestamp    time.Time `parquet:"timestamp"`
}

// WriteErrorsParquet writes error records to a Parquet file
func WriteErrorsParquet(errors []ErrorRecord, path string) error {
    // Implementation: write errors as queryable Parquet
}
```

---

## Phase 4: Schema Management

### 4.1 Schema Inference

```go
// schema/inference.go - NEW FILE

// SchemaInferrer infers Arrow schema from data samples
type SchemaInferrer struct {
    SampleSize      int     // Rows to sample
    SampleBytes     int64   // Max bytes to sample
    ConfidenceLevel float64 // Type inference confidence
}

// Infer samples the source and returns inferred schema
func (i *SchemaInferrer) Infer(ctx context.Context, source IngestSource) (*arrow.Schema, error) {
    // 1. Sample data
    // 2. Detect column types
    // 3. Build Arrow schema
    // 4. Return with confidence metadata
}
```

### 4.2 Schema Evolution

```go
// schema/evolution.go - NEW FILE

// SchemaPolicy defines schema handling behavior
type SchemaPolicy int

const (
    // PolicySchemaStrict rejects mismatched schemas
    PolicySchemaStrict SchemaPolicy = iota

    // PolicySchemaMerge adds new columns as nullable
    PolicySchemaMerge

    // PolicySchemaEvolve allows compatible type changes
    PolicySchemaEvolve
)

// SchemaEvolver handles schema changes
type SchemaEvolver struct {
    Policy          SchemaPolicy
    OnSchemaChange  func(old, new *arrow.Schema, changes SchemaChanges)
}

// SchemaChanges describes differences between schemas
type SchemaChanges struct {
    AddedColumns    []string
    RemovedColumns  []string
    TypeChanges     map[string]TypeChange
    Compatible      bool
}

// TypeChange describes a column type change
type TypeChange struct {
    Column  string
    OldType arrow.DataType
    NewType arrow.DataType
    Safe    bool // Can convert without loss
}

// Evolve attempts to evolve old schema to accommodate new
func (e *SchemaEvolver) Evolve(old, new *arrow.Schema) (*arrow.Schema, error) {
    changes := CompareSchemas(old, new)

    switch e.Policy {
    case PolicySchemaStrict:
        if !changes.Compatible {
            return nil, ErrSchemaIncompatible
        }
        return old, nil

    case PolicySchemaMerge:
        return MergeSchemas(old, new)

    case PolicySchemaEvolve:
        return EvolveSchema(old, new)
    }

    return old, nil
}
```

### 4.3 Schema Cache

```go
// schema/cache.go - NEW FILE

// SchemaCache stores inferred schemas for reuse
type SchemaCache struct {
    cache   map[string]*CachedSchema
    maxAge  time.Duration
    mu      sync.RWMutex
}

// CachedSchema stores schema with metadata
type CachedSchema struct {
    Schema      *arrow.Schema
    InferredAt  time.Time
    SampleCount int
    SourceHash  uint64  // Hash of source characteristics
}

// Get retrieves cached schema if valid
func (c *SchemaCache) Get(key string) (*arrow.Schema, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()

    cached, ok := c.cache[key]
    if !ok {
        return nil, false
    }

    if time.Since(cached.InferredAt) > c.maxAge {
        return nil, false
    }

    return cached.Schema, true
}
```

---

## Phase 5: Flow Control & Backpressure

### 5.1 Bounded Queue

```go
// flow/backpressure.go - NEW FILE

// BoundedQueue provides backpressure for RecordBatch streaming
type BoundedQueue struct {
    batches     chan arrow.Record
    maxSize     int
    maxBytes    int64
    currentBytes int64

    // Metrics
    pushWaitTime  time.Duration
    popWaitTime   time.Duration

    mu sync.Mutex
}

// NewBoundedQueue creates a queue with size/byte limits
func NewBoundedQueue(maxBatches int, maxBytes int64) *BoundedQueue {
    return &BoundedQueue{
        batches:  make(chan arrow.Record, maxBatches),
        maxSize:  maxBatches,
        maxBytes: maxBytes,
    }
}

// Push adds a batch, blocking if queue is full
func (q *BoundedQueue) Push(ctx context.Context, batch arrow.Record) error {
    batchBytes := estimateBatchBytes(batch)

    // Wait for space if needed
    for {
        q.mu.Lock()
        if q.currentBytes+batchBytes <= q.maxBytes {
            q.currentBytes += batchBytes
            q.mu.Unlock()
            break
        }
        q.mu.Unlock()

        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(10 * time.Millisecond):
            // Retry
        }
    }

    select {
    case q.batches <- batch:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}

// Pop removes and returns a batch
func (q *BoundedQueue) Pop(ctx context.Context) (arrow.Record, error) {
    select {
    case batch := <-q.batches:
        q.mu.Lock()
        q.currentBytes -= estimateBatchBytes(batch)
        q.mu.Unlock()
        return batch, nil
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}
```

### 5.2 Concurrency Manager

```go
// flow/concurrency.go - NEW FILE

// ConcurrencyManager controls parallel operations by cost
type ConcurrencyManager struct {
    // Semaphores per cost tier
    semaphores map[int]chan struct{}

    // Limits
    limits map[int]int

    // Metrics
    inFlight map[int]*int64
}

// NewConcurrencyManager creates manager with cost-based limits
func NewConcurrencyManager(limits map[int]int) *ConcurrencyManager {
    cm := &ConcurrencyManager{
        semaphores: make(map[int]chan struct{}),
        limits:     limits,
        inFlight:   make(map[int]*int64),
    }

    for cost, limit := range limits {
        cm.semaphores[cost] = make(chan struct{}, limit)
        cm.inFlight[cost] = new(int64)
    }

    return cm
}

// Acquire gets a slot for the given cost tier
func (cm *ConcurrencyManager) Acquire(ctx context.Context, cost int) error {
    sem := cm.semaphores[cost]
    if sem == nil {
        return nil // No limit
    }

    select {
    case sem <- struct{}{}:
        atomic.AddInt64(cm.inFlight[cost], 1)
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}

// Release returns a slot
func (cm *ConcurrencyManager) Release(cost int) {
    sem := cm.semaphores[cost]
    if sem == nil {
        return
    }

    <-sem
    atomic.AddInt64(cm.inFlight[cost], -1)
}
```

---

## Phase 6: Observability

### 6.1 Metrics Collector

```go
// metrics/collector.go - NEW FILE

// Metrics holds all pipeline metrics
type Metrics struct {
    // Counters
    BytesRead       atomic.Int64
    BytesWritten    atomic.Int64
    RowsRead        atomic.Int64
    RowsWritten     atomic.Int64
    FilesProcessed  atomic.Int64
    FilesFailed     atomic.Int64
    RecordsSkipped  atomic.Int64
    RecordsQuarantined atomic.Int64

    // Timings
    ParseTimeNs     atomic.Int64
    WriteTimeNs     atomic.Int64
    TotalTimeNs     atomic.Int64

    // Per-format metrics
    FormatMetrics   sync.Map // Format -> *FormatMetrics

    // Histograms (optional)
    BatchSizeHist   *Histogram
    LatencyHist     *Histogram
}

// FormatMetrics holds per-format statistics
type FormatMetrics struct {
    BytesRead   atomic.Int64
    RowsRead    atomic.Int64
    ParseTimeNs atomic.Int64
    ErrorCount  atomic.Int64
}

// Snapshot returns a point-in-time copy of metrics
func (m *Metrics) Snapshot() MetricsSnapshot {
    return MetricsSnapshot{
        BytesRead:      m.BytesRead.Load(),
        BytesWritten:   m.BytesWritten.Load(),
        RowsRead:       m.RowsRead.Load(),
        RowsWritten:    m.RowsWritten.Load(),
        // ... etc
    }
}
```

### 6.2 Progress Reporter

```go
// metrics/progress.go - NEW FILE

// Progress represents current progress
type Progress struct {
    BytesRead       int64
    BytesTotal      int64
    RowsRead        int64
    FilesProcessed  int
    FilesTotal      int
    CurrentFile     string
    PercentComplete float64
    ETA             time.Duration
}

// ProgressReporter reports progress via callback
type ProgressReporter struct {
    callback    func(Progress)
    interval    time.Duration
    lastReport  time.Time

    mu sync.Mutex
}

// Report sends progress if interval elapsed
func (r *ProgressReporter) Report(p Progress) {
    r.mu.Lock()
    defer r.mu.Unlock()

    if time.Since(r.lastReport) < r.interval {
        return
    }

    r.lastReport = time.Now()
    if r.callback != nil {
        r.callback(p)
    }
}
```

---

## Phase 7: Configuration Model

### 7.1 Unified Configuration

```go
// config/config.go - NEW FILE

// Config is the unified configuration for the ingestion library
type Config struct {
    // I/O settings
    IO IOConfig `yaml:"io" json:"io"`

    // Parser settings
    Parser ParserConfig `yaml:"parser" json:"parser"`

    // Writer settings
    Writer WriterConfig `yaml:"writer" json:"writer"`

    // Error handling
    Errors ErrorConfig `yaml:"errors" json:"errors"`

    // Schema handling
    Schema SchemaConfig `yaml:"schema" json:"schema"`

    // Observability
    Observability ObservabilityConfig `yaml:"observability" json:"observability"`
}

type IOConfig struct {
    MaxConcurrentCheap     int   `yaml:"max_concurrent_cheap" json:"max_concurrent_cheap"`
    MaxConcurrentMedium    int   `yaml:"max_concurrent_medium" json:"max_concurrent_medium"`
    MaxConcurrentExpensive int   `yaml:"max_concurrent_expensive" json:"max_concurrent_expensive"`
    MaxBytesInFlight       int64 `yaml:"max_bytes_in_flight" json:"max_bytes_in_flight"`
    ReadBufferSize         int   `yaml:"read_buffer_size" json:"read_buffer_size"`
    WriteBufferSize        int   `yaml:"write_buffer_size" json:"write_buffer_size"`
}

type ParserConfig struct {
    DefaultBatchSize    int      `yaml:"default_batch_size" json:"default_batch_size"`
    MaxRecordSize       int      `yaml:"max_record_size" json:"max_record_size"`
    InferenceSampleSize int      `yaml:"inference_sample_size" json:"inference_sample_size"`
    NullValues          []string `yaml:"null_values" json:"null_values"`
}

type WriterConfig struct {
    Compression       string `yaml:"compression" json:"compression"`
    CompressionLevel  int    `yaml:"compression_level" json:"compression_level"`
    RowGroupSize      int    `yaml:"row_group_size" json:"row_group_size"`
    DataPageSize      int    `yaml:"data_page_size" json:"data_page_size"`
    EnableDictionary  bool   `yaml:"enable_dictionary" json:"enable_dictionary"`
    TargetFileSize    int64  `yaml:"target_file_size" json:"target_file_size"`
}

type ErrorConfig struct {
    Policy         string `yaml:"policy" json:"policy"` // strict, permissive, quarantine
    MaxErrors      int    `yaml:"max_errors" json:"max_errors"`
    QuarantinePath string `yaml:"quarantine_path" json:"quarantine_path"`
}

type SchemaConfig struct {
    Policy         string `yaml:"policy" json:"policy"` // strict, merge, evolve
    CacheSchemas   bool   `yaml:"cache_schemas" json:"cache_schemas"`
    CacheTTL       string `yaml:"cache_ttl" json:"cache_ttl"`
    MasterSchemaPath string `yaml:"master_schema_path" json:"master_schema_path"`
}

type ObservabilityConfig struct {
    EnableMetrics   bool   `yaml:"enable_metrics" json:"enable_metrics"`
    MetricsPrefix   string `yaml:"metrics_prefix" json:"metrics_prefix"`
    EnableTracing   bool   `yaml:"enable_tracing" json:"enable_tracing"`
    ProgressInterval string `yaml:"progress_interval" json:"progress_interval"`
}
```

### 7.2 Default Configuration

```go
// config/defaults.go - NEW FILE

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
    cpus := runtime.NumCPU()

    return Config{
        IO: IOConfig{
            MaxConcurrentCheap:     cpus * 4,
            MaxConcurrentMedium:    cpus * 2,
            MaxConcurrentExpensive: cpus / 2,
            MaxBytesInFlight:       256 * 1024 * 1024, // 256 MB
            ReadBufferSize:         256 * 1024,        // 256 KB
            WriteBufferSize:        256 * 1024,
        },
        Parser: ParserConfig{
            DefaultBatchSize:    8192,
            MaxRecordSize:       32 * 1024 * 1024, // 32 MB
            InferenceSampleSize: 10000,
            NullValues:          []string{"", "NULL", "null", "NA", "N/A", "None", "nil"},
        },
        Writer: WriterConfig{
            Compression:      "snappy",
            RowGroupSize:     10000,
            DataPageSize:     1024 * 1024, // 1 MB
            EnableDictionary: true,
            TargetFileSize:   512 * 1024 * 1024, // 512 MB
        },
        Errors: ErrorConfig{
            Policy:    "permissive",
            MaxErrors: 10000,
        },
        Schema: SchemaConfig{
            Policy:       "merge",
            CacheSchemas: true,
            CacheTTL:     "1h",
        },
        Observability: ObservabilityConfig{
            EnableMetrics:    true,
            ProgressInterval: "1s",
        },
    }
}
```

---

## Phase 8: Testing Framework

### 8.1 Round-Trip Testing

```go
// testing/roundtrip.go - NEW FILE

// RoundTripTest verifies data integrity through encode/decode
type RoundTripTest struct {
    Format      Format
    SampleSize  int
    Tolerance   float64 // For float comparisons
}

// Run executes the round-trip test
func (t *RoundTripTest) Run(data arrow.Record) error {
    // 1. Write to Parquet
    var buf bytes.Buffer
    if err := WriteParquet(&buf, data); err != nil {
        return fmt.Errorf("write failed: %w", err)
    }

    // 2. Read back
    result, err := ReadParquet(&buf)
    if err != nil {
        return fmt.Errorf("read failed: %w", err)
    }

    // 3. Compare
    if err := CompareRecords(data, result, t.Tolerance); err != nil {
        return fmt.Errorf("comparison failed: %w", err)
    }

    return nil
}
```

### 8.2 Fuzzing Helpers

```go
// testing/fuzzing.go - NEW FILE

// FuzzDecoder runs fuzz testing on a decoder
func FuzzDecoder(decoder Decoder, seed int64, iterations int) []FuzzResult {
    var results []FuzzResult
    rng := rand.New(rand.NewSource(seed))

    for i := 0; i < iterations; i++ {
        // Generate random malformed input
        input := generateFuzzInput(rng, decoder.SupportedFormats()[0])

        // Try to decode
        source := &MemorySource{Data: input}
        _, err := decoder.Decode(context.Background(), source, nil, DecodeOptions{})

        results = append(results, FuzzResult{
            Input:    input,
            Error:    err,
            Panicked: false, // Caught by recover
        })
    }

    return results
}

// generateFuzzInput creates malformed test data
func generateFuzzInput(rng *rand.Rand, format Format) []byte {
    // Generate plausible but potentially malformed data
    // - Truncated records
    // - Invalid UTF-8
    // - Missing delimiters
    // - Unbalanced quotes
    // - Type mismatches
}
```

---

## Implementation Priority

### Week 1: Core Abstractions
1. Create `core/` package with interfaces
2. Refactor `detect.go` → `detect/` package
3. Implement `IngestSource` for local files

### Week 2: Decoder Plugins
1. Create `registry/` package
2. Refactor `fast_path.go` → `decoders/duckdb.go`
3. Refactor `robust_path.go` → `decoders/csv.go`, `json.go`, `xes.go`

### Week 3: Error & Schema
1. Create `errors/` package with policies
2. Create `schema/` package with inference/evolution
3. Integrate with decoders

### Week 4: Flow Control
1. Create `flow/` package with backpressure
2. Refactor `pipeline.go` to use bounded queues
3. Add concurrency manager

### Week 5: Observability & Config
1. Create `metrics/` package
2. Create `config/` package
3. Add progress reporting

### Week 6: Testing & Polish
1. Create `testing/` package
2. Add round-trip tests for all formats
3. Add fuzzing helpers
4. Documentation

---

## Migration Path (No Breaking Changes)

### Step 1: Add interfaces alongside existing code
```go
// Old API still works
engine, _ := ingest.NewEngine()
result, _ := engine.Ingest(ctx, path, opts)

// New API available
pipeline, _ := ingest.NewPipeline(config)
result, _ := pipeline.Process(ctx, source, sink)
```

### Step 2: Deprecate old API
```go
// Deprecated: Use NewPipeline instead
func NewEngine() (*Engine, error) {
    return &Engine{pipeline: NewPipeline(DefaultConfig())}, nil
}
```

### Step 3: Remove old API in v2.0

---

## Dependencies

### Required
- `github.com/apache/arrow/go/v14` - Arrow columnar format
- `github.com/marcboeker/go-duckdb` - DuckDB for fast path

### Optional (for lakehouse)
- `github.com/apache/iceberg-go` - Iceberg table format
- `github.com/delta-io/delta-go` - Delta Lake format

### Testing
- `github.com/stretchr/testify` - Assertions
- `go test -fuzz` - Built-in fuzzing (Go 1.18+)

---

## Success Criteria

1. **Performance**: Maintain 900K+ rows/sec for clean CSV
2. **Extensibility**: Add new format in <100 lines
3. **Reliability**: <0.001% data loss in permissive mode
4. **Observability**: Full metrics coverage
5. **Testability**: >90% code coverage
6. **Documentation**: Complete API docs
