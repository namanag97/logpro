# LOGPRO: Ideal Pipeline Design Draft

## Philosophy

> "One pipeline, one job, one config, maximum speed, graceful fallback"

The pipeline should be like water flowing downhill - take the fastest path, but find a way around obstacles.

---

## The Single Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           UNIFIED INGESTION PIPELINE                         │
└─────────────────────────────────────────────────────────────────────────────┘

                                    CONFIG (YAML)
                                         │
                                         ▼
┌─────────┐    ┌──────────┐    ┌─────────────┐    ┌──────────┐    ┌─────────┐
│  INPUT  │───▶│  DETECT  │───▶│  HEURISTICS │───▶│  DECODE  │───▶│  SINK   │
│         │    │          │    │             │    │          │    │         │
│ File    │    │ Format   │    │ Strategy    │    │ DuckDB   │    │ Parquet │
│ Stream  │    │ Encoding │    │ Bucket      │    │    or    │    │ Iceberg │
│ URL     │    │ Schema   │    │ Tuning      │    │ Fallback │    │ Delta   │
└─────────┘    └──────────┘    └─────────────┘    └──────────┘    └─────────┘
                    │                 │                 │              │
                    └────────────────┴────────────────┴──────────────┘
                                         │
                                         ▼
                              ┌─────────────────┐
                              │    QUALITY      │
                              │  (Async/Parallel)│
                              │  Checksum       │
                              │  Cardinality    │
                              │  Entropy        │
                              └─────────────────┘
```

---

## Stage 1: INPUT (Source Abstraction)

**One interface, multiple implementations:**

```go
type Source interface {
    ID() string
    Location() string      // path, URL, or identifier
    Size() int64           // -1 if unknown (streams)
    Open(ctx) io.ReadCloser
    Metadata() map[string]string
}
```

**Implementations:**
- `FileSource` - local files
- `GlobSource` - multiple files matching pattern
- `StreamSource` - io.Reader wrapper
- `HTTPSource` - remote URLs (later)

**Key Decision:** Source doesn't know format. Just provides bytes.

---

## Stage 2: DETECT (Once, Cached)

**Purpose:** Analyze bytes → determine format, encoding, schema

```
Input bytes (sample 64KB)
         │
         ▼
    ┌─────────┐
    │ Magic   │──▶ Parquet? Avro? ORC? Excel? (binary formats)
    │ Bytes   │
    └────┬────┘
         │ not binary
         ▼
    ┌─────────┐
    │Encoding │──▶ UTF-8? UTF-16? ISO-8859-1? (charset detection)
    └────┬────┘
         │
         ▼
    ┌─────────┐
    │ Format  │──▶ CSV? JSON? JSONL? XML? XES? (structure detection)
    └────┬────┘
         │
         ▼
    ┌─────────┐
    │Delimiter│──▶ comma? tab? pipe? semicolon? (CSV only)
    └────┬────┘
         │
         ▼
    ┌─────────┐
    │ Schema  │──▶ Column names, types (sample-based inference)
    └─────────┘
         │
         ▼
    FileAnalysis {
        Format, Encoding, Delimiter, HasHeader,
        Schema, RowCount (estimated), Quality hints
    }
```

**Key Rules:**
1. **Detect ONCE per source** - cache result by source hash
2. **Same schema sources share cache** - 1000 CSVs from same system = 1 inference
3. **64KB sample is enough** - don't read whole file for detection

---

## Stage 3: HEURISTICS (Decision Engine)

**Purpose:** Based on detection → choose optimal strategy

```
FileAnalysis
     │
     ▼
┌─────────────────────────────────────────────────────────┐
│                   HEURISTIC ENGINE                       │
│                                                          │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐ │
│  │   BUCKET    │    │  STRATEGY   │    │   TUNING    │ │
│  │             │    │             │    │             │ │
│  │ 1:Native    │───▶│ FastPath    │───▶│ Concurrency │ │
│  │ 2:Cheap     │    │ (DuckDB)    │    │ BatchSize   │ │
│  │ 3:Medium    │    │             │    │ RowGroup    │ │
│  │ 4:Expensive │───▶│ RobustPath  │───▶│ Compression │ │
│  │             │    │ (Go stream) │    │ Memory      │ │
│  └─────────────┘    └─────────────┘    └─────────────┘ │
│                                                          │
└─────────────────────────────────────────────────────────┘
     │
     ▼
ProcessingPlan {
    Strategy: FastPath | RobustPath
    Decoder: "duckdb" | "go-csv" | "go-json"
    BatchSize: 8192
    RowGroupSize: 100000
    Compression: "zstd"
    MaxConcurrency: 8
    MaxMemoryMB: 4096
    EnableQuality: true
}
```

**Bucket Classification (from huristics.md):**

| Bucket | Formats | Speed | Strategy |
|--------|---------|-------|----------|
| 1: Native | Parquet, ORC, Avro | ~500 MB/s | Copy/rewrite, minimal decode |
| 2: Cheap | CSV, TSV, JSONL | ~150 MB/s | DuckDB fast path |
| 3: Medium | JSON, XML | ~50 MB/s | DuckDB or Go streaming |
| 4: Expensive | Excel, PDF | ~10 MB/s | Go streaming, low concurrency |

**Decision Table:**

```
IF format IN (parquet, orc, avro):
    → FastPath + copy/minimal rewrite

IF format IN (csv, tsv, jsonl) AND file_size < 10GB AND clean_data:
    → FastPath (DuckDB)

IF format IN (csv, tsv, jsonl) AND (file_size > 10GB OR dirty_data):
    → RobustPath (streaming with error recovery)

IF format IN (json, xml, xes):
    → RobustPath (Go streaming parsers)

IF format IN (xlsx, pdf):
    → RobustPath + low concurrency cap
```

---

## Stage 4: DECODE (The Actual Work)

**Two paths, one interface:**

```go
type Decoder interface {
    Decode(ctx, source, schema, opts) (<-chan RecordBatch, error)
}
```

### Path A: FastPath (DuckDB) - DEFAULT

```
Source file
     │
     ▼
┌─────────────────────────────────────────┐
│              DuckDB Engine              │
│                                         │
│  read_csv_auto() / read_json_auto()    │
│  read_parquet() / read_xlsx()          │
│                                         │
│  ┌─────────────────────────────────┐   │
│  │     Native Vectorized Parse      │   │
│  │     SIMD optimized               │   │
│  │     Zero-copy where possible     │   │
│  └─────────────────────────────────┘   │
│                    │                    │
│                    ▼                    │
│  ┌─────────────────────────────────┐   │
│  │     Arrow RecordBatches         │   │
│  │     (streaming out)             │   │
│  └─────────────────────────────────┘   │
└─────────────────────────────────────────┘
     │
     ▼
RecordBatch channel (back-pressured)
```

**Why DuckDB:**
- Proven 933K rows/sec on CSV
- Vectorized SIMD parsing
- Handles most formats natively
- Auto-detects types well

### Path B: RobustPath (Go Streaming) - FALLBACK

```
Source file
     │
     ▼
┌─────────────────────────────────────────┐
│           Go Streaming Parser           │
│                                         │
│  ┌─────────────────────────────────┐   │
│  │     Buffered Reader (64KB)      │   │
│  └────────────┬────────────────────┘   │
│               │                         │
│               ▼                         │
│  ┌─────────────────────────────────┐   │
│  │     Format Parser               │   │
│  │     - CSV: encoding/csv         │   │
│  │     - JSON: jsoniter            │   │
│  │     - XML: streaming SAX        │   │
│  └────────────┬────────────────────┘   │
│               │                         │
│               ▼                         │
│  ┌─────────────────────────────────┐   │
│  │     Error Recovery              │   │
│  │     - Skip bad rows             │   │
│  │     - Quarantine option         │   │
│  │     - Type coercion             │   │
│  └────────────┬────────────────────┘   │
│               │                         │
│               ▼                         │
│  ┌─────────────────────────────────┐   │
│  │     Arrow Builder               │   │
│  │     (batch accumulator)         │   │
│  └─────────────────────────────────┘   │
└─────────────────────────────────────────┘
     │
     ▼
RecordBatch channel (back-pressured)
```

**When RobustPath:**
- Dirty data that DuckDB rejects
- Need fine-grained error recovery
- Streaming sources (can't seek)
- Formats DuckDB doesn't handle well

### Automatic Fallback Logic

```
TRY FastPath (DuckDB):
    IF success:
        RETURN batches
    IF partial_failure (some rows bad):
        LOG warning, continue with good rows
    IF total_failure:
        FALLBACK to RobustPath

TRY RobustPath (Go):
    IF success:
        RETURN batches
    IF failure:
        RETURN error with details
```

---

## Stage 5: SINK (Output)

**One interface, format-specific implementations:**

```go
type Sink interface {
    Open(ctx, schema, opts) error
    Write(ctx, batch RecordBatch) error
    Close(ctx) (*Result, error)
}
```

### Parquet Sink (Primary)

```
RecordBatch stream
        │
        ▼
┌─────────────────────────────────────────┐
│            Parquet Writer               │
│                                         │
│  Config from YAML:                      │
│  - compression: zstd (default)          │
│  - row_group_size: 100000               │
│  - page_size: 1MB                       │
│  - dictionary_encoding: auto            │
│                                         │
│  ┌─────────────────────────────────┐   │
│  │   Batch Accumulator             │   │
│  │   (collect until row_group_size) │   │
│  └────────────┬────────────────────┘   │
│               │                         │
│               ▼                         │
│  ┌─────────────────────────────────┐   │
│  │   Row Group Writer              │   │
│  │   - Column encoding selection   │   │
│  │   - Dictionary for low-card     │   │
│  │   - Compression                 │   │
│  └────────────┬────────────────────┘   │
│               │                         │
│               ▼                         │
│  ┌─────────────────────────────────┐   │
│  │   File Writer                   │   │
│  │   - Footer with statistics      │   │
│  │   - Checksum                    │   │
│  └─────────────────────────────────┘   │
└─────────────────────────────────────────┘
        │
        ▼
   output.parquet
```

**Parquet Heuristics (from huristics.md):**
- Row group: 64-128MB compressed (Heuristic #13)
- Compression: ZSTD for storage efficiency, Snappy for speed (Heuristic #15)
- Dictionary encoding for low-cardinality strings (Heuristic #16)
- Target file size: 256MB-1GB (Heuristic #19)

---

## Stage 6: QUALITY (Async/Parallel)

**Runs alongside decode, NOT blocking:**

```
                    RecordBatch stream
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ Checksum │ │ Entropy  │ │Cardintic │
        │ (FNV-1a) │ │(Shannon) │ │(HyperLL) │
        └────┬─────┘ └────┬─────┘ └────┬─────┘
              │            │            │
              └────────────┼────────────┘
                           │
                           ▼
                    QualityReport {
                        Checksum: uint64
                        Entropy: float64
                        Cardinality: map[col]uint64
                        NullCounts: map[col]int64
                        Stats: min/max/avg per column
                    }
```

**Key:** Quality does NOT block ingestion. Computes in parallel, reports at end.

---

## Configuration (Single YAML)

```yaml
# /etc/logpro/config.yaml or ./logpro.yaml

logpro:
  # Detection settings
  detection:
    sample_size: 65536        # bytes to sample for detection
    encoding_detection: true
    schema_cache_ttl: 3600    # seconds

  # Heuristics tuning
  heuristics:
    # Bucket concurrency limits
    bucket_concurrency:
      native: 16      # Parquet/ORC/Avro
      cheap: 8        # CSV/TSV/JSONL
      medium: 4       # JSON/XML
      expensive: 2    # Excel/PDF

    # Task sizing
    target_task_size: 268435456   # 256MB
    min_task_size: 67108864       # 64MB
    max_task_size: 1073741824     # 1GB

  # Decoder settings
  decoder:
    batch_size: 8192
    max_memory_mb: 4096
    duckdb_threads: 0           # 0 = auto (num CPUs)
    fallback_on_error: true     # auto-fallback to RobustPath

  # Parquet output settings
  parquet:
    compression: zstd           # snappy, zstd, gzip, lz4, none
    compression_level: 3        # 1-22 for zstd
    row_group_size: 100000      # rows per row group
    page_size: 1048576          # 1MB
    dictionary_encoding: auto   # auto, always, never
    statistics: true            # write column statistics

  # Quality validation
  quality:
    enabled: true
    checksum: true
    entropy: false              # expensive, opt-in
    cardinality: true
    null_threshold: 0.5         # warn if >50% nulls

  # Error handling
  errors:
    policy: permissive          # strict, permissive, quarantine
    max_errors: 1000            # stop after N errors
    quarantine_path: ./quarantine/

  # Performance tuning
  performance:
    buffer_size: 65536          # 64KB read buffer
    channel_buffer: 100         # batch channel size
    gc_percent: 100             # GOGC value during ingestion
```

**Config Loading Priority:**
1. CLI flags (highest)
2. Environment variables (LOGPRO_PARQUET_COMPRESSION=zstd)
3. ./logpro.yaml (local)
4. ~/.logpro/config.yaml (user)
5. /etc/logpro/config.yaml (system)
6. Compiled defaults (lowest)

---

## Unified Pipeline Orchestration

```go
// The ONE pipeline function
func (p *Pipeline) Ingest(ctx context.Context, source Source, output string) (*Result, error) {
    // 1. DETECT (cached)
    analysis, err := p.detector.Analyze(ctx, source)
    if err != nil {
        return nil, fmt.Errorf("detection failed: %w", err)
    }

    // 2. HEURISTICS (decision)
    plan := p.heuristics.Plan(analysis, p.config)

    // 3. DECODE (fast path first, fallback if needed)
    batches, err := p.decode(ctx, source, analysis.Schema, plan)
    if err != nil {
        return nil, fmt.Errorf("decode failed: %w", err)
    }

    // 4. QUALITY (async, parallel with sink)
    qualityCh := p.quality.ValidateAsync(ctx, batches)

    // 5. SINK (write output)
    result, err := p.sink.Write(ctx, batches, output, plan.SinkOptions)
    if err != nil {
        return nil, fmt.Errorf("sink failed: %w", err)
    }

    // 6. COLLECT quality report
    result.Quality = <-qualityCh

    return result, nil
}

func (p *Pipeline) decode(ctx, source, schema, plan) (<-chan RecordBatch, error) {
    if plan.Strategy == FastPath {
        batches, err := p.duckdbDecoder.Decode(ctx, source, schema, plan.DecodeOpts)
        if err == nil {
            return batches, nil
        }
        // Fallback on error
        if p.config.Decoder.FallbackOnError {
            log.Warn("DuckDB failed, falling back to robust path", "error", err)
            return p.robustDecoder.Decode(ctx, source, schema, plan.DecodeOpts)
        }
        return nil, err
    }
    return p.robustDecoder.Decode(ctx, source, schema, plan.DecodeOpts)
}
```

---

## File Structure (After Cleanup)

```
pkg/
├── config/
│   ├── config.go          # Main Config struct + loading
│   ├── defaults.go        # Default values (single source of truth)
│   └── validation.go      # Config validation
│
├── ingest/
│   ├── pipeline.go        # THE pipeline (only one)
│   ├── engine.go          # High-level API
│   │
│   ├── detect/            # Detection (merged from detect.go)
│   │   ├── detector.go
│   │   ├── format.go
│   │   ├── encoding.go
│   │   └── schema.go
│   │
│   ├── heuristics/        # Heuristics (merged from heuristics.go)
│   │   ├── engine.go
│   │   ├── buckets.go
│   │   └── decisions.go
│   │
│   ├── decode/            # Decoders
│   │   ├── decoder.go     # Interface
│   │   ├── duckdb.go      # Fast path (primary)
│   │   └── robust.go      # Fallback (from robust_path.go)
│   │
│   ├── sink/              # Sinks
│   │   ├── sink.go        # Interface
│   │   └── parquet.go     # Parquet writer
│   │
│   ├── quality/           # Quality (async)
│   │   ├── validator.go
│   │   ├── checksum.go
│   │   ├── entropy.go
│   │   └── cardinality.go
│   │
│   ├── flow/              # Flow control
│   │   ├── backpressure.go
│   │   └── concurrency.go
│   │
│   ├── errors/            # Error handling
│   │   ├── policy.go
│   │   └── quarantine.go
│   │
│   └── sources/           # Input sources
│       ├── file.go
│       ├── glob.go
│       └── stream.go
│
├── interfaces/            # Keep as-is (already clean)
│
└── defaults/              # Keep as-is (already clean)

DELETE:
├── pkg/ingest/detect.go       # → merged into detect/
├── pkg/ingest/heuristics.go   # → merged into heuristics/
├── pkg/ingest/fast_path.go    # → merged into decode/duckdb.go
├── pkg/ingest/robust_path.go  # → merged into decode/robust.go
├── pkg/pipeline/              # → entire folder deleted
├── pkg/fast/                  # → entire folder deleted
├── pkg/quality/               # → use ingest/quality/
```

---

## Summary: The Ideal Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│   INPUT ──▶ DETECT ──▶ HEURISTICS ──▶ DECODE ──▶ SINK         │
│              (once)     (decide)      (DuckDB    (Parquet)     │
│                                        or Go)                   │
│                                          │                      │
│                                          ▼                      │
│                                      QUALITY                    │
│                                      (async)                    │
│                                                                 │
│   Config: YAML (single source of truth)                        │
│   Strategy: Fast path first, robust fallback                   │
│   Heuristics: Auto-tune based on format/size/cleanliness       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Properties:**
- ✅ One pipeline
- ✅ One config (YAML)
- ✅ No duplicates
- ✅ DuckDB for speed (proven 933K rows/sec)
- ✅ Go fallback for robustness
- ✅ Heuristics-driven decisions
- ✅ Async quality (non-blocking)
- ✅ All settings configurable

---

## Next Steps (If Approved)

1. **Merge duplicate code** into this structure
2. **Centralize config** to single YAML
3. **Wire heuristics** to config values
4. **Run benchmarks** to establish baseline
5. **Optimize** based on benchmark data

---

*Draft v1 - Awaiting review*
