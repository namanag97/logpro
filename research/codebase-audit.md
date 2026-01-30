# Codebase Audit: What We Have vs What We Need

> Audit date: 2026-01-30
> Codebase: ~96 Go files, ~48K lines in `pkg/`, ~618K total (incl. generated)

---

## Codebase Map (What Exists Today)

```
logpro/
├── cmd/logflow/                 # CLI + Web server entry point
│   ├── main.go                  # Cobra CLI: convert, info, schema, apply, profiles
│   ├── serve.go                 # HTTP server with embedded web UI
│   ├── commands.go              # Additional CLI commands
│   ├── ingest_cmd.go            # Ingest subcommand
│   ├── pipeline_runner.go       # Pipeline execution glue
│   ├── benchmark.go             # Benchmarking command
│   └── web/index.html           # Embedded HTML UI
│
├── internal/
│   ├── pipe/                    # Original pipeline (DuckDB + Arrow paths)
│   │   ├── pipeline.go          # Arrow-based pipeline
│   │   └── duckdb_pipe.go       # DuckDB-based pipeline
│   ├── model/event.go           # Event model
│   └── pool/                    # Worker pool, helpers, timestamp utils
│
├── pkg/
│   ├── ingest/                  # [53 files] ← LARGEST package
│   │   ├── core/                # Source, Sink, Decoder interfaces (Arrow-native)
│   │   ├── decoders/            # CSV, JSON, XES, Parquet, DuckDB decoders
│   │   ├── sources/             # File, memory, stream, HTTP, glob sources
│   │   ├── sinks/               # Parquet, Arrow IPC, Iceberg sinks
│   │   ├── schema/              # Inference, evolution, OCEL mapping, policy
│   │   ├── flow/                # Backpressure, rate limiter, concurrency
│   │   ├── quality/             # Cardinality, checksum, validator, entropy
│   │   ├── errors/              # Stream errors, policy, quarantine
│   │   ├── telemetry/           # Metrics, benchmark, optimizer
│   │   ├── hooks/               # Lifecycle hooks
│   │   ├── detect/              # Format detection: encoding, delimiter, format
│   │   ├── heuristics/          # Decision engine
│   │   ├── pipeline.go          # Pipeline orchestrator (FastPath + RobustPath)
│   │   ├── streaming.go         # StreamingPipeline (Arrow-native)
│   │   ├── fast_path.go         # DuckDB fast path
│   │   ├── robust_path.go       # Pure Go robust path
│   │   ├── detect.go            # File analysis/detection
│   │   ├── quality.go           # Quality checks
│   │   ├── heuristics.go        # Heuristic computation
│   │   └── unified_config.go    # Configuration
│   │
│   ├── pipeline/                # [10 files] ← SECOND pipeline system
│   │   ├── interfaces.go        # Source/Sink/Processor/Inspector (Event-based)
│   │   ├── orchestrator.go      # V1 orchestrator (channel pipeline)
│   │   ├── orchestrator_v2.go   # V2 orchestrator (errgroup, metrics)
│   │   ├── dlq.go               # Dead Letter Queue
│   │   ├── checkpoint.go        # Checkpointing
│   │   ├── dedup.go             # Deduplication
│   │   ├── enterprise.go        # Enterprise features
│   │   └── errors.go/test       # Error types, tests
│   │
│   ├── server/                  # HTTP server (web UI backend)
│   ├── api/rest/                # REST API (handlers, middleware)
│   ├── storage/                 # S3, catalog, object store, table, compaction
│   ├── checkpoint/              # Checkpoint backends (local, Redis, S3)
│   ├── schema/                  # Schema policy + cache
│   ├── parser/                  # Parser + healing/auto-fix (11 files)
│   ├── ocel/                    # OCEL process mining model
│   ├── pmpt/                    # Process mining performance tree
│   ├── telemetry/               # OTEL exporter + metrics
│   ├── config/                  # Global config
│   ├── quality/                 # Quality validator
│   ├── validation/quality/      # Validation quality rules
│   ├── inspect/                 # Quality inspector
│   ├── contract/                # Data contracts
│   ├── diff/                    # Diff engine
│   ├── sinks/                   # Iceberg sink (standalone)
│   ├── tui/                     # Terminal UI wizard
│   ├── writer/                  # Parquet writer
│   ├── transform/               # Transform system
│   ├── processors/              # Processor implementations
│   ├── adapters/                # Adapter layer
│   ├── plugins/                 # Process mining + quality plugins
│   ├── hooks/                   # Hooks system
│   ├── interfaces/              # Interface definitions
│   ├── registry/                # Component registry
│   ├── defaults/                # Default implementations (auth, metrics, scheduler, tracer, alerting)
│   ├── state/                   # State store
│   ├── lifecycle/               # Lifecycle management
│   ├── resilience/              # Resilience patterns
│   ├── runtime/                 # Runtime utilities
│   ├── perf/                    # Profiler
│   ├── export/                  # Export utilities
│   ├── index/                   # Indexing
│   ├── watch/                   # File watching
│   ├── turbo/                   # Turbo converter
│   ├── core/                    # Core plugins + convert
│   ├── heuristic/               # Heuristic optimizer
│   ├── errors/                  # Error types
│   ├── profile/                 # Profile management
│   ├── util/                    # Reader utilities
│   ├── testing/                 # Test generators + roundtrip
│   └── ingest_backup/           # Backup (empty)
```

---

## PROBLEM 1: Two Competing Pipeline Architectures

This is the root issue. There are two separate pipeline systems that don't share interfaces.

### Pipeline A: `pkg/pipeline/` (Event-Based)
```go
// Uses *Event flowing through channels
type Source interface {
    Read(ctx context.Context, r io.Reader, out chan<- *Event) error
}
type Sink interface {
    Write(ctx context.Context, in <-chan *Event) error
}
type Processor interface {
    Process(ctx context.Context, in <-chan *Event, out chan<- *Event) error
}
```
- Has: Orchestrator V1 + V2, DLQ, checkpoint, dedup, rate limiter, builder pattern
- Strength: Clean pipeline pattern, errgroup coordination, production error handling
- Weakness: Event-based (not Arrow-native), process-mining-coupled (CaseID, Activity, Timestamp in Config)

### Pipeline B: `pkg/ingest/` (Arrow-Native)
```go
// Uses Arrow RecordBatches
type Source interface {
    Open(ctx context.Context) (io.ReadCloser, error)
}
type Decoder interface {
    Decode(ctx context.Context, source Source, opts DecodeOptions) (<-chan DecodedBatch, error)
}
type Sink interface {
    Write(ctx context.Context, batch arrow.Record) error
}
```
- Has: Decoders (CSV/JSON/XES/Parquet/DuckDB), format detection, heuristics, schema inference/evolution, flow control, quality, telemetry
- Strength: Arrow-native, format-aware, performance-oriented (FastPath/RobustPath)
- Weakness: No unified orchestrator tying decode→transform→write. Pipeline.Process() is file-centric, not streaming.

### Verdict
Pipeline B has the right data model (Arrow). Pipeline A has the right orchestration pattern (channel pipeline with errgroup). Neither is complete alone.

---

## PROBLEM 2: Duplicate Subsystems

| Concern | Location 1 | Location 2 | Location 3 |
|---|---|---|---|
| **Source interface** | `pkg/pipeline/interfaces.go` | `pkg/ingest/core/source.go` | — |
| **Sink interface** | `pkg/pipeline/interfaces.go` | `pkg/ingest/core/sink.go` | `pkg/sinks/iceberg.go` |
| **Schema** | `pkg/schema/` | `pkg/ingest/schema/` | — |
| **Quality** | `pkg/quality/` | `pkg/inspect/` | `pkg/validation/quality/` + `pkg/ingest/quality/` |
| **Errors** | `pkg/errors/` | `pkg/pipeline/errors.go` | `pkg/ingest/errors/` |
| **Checkpoint** | `pkg/pipeline/checkpoint.go` | `pkg/checkpoint/` | — |
| **Heuristics** | `pkg/heuristic/` | `pkg/ingest/heuristics/` + `pkg/ingest/heuristics.go` | — |
| **Hooks** | `pkg/hooks/` | `pkg/ingest/hooks/` | — |

---

## PROBLEM 3: Too Many Entry Points

| Surface | Location | How it Works |
|---|---|---|
| **CLI** | `cmd/logflow/main.go` | Cobra commands → `internal/pipe/` directly |
| **TUI Wizard** | `pkg/tui/cli.go` | Interactive wizard → sets CLI flags → runs CLI |
| **Web UI** | `cmd/logflow/serve.go` + `pkg/server/` | Embedded HTML + SSE → calls server logic |
| **REST API** | `pkg/api/rest/` | Separate HTTP handlers → unclear what pipeline they call |
| **Library API** | `pkg/ingest/` | Direct Go API → `Pipeline.Process()` |

These are wired to different backends. The CLI calls `internal/pipe/`, the web calls `pkg/server/`, and the library API calls `pkg/ingest/`. There's no single pipeline engine underneath them all.

---

## PROBLEM 4: Process Mining Coupling

The pipeline Config hard-codes process mining concepts:
```go
CaseIDColumn    string  // Process mining specific
ActivityColumn  string  // Process mining specific
TimestampColumn string  // Process mining specific
ResourceColumn  string  // Process mining specific
```

This leaks into interfaces, decoders, and the CLI. For a general-purpose ingestion library, these should be an optional plugin/transform — not baked into the core.

---

## PROBLEM 5: Root-Level Test Files

Six standalone test files at the project root:
```
test_precommit.go    (2189 lines)
test_all_xes.go
test_comprehensive.go
test_e2e.go
test_full_integration.go
test_telemetry.go
test_xes_verify.go
speedtest.go
```
These are standalone `main` packages, not `_test.go` files. They can't be run with `go test` and don't integrate with CI.

---

## CHECKLIST: What We Have vs What We Need

### Layer 1 — I/O (Sources & Sinks)

| Component | Have? | Where | Quality | Needed |
|---|---|---|---|---|
| **Source interface (Arrow-native)** | YES | `pkg/ingest/core/source.go` | Good | Needs `Read() → (RawMessage, Ack, error)` for streaming |
| **Sink interface (Arrow-native)** | YES | `pkg/ingest/core/sink.go` | Good | Keep as-is |
| **Decoder interface** | YES | `pkg/ingest/core/decoder.go` | Good | Keep as-is |
| **File source** | YES | `pkg/ingest/sources/file.go` | OK | Keep |
| **HTTP source** | YES | `pkg/ingest/sources/http.go` | OK | Keep |
| **Memory source** | YES | `pkg/ingest/sources/memory.go` | OK | Keep |
| **Stream source** | YES | `pkg/ingest/sources/stream.go` | Basic | Needs Kafka/MQTT/NATS support |
| **Glob source** | YES | `pkg/ingest/sources/glob.go` | OK | Keep |
| **S3 source** | PARTIAL | `pkg/storage/s3/` | Has client | Needs Source interface wrapper |
| **Kafka source** | NO | — | — | **BUILD** (P1) |
| **MQTT source** | NO | — | — | **BUILD** (P2) |
| **Postgres CDC source** | NO | — | — | **BUILD** (P1) |
| **MySQL CDC source** | NO | — | — | **BUILD** (P1) |
| **Webhook source** | NO | — | — | **BUILD** (P2) |
| **CSV decoder** | YES | `pkg/ingest/decoders/csv.go` | 574 lines, solid | Keep |
| **JSON decoder** | YES | `pkg/ingest/decoders/json.go` | 480 lines, solid | Keep |
| **XES decoder** | YES | `pkg/ingest/decoders/xes.go` | 826 lines | Move to plugin (process mining specific) |
| **Parquet decoder** | YES | `pkg/ingest/decoders/parquet.go` | OK | Keep |
| **DuckDB decoder** | YES | `pkg/ingest/decoders/duckdb.go` | OK | Keep as fast-path option |
| **Avro decoder** | NO | — | — | **BUILD** (P0) |
| **Protobuf decoder** | NO | — | — | **BUILD** (P0) |
| **Excel decoder** | NO | — | Format enum exists | **BUILD** (P2) |
| **Decoder registry** | YES | `pkg/ingest/decoders/registry.go` | Good | Keep |
| **Parquet sink** | YES | `pkg/ingest/sinks/parquet.go` | OK | Keep, tune parameters |
| **Arrow IPC sink** | YES | `pkg/ingest/sinks/arrow_ipc.go` | OK | Keep |
| **Iceberg sink** | YES | `pkg/sinks/iceberg.go` (522 lines) | Minimal | **REWRITE** — needs real catalog integration |
| **Iceberg sink (ingest)** | YES | `pkg/ingest/sinks/iceberg.go` | Duplicate | **MERGE** with above |

### Layer 2 — Core Transport

| Component | Have? | Where | Quality | Needed |
|---|---|---|---|---|
| **Arrow record batches** | YES | `pkg/ingest/core/batch.go` | Good | Keep |
| **BatchPool (sync.Pool)** | YES | `pkg/ingest/core/batch.go` | Good | Keep |
| **BatchAccumulator** | YES | `pkg/ingest/core/batch.go` | Basic | Needs size-based flush (MB threshold) |
| **Bounded channel pipeline** | YES | `pkg/pipeline/orchestrator.go` | Good pattern | Port to Arrow-native types |
| **Backpressure** | YES | `pkg/ingest/flow/backpressure.go` | OK | Keep |
| **Rate limiter** | YES | `pkg/ingest/flow/rate_limiter.go` + `pkg/pipeline/orchestrator_v2.go` | Duplicate | **MERGE** |
| **Concurrency control** | YES | `pkg/ingest/flow/concurrency.go` | OK | Keep |

### Layer 3 — Processing

| Component | Have? | Where | Quality | Needed |
|---|---|---|---|---|
| **Schema inference** | YES | `pkg/ingest/schema/inference.go` | Has caching | Keep |
| **Schema evolution** | YES | `pkg/ingest/schema/evolution.go` | Change detection | Keep, add Iceberg evolution |
| **Schema policy** | YES | `pkg/ingest/schema/policy.go` + `pkg/schema/policy.go` | Duplicate | **MERGE** |
| **Transform system** | PARTIAL | `pkg/transform/` | 2 files | **BUILD** full transform pipeline |
| **Processor interface** | YES | `pkg/pipeline/interfaces.go` | Event-based | **REWRITE** for Arrow RecordBatch |
| **Process mining transforms** | YES | `pkg/ocel/`, `pkg/pmpt/` | Domain-specific | **MOVE** to plugin |
| **Quality/validation** | YES | 4 locations (see duplicates) | Scattered | **CONSOLIDATE** to one package |
| **Format detection** | YES | `pkg/ingest/detect/` + `pkg/ingest/detect.go` | 793 lines, thorough | Keep |
| **Heuristics engine** | YES | `pkg/ingest/heuristics/` | Good | Keep |
| **Parser healing** | YES | `pkg/parser/healing/` | 3 files, ~1500 lines | Keep as optional transform |

### Layer 4 — Reliability

| Component | Have? | Where | Quality | Needed |
|---|---|---|---|---|
| **Checkpointing** | YES | `pkg/pipeline/checkpoint.go` | Job-level, file-based | Needs offset-level tracking |
| **Checkpoint backends** | YES | `pkg/checkpoint/` | Local, Redis, S3 | Good coverage |
| **DLQ** | YES | `pkg/pipeline/dlq.go` | JSON file output | Needs Parquet DLQ output |
| **Quarantine** | YES | `pkg/ingest/errors/quarantine.go` | Basic | Merge with DLQ |
| **Error policy** | YES | `pkg/ingest/errors/policy.go` + `pkg/ingest/core/decoder.go` | Duplicate | **MERGE** |
| **Deduplication** | YES | `pkg/pipeline/dedup.go` | 413 lines | Port to Arrow-native |
| **Retry logic** | NO | — | — | **BUILD** (exponential backoff) |
| **Idempotent writes** | NO | — | — | **BUILD** (Iceberg snapshot dedup) |

### Layer 5 — Orchestration

| Component | Have? | Where | Quality | Needed |
|---|---|---|---|---|
| **Unified orchestrator** | NO | Two competing systems | — | **BUILD** — single engine over Arrow |
| **Lifecycle management** | PARTIAL | `pkg/lifecycle/` | 1 file | **BUILD** full state machine |
| **YAML config** | PARTIAL | `pkg/config/` + `pkg/ingest/unified_config.go` | Exists | **CONSOLIDATE** and extend |
| **CLI** | YES | `cmd/logflow/main.go` | Cobra, works | **SIMPLIFY** — too many flags |
| **Web UI** | YES | `cmd/logflow/serve.go` + web/ | Embedded HTML + SSE | Keep, rewire to unified engine |
| **REST API** | PARTIAL | `pkg/api/rest/` | Handlers exist | **REWIRE** to unified engine |
| **Prometheus metrics** | PARTIAL | `pkg/defaults/metrics/` + `pkg/ingest/telemetry/` | Exists | **CONSOLIDATE** |
| **OTEL tracing** | YES | `pkg/telemetry/otel.go` | 404 lines | Keep |
| **Health endpoint** | NO | — | — | **BUILD** |
| **Graceful shutdown** | PARTIAL | CLI has signal handling | — | **BUILD** full drain logic |
| **Hot reload** | NO | — | — | Future (not needed now) |

### Cross-Cutting

| Component | Have? | Where | Quality | Needed |
|---|---|---|---|---|
| **Data contracts** | YES | `pkg/contract/` | Nice feature | Keep |
| **Diff engine** | YES | `pkg/diff/` | 459 lines | Keep |
| **File watching** | YES | `pkg/watch/` | 1 file | Keep |
| **State store** | YES | `pkg/state/` | 515 lines | Keep |
| **Profiler** | YES | `pkg/perf/` | 490 lines | Keep |
| **Test generators** | YES | `pkg/testing/` | Roundtrip + generators | Keep |
| **Compaction** | YES | `pkg/compaction/` + `pkg/storage/maintenance/` | Duplicate | **MERGE** |
| **S3 storage** | YES | `pkg/storage/s3/` | ~1100 lines | Keep |
| **Catalog (file-based)** | YES | `pkg/storage/catalog/` | Basic | Needs REST catalog for Iceberg |
| **Process mining plugin** | YES | `pkg/plugins/processmining/` | OK | Keep as plugin |

---

## Summary Scoreboard

| Category | Have | Partial | Missing | Duplicate/Broken |
|---|---|---|---|---|
| **Core interfaces** | 3 | 0 | 0 | 2 (competing sets) |
| **Decoders** | 5 | 0 | 3 (Avro, Protobuf, Excel) | 0 |
| **Sources** | 5 | 1 (S3) | 4 (Kafka, CDC, MQTT, Webhook) | 0 |
| **Sinks** | 3 | 1 (Iceberg) | 0 | 1 (duplicate Iceberg) |
| **Schema** | 2 | 0 | 0 | 2 (duplicate packages) |
| **Quality** | 4 | 0 | 0 | 3 (4 locations!) |
| **Flow control** | 3 | 0 | 0 | 1 (duplicate rate limiter) |
| **Reliability** | 3 | 2 | 2 (retry, idempotent) | 2 (duplicate errors/quarantine) |
| **Orchestration** | 0 | 3 (CLI, API, web) | 1 (unified engine) | 2 (competing pipelines) |
| **Observability** | 2 | 1 (metrics) | 1 (health) | 1 (duplicate metrics) |

**Totals:**
- **Solid and working:** ~30 components
- **Partial / needs improvement:** ~8 components
- **Missing (must build):** ~11 components
- **Duplicate / must merge:** ~14 instances across 7 concerns

---

## The Path Forward (High-Level)

### Step 1: Unify the Core (fix the root problem)
Merge the two pipeline architectures into one. Keep:
- **Arrow-native data model** from `pkg/ingest/core/`
- **Channel pipeline orchestration** from `pkg/pipeline/orchestrator_v2.go`
- **Result:** One `Source → Decoder → chan arrow.Record → Transform → Sink` pipeline

### Step 2: Consolidate Duplicates
Merge the 7 duplicated concerns (schema, quality, errors, checkpoints, heuristics, hooks, flow control) into single packages. Delete `pkg/ingest_backup/`.

### Step 3: Decouple Process Mining
Move process-mining-specific code (OCEL, PMPT, XES decoder, CaseID/Activity/Timestamp config) into `plugin/processmining/`. The core library should be domain-agnostic.

### Step 4: Rewire All Entry Points
CLI, Web UI, and REST API all call the same unified pipeline engine. No more separate code paths.

### Step 5: Build Missing Sources & Decoders
Avro, Protobuf → P0. Kafka, Postgres CDC, MySQL CDC → P1.

### Step 6: Harden the Iceberg Writer
Replace the minimal Iceberg sink with real catalog integration using `apache/iceberg-go`. Add compaction, snapshot management, partition evolution.

### Step 7: Clean Up
- Move root-level test files into proper `_test.go` files
- Delete orphaned/backup directories
- Consolidate single-file packages where they belong

---

## Target Package Structure (After Cleanup)

```
logpro/
├── cmd/logpro/                  # Single CLI binary
│   └── main.go                  # Cobra: ingest, serve, info, schema
├── internal/
│   └── engine/                  # Unified pipeline engine (not exported)
├── pkg/
│   ├── arrow/                   # Arrow helpers, batch utilities
│   ├── config/                  # YAML config (one package)
│   ├── checkpoint/              # Checkpoint store + backends
│   ├── decode/                  # All decoders (CSV, JSON, Avro, Protobuf, Parquet)
│   ├── detect/                  # Format detection + heuristics
│   ├── dlq/                     # Dead letter queue
│   ├── errors/                  # Error types (one package)
│   ├── flow/                    # Backpressure, rate limiting, concurrency
│   ├── observe/                 # Metrics, tracing, logging (one package)
│   ├── quality/                 # Validation + quality (one package)
│   ├── schema/                  # Inference + evolution + policy (one package)
│   ├── sink/                    # Parquet, Iceberg, Arrow IPC sinks
│   ├── source/                  # File, S3, HTTP, Kafka, CDC, MQTT sources
│   ├── storage/                 # S3, catalog, object store
│   ├── transform/               # Transform pipeline
│   └── writer/                  # Parquet file writer internals
├── plugin/
│   └── processmining/           # OCEL, PMPT, XES decoder, column mapping
├── server/                      # Web UI + REST API
└── research/                    # This document
```

38 packages → ~18 packages. Each package has one clear responsibility.
