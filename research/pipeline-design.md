# Pipeline Design: A Modular Go Ingestion & Analytics Library

> Research compiled 2026-01-30. Brainstorming reference — not an implementation plan.

---

## 1. The Mental Model

The entire library reduces to one sentence:

> **Read bytes from anywhere, decode them into Arrow record batches, and write those batches as Parquet files managed by Iceberg tables.**

Every design decision flows from keeping those three concerns — *source*, *processing*, *sink* — completely independent of each other. Two production systems already prove this model works at scale:

| System | Language | Architecture |
|---|---|---|
| **[Benthos / Redpanda Connect](https://github.com/redpanda-data/connect)** | Go | `input → pipeline.processors → output`, declarative YAML, 220+ connectors, transaction-based at-least-once delivery |
| **[Vector](https://github.com/vectordotdev/vector)** | Rust | `sources → transforms → sinks`, DAG topology, 90+ crates, hot-reloadable config |

Both validate the same idea: a small, rigid core with an infinite surface area of plugins.

---

## 2. The Five Layers

The library is not one pipeline — it is five layers stacked on top of each other. Each layer has exactly one job and communicates with its neighbors through a single, well-defined contract.

```
┌─────────────────────────────────────────────────────────────┐
│  LAYER 5: ORCHESTRATION                                     │
│  Scheduling, lifecycle, health checks, config, CLI/API      │
├─────────────────────────────────────────────────────────────┤
│  LAYER 4: RELIABILITY                                       │
│  Checkpointing, offset tracking, DLQ, retries, idempotency  │
├─────────────────────────────────────────────────────────────┤
│  LAYER 3: PROCESSING                                        │
│  Schema inference, transforms, validation, enrichment        │
├─────────────────────────────────────────────────────────────┤
│  LAYER 2: CORE TRANSPORT                                    │
│  Arrow record batches on bounded Go channels                 │
├─────────────────────────────────────────────────────────────┤
│  LAYER 1: I/O                                               │
│  Source connectors (readers) + Sink connectors (writers)      │
└─────────────────────────────────────────────────────────────┘
```

### Layer 1 — I/O (Sources & Sinks)

**What it does:** Reads raw bytes from external systems and writes finished Parquet/Iceberg output to storage.

**The contract:** Every source must satisfy one interface. Every sink must satisfy one interface. That's the entire plugin API.

```
Source interface:
  - Open(config) → error
  - Read(context) → (RawMessage, Ack, error)
  - Close() → error

Sink interface:
  - Open(config) → error
  - Write(context, arrow.RecordBatch) → error
  - Flush() → error
  - Close() → error
```

`RawMessage` is just `[]byte` + metadata (source name, timestamp, partition key, headers). The source knows nothing about Arrow, Parquet, or Iceberg. The sink knows nothing about CSV, JSON, or Kafka.

**Why this matters:** Adding a new source (say, NATS JetStream) requires implementing three methods. Nothing else changes. This is how Benthos reached 220+ connectors — the interface is tiny enough that anyone can write one.

**Source categories and how they differ:**

| Category | Read Pattern | Ack Pattern | Examples |
|---|---|---|---|
| **Pull-file** | Open file, read sequentially, close | Ack = move/delete file | CSV, JSON, Parquet, Excel from local/S3/GCS/FTP |
| **Pull-query** | Execute query, iterate cursor | Ack = save high-water mark | JDBC poll, MongoDB find, REST API pagination |
| **Push-stream** | Subscribe, receive messages | Ack = commit offset / nack | Kafka, MQTT, AMQP, NATS, Kinesis, Pub/Sub |
| **Push-CDC** | Attach to replication slot/binlog | Ack = advance LSN/position | Postgres WAL, MySQL binlog, Mongo change stream |
| **Push-event** | Listen on HTTP/WebSocket endpoint | Ack = HTTP 200 response | Webhooks, SSE, WebSocket |
| **Watch** | Monitor directory/bucket for new files | Ack = mark file processed | S3 events, inotify, fsnotify |

Each category has its own lifecycle and error semantics, but they all output the same `RawMessage`.

---

### Layer 2 — Core Transport (The Arrow Backbone)

**What it does:** Moves data between layers as Apache Arrow record batches flowing over bounded Go channels.

**Why Arrow:**
- It is the universal in-memory columnar format. Parquet writers, Iceberg libraries, DuckDB, Spark, Pandas — they all speak Arrow natively.
- Zero-copy: the bytes on the wire (Arrow IPC) are identical to the bytes in memory. No serialization/deserialization at internal boundaries.
- The Go implementation (`github.com/apache/arrow-go`) gives you typed builders (`Int32Builder`, `Float64Builder`, `StringBuilder`, etc.) that construct record batches without heap allocation churn.

**The channel design:**

```
Source goroutine                          Sink goroutine
     │                                        ▲
     │  RawMessage                             │  arrow.RecordBatch
     ▼                                        │
 [decoder]                               [writer]
     │                                        ▲
     │  arrow.RecordBatch                      │
     ▼                                        │
  ┌──────────────────────────────────────────┐
  │  BOUNDED CHANNEL (chan arrow.Record)       │
  │  capacity = configurable (e.g. 64)        │
  │                                           │
  │  - Provides natural backpressure          │
  │  - When full, source blocks (slows down)  │
  │  - When empty, sink blocks (waits)        │
  └──────────────────────────────────────────┘
```

**Backpressure is automatic.** A bounded channel in Go is a built-in flow control mechanism. If the writer can't keep up, the channel fills, and the source goroutine blocks on send. No special backpressure protocol needed — Go's runtime scheduler handles it. This is the same pattern described in the [official Go blog on pipelines](https://go.dev/blog/pipelines).

**Batch sizing:** Records are not sent one-at-a-time. The decoder accumulates rows into a batch (e.g., 8,192 or 65,536 rows) before pushing it onto the channel. This amortizes channel overhead and aligns with how Parquet row groups are written.

**Concurrency model:**

| Component | Goroutines | Why |
|---|---|---|
| Each source | 1 (or N for partitioned sources like Kafka) | Reads are usually I/O-bound |
| Decoder | 1 per source | CPU-bound; one goroutine per format decoder avoids contention |
| Processors | Worker pool (bounded, configurable) | Transforms can be parallelized |
| Writer | 1 per output partition | Parquet writers are not thread-safe; one goroutine per partition file |

**Memory management:**
- `sync.Pool` for recycling `[]byte` buffers and Arrow builders between batches. This is idiomatic Go and avoids GC pressure on the hot path.
- Arrow record batches use reference counting (`Retain`/`Release`). When a batch flows from decoder → processor → writer, each stage retains it on entry and releases on exit. When the refcount hits zero, the memory returns to the pool.
- For extreme throughput, third-party arena allocators (`fereidani/arena`, `limpo1989/arena`) can pre-allocate memory chunks, giving 2-4x allocation performance gains. But `sync.Pool` is the safe starting point.

---

### Layer 3 — Processing (Schema, Transforms, Validation)

**What it does:** Sits between decode and write. Inspects, shapes, validates, and enriches Arrow record batches.

This layer has three sub-stages that run in sequence on each batch:

#### 3a. Schema Inference & Evolution

The library must handle data that arrives with no schema (CSV, JSON lines) and data that arrives with a schema (Avro, Protobuf, Parquet).

**How schema inference works:**

| Source Format | Inference Strategy |
|---|---|
| CSV | Read first N rows (configurable, e.g. 1000). Infer types by attempting parse: int → float → bool → timestamp → string fallback. All ambiguous columns default to string. |
| JSON / NDJSON | Sample first N records. Union the keys. For each key, find the widest type across all samples. Nested objects become Arrow structs; arrays become Arrow lists. |
| Avro | Schema is embedded in the file header. Direct mapping to Arrow schema. |
| Protobuf | Schema comes from `.proto` file descriptor or Schema Registry. Direct mapping. |
| Parquet | Schema is embedded in the file footer. Read it directly. |
| Database/CDC | Schema comes from `information_schema` or the CDC event envelope. |

**Schema evolution** (when the shape of incoming data changes mid-stream):

This is where Iceberg shines. The library should detect schema drift by comparing each incoming batch's schema against the current Iceberg table schema. Three policies, configurable per pipeline:

| Policy | Behavior |
|---|---|
| **strict** | Reject the batch. Send to DLQ. Alert. |
| **permissive** | Add new columns to the Iceberg table (schema evolution). Missing columns become null. Log a warning. |
| **rescue** | Write non-conforming fields to a `_rescued_data` column (Databricks Auto Loader pattern). Nothing is lost, nothing breaks. |

This mirrors how Databricks Auto Loader and Snowflake handle it in production.

#### 3b. Transforms

Transforms are optional, composable functions that operate on Arrow record batches. Each transform takes a `RecordBatch` and returns a `RecordBatch` (or drops it).

Examples of built-in transforms:

| Transform | What It Does |
|---|---|
| **filter** | Drop rows matching a predicate |
| **project** | Select/rename/reorder columns |
| **cast** | Convert column types (e.g. string → timestamp) |
| **flatten** | Unnest JSON struct columns into top-level columns |
| **hash** | Add a hash column for deduplication |
| **timestamp** | Parse/normalize timestamp columns to a canonical timezone |
| **mask** | Redact PII columns (hash, truncate, or null out) |
| **enrich** | Join with a lookup table (loaded into memory) |
| **deduplicate** | Remove duplicate rows within a batch or across a window |

The key insight from Benthos: transforms are an ordered list (a pipeline), not a DAG. DAGs are powerful but hard to configure and debug. An ordered list covers 95% of use cases and is trivial to reason about.

#### 3c. Validation & Data Quality

Before a batch reaches the writer, it passes through validation rules:

- **Not-null checks** on required columns
- **Type conformance** (does the data match the declared Arrow schema?)
- **Value constraints** (ranges, regex patterns, allowed enum values)
- **Row-level tagging**: each row gets a pass/fail flag. Failed rows can be routed to the DLQ instead of being dropped silently.

---

### Layer 4 — Reliability (The Hard Part)

This is what separates a toy from a production system. Three mechanisms:

#### 4a. Checkpointing & Offset Tracking

Every source has a **position** — the Kafka offset, the WAL LSN, the S3 object key, the CSV line number. The library must periodically save this position to durable storage so that after a crash, it resumes from where it left off instead of reprocessing everything.

**How it works:**

```
1. Source reads messages, tracks position internally
2. Messages flow through processing → writer
3. Writer flushes a Parquet file and commits to Iceberg
4. ONLY THEN: checkpoint the source position

This ordering guarantees at-least-once delivery:
- If crash before checkpoint → messages are reprocessed (duplicates possible)
- If crash after checkpoint → no data loss
- Combined with Iceberg's ACID commits → exactly-once output is achievable
```

This is the same barrier-snapshotting concept used by Apache Flink, simplified for a single-node library. The checkpoint is stored in a pluggable `CheckpointStore` (local file, S3, etcd, Postgres — whatever).

The critical rule: **never checkpoint before the sink confirms the write.** This is how Benthos achieves its delivery guarantees without disk-persisted state — by tying acknowledgments to the completion of output writes.

#### 4b. Dead Letter Queue (DLQ)

When a record cannot be processed (bad schema, validation failure, transform error), it must not be silently dropped and it must not block the pipeline.

**DLQ design:**

```
                    ┌──────────────┐
                    │  GOOD BATCH  │──→ Parquet/Iceberg writer
                    └──────────────┘
arrow.RecordBatch ──┤
                    ┌──────────────┐
                    │  BAD ROWS    │──→ DLQ sink (separate Parquet file,
                    └──────────────┘    Kafka topic, or S3 prefix)
```

Each failed row is enriched with metadata: error message, pipeline stage where it failed, timestamp, original raw bytes. This lets operators inspect and replay.

**Two error types require different handling:**

| Error Type | Example | Strategy |
|---|---|---|
| **Transient** | Network timeout, temporary S3 503 | Retry with exponential backoff (max N attempts) |
| **Permanent (poison pill)** | Malformed JSON, schema mismatch, corrupt bytes | Send to DLQ immediately, do not retry |

#### 4c. Idempotency

For exactly-once output semantics, the writer must be idempotent. Iceberg helps here: each write is an atomic commit with a unique snapshot ID. If the same batch is written twice (due to retry after a crash), the second commit can be detected and skipped by checking the snapshot log.

---

### Layer 5 — Orchestration (Lifecycle & Operations)

This is the layer that makes the library usable as a standalone binary or embedded in another application.

#### Configuration

Following Benthos's model, a single YAML/JSON config file defines the entire pipeline:

```yaml
source:
  type: kafka
  kafka:
    brokers: ["localhost:9092"]
    topic: events
    consumer_group: logpro
    format: avro
    schema_registry: "http://localhost:8081"

processing:
  schema_policy: permissive
  transforms:
    - type: flatten
    - type: cast
      columns:
        created_at: timestamp
    - type: filter
      where: "status != 'deleted'"

sink:
  type: iceberg
  iceberg:
    catalog: rest
    catalog_uri: "http://localhost:8181"
    namespace: analytics
    table: events
    partition_by: ["year(created_at)", "month(created_at)"]
    file_size_mb: 512
    compression: zstd

reliability:
  checkpoint_store: local
  checkpoint_dir: /var/lib/logpro/checkpoints
  dlq:
    type: file
    path: /var/lib/logpro/dlq/
  retry:
    max_attempts: 3
    backoff: exponential
    initial_delay: 1s
    max_delay: 30s

observability:
  metrics: prometheus
  metrics_port: 9090
  tracing: otlp
  otlp_endpoint: "http://localhost:4317"
  log_level: info
```

#### Lifecycle

```
  ┌──────┐    ┌──────┐    ┌─────────┐    ┌──────────┐    ┌────────┐
  │ INIT │───▶│ OPEN │───▶│ RUNNING │───▶│ DRAINING │───▶│ CLOSED │
  └──────┘    └──────┘    └─────────┘    └──────────┘    └────────┘
                                │               ▲
                                │  signal/      │
                                │  error        │
                                ▼               │
                          ┌──────────┐          │
                          │ STOPPING │──────────┘
                          └──────────┘
```

- **INIT**: Parse config, validate, build component graph.
- **OPEN**: Open source connections, open sink connections, restore checkpoints.
- **RUNNING**: Main loop — read, decode, process, write, checkpoint.
- **STOPPING**: Received shutdown signal. Stop reading new data.
- **DRAINING**: Flush in-flight batches. Write final Parquet files. Commit final Iceberg snapshot. Save final checkpoint.
- **CLOSED**: All resources released. Exit.

Graceful shutdown is non-negotiable. A `context.Context` with cancellation propagates through every layer. The DRAINING phase ensures no data is lost in buffers.

#### Health & Observability

| Signal | How |
|---|---|
| **Metrics** | Prometheus counters/histograms: records_read, records_written, records_failed, batch_latency, checkpoint_lag, dlq_size |
| **Traces** | OpenTelemetry spans per batch: source → decode → transform → write → checkpoint |
| **Logs** | Structured JSON logs (slog in Go 1.21+) |
| **Health endpoint** | HTTP `/healthz` — returns 200 if pipeline is RUNNING, 503 otherwise |

---

## 3. The Parquet/Iceberg Writer — Deep Dive

The writer is the most performance-critical component. It receives Arrow record batches and must produce optimally-sized Parquet files committed to an Iceberg table.

### Write Path

```
RecordBatch arrives
       │
       ▼
┌──────────────┐
│ PARTITIONER  │  Route batch rows to partition buckets
│              │  (e.g., by year/month of a timestamp column)
└──────┬───────┘
       │  (one sub-batch per partition)
       ▼
┌──────────────┐
│ ACCUMULATOR  │  Buffer batches until target file size reached
│              │  (e.g., 512 MB of uncompressed Arrow data)
└──────┬───────┘
       │  (when threshold hit)
       ▼
┌──────────────┐
│ PARQUET      │  Write accumulated batches as one Parquet file
│ FILE WRITER  │  - Row group size: 128 MB
│              │  - Compression: zstd (configurable)
│              │  - Sort by frequently-filtered columns
│              │  - Compute column statistics (min/max/null count)
└──────┬───────┘
       │  (file written to storage)
       ▼
┌──────────────┐
│ ICEBERG      │  Atomic commit:
│ COMMITTER    │  - Add new data file to table metadata
│              │  - Update partition stats
│              │  - Create new snapshot
│              │  - Update catalog
└──────────────┘
```

### Parquet Tuning Parameters

| Parameter | Default | Notes |
|---|---|---|
| Target file size | 512 MB | Range: 128 MB – 1 GB. Larger = fewer files, better scan performance. |
| Row group size | 128 MB | One row group = one unit of parallel read. |
| Page size | 1 MB | Within a row group, data is split into pages. |
| Compression | Zstd level 3 | Best ratio for analytics. Use Snappy for lower latency. |
| Dictionary encoding | On (for low-cardinality strings) | Huge wins for columns like `status`, `country`, `event_type`. |
| Sorting | By partition key + most-filtered column | Improves min/max statistics → better predicate pushdown. |

### Compaction Strategy

Streaming pipelines produce many small files (one per flush interval). These must be compacted:

| Strategy | When | How |
|---|---|---|
| **Inline** | At write time if buffer is large enough | Accumulate enough data before writing. Simplest approach. |
| **Background** | Periodic (e.g., every hour) | Merge small files within a partition into one large file. Iceberg's `rewriteDataFiles` operation. |
| **Two-pass** | For IoT / high-frequency streams | Write micro-files (10 MB) immediately for low latency. Background job merges them into 512 MB files. |

---

## 4. The Plugin System

The library's extensibility depends on making plugins trivial to write.

### Interface-Based Extension (Go Idiom)

Go does not need a plugin framework. Its interface system *is* the plugin framework. This is the pattern [DoltHub describes](https://www.dolthub.com/blog/2022-09-12-golang-interface-extension/) — small interfaces, implemented implicitly, extended by optional additional interfaces.

```
Core interface (required):
  Source → Open, Read, Close
  Sink   → Open, Write, Flush, Close

Optional capability interfaces (implement to unlock features):
  Partitioned   → Partitions() []string          // Kafka, Kinesis
  Seekable      → Seek(position) error           // replay from offset
  Transactional → Begin(), Commit(), Rollback()   // exactly-once sinks
  Schematized   → Schema() arrow.Schema           // Avro, Protobuf
  Batchable     → ReadBatch() ([]RawMessage, error) // bulk read
```

A source can start by implementing just `Open/Read/Close`. Later, it can add `Seekable` for checkpoint support, `Partitioned` for parallel reads, etc. The core engine checks with type assertions:

```
if s, ok := source.(Seekable); ok {
    s.Seek(lastCheckpoint)
}
```

This means existing plugins never break when new optional interfaces are added. This is the "interface extension" pattern that makes Go plugin architectures scale.

### Registration

Plugins register themselves via `init()` functions (Go convention) or an explicit `Register()` call:

```
func init() {
    sources.Register("kafka", NewKafkaSource)
    sources.Register("s3", NewS3Source)
    sinks.Register("iceberg", NewIcebergSink)
    transforms.Register("flatten", NewFlattenTransform)
}
```

The config YAML uses `type: kafka` to look up the registered factory. No reflection, no code generation, no magic.

---

## 5. Go-Specific Design Decisions

### Why Go For This

| Concern | Go's Answer |
|---|---|
| Concurrency | Goroutines + channels = natural pipeline stages with backpressure |
| Memory | Value types, `sync.Pool`, manual buffer management where needed |
| Deployment | Single static binary, cross-compiles to Linux/Mac/Windows |
| Ecosystem | `apache/arrow-go`, `apache/iceberg-go`, `confluent-kafka-go`, `parquet-go` all exist |
| Simplicity | No inheritance hierarchy, no annotation magic, no hidden control flow |

### What Go Lacks (And How To Compensate)

| Gap | Mitigation |
|---|---|
| No generics until 1.18 (now available) | Use generics for typed builders and typed channel wrappers |
| No sum types / tagged unions | Use interface + type switch for `RawMessage` variants |
| GC pauses under memory pressure | `sync.Pool` for hot-path allocations; Arrow's own memory management (refcounted buffers) |
| No built-in expression language | Embed a lightweight evaluator for filter/transform expressions (e.g., CEL or a custom DSL) |
| No runtime plugin loading (`.so`) is fragile | Prefer compile-time plugin registration (like Benthos does). Runtime plugins via gRPC sidecar if needed. |

### Project Layout (Standard Go)

```
logpro/
├── cmd/
│   └── logpro/          # CLI entrypoint
├── pkg/
│   ├── arrow/           # Arrow helpers, schema mapping, batch utilities
│   ├── config/          # YAML/JSON config parsing and validation
│   ├── engine/          # Pipeline orchestrator, lifecycle, channel wiring
│   ├── checkpoint/      # Checkpoint store interface + implementations
│   ├── dlq/             # Dead letter queue interface + implementations
│   └── observability/   # Metrics, tracing, logging setup
├── internal/
│   ├── decode/          # Format decoders (CSV, JSON, Avro, Protobuf, etc.)
│   ├── transform/       # Built-in transforms
│   └── writer/          # Parquet file writer, Iceberg committer
├── plugin/
│   ├── source/          # Source connector implementations
│   │   ├── kafka/
│   │   ├── s3/
│   │   ├── postgres/
│   │   ├── mqtt/
│   │   └── file/
│   ├── sink/            # Sink implementations
│   │   ├── iceberg/
│   │   ├── parquet/     # Raw Parquet (no Iceberg)
│   │   └── stdout/      # Debug sink
│   └── format/          # Format-specific decoders
│       ├── csv/
│       ├── json/
│       ├── avro/
│       └── protobuf/
└── research/            # This document
```

`internal/` = private to the module (Go compiler enforces this).
`pkg/` = stable public API that other Go programs can import.
`plugin/` = all pluggable components, each in its own package.

---

## 6. Data Flow: End-to-End Example

A Kafka topic has JSON events. We want them in an Iceberg table, partitioned by date.

```
Step 1: Kafka source reads a batch of 1000 messages ([]byte + offset metadata)
          │
Step 2: JSON decoder parses each []byte into Arrow record batch
        - Schema inferred from first batch (or loaded from Schema Registry)
        - 1000 rows → 1 RecordBatch
          │
Step 3: RecordBatch pushed onto bounded channel (capacity 64)
        - If channel full, Kafka source blocks → backpressure to broker
          │
Step 4: Transform worker picks up batch from channel
        - flatten: expand nested "address" object into top-level columns
        - cast: parse "created_at" string → Arrow timestamp
        - filter: drop rows where "status" == "test"
        - Result: RecordBatch with fewer rows and clean schema
          │
Step 5: Validation
        - Check non-null constraints on "user_id" and "event_type"
        - Rows that fail → DLQ batch (separate channel → DLQ sink)
        - Rows that pass → continue
          │
Step 6: Partitioner splits batch by year(created_at) + month(created_at)
        - e.g., 800 rows → partition 2026-01, 200 rows → partition 2026-02
          │
Step 7: Accumulator for partition 2026-01 adds 800 rows to its buffer
        - Buffer now at 480 MB → below 512 MB threshold → keep buffering
          │
Step 8: (later) Buffer for 2026-01 hits 512 MB
        - Parquet writer flushes: one .parquet file with 4 row groups of 128 MB
        - File uploaded to s3://warehouse/analytics/events/data/2026-01/00001.parquet
          │
Step 9: Iceberg committer creates atomic snapshot
        - New data file added to table metadata
        - Manifest file updated
        - Catalog updated via REST API
          │
Step 10: Checkpoint saved: Kafka offset = 58,392 for partition 0
         - Stored in checkpoint store (e.g., local file or S3)
         - If crash now, restart resumes from offset 58,392
```

---

## 7. What Makes This Production-Grade

A summary of the non-functional requirements and how the design addresses each:

| Requirement | How |
|---|---|
| **No data loss** | At-least-once delivery via checkpoint-after-commit. DLQ captures failures. |
| **Exactly-once output** | Iceberg ACID commits + idempotent writes (dedup by snapshot). |
| **Backpressure** | Bounded Go channels. Sources slow down when sinks can't keep up. |
| **Schema flexibility** | Auto-inference for schemaless formats. Three evolution policies (strict/permissive/rescue). |
| **Performance** | Arrow zero-copy batches. `sync.Pool` buffer recycling. Sorted Parquet with dictionary encoding. |
| **Observability** | Prometheus metrics, OTLP traces, structured logs, health endpoint. |
| **Graceful shutdown** | Context cancellation → drain in-flight → flush → commit → checkpoint → exit. |
| **Extensibility** | Interface-based plugins. Optional capability interfaces. No framework lock-in. |
| **Operability** | Single binary. YAML config. Hot-reload (future). DLQ inspection tools. |
| **Testability** | Every layer behind an interface → mock sources, mock sinks, test transforms in isolation. |

---

## Sources

- [Benthos / Redpanda Connect — GitHub](https://github.com/redpanda-data/connect)
- [Understanding Redpanda Connect / Benthos — Platformatory](https://platformatory.io/blog/Understanding-RedpandaConnect/)
- [Benthos: Reliable Guardian of Ordinary Tasks — Gang Tao](https://taogang.medium.com/the-past-and-present-of-stream-processing-part-16-benthos-the-reliable-guardian-of-ordinary-5a8cdaefad0f)
- [Vector — GitHub](https://github.com/vectordotdev/vector)
- [Vector Architecture — DeepWiki](https://deepwiki.com/vectordotdev/vector)
- [Data Pipeline Architecture: 5 Design Patterns — Dagster](https://dagster.io/guides/data-pipeline-architecture-5-design-patterns-with-examples)
- [Data Pipeline Architecture: 9 Patterns — Alation](https://www.alation.com/blog/data-pipeline-architecture-patterns/)
- [Designing a Universal Data Ingestion Layer — Manik Hossain](https://medium.com/@manik.ruet08/designing-a-universal-data-ingestion-layer-from-apis-to-streaming-in-one-framework-826b426b49bb)
- [Data Pipeline Architecture — Redpanda](https://www.redpanda.com/guides/fundamentals-of-data-engineering-data-pipeline-architecture)
- [Go Plugin Architecture via Interface Extension — DoltHub](https://www.dolthub.com/blog/2022-09-12-golang-interface-extension/)
- [Plugins in Go — Eli Bendersky](https://eli.thegreenplace.net/2021/plugins-in-go/)
- [Go Project Structure: Practices & Patterns — Rost Glukhov](https://www.glukhov.org/post/2025/12/go-project-structure/)
- [5 API Design Patterns in Go (2025)](https://cristiancurteanu.com/5-api-design-patterns-in-go-that-solve-your-biggest-problems-2025/)
- [Data Transfer with Apache Arrow and Golang — Voltron Data](https://voltrondata.com/blog/data-transfer-with-apache-arrow-and-golang)
- [Redefining Data Engineering with Go and Apache Arrow — Thomas McGeehan](https://medium.com/@tfmv/redefining-data-engineering-with-go-and-apache-arrow-df9059ddf55c)
- [Go Concurrency Patterns: Pipelines — Go Blog](https://go.dev/blog/pipelines)
- [Pipeline Pattern for Concurrency in Go (2025)](https://compositecode.blog/2025/06/22/go-concurrency-patternspipeline-pattern/)
- [Backpressure Explained — Jay Phelps](https://medium.com/@jayphelps/backpressure-explained-the-flow-of-data-through-software-2350b3e77ce7)
- [Checkpointing — Apache Flink](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/checkpointing/)
- [Checkpointing — Dagster Glossary](https://dagster.io/glossary/checkpointing)
- [Dead Letter Queue — AWS](https://aws.amazon.com/what-is/dead-letter-queue/)
- [Kafka Dead Letter Queue — Confluent](https://www.confluent.io/learn/kafka-dead-letter-queue/)
- [DLQ Best Practices — Superstream](https://www.superstream.ai/blog/kafka-dead-letter-queue)
- [Schema Inference & Evolution — Databricks Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)
- [Schema Detection & Evolution — Snowflake](https://www.phdata.io/blog/schema-detection-and-evolution-in-snowflake/)
- [Parquet Performance Tuning — Alex Merced / Dremio](https://medium.com/data-engineering-with-dremio/all-about-parquet-part-10-performance-tuning-and-best-practices-with-parquet-d697ba4e8a57)
- [7 Parquet Partition Designs That Work — Modexa](https://medium.com/@Modexa/7-parquet-partition-designs-that-actually-work-69a2a0811ea8)
- [Iceberg Writer Optimizations — Firebolt](https://www.firebolt.io/blog/unlocking-faster-iceberg-queries-the-writer-optimizations-you-are-missing)
- [Go Memory Arenas — mcyoung](https://mcyoung.xyz/2025/04/21/go-arenas/)
- [Go Arena Allocator — fereidani/arena](https://github.com/fereidani/arena)
- [Go Arena Allocator — limpo1989/arena](https://github.com/limpo1989/arena)
