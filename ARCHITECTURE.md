# LogFlow Architecture

## Module Map

```
logflow/
├── cmd/                    # CLI entry points
│   └── logflow/           # Main CLI application
│
├── internal/              # Private implementation details
│   └── model/             # Core event model (Event, CaseID, Activity)
│
└── pkg/                   # Public, reusable packages
    │
    ├── ─────────────────── CORE LAYER ───────────────────
    │
    ├── pipeline/          # Event processing pipeline
    │   ├── orchestrator_v2.go   # Production pipeline coordinator
    │   ├── enterprise.go        # Enterprise wrapper (telemetry, resilience)
    │   ├── dlq.go               # Dead letter queue for failed records
    │   └── interfaces.go        # Source, Sink, Processor interfaces
    │
    ├── sources/           # Data input adapters
    │   ├── csv.go              # CSV file reader
    │   ├── json.go             # JSON/JSONL reader
    │   └── xes.go              # XES (process mining standard) reader
    │
    ├── sinks/             # Data output adapters
    │   └── parquet.go          # Parquet file writer
    │
    ├── ─────────────────── STORAGE LAYER ───────────────────
    │
    ├── storage/           # Storage abstraction
    │   ├── cloud.go            # Unified storage interface
    │   └── s3/                 # AWS S3 implementation
    │       ├── s3.go           # Full S3 client
    │       └── select.go       # S3 Select pushdown queries
    │
    ├── checkpoint/        # Resume capability
    │   ├── checkpoint.go       # Local checkpoint manager
    │   ├── backends.go         # Backend interface
    │   ├── s3.go               # S3 checkpoint backend
    │   └── redis.go            # Redis checkpoint backend
    │
    ├── ─────────────────── PROCESS MINING LAYER ───────────────────
    │
    ├── pmpt/              # Process Merkle Patricia Tree (Case-Centric)
    │   ├── tree.go             # Core tree structure (O(1) comparison)
    │   ├── interval.go         # Interval tree for time queries
    │   ├── builder.go          # Incremental tree construction
    │   └── hybrid.go           # Combined sequence + time queries
    │
    ├── ocel/              # Object-Centric Event Logs (OCEL 2.0)
    │   ├── model.go            # OCEL 2.0 data types
    │   ├── store.go            # DuckDB relational storage
    │   ├── import.go           # Import from CSV/JSON/XML
    │   ├── export.go           # Export to OCEL standard formats
    │   └── discovery.go        # OC-DFG discovery algorithm
    │
    ├── ─────────────────── QUALITY LAYER ───────────────────
    │
    ├── validation/        # Data validation
    │   └── quality/            # Quality rules engine
    │       └── rules.go        # Configurable validation rules
    │
    ├── parser/            # Parsing utilities
    │   └── healing/            # Self-healing parser
    │       ├── rules.go        # Fix rules (encoding, quoting, etc.)
    │       ├── detector.go     # Error pattern detection
    │       └── fixer.go        # Auto-repair wrapper
    │
    ├── ─────────────────── OPERATIONS LAYER ───────────────────
    │
    ├── telemetry/         # Observability
    │   ├── telemetry.go        # Tracer, Metrics, Spans
    │   └── otel.go             # OpenTelemetry OTLP export
    │
    ├── resilience/        # Fault tolerance
    │   └── resilience.go       # Circuit breaker, poison pill handler
    │
    ├── lifecycle/         # Process lifecycle
    │   └── shutdown.go         # Graceful shutdown manager
    │
    └── errors/            # Error handling
        └── errors.go           # Typed errors with codes
```

---

## Layer Dependencies

```
┌─────────────────────────────────────────────────────────────┐
│                        CLI (cmd/)                           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    CORE LAYER                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  Pipeline   │──│   Sources   │──│    Sinks    │          │
│  │ Orchestrator│  │  (CSV,JSON) │  │  (Parquet)  │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────┘
         │                   │                    │
         ▼                   ▼                    ▼
┌─────────────────────────────────────────────────────────────┐
│                   STORAGE LAYER                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │   Storage   │  │ Checkpoint  │  │   DuckDB    │          │
│  │ (Local, S3) │  │(Local,S3,Redis)│ │  (OCEL)    │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────┘
         │                   │                    │
         ▼                   ▼                    ▼
┌─────────────────────────────────────────────────────────────┐
│                PROCESS MINING LAYER                         │
│  ┌─────────────┐  ┌─────────────┐                           │
│  │    PMPT     │  │    OCEL     │                           │
│  │(Case-Centric)│ │(Object-Centric)│                        │
│  └─────────────┘  └─────────────┘                           │
└─────────────────────────────────────────────────────────────┘
         │                   │
         ▼                   ▼
┌─────────────────────────────────────────────────────────────┐
│                  QUALITY LAYER                              │
│  ┌─────────────┐  ┌─────────────┐                           │
│  │ Validation  │  │   Healing   │                           │
│  │   Rules     │  │   Parser    │                           │
│  └─────────────┘  └─────────────┘                           │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│                 OPERATIONS LAYER                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  Telemetry  │  │ Resilience  │  │  Lifecycle  │          │
│  │   (OTLP)    │  │(CircuitBrkr)│  │ (Shutdown)  │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

---

## Module Contracts (Interfaces)

### Pipeline (`pkg/pipeline/`)

```go
// Source reads events from an input
type Source interface {
    Name() string
    Read(ctx context.Context, r io.Reader, out chan<- *Event) error
}

// Sink writes events to an output
type Sink interface {
    Name() string
    Write(ctx context.Context, in <-chan *Event) error
    Close() error
}

// Processor transforms events in a pipeline stage
type Processor interface {
    Name() string
    Process(ctx context.Context, in <-chan *Event, out chan<- *Event) error
}

// Inspector observes events without modifying (read-only)
type Inspector interface {
    Name() string
    Inspect(event *Event)
    Report() interface{}
}
```

### Storage (`pkg/storage/`)

```go
type Storage interface {
    Reader(ctx context.Context, path string) (io.ReadCloser, int64, error)
    Writer(ctx context.Context, path string) (io.WriteCloser, error)
    Stat(ctx context.Context, path string) (*FileInfo, error)
    Scheme() string  // "file", "s3", "gs", "az"
}
```

### Checkpoint (`pkg/checkpoint/`)

```go
type Backend interface {
    Save(ctx context.Context, cp *Checkpoint) error
    Load(ctx context.Context, id string) (*Checkpoint, error)
    Delete(ctx context.Context, id string) error
    List(ctx context.Context, prefix string) ([]*Checkpoint, error)
    Name() string  // "local", "s3", "redis"
}
```

---

## OCEL 2.0 Module (Object-Centric Process Mining)

### Why DuckDB Instead of Parquet Nested Types?

OCEL 2.0 requires:
1. **E2O relations with qualifiers** - ternary relation (event, qualifier, object)
2. **O2O relations** - object-to-object links
3. **Versioned object attributes** - attributes change over time

Parquet nested LIST can store `Event → [ObjectID]` but **cannot express**:
- Qualifiers on relationships ("primary", "resource")
- Object-to-object relations
- Temporal attribute versioning

**DuckDB** provides:
- In-process SQL engine (already a dependency)
- Native Parquet export for analytics
- Relational model matching OCEL 2.0 spec exactly

### OCEL 2.0 Mathematical Model

```
L = (E, O, EA, OA, evtype, time, objtype, eatype, oatype, eaval, oaval, E2O, O2O)

Where:
- E ⊆ 𝕌_ev           : Set of events
- O ⊆ 𝕌_obj          : Set of objects
- E2O ⊆ E × 𝕌_qual × O : Event-to-Object relations (qualified)
- O2O ⊆ O × 𝕌_qual × O : Object-to-Object relations (qualified)
```

### DuckDB Schema (OCEL 2.0 Compliant)

```sql
-- Events
CREATE TABLE event (
    event_id    VARCHAR PRIMARY KEY,
    event_type  VARCHAR NOT NULL,
    timestamp   TIMESTAMP NOT NULL
);

-- Objects
CREATE TABLE object (
    object_id   VARCHAR PRIMARY KEY,
    object_type VARCHAR NOT NULL
);

-- Event-to-Object (E2O) with qualifier
CREATE TABLE event_object (
    event_id    VARCHAR,
    object_id   VARCHAR,
    qualifier   VARCHAR,
    PRIMARY KEY (event_id, object_id, qualifier)
);

-- Object-to-Object (O2O) with qualifier
CREATE TABLE object_object (
    source_id   VARCHAR,
    target_id   VARCHAR,
    qualifier   VARCHAR,
    PRIMARY KEY (source_id, target_id, qualifier)
);

-- Object attributes (versioned - attributes change over time)
CREATE TABLE object_attribute (
    object_id   VARCHAR,
    attr_name   VARCHAR,
    attr_value  VARCHAR,
    attr_type   VARCHAR,
    timestamp   TIMESTAMP,
    PRIMARY KEY (object_id, attr_name, timestamp)
);
```

---

## Usage Examples

### 1. Basic Pipeline (Case-Centric)

```go
pipeline := pipeline.NewOrchestratorV2(cfg).
    SetSource(sources.NewCSVSource()).
    AddProcessor(transforms.NewFilterProcessor(filter)).
    SetSink(sinks.NewParquetSink(parquetCfg))

err := pipeline.Run(ctx)
```

### 2. Enterprise Pipeline (Production)

```go
enterprise, _ := pipeline.NewEnterpriseOrchestrator(pipeline.EnterpriseConfig{
    ServiceName:    "logflow",
    OTLPEndpoint:   "localhost:4317",
    CheckpointDir:  "/tmp/checkpoints",
    DLQPath:        "/tmp/dlq",
})

enterprise.SetSource(sources.NewCSVSource()).SetSink(sinks.NewParquetSink(cfg))
enterprise.HandleSignals(ctx)
err := enterprise.Run(ctx)
```

### 3. OCEL 2.0 (Object-Centric)

```go
// Create OCEL store
store, _ := ocel.NewStore("process.ocel.db")

// Import with object mapping
importer := ocel.NewImporter(store)
importer.ImportCSV(ctx, reader, ocel.CSVMapping{
    EventID:   "event_id",
    Activity:  "activity",
    Timestamp: "timestamp",
    Objects: map[string]string{
        "order_id":    "Order",
        "customer_id": "Customer",
    },
})

// Discover Object-Centric DFG
dfg := store.DiscoverOCDFG()

// Project to traditional case-centric view
orderLog := store.ProjectByObjectType("Order")
```

---

## Adding New Modules

| To Add | Implement | Location |
|--------|-----------|----------|
| New data source | `Source` interface | `pkg/sources/` |
| New output format | `Sink` interface | `pkg/sinks/` |
| New transformation | `Processor` interface | `pkg/pipeline/` |
| New storage backend | `Storage` interface | `pkg/storage/` |
| New checkpoint backend | `Backend` interface | `pkg/checkpoint/` |

---

## Universal Event Store API (OCEL + N-Column Ingest)

### Schema Injection (`pkg/ingest/schema/`)

The schema package extends Arrow schema inference with OCEL 2.0 nested column support.

```go
import "github.com/logflow/logflow/pkg/ingest/schema"

// Get the OCEL nested column type: LIST<STRUCT<object_id: STRING, object_type: STRING>>
ocelType := schema.OCELObjectType()

// Inject the ocel_objects column into any inferred schema
enrichedSchema := schema.InjectOCELColumn(existingSchema)

// Detect which columns are likely object references (e.g., "order_id" → type "order")
hints := schema.IdentifyObjectColumns(existingSchema)

// Build a new RecordBatch with ocel_objects populated from identified columns
enrichedBatch, err := schema.EnrichBatchWithOCEL(alloc, batch, hints)
```

**Key types:**

| Type | Purpose |
|------|---------|
| `ObjectColumnHint` | Describes a column identified as an OCEL object reference (field index, object type, role) |
| `ObjectRole` | `ObjectRoleID` (column holds object IDs) or `ObjectRoleType` (column holds type labels) |

**Constants:**

- `OCELObjectsColumn = "ocel_objects"` — reserved column name

### Dynamic Parquet Writer (`pkg/ingest/decoders/`, `pkg/ingest/sinks/`)

The CSV decoder's `createBuilders()` handles Arrow nested types:

```go
// These Arrow types are now supported in builder creation:
// arrow.LIST   → array.NewListBuilder(alloc, elemType)
// arrow.STRUCT → array.NewStructBuilder(alloc, structType)
```

The Parquet sink writes OCEL metadata alongside existing `logflow.*` keys:

| Metadata Key | Value | When Written |
|-------------|-------|--------------|
| `ocel:attributes` | Comma-separated list of all column names | Always |
| `ocel:object_types` | Comma-separated object types | When `ocel_objects` column present |
| `ocel:relationships` | Relationship descriptors | When `ocel_objects` column present |

Pass object types via `opts.Metadata["ocel_object_types"]` and relationships via `opts.Metadata["ocel_relationships"]`.

### Attribute Bitmap Indexes (`pkg/index/`)

Roaring bitmap indexes for fast attribute lookups across Arrow RecordBatches.

```go
import "github.com/logflow/logflow/pkg/index"

idx := index.NewAttributeIndex()

// Index Arrow batches (multi-batch files use rowOffset for global positioning)
idx.IndexBatch(batch, 0)
idx.IndexBatch(batch2, uint32(batch.NumRows()))

// Point lookup: which rows have activity == "Submit Order"?
bm := idx.Lookup("activity", "Submit Order")

// Multi-attribute AND: activity == "Submit Order" AND resource == "Alice"
bm := idx.LookupAnd(map[string]string{
    "activity": "Submit Order",
    "resource": "Alice",
})

// Multi-attribute OR
bm := idx.LookupOr(conditions)

// Inspect the index
idx.Columns()                   // []string of indexed column names
idx.Cardinality("activity")     // number of distinct values
idx.DistinctValues("activity")  // all distinct values
idx.RowCount()                  // total rows indexed

// Serialize / deserialize
idx.WriteTo(writer)
idx.ReadFrom(reader)
```

**Design details:**
- Skips `LIST` and `STRUCT` columns (not flat-indexable)
- Dictionary-encoded Arrow columns are resolved through the dictionary automatically
- Thread-safe via `sync.RWMutex` (concurrent reads, exclusive writes)

### DuckDB Parquet Querier (`pkg/ocel/`)

Query OCEL-enriched Parquet files using DuckDB `read_parquet()` and `UNNEST()`.

```go
import "github.com/logflow/logflow/pkg/ocel"

q, err := ocel.NewParquetQuerier()
defer q.Close()

// Query flat columns only
rows, err := q.QueryFlat(ctx, "events.parquet", `activity = 'Submit'`, "case_id", "activity")

// UNNEST ocel_objects to join flat attributes with object references
rows, err := q.QueryWithObjects(ctx, "events.parquet", `obj.object_type = 'Order'`)

// Filter by object type or object ID
rows, err := q.QueryByObjectType(ctx, "events.parquet", "Order")
rows, err := q.QueryByObjectID(ctx, "events.parquet", "ORD-12345")

// Aggregate queries
types, err := q.QueryObjectTypes(ctx, "events.parquet")
counts, err := q.QueryObjectCounts(ctx, "events.parquet") // map[type]count

// Discover an Object-Centric DFG directly from Parquet
dfg, err := q.DiscoverDFGFromParquet(ctx, "events.parquet", "activity", "timestamp")
// dfg.Edges["Order"]["Submit"]["Approve"] = 42
// dfg.ObjectTypes = ["Order", "Customer"]
// dfg.Activities = ["Submit", "Approve", "Ship"]

// Raw SQL with read_parquet() and UNNEST()
rows, err := q.Raw(ctx, `
    SELECT obj.object_type, COUNT(*)
    FROM read_parquet('events.parquet') t,
    UNNEST(t.ocel_objects) AS obj(object_id, object_type)
    GROUP BY obj.object_type
`)
```

### PMPT Object Awareness (`pkg/pmpt/`)

ProcessNode now carries roaring bitmaps for cases and OCEL objects.

```go
import "github.com/logflow/logflow/pkg/pmpt"

// Enable object tracking in the builder
cfg := pmpt.DefaultBuilderConfig()
cfg.IncludeObjects = true
builder := pmpt.NewBuilder(cfg)

// Add events with object references (model.Event.Objects field)
builder.Add(event)

tree := builder.FlushAll()

// Each ProcessNode now has:
node.CaseBitmap                  // *roaring.Bitmap — which cases passed through this node
node.ObjectCounts                // map[string]int64 — distinct objects per type
node.ObjectBitmap                // map[string]*roaring.Bitmap — object IDs per type
```

**New types:**

| Type | Purpose |
|------|---------|
| `OCELNode` | Wraps `ProcessNode` with `ObjectTypes []string` and `ObjectTypeFrequency map[string]float64` |
| `ObjectTrace` | Extends `Trace` with per-step `Objects [][]ObjectRef` |
| `ObjectRef` | `{ObjectID, ObjectType string}` — OCEL object reference on a tree node |

**Event model extension (`internal/model/`):**

```go
type Event struct {
    CaseID     []byte
    Activity   []byte
    Timestamp  int64
    Resource   []byte
    Attributes []Attribute
    Objects    []ObjectRef  // NEW: OCEL object references
}

type ObjectRef struct {
    ObjectID   []byte
    ObjectType []byte
}
```

---

## Research References

- [OCEL 2.0 Specification](https://arxiv.org/html/2403.01975v1)
- [Apache Arrow Parquet Nested Types](https://arrow.apache.org/blog/2022/10/17/arrow-parquet-encoding-part-3/)
- [Go Concurrency Patterns: Pipelines](https://go.dev/blog/pipelines)
- [DuckDB Zero-Copy Arrow Integration](https://duckdb.org/2021/12/03/duck-arrow)
