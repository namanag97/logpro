# Codebase Audit: Static Reachability Analysis

> Audit date: 2026-01-30
> Method: `go list -deps` transitive dependency analysis from `cmd/logflow` binary entry point, plus `grep` import tracing for every package.

---

## How the Two Code Paths Actually Work

The codebase has **two legitimate, non-duplicate paths** serving different use cases:

### Path 1: CLI (local file conversion)
```
cmd/logflow/main.go
  → runConvert()
    → runDuckDBConvert()  ← CSV always takes this path (or --duckdb flag)
        → internal/pipe.NewDuckDBPipeline()
            → DuckDB SQL engine for CSV/JSON → Parquet
    → runArrowConvert()   ← XES, stdin, non-CSV
        → internal/pipe.NewPipeline()
            → pkg/parser → pkg/writer (Arrow-based)
```

### Path 2: Web Server (HTTP upload → conversion)
```
cmd/logflow/serve.go
  → server.NewServer()
    → pkg/core.NewConverter()  ← Also uses DuckDB (sql.Open("duckdb", ""))
        → core.Convert() dispatches by format:
            CSV  → DuckDB SQL
            JSON → DuckDB SQL
            XLSX → DuckDB SQL
            Parquet → DuckDB copy
```

**Finding:** Both paths currently use DuckDB. The server path uses it through `pkg/core/convert.go:62` which calls `sql.Open("duckdb", "")`. The CLI path uses it through `internal/pipe/duckdb_pipe.go`.

---

## Reachability Results

### Tier 1 — LIVE (reachable from the compiled binary)

37 packages are transitively reachable from `cmd/logflow`:

```
cmd/logflow              ← entry point
internal/model           ← Event model
internal/pipe            ← Pipeline (DuckDB + Arrow)
internal/pool            ← Worker pool, helpers
pkg/adapters             ← Adapter layer
pkg/checkpoint           ← Checkpoint backends (local, Redis, S3)
pkg/config               ← Global config
pkg/contract             ← Data contracts
pkg/core                 ← Zero-config converter (DuckDB-based)
pkg/diff                 ← Diff engine
pkg/errors               ← Error types
pkg/export               ← Export utilities
pkg/ingest               ← Ingestion pipeline (FastPath + RobustPath)
pkg/ingest/core          ← Source/Sink/Decoder interfaces (Arrow-native)
pkg/ingest/decoders      ← CSV, JSON, XES, Parquet, DuckDB decoders
pkg/ingest/hooks         ← Lifecycle hooks
pkg/ingest/schema        ← Inference, evolution, OCEL mapping, policy
pkg/ingest/telemetry     ← Metrics, benchmark, optimizer
pkg/inspect              ← Quality inspector
pkg/interfaces           ← Interface definitions
pkg/lifecycle            ← Lifecycle management
pkg/parser               ← Parser (CSV, XES, JSON)
pkg/pipeline             ← Orchestrator V1+V2, DLQ, checkpoint, dedup
pkg/plugins/processmining← Process mining plugin
pkg/plugins/quality      ← Quality plugin
pkg/pmpt                 ← Process mining performance tree
pkg/processors           ← Processor implementations
pkg/profile              ← Profile management
pkg/registry             ← Component registry
pkg/resilience           ← Resilience patterns
pkg/server               ← HTTP server (web UI backend)
pkg/telemetry            ← OTEL exporter + metrics
pkg/transform            ← Transform system
pkg/tui                  ← Terminal UI wizard
pkg/util                 ← Reader utilities
pkg/watch                ← File watching
pkg/writer               ← Parquet writer
```

### Tier 2 — TEST-ONLY (imported only by root test_*.go or pkg/logflow.go)

These packages are **not in the binary** but are used by standalone test harnesses or the library facade:

| Package | Imported By | Lines |
|---|---|---|
| `pkg/api/rest` | test_full_integration.go, test_e2e.go | ~200 |
| `pkg/defaults/alerting` | pkg/logflow.go | ~100 |
| `pkg/defaults/auth` | pkg/logflow.go, test_*.go | ~100 |
| `pkg/defaults/metrics` | pkg/logflow.go | ~100 |
| `pkg/defaults/scheduler` | pkg/logflow.go | ~100 |
| `pkg/defaults/tracer` | pkg/logflow.go | ~100 |
| `pkg/ingest/sinks` | test_precommit.go, test_all_xes.go | ~300 |
| `pkg/query/engine` | test_*.go, pkg/logflow.go | ~200 |
| `pkg/storage/catalog` | test_full_integration.go, pkg/logflow.go | ~150 |
| `pkg/storage/object` | pkg/logflow.go | ~100 |
| `pkg/testing/generators` | test_*.go | ~200 |

**Verdict:** These are legitimate — they're the library API (`pkg/logflow.go`) and integration test infrastructure. Not dead code, but not compiled into the CLI binary either.

### Tier 3 — DEAD CODE (zero importers anywhere in the codebase)

**27 packages, 18,412 lines** imported by nothing — not by the binary, not by tests, not by each other.

| Package | Lines | Files | What It Contains |
|---|---|---|---|
| `pkg/storage` | 2,775 | 7 | Cloud storage abstraction layer |
| `pkg/ocel` | 1,722 | 4 | OCEL 2.0 process mining model + discovery |
| `pkg/parser/healing` | 1,597 | 3 | Auto-fix broken CSV/JSON (detector, rules, fixer) |
| `pkg/storage/s3` | 1,138 | 2 | S3 client with select support |
| `pkg/schema` | 1,010 | 3 | Schema policy + cache (separate from `pkg/ingest/schema`) |
| `pkg/validation` | 840 | 2 | Validation framework |
| `pkg/compaction` | 818 | 2 | Parquet file compactor |
| `pkg/ingest/detect` | 690 | 4 | Format detection (encoding, delimiter, format) |
| `pkg/ingest/sources` | 565 | 5 | File, memory, stream, HTTP, glob sources |
| `pkg/validation/quality` | 535 | 1 | Quality validation rules |
| `pkg/sinks` | 522 | 1 | Standalone Iceberg sink |
| `pkg/ingest/flow` | 520 | 3 | Backpressure, rate limiter, concurrency |
| `pkg/state` | 515 | 1 | State store |
| `pkg/ingest/errors` | 505 | 3 | Stream errors, policy, quarantine |
| `pkg/hooks` | 497 | 2 | Hooks system |
| `pkg/perf` | 490 | 1 | Profiler |
| `pkg/ingest/quality` | 423 | 4 | Cardinality, checksum, validator, entropy |
| `pkg/quality` | 410 | 1 | Quality validator |
| `pkg/storage/maintenance` | 396 | 1 | Compaction maintenance |
| `pkg/testing/roundtrip` | 371 | 1 | Roundtrip test utilities |
| `pkg/turbo` | 361 | 1 | Turbo converter |
| `pkg/index` | 343 | 1 | Indexing |
| `pkg/runtime` | 283 | 1 | Runtime utilities |
| `pkg/heuristic` | 283 | 1 | Heuristic optimizer |
| `pkg/ingest/heuristics` | 278 | 2 | Decision engine |
| `pkg/storage/table` | 268 | 1 | Table management |
| `pkg/query/cache` | 257 | 1 | Query cache |

**Total dead: 18,412 lines across 27 packages.**

Additionally, **6 root-level test files** (4,578 lines) are standalone `main` packages that can't run with `go test`:
```
test_precommit.go       2,189 lines
test_comprehensive.go     844 lines
test_e2e.go               614 lines
test_full_integration.go  388 lines
test_telemetry.go         233 lines
test_all_xes.go           165 lines
test_xes_verify.go         67 lines
speedtest.go               78 lines
```

---

## Categorizing the Dead Code

Not all dead code is worthless. Some was built for future use and should be wired in. Some is genuinely orphaned.

### Category A — Built for the Future (valuable, needs wiring)

These are real implementations of things the pipeline design document says we need. They just aren't connected yet.

| Package | Lines | What to Do |
|---|---|---|
| `pkg/ingest/sources` | 565 | Wire into unified pipeline (file, HTTP, stream, glob sources) |
| `pkg/ingest/flow` | 520 | Wire into pipeline engine (backpressure, rate limiting) |
| `pkg/ingest/detect` | 690 | Wire into pipeline (format auto-detection) |
| `pkg/ingest/quality` | 423 | Wire into pipeline (quality validation step) |
| `pkg/ingest/errors` | 505 | Wire into pipeline (error handling, quarantine) |
| `pkg/ingest/heuristics` | 278 | Wire into pipeline (strategy selection) |
| `pkg/sinks` (Iceberg) | 522 | Wire as output sink |
| `pkg/compaction` | 818 | Wire as post-write maintenance |
| `pkg/storage/s3` | 1,138 | Wire as storage backend |
| `pkg/storage/table` | 268 | Wire as table management |
| `pkg/storage/maintenance` | 396 | Wire for compaction |
| `pkg/schema` | 1,010 | Merge with `pkg/ingest/schema` or delete |
| `pkg/state` | 515 | Wire as pipeline state tracking |
| **Subtotal** | **7,648** | |

### Category B — Process Mining Specific (keep as plugin, not wired yet)

| Package | Lines | What to Do |
|---|---|---|
| `pkg/ocel` | 1,722 | Keep — move to `plugin/processmining/` |
| `pkg/parser/healing` | 1,597 | Keep — useful for dirty CSV recovery |
| **Subtotal** | **3,319** | |

### Category C — Duplicate or Superseded (safe to delete)

| Package | Lines | Why Dead |
|---|---|---|
| `pkg/quality` | 410 | Duplicate of `pkg/ingest/quality` |
| `pkg/validation` | 840 | No importers, overlaps with quality packages |
| `pkg/validation/quality` | 535 | No importers, overlaps with quality packages |
| `pkg/heuristic` | 283 | Duplicate of `pkg/ingest/heuristics` |
| `pkg/hooks` | 497 | Duplicate of `pkg/ingest/hooks` |
| `pkg/turbo` | 361 | Superseded by `pkg/core` converter |
| `pkg/runtime` | 283 | Generic runtime utils, unused |
| `pkg/index` | 343 | Indexing — never connected |
| `pkg/perf` | 490 | Profiler — never connected |
| `pkg/query/cache` | 257 | Query cache — never connected |
| `pkg/testing/roundtrip` | 371 | Test util — no test uses it |
| `pkg/storage` (root) | 2,775 | Abstraction layer — nothing imports the root, sub-packages handle it |
| **Subtotal** | **7,445** | |

---

## Summary

```
Total Go code in pkg/:               ~48,000 lines
  ├── Reachable from binary:          ~27,600 lines (58%)  ← LIVE
  ├── Test/library-only:               ~1,900 lines (4%)   ← LEGITIMATE
  ├── Dead — valuable, needs wiring:   ~7,600 lines (16%)  ← WIRE IN
  ├── Dead — process mining (plugin):  ~3,300 lines (7%)   ← MOVE TO PLUGIN
  └── Dead — delete:                   ~7,400 lines (15%)  ← DELETE

Root test harnesses:                    4,578 lines         ← CONVERT TO go test
```

### Quick Win: Delete ~7,400 lines of truly orphaned code
These 12 packages can be removed immediately with zero impact:
```
pkg/quality, pkg/validation, pkg/validation/quality,
pkg/heuristic, pkg/hooks, pkg/turbo, pkg/runtime,
pkg/index, pkg/perf, pkg/query/cache, pkg/testing/roundtrip,
pkg/storage (root cloud.go only — keep sub-packages)
```

### Medium Win: Wire in ~7,600 lines of already-built code
These packages implement what the pipeline design calls for — they just need to be imported and connected.

### Domain Win: Move ~3,300 lines to plugin
OCEL model and parser healing are process-mining-specific. Move to `plugin/processmining/`.
