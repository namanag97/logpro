# LogFlow Architecture Document

## Executive Summary

LogFlow is a high-performance, open-source CLI tool for converting process mining data (CSV, XES, JSON, OCEL) to Apache Parquet format. This document outlines the architectural decisions, research validation, and production-readiness assessment.

## Research Validation

### Sources Consulted
- [Apache Arrow Official Documentation](https://arrow.apache.org/)
- [DuckDB Zero-Copy Arrow Integration](https://duckdb.org/2021/12/03/duck-arrow)
- [Go Concurrency Patterns: Pipelines](https://go.dev/blog/pipelines)
- [IEEE XES Standard 1849-2023](https://www.tf-pm.org/)
- [OCEL 2.0 Standard](https://www.tf-pm.org/)
- [Parquet Performance Tuning](https://www.dremio.com/blog/tuning-parquet/)

---

## Architectural Decisions

### Decision 1: Hybrid Go + DuckDB Architecture

**Choice:** Use Go for I/O and parsing, DuckDB for analytics.

**Rationale:**
- Go excels at concurrent I/O and streaming (goroutines + channels)
- DuckDB provides SIMD-optimized vectorized execution
- Zero-copy Arrow integration eliminates serialization overhead
- [Research shows](https://medium.com/@tfmv/redefining-data-engineering-with-go-and-apache-arrow-df9059ddf55c) this hybrid approach achieves 10-100x speedup over pure Go

**Validation:** ✅ Industry standard (used by Dremio, Polars ecosystem)

---

### Decision 2: Pipeline Pattern (Filters & Pipes)

**Choice:** Implement Unix-style pipeline architecture.

**Rationale:**
- Decouples concerns (Source → Processors → Sink)
- Enables composition without code changes
- Matches [official Go concurrency patterns](https://go.dev/blog/pipelines)
- Allows parallel execution stages

**Validation:** ✅ Official Go best practice

---

### Decision 3: sync.Pool for Buffer Reuse

**Choice:** Use sync.Pool to minimize heap allocations.

**Rationale:**
- [Research shows](https://wundergraph.com/blog/golang-sync-pool) 40% reduction in GC pause time
- ~15% throughput increase in high-frequency scenarios
- Critical for streaming large files with constant memory

**Concerns Identified:**
- ⚠️ Must reset objects before returning to pool
- ⚠️ Variable-sized objects can cause memory bloat
- ⚠️ Pool cleared on GC - don't rely on persistence

---

### Decision 4: Apache Arrow Columnar Format

**Choice:** Use Arrow as internal representation, Parquet for output.

**Rationale:**
- [Arrow provides](https://arrow.apache.org/) zero-copy data sharing
- Columnar format enables SIMD operations
- Direct integration with DuckDB (no serialization)
- Industry standard for analytics workloads

**Validation:** ✅ Used by Pandas, Spark, DuckDB, Polars

---

### Decision 5: Context-Based Cancellation

**Choice:** Use context.Context for lifecycle management.

**Rationale:**
- [Prevents goroutine leaks](https://dev.to/serifcolakel/go-concurrency-mastery-preventing-goroutine-leaks-with-context-timeout-cancellation-best-1lg0)
- Enables timeout and deadline propagation
- Standard Go pattern for cancellation

**Concerns Identified:**
- ⚠️ Must check ctx.Done() in all select statements
- ⚠️ Must use buffered channels to prevent blocking on cancel
- ⚠️ Should use errgroup for coordinated shutdown

---

## Critical Improvements Needed

### 1. Error Handling (HIGH PRIORITY)

**Current State:** Basic error returns
**Target State:** Production-grade error chain with context

```go
// Current (Bad)
return err

// Target (Good)
return fmt.Errorf("parsing row %d: %w", lineNum, err)
```

**Actions:**
- [ ] Implement custom error types with stack traces
- [ ] Add error codes for programmatic handling
- [ ] Wrap all errors with context

---

### 2. Goroutine Leak Prevention (HIGH PRIORITY)

**Current State:** Basic context cancellation
**Target State:** errgroup with coordinated shutdown

**Actions:**
- [ ] Replace manual WaitGroup with errgroup.WithContext
- [ ] Add buffered channels (size 1) for all sends
- [ ] Implement goleak testing

---

### 3. Row Group Size (MEDIUM PRIORITY)

**Current State:** 64KB row groups
**Target State:** 128MB (Parquet default)

**Rationale:** [Research shows](https://medium.com/data-engineering-with-dremio/all-about-parquet-part-10-performance-tuning-and-best-practices-with-parquet-d697ba4e8a57) larger row groups reduce I/O overhead and improve compression.

---

### 4. OCEL 2.0 Support (MEDIUM PRIORITY)

**Current State:** XES and flat CSV only
**Target State:** Full OCEL 2.0 support

**Rationale:** [OCEL 2.0](https://www.tf-pm.org/) is the future of process mining, supporting multi-object event logs.

---

### 5. Observability (LOW PRIORITY for MVP)

**Current State:** No telemetry
**Target State:** OpenTelemetry integration

**Actions:**
- [ ] Add optional metrics (events/sec, memory usage)
- [ ] Add tracing for debugging
- [ ] Add structured logging

---

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Throughput | >100K events/sec | Benchmark on 1M events |
| Memory | <100MB for any file size | Profile with pprof |
| Startup | <100ms | Measure cold start |
| Compression | >5x vs CSV | Compare file sizes |

---

## Security Considerations

1. **Input Validation:** All parsers must handle malformed input without panic
2. **Path Traversal:** Sanitize all file paths
3. **Memory Limits:** Cap buffer sizes to prevent DoS
4. **Anonymization:** Use cryptographically secure hashing (SHA-256)

---

## Testing Strategy

1. **Golden File Tests:** Verify output correctness
2. **Fuzz Testing:** `go test -fuzz` for parser robustness
3. **Benchmark Tests:** `go test -bench` for performance regression
4. **Leak Detection:** goleak for goroutine leaks
5. **Integration Tests:** Round-trip with PM4Py/DuckDB

---

## Future Roadmap

### Phase 1 (MVP) ✅
- CSV, XES, JSONL parsing
- Parquet output
- Basic CLI

### Phase 2 (Current)
- Pipeline architecture
- Quality inspection
- Sampling & anonymization
- PowerBI export

### Phase 3 (Next)
- OCEL 2.0 support
- Excel parsing
- SQL source adapter
- Arrow Flight server

### Phase 4 (Future)
- Web UI
- Cloud storage (S3, GCS)
- Distributed processing
