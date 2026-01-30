# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LogFlow is a high-performance Go CLI for converting process mining data (CSV, XES, XLSX, JSON) to Apache Parquet format. It uses Apache Arrow for columnar processing and DuckDB for vectorized SQL operations.

## Build & Test Commands

```bash
# Build the CLI binary
go build -o logflow ./cmd/logflow

# Run all tests
go test ./...

# Run tests for a specific package
go test ./pkg/pipeline/
go test ./pkg/ingest/decoders/

# Run a single test
go test ./pkg/pipeline/ -run TestOrchestratorGoroutineLeaks

# Run benchmarks
go test ./pkg/ingest/ -bench=. -benchmem
```

## Architecture

### Two Pipeline Systems

The codebase has **two distinct pipeline architectures** that coexist:

1. **Original pipeline** (`internal/pipe/`) — Used by the CLI's `convert` command. Event-channel-based: `Parser → chan *Event → Writer`. Parsers live in `pkg/parser/`, writers in `pkg/writer/`. The DuckDB path (`internal/pipe/duckdb_pipe.go`) bypasses Arrow and uses SQL `COPY` for CSV-to-Parquet.

2. **Modular pipeline** (`pkg/pipeline/`) — Newer architecture using Source/Sink/Processor interfaces with a registry (`pkg/registry/`). Orchestrated by `OrchestratorV2` in `pkg/pipeline/orchestrator_v2.go`. Sources in `pkg/adapters/`, sinks in `pkg/sinks/`, processors in `pkg/processors/`.

Additionally, `pkg/ingest/` provides a **streaming ingestion layer** with Arrow RecordBatch processing, schema inference/evolution (`pkg/ingest/schema/`), multiple decoders (`pkg/ingest/decoders/`), and hook-based extensibility (`pkg/ingest/hooks/`).

### Core Data Model

`internal/model/event.go` defines the `Event` struct — all fields are `[]byte` or `int64` to minimize allocations and align with Arrow columnar storage. Timestamps are nanoseconds since Unix epoch. OCEL object references are carried as `[]ObjectRef`.

### Key Package Roles

- `cmd/logflow/` — CLI entry point using Cobra. `main.go` has core commands (convert, wizard, apply), `commands.go` has extended commands (diff, inspect, watch, batch, sample, anonymize, export, stats).
- `internal/pipe/` — Production pipeline connecting parsers to writers via event channels.
- `pkg/pipeline/` — Modular pipeline interfaces (`Source`, `Sink`, `Processor`, `Inspector`) following the filters-and-pipes pattern.
- `pkg/pmpt/` — Process Merkle Patricia Tree for O(1) process fingerprint comparison and structural diff.
- `pkg/ocel/` — OCEL 2.0 (Object-Centric Event Log) support using DuckDB as relational store.
- `pkg/ingest/schema/` — Arrow schema inference, evolution policies (strict/merge-nullable/evolving), and OCEL column injection.
- `pkg/resilience/` — Circuit breaker and poison pill handler for fault tolerance.
- `pkg/checkpoint/` — Resumable processing with local, S3, and Redis backends.
- `pkg/telemetry/` — OpenTelemetry tracing and metrics.

### Module Path

The Go module is `github.com/logflow/logflow` (defined in `go.mod`), though the git repo is `namanag97/logpro`.

### Key Dependencies

- `github.com/apache/arrow/go/v14` — Arrow columnar format
- `github.com/marcboeker/go-duckdb` — Embedded DuckDB (CGo)
- `github.com/spf13/cobra` — CLI framework
- `github.com/RoaringBitmap/roaring` — Bitmap indexes for PMPT
- `github.com/redis/go-redis/v9` — Redis checkpoint backend
- `github.com/aws/aws-sdk-go-v2` — S3 storage/checkpoint
