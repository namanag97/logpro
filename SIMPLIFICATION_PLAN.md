# Simplification Plan

## Current State: TWO parallel systems
- `pkg/core/convert.go` (384 lines) - Used by API, plugins
- `pkg/ingest/*` (4,500+ lines) - Used by CLI, library

## Target State: ONE system
- `pkg/core/` - Single conversion engine (DuckDB + XES)
- `pkg/ingest/` - Thin wrapper for library API compatibility

## Phase 1: Consolidate core.Converter
1. Add XES support to `pkg/core/convert.go`
2. Add YAML config loading
3. Add checksum verification
4. Add error handling options (ignore_errors)

## Phase 2: Make ingest a thin wrapper
1. `pkg/ingest/ingest.go` - Calls core.Converter
2. Delete: detect.go, heuristics.go, robust_path.go, fast_path.go, quality.go, pipeline.go
3. Keep types for API compatibility (Result, Options, FileAnalysis)

## Phase 3: Delete dead code
- `pkg/pipeline/`
- `pkg/fast/`
- Duplicate quality validators

## Phase 4: Unify config
- Single YAML config file
- All settings in one place

## Files to modify:
1. `pkg/core/convert.go` - Add XES, config, checksum
2. `pkg/ingest/ingest.go` - Simplify to wrapper
3. `pkg/ingest/types.go` - Keep types only

## Files to delete:
- `pkg/ingest/detect.go` (793 lines)
- `pkg/ingest/heuristics.go` (707 lines)
- `pkg/ingest/robust_path.go` (830 lines)
- `pkg/ingest/fast_path.go` (356 lines)
- `pkg/ingest/quality.go` (612 lines)
- `pkg/ingest/pipeline.go` (542 lines)
- `pkg/ingest/unified_config.go` (187 lines)
- `pkg/pipeline/` (entire folder)
- `pkg/fast/` (entire folder)

Total deletion: ~4,000+ lines
