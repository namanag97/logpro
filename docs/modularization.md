# Pipeline Modularization Plan

## Current State

The codebase has plugin-ready abstractions that aren't wired up:
- `pkg/registry/registry.go` defines `SourceFactory`, `SinkFactory`, `ProcessorFactory` — never initialized
- `pkg/ingest/hooks/hooks.go` has a full lifecycle hook system — partially used
- `pkg/core/plugins.go` defines `PreProcessor`, `PostProcessor`, `Analyzer` — only `Analyzer` is implemented

Several critical subsystems bypass these abstractions entirely via hardcoded switches and direct imports.

---

## Opportunities

### 1. Activate the Registry
`pkg/registry/registry.go` has the right abstractions. Add `init()` self-registration in each concrete source, sink, and processor. Pipeline resolves components by name instead of direct imports.

### 2. Strategy Plugin
`pkg/ingest/pipeline.go:192-206` hardcodes a switch over processing strategies. Define a `Strategy` interface with `CanHandle(analysis) (bool, confidence)` and `Process(ctx, source, opts)`. Register strategies; pipeline picks highest confidence.

### 3. Sink Registry
Only `IcebergSink` exists. Add `ParquetSink`, `JSONLSink`, `StdoutSink` implementing the existing `Sink` interface. Select via `--output-format` from registry.

### 4. Detection Rules Engine
`pkg/ingest/detect/detector.go:202-223` has hardcoded thresholds for strategy selection. Replace with scored `DetectionRule` interface. Register rules with priority; detector picks highest-confidence match.

### 5. Error Policy Plugin
`pkg/ingest/errors/policy.go:62-97` switches over four hardcoded policies. Define `PolicyHandler` interface returning `{Continue, Skip, Abort}`. Custom policies register without touching the switch.

### 6. Wire Pre/Post Processing Plugins
`PreProcessor` and `PostProcessor` interfaces exist in `pkg/core/plugins.go` but are never called. Wire them into `pkg/ingest/pipeline.go` before and after strategy execution.

### 7. Schema Type Inference Plugin
`pkg/ingest/schema/inference.go:121-143` has hardcoded type mapping. Define `TypeInferrer` interface with `Infer(sample) (arrowType, confidence)`. Register standard + domain-specific inferrers.

### 8. DuckDB Abstraction Layer
`pkg/core/convert.go` and `pkg/plugins/processmining/plugin.go` execute DuckDB SQL directly. Define an `Engine` interface with `ReadFormat()`, `WriteParquet()`, `Query()`. `DuckDBEngine` implements it; future engines slot in.

### 9. Filter Operator Registry
`pkg/processors/filter.go:108-130` hardcodes operators. Register operators as `func(fieldValue, ruleValue string) bool`. Filter processor looks them up at runtime.

### 10. Hook Priorities
`pkg/ingest/hooks/hooks.go` runs hooks in append order unconditionally. Add priority ordering and optional condition predicates.

---

## Execution Order

| # | Opportunity | Depends On | Risk |
|---|---|---|---|
| 1 | Activate registry | — | Low |
| 2 | Strategy plugin | 1 | Medium |
| 3 | Sink registry | 1 | Low |
| 4 | Detection rules engine | 2 | Low |
| 5 | Error policy plugin | — | Low |
| 6 | Wire pre/post plugins | 1 | Low |
| 7 | Schema type inference plugin | — | Medium |
| 8 | DuckDB abstraction layer | — | Medium |
| 9 | Filter operator registry | 1 | Low |
| 10 | Hook priorities | — | Low |
