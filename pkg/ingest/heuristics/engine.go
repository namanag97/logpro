// Package heuristics provides adaptive tuning for ingestion.
package heuristics

import (
	"runtime"
	"sync"
	"time"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// Engine computes optimal parameters based on analysis.
type Engine struct {
	mu sync.RWMutex

	// Schema cache
	schemaCache map[string]*CachedSchema

	// Performance metrics
	metrics *Metrics
}

// CachedSchema stores schema for reuse.
type CachedSchema struct {
	SourceHash string
	Columns    []string
	Types      []string
	CreatedAt  time.Time
	UseCount   int
}

// Metrics tracks performance for adaptive tuning.
type Metrics struct {
	mu sync.Mutex

	ParseRates map[core.Format][]float64
	WriteRates map[core.Format][]float64

	AvgParseRate map[core.Format]float64
	AvgWriteRate map[core.Format]float64
}

// NewEngine creates a heuristic engine.
func NewEngine() *Engine {
	return &Engine{
		schemaCache: make(map[string]*CachedSchema),
		metrics: &Metrics{
			ParseRates:   make(map[core.Format][]float64),
			WriteRates:   make(map[core.Format][]float64),
			AvgParseRate: make(map[core.Format]float64),
			AvgWriteRate: make(map[core.Format]float64),
		},
	}
}

// Plan generates a processing plan.
type Plan struct {
	Strategy       Strategy
	Confidence     float64
	BatchSize      int
	RowGroupSize   int
	Compression    string
	MaxConcurrency int
	FlushThreshold int64
	Bucket         core.SourceBucket
}

// Strategy represents processing strategy.
type Strategy uint8

const (
	StrategyFastDuckDB Strategy = iota
	StrategyRobustGo
	StrategyStreaming
	StrategyHybrid
)

// Compute generates a plan from analysis.
func (e *Engine) Compute(format core.Format, size int64, isClean bool) *Plan {
	plan := &Plan{
		Strategy:       StrategyFastDuckDB,
		Confidence:     0.8,
		BatchSize:      8192,
		RowGroupSize:   10000,
		Compression:    "snappy",
		MaxConcurrency: runtime.NumCPU() * 2,
		FlushThreshold: 64 * 1024 * 1024,
		Bucket:         format.Bucket(),
	}

	// Adjust concurrency by bucket
	switch plan.Bucket {
	case core.BucketNative:
		plan.MaxConcurrency = runtime.NumCPU() * 4
	case core.BucketCheap:
		plan.MaxConcurrency = runtime.NumCPU() * 2
	case core.BucketMedium:
		plan.MaxConcurrency = runtime.NumCPU()
	case core.BucketExpensive:
		plan.MaxConcurrency = 2
	}

	// Strategy selection
	if !isClean {
		plan.Strategy = StrategyRobustGo
		plan.Confidence = 0.85
	}

	// Size-based tuning
	if size > 1024*1024*1024 { // 1GB
		plan.Strategy = StrategyStreaming
		plan.BatchSize = 16384
		plan.RowGroupSize = 100000
		plan.FlushThreshold = 128 * 1024 * 1024
	}

	// Format-specific
	switch format {
	case core.FormatXES:
		plan.Strategy = StrategyRobustGo
		plan.Compression = "zstd"
	case core.FormatParquet:
		plan.Compression = "zstd"
	}

	return plan
}

// RecordMetrics records performance data.
func (e *Engine) RecordMetrics(format core.Format, parseRate, writeRate float64) {
	e.metrics.mu.Lock()
	defer e.metrics.mu.Unlock()

	e.metrics.ParseRates[format] = append(e.metrics.ParseRates[format], parseRate)
	if len(e.metrics.ParseRates[format]) > 10 {
		e.metrics.ParseRates[format] = e.metrics.ParseRates[format][1:]
	}

	e.metrics.WriteRates[format] = append(e.metrics.WriteRates[format], writeRate)
	if len(e.metrics.WriteRates[format]) > 10 {
		e.metrics.WriteRates[format] = e.metrics.WriteRates[format][1:]
	}

	e.metrics.AvgParseRate[format] = average(e.metrics.ParseRates[format])
	e.metrics.AvgWriteRate[format] = average(e.metrics.WriteRates[format])
}

// CacheSchema caches a schema.
func (e *Engine) CacheSchema(hash string, cols, types []string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.schemaCache[hash] = &CachedSchema{
		SourceHash: hash,
		Columns:    cols,
		Types:      types,
		CreatedAt:  time.Now(),
	}
}

// GetCachedSchema retrieves a cached schema.
func (e *Engine) GetCachedSchema(hash string) *CachedSchema {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if s, ok := e.schemaCache[hash]; ok {
		s.UseCount++
		return s
	}
	return nil
}

func average(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}
