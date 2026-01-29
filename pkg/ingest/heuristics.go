package ingest

import (
	"runtime"
	"sync"
	"time"
)

// SourceBucket categorizes sources by processing cost.
// From huristics.md: bucket sources by type for concurrency control.
type SourceBucket int

const (
	BucketNative   SourceBucket = iota // Parquet, ORC, Avro - ~500 MB/s
	BucketCheap                        // CSV, TSV, JSONL - ~150 MB/s
	BucketMedium                       // JSON, XML - ~50 MB/s
	BucketExpensive                    // Excel, PDF - ~10 MB/s
)

func (b SourceBucket) String() string {
	switch b {
	case BucketNative:
		return "native"
	case BucketCheap:
		return "cheap"
	case BucketMedium:
		return "medium"
	case BucketExpensive:
		return "expensive"
	default:
		return "unknown"
	}
}

// BucketConcurrency returns max concurrent operations per bucket.
// From huristics.md: limit concurrency per bucket type.
func BucketConcurrency(bucket SourceBucket) int {
	cpus := runtime.NumCPU()
	switch bucket {
	case BucketNative:
		return cpus * 4 // Native formats are I/O bound
	case BucketCheap:
		return cpus * 2 // CSV/JSONL can parallelize well
	case BucketMedium:
		return cpus // JSON/XML need more CPU
	case BucketExpensive:
		return 2 // Excel/PDF are very CPU intensive
	default:
		return cpus
	}
}

// GetBucket returns the bucket for a format.
func GetBucket(format Format) SourceBucket {
	switch format {
	case FormatParquet:
		return BucketNative
	case FormatCSV, FormatTSV, FormatJSONL:
		return BucketCheap
	case FormatJSON, FormatXML, FormatXES:
		return BucketMedium
	case FormatXLSX:
		return BucketExpensive
	default:
		return BucketMedium
	}
}

// Heuristics contains computed processing parameters.
type Heuristics struct {
	// Strategy selection
	Strategy   Strategy
	Confidence float64

	// Tuned parameters
	BatchSize      int
	RowGroupSize   int
	Compression    string
	MaxConcurrency int
	FlushThreshold int64

	// Bucket classification
	Bucket SourceBucket

	// Resource estimates
	EstimatedMemoryMB int
	EstimatedTimeMS   int64
}

// DefaultHeuristics returns sensible defaults.
func DefaultHeuristics() *Heuristics {
	return &Heuristics{
		Strategy:       StrategyFastDuckDB,
		Confidence:     0.8,
		BatchSize:      8192,
		RowGroupSize:   10000,
		Compression:    "snappy",
		MaxConcurrency: runtime.NumCPU() * 2,
		FlushThreshold: 64 * 1024 * 1024, // 64MB
		Bucket:         BucketCheap,
	}
}

// HeuristicEngine computes optimal parameters based on file analysis.
type HeuristicEngine struct {
	mu sync.RWMutex

	// Cached schemas for reuse (huristics.md #10)
	schemaCache map[string]*CachedSchema

	// Performance metrics for adaptive tuning
	metrics *PerformanceMetrics

	// Configuration
	config *UnifiedConfig
}

// CachedSchema stores inferred schema for reuse.
type CachedSchema struct {
	SourceHash string
	Columns    []string
	Types      []string
	CreatedAt  time.Time
	UseCount   int
}

// PerformanceMetrics tracks performance for adaptive tuning.
type PerformanceMetrics struct {
	mu sync.Mutex

	// Per-format metrics
	ParseRates map[Format][]float64 // rows/sec
	WriteRates map[Format][]float64 // rows/sec

	// Moving averages
	AvgParseRate map[Format]float64
	AvgWriteRate map[Format]float64
}

// NewHeuristicEngine creates a new heuristic engine.
func NewHeuristicEngine() *HeuristicEngine {
	return &HeuristicEngine{
		schemaCache: make(map[string]*CachedSchema),
		metrics: &PerformanceMetrics{
			ParseRates:   make(map[Format][]float64),
			WriteRates:   make(map[Format][]float64),
			AvgParseRate: make(map[Format]float64),
			AvgWriteRate: make(map[Format]float64),
		},
		config: GlobalConfig,
	}
}

// Compute generates heuristics based on file analysis.
func (h *HeuristicEngine) Compute(analysis *FileAnalysis) *Heuristics {
	heur := DefaultHeuristics()

	// 1. Classify into bucket
	heur.Bucket = GetBucket(analysis.Format)
	heur.MaxConcurrency = BucketConcurrency(heur.Bucket)

	// 2. Determine strategy based on cleanliness
	if analysis.IsClean {
		heur.Strategy = StrategyFastDuckDB
		heur.Confidence = 0.95
	} else {
		// Check severity of issues
		issues := 0
		if analysis.HasMalformedQuotes {
			issues++
		}
		if analysis.HasEncodingErrors {
			issues++
		}
		if analysis.HasRaggedRows {
			issues++
		}
		if analysis.HasEmbeddedNewlines {
			issues++
		}

		if issues >= 2 {
			heur.Strategy = StrategyRobustGo
			heur.Confidence = 0.85
		} else {
			heur.Strategy = StrategyFastDuckDB
			heur.Confidence = 0.75
		}
	}

	// 3. Size-based tuning (huristics.md #4, #13)
	h.tuneBySizeAndFormat(analysis, heur)

	// 4. Apply adaptive tuning if enabled
	if h.config.AdaptiveTuning {
		h.applyAdaptiveTuning(analysis.Format, heur)
	}

	// 5. Estimate resources
	h.estimateResources(analysis, heur)

	return heur
}

// tuneBySizeAndFormat adjusts parameters based on file size and format.
func (h *HeuristicEngine) tuneBySizeAndFormat(analysis *FileAnalysis, heur *Heuristics) {
	size := analysis.Size

	// Small files: smaller batches, quick processing
	if size < h.config.SmallFileSize {
		heur.BatchSize = 4096
		heur.RowGroupSize = 5000
		heur.FlushThreshold = 32 * 1024 * 1024
	}

	// Large files: larger batches, streaming
	if size > h.config.LargeFileSize {
		heur.BatchSize = 16384
		heur.RowGroupSize = 100000
		heur.FlushThreshold = 128 * 1024 * 1024
		heur.Strategy = StrategyStreaming
	}

	// Format-specific tuning
	switch analysis.Format {
	case FormatXES:
		// XES benefits from zstd compression (huristics.md #15)
		heur.Compression = "zstd"
		heur.Strategy = StrategyRobustGo // DuckDB doesn't support XES
	case FormatParquet:
		// Re-compression: match or improve existing
		heur.Compression = "zstd"
	case FormatJSON:
		// JSON parsing is CPU-intensive
		heur.BatchSize = 2048
	}
}

// applyAdaptiveTuning adjusts based on historical performance.
func (h *HeuristicEngine) applyAdaptiveTuning(format Format, heur *Heuristics) {
	h.metrics.mu.Lock()
	defer h.metrics.mu.Unlock()

	avgParse, hasParseData := h.metrics.AvgParseRate[format]
	avgWrite, hasWriteData := h.metrics.AvgWriteRate[format]

	if !hasParseData || !hasWriteData {
		return
	}

	// If write is bottleneck, reduce batch size
	if avgWrite < avgParse*0.8 {
		heur.BatchSize = heur.BatchSize * 3 / 4
		heur.RowGroupSize = heur.RowGroupSize * 3 / 4
	}

	// If parse is bottleneck, increase batch size
	if avgParse < avgWrite*0.8 {
		heur.BatchSize = heur.BatchSize * 5 / 4
	}
}

// estimateResources estimates memory and time requirements.
func (h *HeuristicEngine) estimateResources(analysis *FileAnalysis, heur *Heuristics) {
	// Rough memory estimate: 2x input size for processing buffers
	heur.EstimatedMemoryMB = int(analysis.Size / (1024 * 1024) * 2)
	if heur.EstimatedMemoryMB < 100 {
		heur.EstimatedMemoryMB = 100
	}

	// Time estimate based on bucket
	var mbPerSec float64
	switch heur.Bucket {
	case BucketNative:
		mbPerSec = 500
	case BucketCheap:
		mbPerSec = 150
	case BucketMedium:
		mbPerSec = 50
	case BucketExpensive:
		mbPerSec = 10
	}

	sizeMB := float64(analysis.Size) / (1024 * 1024)
	heur.EstimatedTimeMS = int64(sizeMB / mbPerSec * 1000)
}

// RecordMetrics records performance metrics for adaptive tuning.
func (h *HeuristicEngine) RecordMetrics(format Format, inputSize, outputSize int64, parseTime, writeTime time.Duration) {
	h.metrics.mu.Lock()
	defer h.metrics.mu.Unlock()

	// Calculate rates
	sizeMB := float64(inputSize) / (1024 * 1024)
	parseRate := sizeMB / parseTime.Seconds()
	writeRate := sizeMB / writeTime.Seconds()

	// Store rates (keep last 10)
	h.metrics.ParseRates[format] = append(h.metrics.ParseRates[format], parseRate)
	if len(h.metrics.ParseRates[format]) > 10 {
		h.metrics.ParseRates[format] = h.metrics.ParseRates[format][1:]
	}

	h.metrics.WriteRates[format] = append(h.metrics.WriteRates[format], writeRate)
	if len(h.metrics.WriteRates[format]) > 10 {
		h.metrics.WriteRates[format] = h.metrics.WriteRates[format][1:]
	}

	// Update moving averages
	h.metrics.AvgParseRate[format] = average(h.metrics.ParseRates[format])
	h.metrics.AvgWriteRate[format] = average(h.metrics.WriteRates[format])
}

// CacheSchema caches a schema for reuse.
func (h *HeuristicEngine) CacheSchema(sourceHash string, columns, types []string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.schemaCache[sourceHash] = &CachedSchema{
		SourceHash: sourceHash,
		Columns:    columns,
		Types:      types,
		CreatedAt:  time.Now(),
		UseCount:   0,
	}
}

// GetCachedSchema retrieves a cached schema.
func (h *HeuristicEngine) GetCachedSchema(sourceHash string) *CachedSchema {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if schema, ok := h.schemaCache[sourceHash]; ok {
		schema.UseCount++
		return schema
	}
	return nil
}

// ApplyRule applies a decision rule to heuristics.
func (heur *Heuristics) ApplyRule(rule *DecisionRule) {
	if rule.Strategy != 0 {
		heur.Strategy = rule.Strategy
	}
	if rule.BatchSize > 0 {
		heur.BatchSize = rule.BatchSize
	}
	if rule.RowGroupSize > 0 {
		heur.RowGroupSize = rule.RowGroupSize
	}
	if rule.Compression != "" {
		heur.Compression = rule.Compression
	}
}

// DecisionTable provides rule-based overrides for heuristics.
type DecisionTable struct {
	rules []DecisionRule
}

// DecisionRule defines conditions and actions for heuristic overrides.
type DecisionRule struct {
	// Conditions
	Format      Format
	MinSize     int64
	MaxSize     int64
	IsClean     bool
	CheckClean  bool // Whether to check IsClean condition

	// Actions
	Strategy     Strategy
	BatchSize    int
	RowGroupSize int
	Compression  string
}

// NewDecisionTable creates a decision table with default rules.
func NewDecisionTable() *DecisionTable {
	return &DecisionTable{
		rules: []DecisionRule{
			// Large clean CSV: maximize throughput
			{
				Format:       FormatCSV,
				MinSize:      100 * 1024 * 1024, // 100MB+
				CheckClean:   true,
				IsClean:      true,
				Strategy:     StrategyFastDuckDB,
				BatchSize:    16384,
				RowGroupSize: 100000,
				Compression:  "snappy",
			},
			// XES always uses robust path
			{
				Format:   FormatXES,
				Strategy: StrategyRobustGo,
			},
			// Parquet re-compression
			{
				Format:      FormatParquet,
				Strategy:    StrategyFastDuckDB,
				Compression: "zstd",
			},
		},
	}
}

// Lookup finds the best matching rule.
func (t *DecisionTable) Lookup(format Format, size int64, isClean bool) *DecisionRule {
	for i := range t.rules {
		rule := &t.rules[i]

		// Check format
		if rule.Format != FormatUnknown && rule.Format != format {
			continue
		}

		// Check size range
		if rule.MinSize > 0 && size < rule.MinSize {
			continue
		}
		if rule.MaxSize > 0 && size > rule.MaxSize {
			continue
		}

		// Check cleanliness
		if rule.CheckClean && rule.IsClean != isClean {
			continue
		}

		return rule
	}
	return nil
}

// Helper function
func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}
