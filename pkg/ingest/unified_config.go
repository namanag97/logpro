package ingest

import (
	"runtime"
)

// UnifiedConfig consolidates ALL ingestion settings into one place.
// This is the single source of truth for tuning parameters.
type UnifiedConfig struct {
	// === Detection Settings ===
	SampleSize      int64 // Bytes to sample for format detection (default: 64KB)
	EncodingCheck   bool  // Check for encoding errors
	QualityCheck    bool  // Check for data quality issues

	// === DuckDB Settings ===
	DuckDBThreads            int  // Number of DuckDB threads (0 = auto)
	PreserveInsertionOrder   bool // Maintain row order (false = faster)
	ParallelCSV              bool // Enable parallel CSV reading
	SampleSizeForInference   int  // Rows to sample for type inference

	// === Parquet Output Settings ===
	Compression    string // snappy, zstd, gzip, lz4, none
	RowGroupSize   int    // Rows per row group (default: 10000)
	PageSize       int    // Page size in bytes
	DictEncoding   bool   // Enable dictionary encoding
	StatsEnabled   bool   // Write column statistics

	// === Performance Tuning ===
	BatchSize      int   // Records per batch
	BufferSize     int   // I/O buffer size
	MaxConcurrency int   // Max parallel operations
	MemoryLimitMB  int   // Memory limit for operations

	// === Error Handling ===
	MaxErrors       int  // Max errors before abort (0 = unlimited)
	IgnoreErrors    bool // Skip bad rows instead of failing
	NullPadding     bool // Pad missing columns with NULL
	RescueMalformed bool // Attempt to recover malformed rows

	// === Heuristics ===
	AdaptiveTuning   bool    // Enable adaptive parameter tuning
	CleanThreshold   float64 // Threshold for "clean" file detection
	LargeFileSize    int64   // Size threshold for large file optimizations
	SmallFileSize    int64   // Size threshold for small file batching
}

// DefaultConfig returns production-ready defaults.
func DefaultConfig() *UnifiedConfig {
	cpus := runtime.NumCPU()
	return &UnifiedConfig{
		// Detection
		SampleSize:    64 * 1024, // 64KB
		EncodingCheck: true,
		QualityCheck:  true,

		// DuckDB
		DuckDBThreads:            cpus,
		PreserveInsertionOrder:   true,
		ParallelCSV:              true,
		SampleSizeForInference:   10000,

		// Parquet
		Compression:  "snappy",
		RowGroupSize: 10000,
		PageSize:     1024 * 1024, // 1MB
		DictEncoding: true,
		StatsEnabled: true,

		// Performance
		BatchSize:      8192,
		BufferSize:     256 * 1024, // 256KB
		MaxConcurrency: cpus * 2,
		MemoryLimitMB:  4096,

		// Error handling
		MaxErrors:       1000,
		IgnoreErrors:    true,
		NullPadding:     true,
		RescueMalformed: true,

		// Heuristics
		AdaptiveTuning: true,
		CleanThreshold: 0.95,
		LargeFileSize:  1024 * 1024 * 1024,  // 1GB
		SmallFileSize:  64 * 1024 * 1024,    // 64MB
	}
}

// HighSpeedConfig returns settings optimized for maximum throughput.
func HighSpeedConfig() *UnifiedConfig {
	cfg := DefaultConfig()
	cfg.PreserveInsertionOrder = false // Faster parallel processing
	cfg.ParallelCSV = true
	cfg.DuckDBThreads = runtime.NumCPU()
	cfg.RowGroupSize = 100000 // Larger row groups
	cfg.BatchSize = 16384
	cfg.QualityCheck = false // Skip quality checks
	cfg.IgnoreErrors = true
	return cfg
}

// HighQualityConfig returns settings optimized for data quality.
func HighQualityConfig() *UnifiedConfig {
	cfg := DefaultConfig()
	cfg.QualityCheck = true
	cfg.EncodingCheck = true
	cfg.IgnoreErrors = false
	cfg.MaxErrors = 0 // Fail on any error
	cfg.Compression = "zstd"
	cfg.StatsEnabled = true
	return cfg
}

// LowMemoryConfig returns settings for memory-constrained environments.
func LowMemoryConfig() *UnifiedConfig {
	cfg := DefaultConfig()
	cfg.BatchSize = 1024
	cfg.RowGroupSize = 5000
	cfg.BufferSize = 64 * 1024
	cfg.MaxConcurrency = 2
	cfg.MemoryLimitMB = 512
	return cfg
}

// ApplyToOptions applies config to Options struct.
func (c *UnifiedConfig) ApplyToOptions(opts *Options) {
	opts.Compression = c.Compression
	opts.RowGroupSize = c.RowGroupSize
	opts.DuckDBThreads = c.DuckDBThreads
	opts.MaxErrors = c.MaxErrors
	opts.RescueMalformed = c.RescueMalformed
	opts.BufferSize = c.BufferSize
	opts.Workers = c.MaxConcurrency
}

// GlobalConfig is the default global configuration.
var GlobalConfig = DefaultConfig()

// UseHighSpeedMode switches to high-speed configuration.
func UseHighSpeedMode() {
	GlobalConfig = HighSpeedConfig()
}

// UseHighQualityMode switches to high-quality configuration.
func UseHighQualityMode() {
	GlobalConfig = HighQualityConfig()
}

// UseLowMemoryMode switches to low-memory configuration.
func UseLowMemoryMode() {
	GlobalConfig = LowMemoryConfig()
}
