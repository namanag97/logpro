package ingest

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	ingerrors "github.com/logflow/logflow/pkg/ingest/errors"
	"github.com/logflow/logflow/pkg/ingest/sources"
)

// Result contains the outcome of an ingestion operation.
type Result struct {
	InputPath       string
	OutputPath      string
	Format          Format
	Strategy        Strategy
	RowCount        int64
	ColumnCount     int
	InputSize       int64
	OutputSize      int64
	Duration        time.Duration
	Throughput      float64 // rows/sec
	Speed           float64 // MB/sec
	CompressionRate float64
	Quality         *QualityMetrics
}

// QualityMetrics contains data quality information.
type QualityMetrics struct {
	Checksum      uint64
	MalformedRows int64
	RecoveredRows int64
}

// Options configures the ingestion pipeline.
type Options struct {
	OutputPath      string
	Compression     string // snappy, zstd, gzip, lz4, none
	ForceStrategy   Strategy
	EnableQuality   bool
	EnableChecksum  bool
	Workers         int
	RowGroupSize    int
	MaxErrors       int
	RescueMalformed bool
	DuckDBThreads   int
	BufferSize      int
	ChunkSize       int64

	// ErrorPolicy controls how errors are handled: strict, skip, quarantine, recover
	ErrorPolicy    ingerrors.Policy
	// QuarantinePath is the output file for quarantined rows (used when ErrorPolicy=quarantine)
	QuarantinePath string
}

// DefaultOptions returns sensible defaults.
func DefaultOptions() Options {
	cfg := GlobalConfig
	return Options{
		Compression:     cfg.Compression,
		RowGroupSize:    cfg.RowGroupSize,
		MaxErrors:       cfg.MaxErrors,
		RescueMalformed: cfg.RescueMalformed,
		DuckDBThreads:   cfg.DuckDBThreads,
		BufferSize:      cfg.BufferSize,
		Workers:         cfg.MaxConcurrency,
	}
}

// Engine is the main ingestion engine.
type Engine struct {
	detector      *Detector
	fastPath      *FastPath
	robust        *RobustPath
	heuristics    *HeuristicEngine
	decisionTable *DecisionTable
}

// NewEngine creates a new ingestion engine.
func NewEngine() (*Engine, error) {
	fastPath, err := NewFastPath()
	if err != nil {
		return nil, fmt.Errorf("failed to create fast path: %w", err)
	}

	return &Engine{
		detector:      NewDetector(),
		fastPath:      fastPath,
		robust:        NewRobustPath(),
		heuristics:    NewHeuristicEngine(),
		decisionTable: NewDecisionTable(),
	}, nil
}

// Close releases resources.
func (e *Engine) Close() error {
	return e.fastPath.Close()
}

// Ingest processes a file and converts it to Parquet.
func (e *Engine) Ingest(ctx context.Context, inputPath string, opts Options) (*Result, error) {
	start := time.Now()

	// Step 1: Analyze the file
	analysis, err := e.detector.Analyze(inputPath)
	if err != nil {
		return nil, fmt.Errorf("analysis failed: %w", err)
	}

	// Step 2: Compute heuristics based on analysis
	heur := e.heuristics.Compute(analysis)

	// Step 3: Look up decision table for fine-tuning
	rule := e.decisionTable.Lookup(analysis.Format, analysis.Size, analysis.IsClean)
	if rule != nil {
		heur.ApplyRule(rule)
	}

	// Step 4: Determine strategy (with override support)
	strategy := heur.Strategy
	if opts.ForceStrategy != 0 {
		strategy = opts.ForceStrategy
	}

	// Step 5: Apply heuristics to options
	if opts.RowGroupSize <= 0 {
		opts.RowGroupSize = heur.RowGroupSize
	}
	if opts.Compression == "" {
		opts.Compression = heur.Compression
	}
	if opts.Workers <= 0 {
		opts.Workers = heur.MaxConcurrency
	}
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = heur.FlushThreshold
	}

	// Step 6: Generate output path if not specified
	outputPath := opts.OutputPath
	if outputPath == "" {
		outputPath = generateOutputPath(inputPath)
	}

	// Step 7: Execute based on strategy
	var result *Result
	parseStart := time.Now()

	switch strategy {
	case StrategyFastDuckDB:
		result, err = e.fastPath.Process(ctx, inputPath, outputPath, analysis, opts)
	case StrategyRobustGo:
		result, err = e.robust.Process(ctx, inputPath, outputPath, analysis, opts)
	case StrategyStreaming:
		result, err = e.processStreaming(ctx, inputPath, outputPath, analysis, opts)
	case StrategyHybrid:
		result, err = e.processHybrid(ctx, inputPath, outputPath, analysis, opts)
	default:
		result, err = e.fastPath.Process(ctx, inputPath, outputPath, analysis, opts)
	}

	parseTime := time.Since(parseStart)

	if err != nil {
		return nil, err
	}

	// Step 8: Calculate metrics
	result.Duration = time.Since(start)
	result.Throughput = float64(result.RowCount) / result.Duration.Seconds()
	result.Speed = float64(result.InputSize) / result.Duration.Seconds() / (1024 * 1024)

	// Step 9: Record metrics for adaptive tuning
	writeTime := result.Duration - parseTime/2 // Rough estimate
	if writeTime < time.Millisecond {
		writeTime = time.Millisecond
	}
	e.heuristics.RecordMetrics(analysis.Format, result.InputSize, result.OutputSize, parseTime, writeTime)

	return result, nil
}

// IngestSource processes a source (file, HTTP, etc.) and converts it to Parquet.
// The sourceURI can be:
//   - a local file path
//   - file:///path/to/file
//   - http:// or https:// URL
func (e *Engine) IngestSource(ctx context.Context, sourceURI string, opts Options) (*Result, error) {
	// Detect scheme
	switch {
	case strings.HasPrefix(sourceURI, "http://") || strings.HasPrefix(sourceURI, "https://"):
		return e.ingestHTTP(ctx, sourceURI, opts)
	case strings.HasPrefix(sourceURI, "file://"):
		localPath := strings.TrimPrefix(sourceURI, "file://")
		return e.Ingest(ctx, localPath, opts)
	default:
		// Assume local file path
		return e.Ingest(ctx, sourceURI, opts)
	}
}

// ingestHTTP downloads from HTTP and ingests.
func (e *Engine) ingestHTTP(ctx context.Context, rawURL string, opts Options) (*Result, error) {
	src, err := sources.NewHTTPSource(rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("invalid HTTP source: %w", err)
	}

	// Download to temp file, then ingest
	reader, err := src.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open HTTP source: %w", err)
	}
	defer reader.Close()

	tmpFile, err := os.CreateTemp("", "logflow-http-*.dat")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := io.Copy(tmpFile, reader); err != nil {
		tmpFile.Close()
		return nil, fmt.Errorf("failed to download: %w", err)
	}
	tmpFile.Close()

	return e.Ingest(ctx, tmpPath, opts)
}

// processStreaming handles large files with chunked processing.
func (e *Engine) processStreaming(ctx context.Context, inputPath, outputPath string, analysis *FileAnalysis, opts Options) (*Result, error) {
	// For now, delegate to robust path with streaming enabled
	return e.robust.Process(ctx, inputPath, outputPath, analysis, opts)
}

// processHybrid uses DuckDB for reading and Go for validation.
func (e *Engine) processHybrid(ctx context.Context, inputPath, outputPath string, analysis *FileAnalysis, opts Options) (*Result, error) {
	// Try fast path first, fall back to robust on error
	result, err := e.fastPath.Process(ctx, inputPath, outputPath, analysis, opts)
	if err != nil {
		return e.robust.Process(ctx, inputPath, outputPath, analysis, opts)
	}
	return result, nil
}

// Analyze returns file analysis without processing.
func (e *Engine) Analyze(path string) (*FileAnalysis, error) {
	return e.detector.Analyze(path)
}

// generateOutputPath creates output path from input path.
func generateOutputPath(inputPath string) string {
	base := inputPath
	for {
		ext := ""
		for i := len(base) - 1; i >= 0; i-- {
			if base[i] == '.' {
				ext = base[i:]
				base = base[:i]
				break
			}
			if base[i] == '/' || base[i] == '\\' {
				break
			}
		}
		if ext == "" || ext == base {
			break
		}
		switch ext {
		case ".csv", ".tsv", ".json", ".jsonl", ".xes", ".xml", ".gz", ".zip":
			continue
		default:
			base = base + ext
		}
		break
	}
	return base + ".parquet"
}

// PrintAnalysis outputs a human-readable analysis report.
func PrintAnalysis(a *FileAnalysis) {
	fmt.Printf("File Analysis Report\n")
	fmt.Printf("====================\n\n")
	fmt.Printf("Size:     %s\n", formatBytes(a.Size))
	fmt.Printf("Format:   %s\n", a.Format)
	fmt.Printf("Encoding: %s\n", a.Encoding)
	fmt.Println()

	if a.Format == FormatCSV || a.Format == FormatTSV {
		fmt.Printf("CSV Details:\n")
		fmt.Printf("  Delimiter:     %q\n", a.Delimiter)
		fmt.Printf("  Quote char:    %q\n", a.QuoteChar)
		fmt.Printf("  Has header:    %v\n", a.HasHeader)
		fmt.Printf("  Columns:       %d\n", a.EstimatedCols)
		fmt.Println()
	}

	fmt.Printf("Quality Indicators:\n")
	fmt.Printf("  Is clean:              %v\n", a.IsClean)
	fmt.Printf("  Embedded newlines:     %v\n", a.HasEmbeddedNewlines)
	fmt.Printf("  Encoding errors:       %v\n", a.HasEncodingErrors)
	fmt.Printf("  Ragged rows:           %v\n", a.HasRaggedRows)
	fmt.Printf("  Malformed quotes:      %v\n", a.HasMalformedQuotes)
	fmt.Println()

	fmt.Printf("Recommended Strategy: %s (confidence: %.0f%%)\n",
		a.RecommendedStrategy, a.Confidence*100)
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// QuickIngest is a convenience function for simple ingestion.
func QuickIngest(ctx context.Context, inputPath, outputPath string) (*Result, error) {
	engine, err := NewEngine()
	if err != nil {
		return nil, err
	}
	defer engine.Close()

	opts := DefaultOptions()
	opts.OutputPath = outputPath

	return engine.Ingest(ctx, inputPath, opts)
}

// Stat returns basic file stats without full analysis.
func Stat(path string) (int64, Format, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, FormatUnknown, err
	}

	f, err := os.Open(path)
	if err != nil {
		return info.Size(), FormatUnknown, err
	}
	defer f.Close()

	header := make([]byte, 512)
	n, _ := f.Read(header)
	header = header[:n]

	d := NewDetector()
	format := d.detectFormat(path, header)

	return info.Size(), format, nil
}

// QuickProfile and DataProfile are defined in quality.go
