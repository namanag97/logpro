package ingest

import (
	"context"
	"fmt"
	"os"
	"time"
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
}

// DefaultOptions returns sensible defaults.
func DefaultOptions() Options {
	return Options{
		Compression:     "snappy",
		RowGroupSize:    10000,
		MaxErrors:       1000,
		RescueMalformed: true,
	}
}

// Engine is the main ingestion engine.
type Engine struct {
	detector *Detector
	fastPath *FastPath
	robust   *RobustPath
}

// NewEngine creates a new ingestion engine.
func NewEngine() (*Engine, error) {
	fastPath, err := NewFastPath()
	if err != nil {
		return nil, fmt.Errorf("failed to create fast path: %w", err)
	}

	return &Engine{
		detector: NewDetector(),
		fastPath: fastPath,
		robust:   NewRobustPath(),
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

	// Step 2: Determine strategy (with override support)
	strategy := analysis.RecommendedStrategy
	if opts.ForceStrategy != 0 {
		strategy = opts.ForceStrategy
	}

	// Step 3: Generate output path if not specified
	outputPath := opts.OutputPath
	if outputPath == "" {
		outputPath = generateOutputPath(inputPath)
	}

	// Step 4: Execute based on strategy
	var result *Result

	switch strategy {
	case StrategyFastDuckDB:
		result, err = e.fastPath.Process(ctx, inputPath, outputPath, analysis, opts)
	case StrategyRobustGo:
		result, err = e.robust.Process(ctx, inputPath, outputPath, analysis, opts)
	case StrategyStreaming:
		// Fall back to robust for now
		result, err = e.robust.Process(ctx, inputPath, outputPath, analysis, opts)
	case StrategyHybrid:
		// Try fast path first, fall back to robust
		result, err = e.fastPath.Process(ctx, inputPath, outputPath, analysis, opts)
		if err != nil {
			result, err = e.robust.Process(ctx, inputPath, outputPath, analysis, opts)
		}
	default:
		result, err = e.fastPath.Process(ctx, inputPath, outputPath, analysis, opts)
	}

	if err != nil {
		return nil, err
	}

	// Step 5: Calculate metrics
	result.Duration = time.Since(start)
	result.Throughput = float64(result.RowCount) / result.Duration.Seconds()
	result.Speed = float64(result.InputSize) / result.Duration.Seconds() / (1024 * 1024)

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
