package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/logflow/logflow/pkg/ingest"
)

var (
	ingestOutput      string
	ingestCompression string
	ingestStrategy    string
	ingestQuality     bool
	ingestChecksum    bool
	ingestAnalyze     bool
	ingestProfile     bool
	ingestWorkers     int
	ingestMaxErrors   int
	ingestSource      string
)

var ingestCmd = &cobra.Command{
	Use:   "ingest <input-file>",
	Short: "High-performance data ingestion with auto-detection",
	Long: `Ingest data from various formats to Parquet with automatic optimization.

The ingest command automatically detects file format, delimiter, encoding,
and data quality issues. It chooses the optimal processing strategy:

  - Fast Path:   Uses DuckDB for clean files (fastest)
  - Robust Path: Uses Go streaming for dirty data (error recovery)
  - Streaming:   Uses chunked processing for large files (low memory)

Supported formats:
  - CSV, TSV (with automatic delimiter detection)
  - JSON, JSONL
  - XES (process mining)
  - Parquet (re-compression)
  - Gzip compressed files

Examples:
  # Basic ingestion (auto-detect everything)
  logflow ingest data.csv

  # Specify output and compression
  logflow ingest data.csv -o output.parquet --compression zstd

  # With quality validation
  logflow ingest data.csv --quality --checksum

  # Analyze file without processing
  logflow ingest data.csv --analyze

  # Generate data profile
  logflow ingest data.csv --profile

  # Force robust mode for problematic files
  logflow ingest messy.csv --strategy robust`,

	Args: cobra.ExactArgs(1),
	RunE: runIngest,
}

func init() {
	ingestCmd.Flags().StringVarP(&ingestOutput, "output", "o", "", "Output Parquet file (auto-generated if empty)")
	ingestCmd.Flags().StringVar(&ingestCompression, "compression", "snappy", "Compression: snappy, zstd, gzip, lz4, none")
	ingestCmd.Flags().StringVar(&ingestStrategy, "strategy", "", "Force strategy: fast, robust, streaming")
	ingestCmd.Flags().BoolVar(&ingestQuality, "quality", false, "Enable quality validation")
	ingestCmd.Flags().BoolVar(&ingestChecksum, "checksum", false, "Compute file checksum")
	ingestCmd.Flags().BoolVar(&ingestAnalyze, "analyze", false, "Analyze file only (no conversion)")
	ingestCmd.Flags().BoolVar(&ingestProfile, "profile", false, "Generate data profile")
	ingestCmd.Flags().IntVar(&ingestWorkers, "workers", 0, "Number of workers (0=auto)")
	ingestCmd.Flags().IntVar(&ingestMaxErrors, "max-errors", 1000, "Max errors before abort (0=unlimited)")

	rootCmd.AddCommand(ingestCmd)
}

func runIngest(cmd *cobra.Command, args []string) error {
	inputPath := args[0]

	// Check input exists
	if _, err := os.Stat(inputPath); err != nil {
		return fmt.Errorf("input file not found: %s", inputPath)
	}

	// Analyze only mode
	if ingestAnalyze {
		return runIngestAnalyze(inputPath)
	}

	// Profile only mode
	if ingestProfile {
		return runIngestProfile(inputPath)
	}

	// Create engine
	engine, err := ingest.NewEngine()
	if err != nil {
		return fmt.Errorf("failed to initialize engine: %w", err)
	}
	defer engine.Close()

	// Build options
	opts := ingest.DefaultOptions()
	opts.OutputPath = ingestOutput
	opts.Compression = ingestCompression
	opts.EnableQuality = ingestQuality
	opts.EnableChecksum = ingestChecksum
	opts.Workers = ingestWorkers
	opts.MaxErrors = ingestMaxErrors

	// Parse strategy
	if ingestStrategy != "" {
		switch ingestStrategy {
		case "fast", "duckdb":
			opts.ForceStrategy = ingest.StrategyFastDuckDB
		case "robust", "go":
			opts.ForceStrategy = ingest.StrategyRobustGo
		case "streaming", "chunked":
			opts.ForceStrategy = ingest.StrategyStreaming
		case "hybrid":
			opts.ForceStrategy = ingest.StrategyHybrid
		default:
			return fmt.Errorf("unknown strategy: %s", ingestStrategy)
		}
	}

	// Run ingestion
	fmt.Printf("Ingesting: %s\n", inputPath)
	start := time.Now()

	result, err := engine.Ingest(context.Background(), inputPath, opts)
	if err != nil {
		return fmt.Errorf("ingestion failed: %w", err)
	}

	// Print results
	fmt.Println()
	fmt.Println("Ingestion Complete")
	fmt.Println("==================")
	fmt.Printf("Input:       %s (%s)\n", result.InputPath, formatSize(result.InputSize))
	fmt.Printf("Output:      %s (%s)\n", result.OutputPath, formatSize(result.OutputSize))
	fmt.Printf("Format:      %s\n", result.Format)
	fmt.Printf("Strategy:    %s\n", result.Strategy)
	fmt.Printf("Rows:        %s\n", formatIngestNumber(result.RowCount))
	fmt.Printf("Columns:     %d\n", result.ColumnCount)
	fmt.Printf("Duration:    %v\n", time.Since(start).Round(time.Millisecond))
	fmt.Printf("Throughput:  %.0f rows/sec\n", result.Throughput)
	fmt.Printf("Speed:       %.1f MB/sec\n", result.Speed)
	fmt.Printf("Compression: %.1fx\n", result.CompressionRate)

	if result.Quality != nil {
		fmt.Println()
		fmt.Println("Quality Metrics")
		fmt.Println("---------------")
		if result.Quality.Checksum != 0 {
			fmt.Printf("Checksum:        %016x\n", result.Quality.Checksum)
		}
		if result.Quality.MalformedRows > 0 {
			fmt.Printf("Malformed rows:  %d\n", result.Quality.MalformedRows)
		}
		if result.Quality.RecoveredRows > 0 {
			fmt.Printf("Recovered rows:  %d\n", result.Quality.RecoveredRows)
		}
	}

	return nil
}

func runIngestAnalyze(path string) error {
	fmt.Printf("Analyzing: %s\n\n", path)

	engine, err := ingest.NewEngine()
	if err != nil {
		return fmt.Errorf("failed to create engine: %w", err)
	}
	defer engine.Close()

	analysis, err := engine.Analyze(path)
	if err != nil {
		return fmt.Errorf("analysis failed: %w", err)
	}

	ingest.PrintAnalysis(analysis)
	return nil
}

func runIngestProfile(path string) error {
	fmt.Printf("Profiling: %s\n\n", path)

	profile, err := ingest.QuickProfile(path, 10000)
	if err != nil {
		return fmt.Errorf("profiling failed: %w", err)
	}

	if profile == nil {
		return fmt.Errorf("profiling not supported for this format")
	}

	fmt.Printf("Data Profile\n")
	fmt.Printf("============\n\n")
	fmt.Printf("Total Rows (sampled): %d\n", profile.TotalRows)
	fmt.Printf("Checksum:             %016x\n", profile.Checksum)
	fmt.Printf("Columns:              %d\n\n", profile.ColumnCount)

	fmt.Printf("%-20s %-8s %-12s %-8s %-10s\n", "Column", "Nulls%", "Cardinality", "Entropy", "Uniqueness")
	fmt.Println(string(make([]byte, 70)))

	for _, col := range profile.ColumnDetails {
		fmt.Printf("%-20s %6.1f%%  %-12d %-8.2f %-10s\n",
			truncate(col.Name, 20),
			col.NullPercentage,
			col.Cardinality,
			col.NormalizedEntropy,
			col.Uniqueness)
	}

	return nil
}

func formatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatIngestNumber(n int64) string {
	if n >= 1e9 {
		return fmt.Sprintf("%.1fB", float64(n)/1e9)
	}
	if n >= 1e6 {
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	}
	if n >= 1e3 {
		return fmt.Sprintf("%.1fK", float64(n)/1e3)
	}
	return fmt.Sprintf("%d", n)
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
