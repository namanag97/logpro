package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/spf13/cobra"

	"github.com/logflow/logflow/pkg/core"
)

var (
	benchRows    int
	benchColumns int
	benchFormat  string
	benchRuns    int
	benchOutput  string
	benchKeep    bool
)

var benchmarkCmd = &cobra.Command{
	Use:   "benchmark",
	Short: "Run performance benchmarks",
	Long: `Run performance benchmarks to measure conversion speed.

Generates synthetic data and measures conversion performance.
Useful for comparing different configurations and tracking regressions.

Examples:
  logflow benchmark                          # Default: 1M rows, 10 columns
  logflow benchmark --rows 10000000          # 10M rows
  logflow benchmark --columns 50             # 50 columns
  logflow benchmark --runs 5                 # Average of 5 runs
  logflow benchmark --format json            # Test JSON format`,
	RunE: runBenchmark,
}

func init() {
	benchmarkCmd.Flags().IntVar(&benchRows, "rows", 1000000, "Number of rows to generate")
	benchmarkCmd.Flags().IntVar(&benchColumns, "columns", 10, "Number of columns")
	benchmarkCmd.Flags().StringVar(&benchFormat, "format", "csv", "Input format (csv, json)")
	benchmarkCmd.Flags().IntVar(&benchRuns, "runs", 3, "Number of benchmark runs")
	benchmarkCmd.Flags().StringVar(&benchOutput, "output", "", "Output directory for reports")
	benchmarkCmd.Flags().BoolVar(&benchKeep, "keep", false, "Keep generated files after benchmark")

	rootCmd.AddCommand(benchmarkCmd)
}

func runBenchmark(cmd *cobra.Command, args []string) error {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║              LOGFLOW PERFORMANCE BENCHMARK                 ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	// System info
	fmt.Println("SYSTEM:")
	fmt.Printf("  OS:      %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Printf("  CPUs:    %d\n", runtime.NumCPU())
	fmt.Printf("  GoMax:   %d\n", runtime.GOMAXPROCS(0))
	fmt.Println()

	// Benchmark parameters
	fmt.Println("PARAMETERS:")
	fmt.Printf("  Rows:    %s\n", formatNumber(int64(benchRows)))
	fmt.Printf("  Columns: %d\n", benchColumns)
	fmt.Printf("  Format:  %s\n", benchFormat)
	fmt.Printf("  Runs:    %d\n", benchRuns)
	fmt.Println()

	// Create temp directory
	tempDir, err := os.MkdirTemp("", "logflow-bench-")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	if !benchKeep {
		defer os.RemoveAll(tempDir)
	}

	// Generate test data
	fmt.Println("GENERATING TEST DATA...")
	inputPath := filepath.Join(tempDir, "benchmark."+benchFormat)
	genStart := time.Now()
	inputSize, err := generateTestData(inputPath, benchRows, benchColumns, benchFormat)
	if err != nil {
		return fmt.Errorf("failed to generate test data: %w", err)
	}
	genTime := time.Since(genStart)
	fmt.Printf("  Generated %s in %v\n", formatBytes(inputSize), genTime.Round(time.Millisecond))
	fmt.Println()

	// Run benchmarks
	fmt.Println("RUNNING BENCHMARKS...")
	fmt.Println()

	converter, err := core.NewConverter()
	if err != nil {
		return fmt.Errorf("failed to create converter: %w", err)
	}
	defer converter.Close()

	var results []BenchmarkResult
	for i := 1; i <= benchRuns; i++ {
		fmt.Printf("  Run %d/%d: ", i, benchRuns)

		outputPath := filepath.Join(tempDir, fmt.Sprintf("output_%d.parquet", i))
		result, err := runSingleBenchmark(converter, inputPath, outputPath)
		if err != nil {
			fmt.Printf("FAILED: %v\n", err)
			continue
		}

		results = append(results, result)
		fmt.Printf("%v (%.0f rows/sec, %.1f MB/sec)\n",
			result.Duration.Round(time.Millisecond),
			result.RowsPerSec,
			float64(result.BytesPerSec)/(1024*1024))

		if !benchKeep && i < benchRuns {
			os.Remove(outputPath)
		}
	}

	if len(results) == 0 {
		return fmt.Errorf("all benchmark runs failed")
	}

	// Calculate statistics
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("RESULTS:")
	fmt.Println()

	avgDuration := averageDuration(results)
	avgRowsPerSec := averageRowsPerSec(results)
	avgBytesPerSec := averageBytesPerSec(results)
	avgCompression := averageCompression(results)

	fmt.Printf("  Average Duration:    %v\n", avgDuration.Round(time.Millisecond))
	fmt.Printf("  Average Throughput:  %.0f rows/sec\n", avgRowsPerSec)
	fmt.Printf("  Average Speed:       %.1f MB/sec\n", float64(avgBytesPerSec)/(1024*1024))
	fmt.Printf("  Compression Ratio:   %.1fx\n", avgCompression)
	fmt.Println()

	// Performance grade
	grade := performanceGrade(avgRowsPerSec, avgBytesPerSec)
	fmt.Printf("  PERFORMANCE GRADE:   %s\n", grade)
	fmt.Println()

	if benchKeep {
		fmt.Printf("  Files kept in: %s\n", tempDir)
	}

	fmt.Println("═══════════════════════════════════════════════════════════")

	return nil
}

// BenchmarkResult holds results from a single run.
type BenchmarkResult struct {
	Duration    time.Duration
	RowCount    int64
	InputSize   int64
	OutputSize  int64
	RowsPerSec  float64
	BytesPerSec int64
	Compression float64
}

func runSingleBenchmark(converter *core.Converter, inputPath, outputPath string) (BenchmarkResult, error) {
	start := time.Now()

	result, err := converter.Convert(context.Background(), inputPath, core.ConversionOptions{
		OutputPath:  outputPath,
		Compression: "snappy",
	})
	if err != nil {
		return BenchmarkResult{}, err
	}

	duration := time.Since(start)

	return BenchmarkResult{
		Duration:    duration,
		RowCount:    result.RowCount,
		InputSize:   result.InputSize,
		OutputSize:  result.OutputSize,
		RowsPerSec:  float64(result.RowCount) / duration.Seconds(),
		BytesPerSec: int64(float64(result.InputSize) / duration.Seconds()),
		Compression: result.Compression,
	}, nil
}

func generateTestData(path string, rows, columns int, format string) (int64, error) {
	file, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	switch format {
	case "csv":
		return generateCSV(file, rows, columns)
	case "json":
		return generateJSON(file, rows, columns)
	default:
		return 0, fmt.Errorf("unsupported format: %s", format)
	}
}

func generateCSV(file *os.File, rows, columns int) (int64, error) {
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Header
	header := make([]string, columns)
	for i := 0; i < columns; i++ {
		header[i] = fmt.Sprintf("col_%d", i)
	}
	writer.Write(header)

	// Data
	row := make([]string, columns)
	for r := 0; r < rows; r++ {
		for c := 0; c < columns; c++ {
			switch c % 4 {
			case 0: // ID
				row[c] = fmt.Sprintf("%d", r)
			case 1: // String
				row[c] = randomString(10)
			case 2: // Number
				row[c] = fmt.Sprintf("%.2f", rand.Float64()*1000)
			case 3: // Timestamp
				row[c] = time.Now().Add(time.Duration(r) * time.Second).Format(time.RFC3339)
			}
		}
		writer.Write(row)
	}

	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func generateJSON(file *os.File, rows, columns int) (int64, error) {
	for r := 0; r < rows; r++ {
		file.WriteString("{")
		for c := 0; c < columns; c++ {
			if c > 0 {
				file.WriteString(",")
			}
			switch c % 4 {
			case 0:
				fmt.Fprintf(file, `"col_%d":%d`, c, r)
			case 1:
				fmt.Fprintf(file, `"col_%d":"%s"`, c, randomString(10))
			case 2:
				fmt.Fprintf(file, `"col_%d":%.2f`, c, rand.Float64()*1000)
			case 3:
				fmt.Fprintf(file, `"col_%d":"%s"`, c, time.Now().Add(time.Duration(r)*time.Second).Format(time.RFC3339))
			}
		}
		file.WriteString("}\n")
	}

	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func averageDuration(results []BenchmarkResult) time.Duration {
	var total time.Duration
	for _, r := range results {
		total += r.Duration
	}
	return total / time.Duration(len(results))
}

func averageRowsPerSec(results []BenchmarkResult) float64 {
	var total float64
	for _, r := range results {
		total += r.RowsPerSec
	}
	return total / float64(len(results))
}

func averageBytesPerSec(results []BenchmarkResult) int64 {
	var total int64
	for _, r := range results {
		total += r.BytesPerSec
	}
	return total / int64(len(results))
}

func averageCompression(results []BenchmarkResult) float64 {
	var total float64
	for _, r := range results {
		total += r.Compression
	}
	return total / float64(len(results))
}

func performanceGrade(rowsPerSec float64, bytesPerSec int64) string {
	mbPerSec := float64(bytesPerSec) / (1024 * 1024)

	if rowsPerSec >= 500000 && mbPerSec >= 100 {
		return "★★★★★ EXCELLENT"
	}
	if rowsPerSec >= 200000 && mbPerSec >= 50 {
		return "★★★★☆ VERY GOOD"
	}
	if rowsPerSec >= 100000 && mbPerSec >= 20 {
		return "★★★☆☆ GOOD"
	}
	if rowsPerSec >= 50000 && mbPerSec >= 10 {
		return "★★☆☆☆ FAIR"
	}
	return "★☆☆☆☆ NEEDS IMPROVEMENT"
}

func formatNumber(n int64) string {
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

func formatBytes(b int64) string {
	if b >= 1e9 {
		return fmt.Sprintf("%.1f GB", float64(b)/1e9)
	}
	if b >= 1e6 {
		return fmt.Sprintf("%.1f MB", float64(b)/1e6)
	}
	if b >= 1e3 {
		return fmt.Sprintf("%.1f KB", float64(b)/1e3)
	}
	return fmt.Sprintf("%d B", b)
}
