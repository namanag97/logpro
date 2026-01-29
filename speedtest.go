package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/logflow/logflow/pkg/ingest"
)

func main() {
	ctx := context.Background()
	inputPath := "/Users/namanagarwal/logpro/Insurance_claims_event_log copy.csv"
	outputPath := "/Users/namanagarwal/logpro/test_output.parquet"

	os.Remove(outputPath)

	fmt.Println("=== LogFlow Speed Test ===")

	// Create engine
	engine, err := ingest.NewEngine()
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}
	defer engine.Close()

	// Analyze file
	analysis, _ := engine.Analyze(inputPath)
	fmt.Println("Detection Results:")
	fmt.Printf("  Format: %s\n", analysis.Format)
	fmt.Printf("  IsClean: %v\n", analysis.IsClean)
	fmt.Printf("  Recommended Strategy: %s\n", analysis.RecommendedStrategy)
	fmt.Println()

	// Create options
	opts := ingest.DefaultOptions()
	opts.OutputPath = outputPath
	opts.Compression = "snappy"
	opts.RowGroupSize = 10000

	fmt.Printf("Config: Compression=%s, RowGroup=%d\n", opts.Compression, opts.RowGroupSize)
	fmt.Println()

	// Run ingestion
	fmt.Println("Running ingestion...")
	start := time.Now()
	result, err := engine.Ingest(ctx, inputPath, opts)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}
	duration := time.Since(start)

	rowsPerSec := float64(result.RowCount) / duration.Seconds()
	mbPerSec := float64(result.InputSize) / duration.Seconds() / (1024 * 1024)

	fmt.Println()
	fmt.Println("=== RESULTS ===")
	fmt.Printf("Rows:        %d\n", result.RowCount)
	fmt.Printf("Strategy:    %s\n", result.Strategy)
	fmt.Printf("Duration:    %v\n", duration)
	fmt.Printf("Throughput:  %.0f rows/sec\n", rowsPerSec)
	fmt.Printf("Speed:       %.2f MB/sec\n", mbPerSec)
	fmt.Printf("Compression: %.2fx\n", result.CompressionRate)
	fmt.Println()

	if rowsPerSec > 900000 {
		fmt.Println("STATUS: EXCELLENT (>900K rows/sec)")
	} else if rowsPerSec > 500000 {
		fmt.Println("STATUS: GOOD (>500K rows/sec)")
	} else {
		fmt.Printf("STATUS: %.1f%% of 900K target\n", rowsPerSec/900000*100)
	}

	os.Remove(outputPath)
}
