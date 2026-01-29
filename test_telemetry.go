// +build ignore

// Test telemetry, benchmarks, and self-optimization
// Run: go run test_telemetry.go
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/logflow/logflow/pkg/ingest/core"
	"github.com/logflow/logflow/pkg/ingest/decoders"
	"github.com/logflow/logflow/pkg/ingest/sinks"
	"github.com/logflow/logflow/pkg/ingest/telemetry"
)

const testFile = "/Users/namanagarwal/logpro/Insurance_claims_event_log copy.csv"

func main() {
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║       LogFlow Telemetry & Self-Optimization Demo               ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	ctx := context.Background()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 1: Metrics Collection
	// ═══════════════════════════════════════════════════════════════════════
	fmt.Println("═══ SECTION 1: Metrics Collection ═══")

	metrics := telemetry.NewMetrics()

	source := &fileSource{
		path:   testFile,
		format: core.FormatCSV,
	}

	decoder := decoders.NewCSVDecoder()
	opts := core.DefaultDecodeOptions()
	opts.BatchSize = 5000

	// Decode with metrics
	start := time.Now()
	batches, err := decoder.Decode(ctx, source, opts)
	if err != nil {
		fatal("Decode failed: %v", err)
	}

	var allBatches []core.DecodedBatch
	for batch := range batches {
		if batch.Batch != nil {
			metrics.AddRowsRead(batch.Batch.NumRows())
			metrics.AddBytesRead(batch.BytesRead)
			metrics.RecordPhase("decode", time.Since(start))
			allBatches = append(allBatches, batch)
		}
	}

	snapshot := metrics.Snapshot()
	fmt.Printf("   Rows Read:      %d\n", snapshot.RowsRead)
	fmt.Printf("   Bytes Read:     %d\n", snapshot.BytesRead)
	fmt.Printf("   Rows/sec:       %.0f\n", snapshot.RowsPerSecond)
	fmt.Printf("   Elapsed:        %v\n", snapshot.Elapsed)
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 2: Built-in Benchmarks
	// ═══════════════════════════════════════════════════════════════════════
	fmt.Println("═══ SECTION 2: Built-in Benchmarks ═══")

	benchmark := telemetry.NewBenchmark()

	// Benchmark different batch sizes
	batchSizes := []int{1000, 5000, 10000}
	for _, bs := range batchSizes {
		result := benchmark.RunDecode(ctx, decoders.NewCSVDecoder(), source, bs)
		benchmark.AddResult(result)
		fmt.Printf("   Batch %5d: %8.0f rows/sec, %.1f MB memory\n",
			bs, result.RowsPerSecond, result.MemoryUsedMB)
	}

	// Get best configuration
	bestBatch, bestComp := benchmark.BestConfig()
	fmt.Printf("\n   Best config: batch=%d, compression=%s\n", bestBatch, bestComp)
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 3: Profiler
	// ═══════════════════════════════════════════════════════════════════════
	fmt.Println("═══ SECTION 3: Profiler ═══")

	profiler := telemetry.NewProfiler()

	// Simulate pipeline phases
	// Decode phase
	done := profiler.StartPhase("decode")
	batches2, _ := decoder.Decode(ctx, source, opts)
	var batches2List []core.DecodedBatch
	for b := range batches2 {
		if b.Batch != nil {
			batches2List = append(batches2List, b)
		}
	}
	done()

	// Transform phase (simulated)
	done = profiler.StartPhase("transform")
	time.Sleep(10 * time.Millisecond) // Simulate transform
	done()

	// Write phase
	done = profiler.StartPhase("write")
	tmpFile, _ := os.CreateTemp("", "profile-*.parquet")
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = tmpPath

	if len(batches2List) > 0 {
		sink.Open(ctx, batches2List[0].Batch.Schema(), sinkOpts)
		for _, b := range batches2List {
			sink.Write(ctx, b.Batch)
			b.Batch.Release()
		}
		sink.Close(ctx)
	}
	done()

	report := profiler.Report()
	report.Print(os.Stdout)
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 4: Self-Optimization
	// ═══════════════════════════════════════════════════════════════════════
	fmt.Println("═══ SECTION 4: Self-Optimization ═══")

	optimizer := telemetry.NewOptimizer()

	// Simulate feeding performance samples
	for i := 0; i < 10; i++ {
		sample := telemetry.PerformanceSample{
			BatchSize:     5000 + i*1000,
			Compression:   core.CompressionSnappy,
			RowsPerSecond: 100000 + float64(i)*10000,
			MemoryMB:      200 + float64(i)*20,
			Latency:       time.Duration(50+i*5) * time.Millisecond,
		}
		optimizer.RecordSample(sample)
	}

	// Run optimization
	optimizedConfig := optimizer.Optimize()

	fmt.Printf("   Initial Config:\n")
	fmt.Printf("     Batch Size:   8192\n")
	fmt.Printf("     Compression:  snappy\n")
	fmt.Printf("     Concurrency:  auto\n")
	fmt.Println()
	fmt.Printf("   Optimized Config:\n")
	fmt.Printf("     Batch Size:   %d\n", optimizedConfig.BatchSize)
	fmt.Printf("     Compression:  %s\n", optimizedConfig.Compression)
	fmt.Printf("     Concurrency:  %d\n", optimizedConfig.Concurrency)
	fmt.Printf("     Expected:     %.0f rows/sec\n", optimizedConfig.ExpectedRowsPerSec)
	fmt.Println()

	stats := optimizer.Statistics()
	fmt.Printf("   Optimizer Stats:\n")
	fmt.Printf("     Samples:        %d\n", stats.SampleCount)
	fmt.Printf("     Optimizations:  %d\n", stats.OptimizationCount)
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 5: Auto-Tuner Demo
	// ═══════════════════════════════════════════════════════════════════════
	fmt.Println("═══ SECTION 5: Auto-Tuner ═══")

	globalMetrics := telemetry.NewMetrics()
	tuner := telemetry.NewAutoTuner(globalMetrics)

	fmt.Printf("   Starting auto-tuner...\n")
	tuner.Start(ctx)

	// Simulate some work
	for i := 0; i < 5; i++ {
		globalMetrics.AddRowsRead(10000)
		globalMetrics.AddBytesRead(1024 * 1024)
		time.Sleep(100 * time.Millisecond)
	}

	config := tuner.GetConfig()
	fmt.Printf("   Current Config: %s\n", config)
	tuner.Stop()
	fmt.Printf("   Auto-tuner stopped.\n")
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SUMMARY
	// ═══════════════════════════════════════════════════════════════════════
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                          COMPLETE                              ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")

	// Clean up remaining batches
	for _, b := range allBatches {
		b.Batch.Release()
	}
}

type fileSource struct {
	path   string
	format core.Format
}

func (s *fileSource) ID() string                             { return s.path }
func (s *fileSource) Location() string                       { return s.path }
func (s *fileSource) Format() core.Format                    { return s.format }
func (s *fileSource) Size() int64                            { info, _ := os.Stat(s.path); if info != nil { return info.Size() }; return 0 }
func (s *fileSource) ModTime() time.Time                     { return time.Now() }
func (s *fileSource) Metadata() map[string]string            { return nil }
func (s *fileSource) Open(ctx context.Context) (io.ReadCloser, error) { return os.Open(s.path) }

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(1)
}
