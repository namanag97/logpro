package telemetry

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// Benchmark runs performance benchmarks on the ingestion pipeline.
type Benchmark struct {
	Results []BenchmarkResult
	mu      sync.Mutex
}

// BenchmarkResult contains results from a benchmark run.
type BenchmarkResult struct {
	Name           string
	BatchSize      int
	Compression    string
	RowCount       int64
	BytesRead      int64
	BytesWritten   int64
	Duration       time.Duration
	RowsPerSecond  float64
	BytesPerSecond float64
	MemoryUsedMB   float64
	GCPauses       int
	Errors         int
}

// BenchmarkConfig configures benchmark behavior.
type BenchmarkConfig struct {
	// Test different batch sizes
	BatchSizes []int

	// Test different compressions
	Compressions []core.Compression

	// Warmup iterations before measurement
	WarmupIterations int

	// Number of iterations per test
	Iterations int

	// Source for benchmarking
	SourcePath string
	SourceSize int64
}

// DefaultBenchmarkConfig returns sensible defaults.
func DefaultBenchmarkConfig() BenchmarkConfig {
	return BenchmarkConfig{
		BatchSizes:       []int{1000, 5000, 10000, 50000},
		Compressions:    []core.Compression{core.CompressionSnappy, core.CompressionZstd, core.CompressionNone},
		WarmupIterations: 1,
		Iterations:       3,
	}
}

// NewBenchmark creates a benchmark runner.
func NewBenchmark() *Benchmark {
	return &Benchmark{}
}

// RunDecode benchmarks decoding performance.
func (b *Benchmark) RunDecode(ctx context.Context, decoder core.Decoder, source core.Source, batchSize int) BenchmarkResult {
	// Force GC before benchmark
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	start := time.Now()

	opts := core.DefaultDecodeOptions()
	opts.BatchSize = batchSize

	batches, err := decoder.Decode(ctx, source, opts)
	if err != nil {
		return BenchmarkResult{Name: "decode", Errors: 1}
	}

	var rowCount, bytesRead int64
	for batch := range batches {
		if batch.Batch != nil {
			rowCount += batch.Batch.NumRows()
			bytesRead += batch.BytesRead
			batch.Batch.Release()
		}
	}

	duration := time.Since(start)

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	return BenchmarkResult{
		Name:           "decode",
		BatchSize:      batchSize,
		RowCount:       rowCount,
		BytesRead:      source.Size(),
		Duration:       duration,
		RowsPerSecond:  float64(rowCount) / duration.Seconds(),
		BytesPerSecond: float64(source.Size()) / duration.Seconds(),
		MemoryUsedMB:   float64(memAfter.Alloc-memBefore.Alloc) / 1024 / 1024,
		GCPauses:       int(memAfter.NumGC - memBefore.NumGC),
	}
}

// RunEncode benchmarks encoding/writing performance.
func (b *Benchmark) RunEncode(ctx context.Context, batches []core.DecodedBatch, sink core.Sink, compression core.Compression) BenchmarkResult {
	if len(batches) == 0 {
		return BenchmarkResult{Name: "encode", Errors: 1}
	}

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Create temp file
	tmpFile, err := os.CreateTemp("", "bench-*.parquet")
	if err != nil {
		return BenchmarkResult{Name: "encode", Errors: 1}
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	start := time.Now()

	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = tmpPath
	sinkOpts.Compression = compression

	if err := sink.Open(ctx, batches[0].Batch.Schema(), sinkOpts); err != nil {
		return BenchmarkResult{Name: "encode", Errors: 1}
	}

	var rowCount int64
	for _, batch := range batches {
		if batch.Batch != nil {
			sink.Write(ctx, batch.Batch)
			rowCount += batch.Batch.NumRows()
		}
	}

	result, err := sink.Close(ctx)
	if err != nil {
		return BenchmarkResult{Name: "encode", Errors: 1}
	}

	duration := time.Since(start)

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	return BenchmarkResult{
		Name:           "encode",
		Compression:    compression.String(),
		RowCount:       rowCount,
		BytesWritten:   result.BytesWritten,
		Duration:       duration,
		RowsPerSecond:  float64(rowCount) / duration.Seconds(),
		BytesPerSecond: float64(result.BytesWritten) / duration.Seconds(),
		MemoryUsedMB:   float64(memAfter.Alloc-memBefore.Alloc) / 1024 / 1024,
		GCPauses:       int(memAfter.NumGC - memBefore.NumGC),
	}
}

// AddResult adds a benchmark result.
func (b *Benchmark) AddResult(result BenchmarkResult) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Results = append(b.Results, result)
}

// Summary returns a summary of benchmark results.
func (b *Benchmark) Summary() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	var summary string
	summary += fmt.Sprintf("╔═════════════════════════════════════════════════════════════════╗\n")
	summary += fmt.Sprintf("║                    BENCHMARK SUMMARY                             ║\n")
	summary += fmt.Sprintf("╠═════════════════════════════════════════════════════════════════╣\n")

	for _, r := range b.Results {
		summary += fmt.Sprintf("║ %-15s batch=%-6d comp=%-8s                        ║\n",
			r.Name, r.BatchSize, r.Compression)
		summary += fmt.Sprintf("║   Rows: %12d    Rate: %12.0f rows/sec              ║\n",
			r.RowCount, r.RowsPerSecond)
		summary += fmt.Sprintf("║   Time: %12v    Memory: %8.1f MB                    ║\n",
			r.Duration.Round(time.Millisecond), r.MemoryUsedMB)
		summary += fmt.Sprintf("╟─────────────────────────────────────────────────────────────────╢\n")
	}

	summary += fmt.Sprintf("╚═════════════════════════════════════════════════════════════════╝\n")
	return summary
}

// BestConfig returns the best performing configuration.
func (b *Benchmark) BestConfig() (batchSize int, compression core.Compression) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var bestRate float64
	batchSize = 10000 // default
	compression = core.CompressionSnappy

	for _, r := range b.Results {
		if r.RowsPerSecond > bestRate {
			bestRate = r.RowsPerSecond
			batchSize = r.BatchSize
			if r.Compression != "" {
				compression = core.ParseCompression(r.Compression)
			}
		}
	}

	return
}

// ProfilerReport generates a profiler report.
type ProfilerReport struct {
	TotalTime    time.Duration
	Phases       map[string]PhaseReport
	Bottleneck   string
	Suggestions  []string
}

// PhaseReport contains timing for a phase.
type PhaseReport struct {
	Name       string
	TotalTime  time.Duration
	Percentage float64
	Calls      int64
}

// Profiler profiles pipeline execution.
type Profiler struct {
	mu        sync.Mutex
	phases    map[string]*profilePhase
	startTime time.Time
}

type profilePhase struct {
	name      string
	totalTime time.Duration
	calls     int64
}

// NewProfiler creates a profiler.
func NewProfiler() *Profiler {
	return &Profiler{
		phases:    make(map[string]*profilePhase),
		startTime: time.Now(),
	}
}

// StartPhase starts timing a phase.
func (p *Profiler) StartPhase(name string) func() {
	start := time.Now()
	return func() {
		elapsed := time.Since(start)
		p.mu.Lock()
		defer p.mu.Unlock()

		phase, ok := p.phases[name]
		if !ok {
			phase = &profilePhase{name: name}
			p.phases[name] = phase
		}
		phase.totalTime += elapsed
		phase.calls++
	}
}

// Report generates a profiler report.
func (p *Profiler) Report() *ProfilerReport {
	p.mu.Lock()
	defer p.mu.Unlock()

	totalTime := time.Since(p.startTime)
	report := &ProfilerReport{
		TotalTime: totalTime,
		Phases:    make(map[string]PhaseReport),
	}

	var maxTime time.Duration
	for name, phase := range p.phases {
		pct := float64(phase.totalTime) / float64(totalTime) * 100
		report.Phases[name] = PhaseReport{
			Name:       name,
			TotalTime:  phase.totalTime,
			Percentage: pct,
			Calls:      phase.calls,
		}

		if phase.totalTime > maxTime {
			maxTime = phase.totalTime
			report.Bottleneck = name
		}
	}

	// Generate suggestions
	if report.Bottleneck == "decode" {
		report.Suggestions = append(report.Suggestions, "Consider using DuckDB for decoding")
		report.Suggestions = append(report.Suggestions, "Try increasing batch size")
	}
	if report.Bottleneck == "write" {
		report.Suggestions = append(report.Suggestions, "Try faster compression (snappy/lz4)")
		report.Suggestions = append(report.Suggestions, "Consider using SSD storage")
	}

	return report
}

// PrintReport prints the profiler report.
func (r *ProfilerReport) Print(w io.Writer) {
	fmt.Fprintf(w, "\n=== Profiler Report ===\n")
	fmt.Fprintf(w, "Total Time: %v\n", r.TotalTime)
	fmt.Fprintf(w, "\nPhase Breakdown:\n")

	for _, phase := range r.Phases {
		fmt.Fprintf(w, "  %-15s %10v (%5.1f%%) [%d calls]\n",
			phase.Name, phase.TotalTime, phase.Percentage, phase.Calls)
	}

	fmt.Fprintf(w, "\nBottleneck: %s\n", r.Bottleneck)

	if len(r.Suggestions) > 0 {
		fmt.Fprintf(w, "\nSuggestions:\n")
		for _, s := range r.Suggestions {
			fmt.Fprintf(w, "  • %s\n", s)
		}
	}
}
