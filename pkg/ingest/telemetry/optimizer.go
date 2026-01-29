package telemetry

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// Optimizer automatically tunes pipeline parameters based on runtime metrics.
type Optimizer struct {
	mu sync.RWMutex

	// Current configuration
	config OptimizedConfig

	// History of performance samples
	samples []PerformanceSample

	// Constraints
	maxMemoryMB int64
	targetLatencyMs int64

	// State
	lastOptimization time.Time
	optimizationCount int
}

// OptimizedConfig contains auto-tuned parameters.
type OptimizedConfig struct {
	BatchSize        int
	Compression      core.Compression
	Concurrency      int
	BufferSize       int
	RowGroupSize     int
	UseStreaming     bool

	// Computed metrics
	ExpectedRowsPerSec float64
	ExpectedMemoryMB   float64
}

// PerformanceSample records performance at a point in time.
type PerformanceSample struct {
	Timestamp     time.Time
	BatchSize     int
	Compression   core.Compression
	RowsPerSecond float64
	MemoryMB      float64
	ErrorRate     float64
	Latency       time.Duration
}

// NewOptimizer creates an optimizer.
func NewOptimizer() *Optimizer {
	return &Optimizer{
		config: DefaultOptimizedConfig(),
		maxMemoryMB: int64(runtime.GOMAXPROCS(0)) * 512, // 512MB per CPU
		targetLatencyMs: 100,
	}
}

// DefaultOptimizedConfig returns initial configuration.
func DefaultOptimizedConfig() OptimizedConfig {
	return OptimizedConfig{
		BatchSize:    8192,
		Compression:  core.CompressionSnappy,
		Concurrency:  runtime.GOMAXPROCS(0),
		BufferSize:   256 * 1024,
		RowGroupSize: 10000,
		UseStreaming: true,
	}
}

// RecordSample records a performance sample.
func (o *Optimizer) RecordSample(sample PerformanceSample) {
	o.mu.Lock()
	defer o.mu.Unlock()

	sample.Timestamp = time.Now()
	o.samples = append(o.samples, sample)

	// Keep only recent samples (last 100)
	if len(o.samples) > 100 {
		o.samples = o.samples[len(o.samples)-100:]
	}
}

// Optimize runs the optimization algorithm.
func (o *Optimizer) Optimize() OptimizedConfig {
	o.mu.Lock()
	defer o.mu.Unlock()

	if len(o.samples) < 3 {
		return o.config
	}

	// Don't optimize too frequently
	if time.Since(o.lastOptimization) < 10*time.Second {
		return o.config
	}

	// Analyze recent samples
	recent := o.samples[len(o.samples)-min(10, len(o.samples)):]

	// Calculate averages
	var avgRowsPerSec, avgMemory, avgLatency float64
	for _, s := range recent {
		avgRowsPerSec += s.RowsPerSecond
		avgMemory += s.MemoryMB
		avgLatency += float64(s.Latency.Milliseconds())
	}
	avgRowsPerSec /= float64(len(recent))
	avgMemory /= float64(len(recent))
	avgLatency /= float64(len(recent))

	newConfig := o.config

	// Optimize batch size based on throughput
	if avgRowsPerSec > 0 {
		// If memory is low and throughput is good, increase batch size
		if avgMemory < float64(o.maxMemoryMB)*0.5 && o.config.BatchSize < 100000 {
			newConfig.BatchSize = min(o.config.BatchSize*2, 100000)
		}

		// If memory is high, decrease batch size
		if avgMemory > float64(o.maxMemoryMB)*0.8 && o.config.BatchSize > 1000 {
			newConfig.BatchSize = max(o.config.BatchSize/2, 1000)
		}
	}

	// Optimize compression based on CPU vs I/O bound
	// If write is the bottleneck, use faster compression
	if avgLatency > float64(o.targetLatencyMs)*1.5 {
		if o.config.Compression == core.CompressionZstd || o.config.Compression == core.CompressionGzip {
			newConfig.Compression = core.CompressionSnappy
		}
	}

	// Optimize concurrency based on CPU utilization
	// This is simplified - real implementation would check actual CPU usage
	numCPU := runtime.NumCPU()
	if avgMemory < float64(o.maxMemoryMB)*0.3 && o.config.Concurrency < numCPU {
		newConfig.Concurrency = min(o.config.Concurrency+1, numCPU)
	}
	if avgMemory > float64(o.maxMemoryMB)*0.9 && o.config.Concurrency > 1 {
		newConfig.Concurrency = max(o.config.Concurrency-1, 1)
	}

	// Update expected metrics
	newConfig.ExpectedRowsPerSec = avgRowsPerSec * 1.1 // Expect 10% improvement
	newConfig.ExpectedMemoryMB = avgMemory

	o.config = newConfig
	o.lastOptimization = time.Now()
	o.optimizationCount++

	return newConfig
}

// GetConfig returns current optimized configuration.
func (o *Optimizer) GetConfig() OptimizedConfig {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.config
}

// SetConstraints sets optimization constraints.
func (o *Optimizer) SetConstraints(maxMemoryMB, targetLatencyMs int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.maxMemoryMB = maxMemoryMB
	o.targetLatencyMs = targetLatencyMs
}

// Statistics returns optimizer statistics.
func (o *Optimizer) Statistics() OptimizerStats {
	o.mu.RLock()
	defer o.mu.RUnlock()

	return OptimizerStats{
		SampleCount:       len(o.samples),
		OptimizationCount: o.optimizationCount,
		LastOptimization:  o.lastOptimization,
		CurrentConfig:     o.config,
	}
}

// OptimizerStats contains optimizer statistics.
type OptimizerStats struct {
	SampleCount       int
	OptimizationCount int
	LastOptimization  time.Time
	CurrentConfig     OptimizedConfig
}

// String returns string representation of config.
func (c OptimizedConfig) String() string {
	return fmt.Sprintf("batch=%d, compression=%s, concurrency=%d, streaming=%v",
		c.BatchSize, c.Compression, c.Concurrency, c.UseStreaming)
}

// AutoTuner continuously monitors and optimizes the pipeline.
type AutoTuner struct {
	optimizer *Optimizer
	metrics   *Metrics
	interval  time.Duration
	stopCh    chan struct{}
	running   bool
	mu        sync.Mutex
}

// NewAutoTuner creates an auto-tuner.
func NewAutoTuner(metrics *Metrics) *AutoTuner {
	return &AutoTuner{
		optimizer: NewOptimizer(),
		metrics:   metrics,
		interval:  5 * time.Second,
		stopCh:    make(chan struct{}),
	}
}

// Start begins auto-tuning.
func (t *AutoTuner) Start(ctx context.Context) {
	t.mu.Lock()
	if t.running {
		t.mu.Unlock()
		return
	}
	t.running = true
	t.mu.Unlock()

	go t.run(ctx)
}

// Stop stops auto-tuning.
func (t *AutoTuner) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.running {
		close(t.stopCh)
		t.running = false
	}
}

func (t *AutoTuner) run(ctx context.Context) {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.stopCh:
			return
		case <-ticker.C:
			t.sample()
		}
	}
}

func (t *AutoTuner) sample() {
	snapshot := t.metrics.Snapshot()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	config := t.optimizer.GetConfig()

	sample := PerformanceSample{
		BatchSize:     config.BatchSize,
		Compression:   config.Compression,
		RowsPerSecond: snapshot.RowsPerSecond,
		MemoryMB:      float64(memStats.Alloc) / 1024 / 1024,
		ErrorRate:     float64(snapshot.ErrorCount) / float64(max64(snapshot.RowsRead, 1)),
	}

	t.optimizer.RecordSample(sample)
	t.optimizer.Optimize()
}

// GetConfig returns current optimized config.
func (t *AutoTuner) GetConfig() OptimizedConfig {
	return t.optimizer.GetConfig()
}

// Optimizer returns the underlying optimizer.
func (t *AutoTuner) Optimizer() *Optimizer {
	return t.optimizer
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
