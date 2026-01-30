package ingest

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/logflow/logflow/pkg/ingest/detect"
	"github.com/logflow/logflow/pkg/ingest/flow"
)

// Pipeline orchestrates the ingestion flow.
type Pipeline struct {
	// Components
	detector   *Detector
	fastPath   *FastPath
	robustPath *RobustPath
	heuristics *HeuristicEngine

	// Enhanced detection (from pkg/ingest/detect)
	advDetector *detect.Detector

	// Flow control (from pkg/ingest/flow)
	concLimiter *flow.ConcurrencyLimiter

	// Configuration
	config *UnifiedConfig

	// Runtime state
	mu        sync.RWMutex
	running   atomic.Bool
	cancelled atomic.Bool

	// Metrics
	metrics *PipelineMetrics

	// Worker pool
	workers    int
	workerPool chan struct{}

	// Batch queue with backpressure
	batchQueue chan *batchWork
	queueSize  int

	// Progress tracking
	progress   atomic.Int64
	totalBytes atomic.Int64
	onProgress func(PipelineProgress)
}

// PipelineMetrics tracks pipeline performance.
type PipelineMetrics struct {
	mu sync.Mutex

	FilesProcessed  int64
	FilesFailed     int64
	RowsProcessed   int64
	BytesRead       int64
	BytesWritten    int64
	TotalDuration   time.Duration
	ParseDuration   time.Duration
	WriteDuration   time.Duration
	ErrorCount      int64
	RecoveredCount  int64
}

// PipelineProgress reports current progress.
type PipelineProgress struct {
	FilesTotal      int
	FilesCompleted  int
	FilesCurrent    string
	BytesTotal      int64
	BytesProcessed  int64
	RowsProcessed   int64
	PercentComplete float64
	CurrentRate     float64 // rows/sec
	EstimatedETA    time.Duration
	Errors          int
}

// batchWork represents a unit of work.
type batchWork struct {
	inputPath  string
	outputPath string
	analysis   *FileAnalysis
	opts       Options
	resultCh   chan *workResult
}

// workResult contains the outcome of a work unit.
type workResult struct {
	result *Result
	err    error
}

// NewPipeline creates a new pipeline.
func NewPipeline() (*Pipeline, error) {
	fastPath, err := NewFastPath()
	if err != nil {
		return nil, fmt.Errorf("failed to create fast path: %w", err)
	}

	workers := runtime.NumCPU() * 2
	queueSize := workers * 10

	return &Pipeline{
		detector:   NewDetector(),
		fastPath:   fastPath,
		robustPath: NewRobustPath(),
		heuristics: NewHeuristicEngine(),
		config:     GlobalConfig,
		workers:    workers,
		workerPool: make(chan struct{}, workers),
		batchQueue: make(chan *batchWork, queueSize),
		queueSize:  queueSize,
		metrics:    &PipelineMetrics{},
	}, nil
}

// Close releases pipeline resources.
func (p *Pipeline) Close() error {
	p.cancelled.Store(true)
	close(p.batchQueue)
	return p.fastPath.Close()
}

// SetWorkers sets the number of workers.
func (p *Pipeline) SetWorkers(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.workers = n
	p.workerPool = make(chan struct{}, n)
}

// SetProgressCallback sets progress callback.
func (p *Pipeline) SetProgressCallback(fn func(PipelineProgress)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onProgress = fn
}

// Process processes a single file.
func (p *Pipeline) Process(ctx context.Context, inputPath string, opts Options) (*Result, error) {
	if p.cancelled.Load() {
		return nil, fmt.Errorf("pipeline cancelled")
	}

	start := time.Now()

	// Step 1: Analyze
	analysis, err := p.detector.Analyze(inputPath)
	if err != nil {
		p.recordError()
		return nil, fmt.Errorf("analysis failed: %w", err)
	}

	// Step 2: Compute heuristics
	heur := p.heuristics.Compute(analysis)

	// Step 3: Apply heuristics to options
	p.applyHeuristics(&opts, heur)

	// Step 4: Generate output path
	outputPath := opts.OutputPath
	if outputPath == "" {
		outputPath = generateOutputPath(inputPath)
	}

	// Step 5: Acquire worker slot (backpressure)
	select {
	case p.workerPool <- struct{}{}:
		defer func() { <-p.workerPool }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Step 6: Execute
	var result *Result
	parseStart := time.Now()

	switch heur.Strategy {
	case StrategyFastDuckDB:
		result, err = p.fastPath.Process(ctx, inputPath, outputPath, analysis, opts)
	case StrategyRobustGo:
		result, err = p.robustPath.Process(ctx, inputPath, outputPath, analysis, opts)
	case StrategyStreaming:
		result, err = p.robustPath.Process(ctx, inputPath, outputPath, analysis, opts)
	case StrategyHybrid:
		result, err = p.fastPath.Process(ctx, inputPath, outputPath, analysis, opts)
		if err != nil {
			result, err = p.robustPath.Process(ctx, inputPath, outputPath, analysis, opts)
		}
	default:
		result, err = p.fastPath.Process(ctx, inputPath, outputPath, analysis, opts)
	}

	parseTime := time.Since(parseStart)

	if err != nil {
		p.recordError()
		return nil, err
	}

	// Step 7: Update metrics
	result.Duration = time.Since(start)
	result.Throughput = float64(result.RowCount) / result.Duration.Seconds()
	result.Speed = float64(result.InputSize) / result.Duration.Seconds() / (1024 * 1024)

	p.recordSuccess(result, parseTime)

	return result, nil
}

// ProcessBatch processes multiple files.
func (p *Pipeline) ProcessBatch(ctx context.Context, inputs []string, opts Options) ([]*Result, error) {
	if len(inputs) == 0 {
		return nil, nil
	}

	p.running.Store(true)
	defer p.running.Store(false)

	// Calculate total size for progress
	var totalSize int64
	for _, input := range inputs {
		if analysis, err := p.detector.Analyze(input); err == nil {
			totalSize += analysis.Size
		}
	}
	p.totalBytes.Store(totalSize)

	// Process files
	results := make([]*Result, len(inputs))
	errors := make([]error, len(inputs))

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, p.workers)

	for i, input := range inputs {
		if p.cancelled.Load() {
			break
		}

		wg.Add(1)
		go func(idx int, inputPath string) {
			defer wg.Done()

			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				errors[idx] = ctx.Err()
				return
			}

			fileOpts := opts
			result, err := p.Process(ctx, inputPath, fileOpts)
			results[idx] = result
			errors[idx] = err

			// Update progress
			if result != nil {
				p.progress.Add(result.InputSize)
				p.reportProgress(len(inputs), idx+1, inputPath)
			}
		}(i, input)
	}

	wg.Wait()

	// Collect errors
	var firstErr error
	for _, err := range errors {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return results, firstErr
}

// ProcessGlob processes files matching a pattern.
func (p *Pipeline) ProcessGlob(ctx context.Context, pattern string, opts Options) ([]*Result, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid glob pattern: %w", err)
	}

	if len(matches) == 0 {
		return nil, fmt.Errorf("no files match pattern: %s", pattern)
	}

	return p.ProcessBatch(ctx, matches, opts)
}

// applyHeuristics applies computed heuristics to options.
func (p *Pipeline) applyHeuristics(opts *Options, heur *Heuristics) {
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
}

// recordSuccess records successful processing.
func (p *Pipeline) recordSuccess(result *Result, parseTime time.Duration) {
	p.metrics.mu.Lock()
	defer p.metrics.mu.Unlock()

	p.metrics.FilesProcessed++
	p.metrics.RowsProcessed += result.RowCount
	p.metrics.BytesRead += result.InputSize
	p.metrics.BytesWritten += result.OutputSize
	p.metrics.TotalDuration += result.Duration
	p.metrics.ParseDuration += parseTime
	p.metrics.WriteDuration += result.Duration - parseTime
}

// recordError records an error.
func (p *Pipeline) recordError() {
	p.metrics.mu.Lock()
	defer p.metrics.mu.Unlock()
	p.metrics.FilesFailed++
	p.metrics.ErrorCount++
}

// reportProgress reports current progress.
func (p *Pipeline) reportProgress(total, completed int, current string) {
	p.mu.RLock()
	callback := p.onProgress
	p.mu.RUnlock()

	if callback == nil {
		return
	}

	processed := p.progress.Load()
	totalBytes := p.totalBytes.Load()

	var pct float64
	if totalBytes > 0 {
		pct = float64(processed) / float64(totalBytes) * 100
	}

	p.metrics.mu.Lock()
	rows := p.metrics.RowsProcessed
	dur := p.metrics.TotalDuration
	errs := int(p.metrics.ErrorCount)
	p.metrics.mu.Unlock()

	var rate float64
	if dur > 0 {
		rate = float64(rows) / dur.Seconds()
	}

	var eta time.Duration
	if pct > 0 && dur > 0 {
		eta = time.Duration(float64(dur) / pct * (100 - pct))
	}

	callback(PipelineProgress{
		FilesTotal:      total,
		FilesCompleted:  completed,
		FilesCurrent:    current,
		BytesTotal:      totalBytes,
		BytesProcessed:  processed,
		RowsProcessed:   rows,
		PercentComplete: pct,
		CurrentRate:     rate,
		EstimatedETA:    eta,
		Errors:          errs,
	})
}

// Metrics returns current metrics.
func (p *Pipeline) Metrics() PipelineMetrics {
	p.metrics.mu.Lock()
	defer p.metrics.mu.Unlock()
	return *p.metrics
}

// Cancel cancels the pipeline.
func (p *Pipeline) Cancel() {
	p.cancelled.Store(true)
}

// IsRunning returns whether the pipeline is running.
func (p *Pipeline) IsRunning() bool {
	return p.running.Load()
}

// SmallFileBatcher batches small files for efficient processing.
type SmallFileBatcher struct {
	threshold int64 // Size threshold for "small"
	maxBatch  int   // Max files per batch
	files     []string
	sizes     []int64
	totalSize int64
}

// NewSmallFileBatcher creates a batcher.
func NewSmallFileBatcher(threshold int64, maxBatch int) *SmallFileBatcher {
	return &SmallFileBatcher{
		threshold: threshold,
		maxBatch:  maxBatch,
		files:     make([]string, 0),
		sizes:     make([]int64, 0),
	}
}

// Add adds a file to the batcher.
func (b *SmallFileBatcher) Add(path string, size int64) bool {
	if size > b.threshold {
		return false // Too large
	}
	if len(b.files) >= b.maxBatch {
		return false // Batch full
	}

	b.files = append(b.files, path)
	b.sizes = append(b.sizes, size)
	b.totalSize += size
	return true
}

// Files returns batched files.
func (b *SmallFileBatcher) Files() []string {
	return b.files
}

// TotalSize returns total size of batched files.
func (b *SmallFileBatcher) TotalSize() int64 {
	return b.totalSize
}

// Count returns number of files.
func (b *SmallFileBatcher) Count() int {
	return len(b.files)
}

// Reset clears the batcher.
func (b *SmallFileBatcher) Reset() {
	b.files = b.files[:0]
	b.sizes = b.sizes[:0]
	b.totalSize = 0
}
