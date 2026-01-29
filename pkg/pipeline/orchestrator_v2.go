package pipeline

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	lferrors "github.com/logflow/logflow/pkg/errors"
)

// OrchestratorV2 is a production-grade pipeline orchestrator with:
// - errgroup for coordinated shutdown
// - Buffered channels to prevent goroutine leaks
// - Metrics collection
// - Graceful degradation
type OrchestratorV2 struct {
	cfg        Config
	source     Source
	sink       Sink
	processors []Processor
	inspectors []*PassthroughInspector

	// Configuration
	bufferSize int
	batchSize  int

	// Metrics
	metrics *PipelineMetrics

	// State
	running atomic.Bool
}

// PipelineMetrics collects runtime statistics.
type PipelineMetrics struct {
	StartTime       time.Time
	EndTime         time.Time
	EventsRead      atomic.Int64
	EventsWritten   atomic.Int64
	EventsDropped   atomic.Int64
	BytesRead       atomic.Int64
	BytesWritten    atomic.Int64
	ErrorCount      atomic.Int64
	GoroutinesStart int
	GoroutinesEnd   int
}

// NewOrchestratorV2 creates a production-grade orchestrator.
func NewOrchestratorV2(cfg Config) *OrchestratorV2 {
	bufSize := cfg.BufferSize
	if bufSize <= 0 {
		bufSize = 4096
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1024
	}

	return &OrchestratorV2{
		cfg:        cfg,
		processors: make([]Processor, 0),
		inspectors: make([]*PassthroughInspector, 0),
		bufferSize: bufSize,
		batchSize:  batchSize,
		metrics:    &PipelineMetrics{},
	}
}

// SetSource sets the data source.
func (o *OrchestratorV2) SetSource(s Source) *OrchestratorV2 {
	o.source = s
	return o
}

// SetSink sets the data sink.
func (o *OrchestratorV2) SetSink(s Sink) *OrchestratorV2 {
	o.sink = s
	return o
}

// AddProcessor adds a processor to the pipeline.
func (o *OrchestratorV2) AddProcessor(p Processor) *OrchestratorV2 {
	o.processors = append(o.processors, p)
	return o
}

// AddInspector adds an inspector that passes through events.
func (o *OrchestratorV2) AddInspector(i Inspector) *OrchestratorV2 {
	pt := NewPassthroughInspector(i)
	o.inspectors = append(o.inspectors, pt)
	o.processors = append(o.processors, pt)
	return o
}

// Run executes the pipeline with production-grade error handling.
func (o *OrchestratorV2) Run(ctx context.Context) error {
	if o.running.Load() {
		return lferrors.New(lferrors.CodeParseFailed, "pipeline already running")
	}
	o.running.Store(true)
	defer o.running.Store(false)

	// Validate configuration
	if err := o.validate(); err != nil {
		return err
	}

	// Initialize metrics
	o.metrics.StartTime = time.Now()
	o.metrics.GoroutinesStart = runtime.NumGoroutine()

	// Open source reader
	reader, cleanup, err := o.openSource()
	if err != nil {
		return err
	}
	defer cleanup()

	// Create errgroup with context for coordinated shutdown
	// When any goroutine returns an error, the context is canceled
	// and all other goroutines receive the cancellation signal
	g, ctx := errgroup.WithContext(ctx)

	// Create buffered channel chain
	// Buffer size of 1 minimum prevents goroutine leaks on early return
	channels := o.createChannelChain()

	// Start source goroutine
	g.Go(func() error {
		defer close(channels[0])
		return o.runSource(ctx, reader, channels[0])
	})

	// Start processor goroutines
	for i, proc := range o.processors {
		inChan := channels[i]
		outChan := channels[i+1]
		processor := proc
		idx := i

		g.Go(func() error {
			defer close(outChan)
			return o.runProcessor(ctx, processor, inChan, outChan, idx)
		})
	}

	// Start sink goroutine
	finalChan := channels[len(channels)-1]
	g.Go(func() error {
		return o.runSink(ctx, finalChan)
	})

	// Wait for all goroutines to complete
	err = g.Wait()

	// Finalize metrics
	o.metrics.EndTime = time.Now()
	o.metrics.GoroutinesEnd = runtime.NumGoroutine()

	// Close sink
	if closeErr := o.sink.Close(); closeErr != nil && err == nil {
		err = lferrors.Wrap(closeErr, lferrors.CodeWriteFailed, "failed to close sink")
	}

	// Check for goroutine leaks
	if leaked := o.metrics.GoroutinesEnd - o.metrics.GoroutinesStart; leaked > 0 {
		// Log warning but don't fail
		fmt.Printf("WARNING: %d goroutines may have leaked\n", leaked)
	}

	return err
}

// validate checks that the pipeline is properly configured.
func (o *OrchestratorV2) validate() error {
	if o.source == nil {
		return lferrors.New(lferrors.CodeParseFailed, "no source configured")
	}
	if o.sink == nil {
		return lferrors.New(lferrors.CodeParseFailed, "no sink configured")
	}
	return nil
}

// openSource opens the source file/reader.
func (o *OrchestratorV2) openSource() (io.Reader, func(), error) {
	if o.cfg.SourcePath == "-" {
		return os.Stdin, func() {}, nil
	}

	file, err := os.Open(o.cfg.SourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, lferrors.FileNotFound(o.cfg.SourcePath)
		}
		if os.IsPermission(err) {
			return nil, nil, lferrors.Wrap(err, lferrors.CodeFilePermission, "permission denied")
		}
		return nil, nil, lferrors.Wrap(err, lferrors.CodeFileNotFound, "failed to open source")
	}

	cleanup := func() { file.Close() }
	return file, cleanup, nil
}

// createChannelChain creates the buffered channel chain.
func (o *OrchestratorV2) createChannelChain() []chan *Event {
	numStages := len(o.processors) + 1
	channels := make([]chan *Event, numStages)

	for i := range channels {
		// Use buffered channels to prevent blocking on cancellation
		// This is critical for preventing goroutine leaks
		channels[i] = make(chan *Event, o.bufferSize)
	}

	return channels
}

// runSource runs the source stage with error handling.
func (o *OrchestratorV2) runSource(ctx context.Context, reader io.Reader, out chan<- *Event) error {
	err := o.source.Read(ctx, reader, out)
	if err != nil {
		if ctx.Err() != nil {
			return lferrors.ContextCanceled("source read")
		}
		return lferrors.Wrap(err, lferrors.CodeParseFailed, fmt.Sprintf("source %s failed", o.source.Name()))
	}
	return nil
}

// runProcessor runs a processor stage with error handling.
func (o *OrchestratorV2) runProcessor(ctx context.Context, proc Processor, in <-chan *Event, out chan<- *Event, idx int) error {
	err := proc.Process(ctx, in, out)
	if err != nil {
		if ctx.Err() != nil {
			return lferrors.ContextCanceled(fmt.Sprintf("processor %s", proc.Name()))
		}
		return lferrors.Wrap(err, lferrors.CodeTransformFailed,
			fmt.Sprintf("processor %s (stage %d) failed", proc.Name(), idx))
	}
	return nil
}

// runSink runs the sink stage with error handling.
func (o *OrchestratorV2) runSink(ctx context.Context, in <-chan *Event) error {
	err := o.sink.Write(ctx, in)
	if err != nil {
		if ctx.Err() != nil {
			return lferrors.ContextCanceled("sink write")
		}
		return lferrors.Wrap(err, lferrors.CodeWriteFailed, "sink failed")
	}
	return nil
}

// Metrics returns the pipeline metrics.
func (o *OrchestratorV2) Metrics() *PipelineMetrics {
	return o.metrics
}

// GetInspectorReports returns reports from all inspectors.
func (o *OrchestratorV2) GetInspectorReports() map[string]interface{} {
	reports := make(map[string]interface{})
	for _, inspector := range o.inspectors {
		reports[inspector.Name()] = inspector.Report()
	}
	return reports
}

// Duration returns the pipeline execution duration.
func (m *PipelineMetrics) Duration() time.Duration {
	if m.EndTime.IsZero() {
		return time.Since(m.StartTime)
	}
	return m.EndTime.Sub(m.StartTime)
}

// EventsPerSecond returns the processing rate.
func (m *PipelineMetrics) EventsPerSecond() float64 {
	duration := m.Duration().Seconds()
	if duration == 0 {
		return 0
	}
	return float64(m.EventsWritten.Load()) / duration
}

// Summary returns a human-readable summary.
func (m *PipelineMetrics) Summary() string {
	return fmt.Sprintf(
		"Processed %d events in %s (%.0f events/sec)",
		m.EventsWritten.Load(),
		m.Duration().Round(time.Millisecond),
		m.EventsPerSecond(),
	)
}

// --- Panic Recovery Wrapper ---

// SafeProcessor wraps a processor with panic recovery.
type SafeProcessor struct {
	inner Processor
}

// NewSafeProcessor creates a panic-safe processor wrapper.
func NewSafeProcessor(p Processor) *SafeProcessor {
	return &SafeProcessor{inner: p}
}

// Name returns the wrapped processor's name.
func (s *SafeProcessor) Name() string {
	return s.inner.Name()
}

// Process runs the processor with panic recovery.
func (s *SafeProcessor) Process(ctx context.Context, in <-chan *Event, out chan<- *Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = lferrors.New(lferrors.CodePanic, fmt.Sprintf("processor panic: %v", r))
		}
	}()

	return s.inner.Process(ctx, in, out)
}

// --- Rate Limiter for Backpressure ---

// RateLimitedProcessor applies backpressure to prevent overwhelming downstream.
type RateLimitedProcessor struct {
	maxEventsPerSecond int
	lastEvent          time.Time
	interval           time.Duration
}

// NewRateLimitedProcessor creates a rate-limited passthrough.
func NewRateLimitedProcessor(eventsPerSecond int) *RateLimitedProcessor {
	return &RateLimitedProcessor{
		maxEventsPerSecond: eventsPerSecond,
		interval:           time.Second / time.Duration(eventsPerSecond),
	}
}

// Name returns the processor name.
func (r *RateLimitedProcessor) Name() string {
	return "rate_limiter"
}

// Process applies rate limiting.
func (r *RateLimitedProcessor) Process(ctx context.Context, in <-chan *Event, out chan<- *Event) error {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-in:
			if !ok {
				return nil
			}

			// Apply rate limiting
			if elapsed := time.Since(r.lastEvent); elapsed < r.interval {
				select {
				case <-time.After(r.interval - elapsed):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			r.lastEvent = time.Now()

			select {
			case out <- event:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
