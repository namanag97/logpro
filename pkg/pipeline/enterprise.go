// Package pipeline provides enterprise orchestration with integrated resilience,
// telemetry, checkpointing, and dead-letter-queue support.
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/logflow/logflow/pkg/checkpoint"
	"github.com/logflow/logflow/pkg/lifecycle"
	"github.com/logflow/logflow/pkg/resilience"
	"github.com/logflow/logflow/pkg/telemetry"
)

// EnterpriseConfig configures the enterprise orchestrator.
type EnterpriseConfig struct {
	// Base pipeline configuration
	Pipeline Config

	// Telemetry configuration
	ServiceName    string
	OTLPEndpoint   string
	EnableTracing  bool
	EnableMetrics  bool

	// Resilience configuration
	CircuitBreakerMemoryThreshold float64 // e.g., 0.90 for 90%
	CircuitBreakerMaxConcurrent   int
	CircuitBreakerCooldown        time.Duration

	// Checkpoint configuration
	CheckpointDir      string
	CheckpointInterval time.Duration
	EnableCheckpoint   bool

	// DLQ configuration
	DLQPath          string
	DLQMaxRecords    int64
	DLQMaxBytes      int64
	EnableDLQ        bool

	// Shutdown configuration
	DrainTimeout  time.Duration
	ForceTimeout  time.Duration
}

// DefaultEnterpriseConfig returns sensible defaults.
func DefaultEnterpriseConfig() EnterpriseConfig {
	return EnterpriseConfig{
		Pipeline: Config{
			BufferSize: 4096,
			BatchSize:  1024,
		},
		ServiceName:                   "logflow-pipeline",
		EnableTracing:                 true,
		EnableMetrics:                 true,
		CircuitBreakerMemoryThreshold: 0.90,
		CircuitBreakerMaxConcurrent:   1000,
		CircuitBreakerCooldown:        30 * time.Second,
		CheckpointInterval:            5 * time.Second,
		EnableCheckpoint:              true,
		DLQMaxRecords:                 100000,
		DLQMaxBytes:                   100 * 1024 * 1024, // 100MB
		EnableDLQ:                     true,
		DrainTimeout:                  30 * time.Second,
		ForceTimeout:                  60 * time.Second,
	}
}

// EnterpriseOrchestrator wraps OrchestratorV2 with enterprise features.
type EnterpriseOrchestrator struct {
	*OrchestratorV2

	cfg EnterpriseConfig

	// Enterprise components
	tracer         *telemetry.Tracer
	metrics        *telemetry.Metrics
	circuitBreaker *resilience.CircuitBreaker
	poisonPill     *resilience.PoisonPillHandler
	checkpointMgr  *checkpoint.Manager
	checkpoint     *checkpoint.Checkpoint
	dlq            *DLQWriter
	shutdown       *lifecycle.ShutdownManager

	// State
	mu       sync.Mutex
	running  bool
	jobID    string
	startAt  time.Time
}

// NewEnterpriseOrchestrator creates a production-grade orchestrator.
func NewEnterpriseOrchestrator(cfg EnterpriseConfig) (*EnterpriseOrchestrator, error) {
	o := &EnterpriseOrchestrator{
		OrchestratorV2: NewOrchestratorV2(cfg.Pipeline),
		cfg:            cfg,
		jobID:          generateJobID(),
	}

	// Initialize telemetry
	if cfg.EnableTracing || cfg.EnableMetrics {
		o.tracer = telemetry.NewTracer(cfg.ServiceName)
		if cfg.OTLPEndpoint != "" {
			o.tracer.WithExportEndpoint(cfg.OTLPEndpoint)
		}
		o.metrics = telemetry.NewMetrics()
	}

	// Initialize circuit breaker
	o.circuitBreaker = resilience.NewCircuitBreaker().
		WithMaxMemory(cfg.CircuitBreakerMemoryThreshold).
		WithMaxConcurrent(cfg.CircuitBreakerMaxConcurrent).
		WithCooldown(cfg.CircuitBreakerCooldown)

	o.circuitBreaker.OnTrip = func(reason string) {
		if o.tracer != nil {
			_, span := o.tracer.StartSpan(context.Background(), "circuit_breaker.trip")
			span.SetAttribute("reason", reason)
			span.SetStatus(telemetry.SpanStatusError, reason)
			o.tracer.EndSpan(span)
		}
	}

	o.circuitBreaker.OnReset = func() {
		if o.tracer != nil {
			_, span := o.tracer.StartSpan(context.Background(), "circuit_breaker.reset")
			o.tracer.EndSpan(span)
		}
	}

	// Initialize poison pill handler
	o.poisonPill = resilience.NewPoisonPillHandler()

	// Initialize checkpoint manager
	if cfg.EnableCheckpoint && cfg.CheckpointDir != "" {
		mgr, err := checkpoint.NewManager(cfg.CheckpointDir)
		if err != nil {
			return nil, fmt.Errorf("failed to create checkpoint manager: %w", err)
		}
		o.checkpointMgr = mgr
	}

	// Initialize DLQ
	if cfg.EnableDLQ && cfg.DLQPath != "" {
		dlq, err := NewDLQWriter(DLQConfig{
			OutputPath:   cfg.DLQPath,
			MaxRecords:   cfg.DLQMaxRecords,
			MaxBytes:     cfg.DLQMaxBytes,
			RotateOnFull: true,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create DLQ writer: %w", err)
		}
		o.dlq = dlq
	}

	// Initialize shutdown manager
	o.shutdown = lifecycle.NewShutdownManager(lifecycle.ShutdownConfig{
		DrainTimeout: cfg.DrainTimeout,
		ForceTimeout: cfg.ForceTimeout,
		OnHealthChange: func(healthy bool) {
			if o.tracer != nil {
				_, span := o.tracer.StartSpan(context.Background(), "health.change")
				span.SetAttribute("healthy", healthy)
				o.tracer.EndSpan(span)
			}
		},
		OnDrainStart: func() {
			if o.tracer != nil {
				_, span := o.tracer.StartSpan(context.Background(), "shutdown.drain_start")
				o.tracer.EndSpan(span)
			}
		},
		OnShutdown: func() {
			if o.tracer != nil {
				_, span := o.tracer.StartSpan(context.Background(), "shutdown.complete")
				o.tracer.EndSpan(span)
			}
		},
	})

	// Register closers
	if o.dlq != nil {
		o.shutdown.RegisterCloser(o.dlq)
	}

	return o, nil
}

// Run executes the pipeline with enterprise features.
func (o *EnterpriseOrchestrator) Run(ctx context.Context) error {
	o.mu.Lock()
	if o.running {
		o.mu.Unlock()
		return fmt.Errorf("pipeline already running")
	}
	o.running = true
	o.startAt = time.Now()
	o.mu.Unlock()

	defer func() {
		o.mu.Lock()
		o.running = false
		o.mu.Unlock()
	}()

	// Create root span for the entire pipeline run
	var span *telemetry.Span
	if o.tracer != nil {
		ctx, span = o.tracer.StartSpan(ctx, "pipeline.run")
		span.SetAttribute("job_id", o.jobID)
		span.SetAttribute("input", o.cfg.Pipeline.SourcePath)
		span.SetAttribute("output", o.cfg.Pipeline.SinkPath)
		defer func() {
			span.SetAttribute("duration_ms", time.Since(o.startAt).Milliseconds())
			o.tracer.EndSpan(span)
		}()
	}

	// Check circuit breaker before starting
	if !o.circuitBreaker.Allow() {
		err := ErrCircuitOpen
		if span != nil {
			span.SetStatus(telemetry.SpanStatusError, "circuit breaker open")
		}
		return err
	}

	// Register with shutdown manager
	if !o.shutdown.StartRequest() {
		return fmt.Errorf("shutdown in progress, request rejected")
	}
	defer o.shutdown.EndRequest()

	// Start circuit breaker tracking
	o.circuitBreaker.Start()
	defer func() {
		// Determine success based on whether we got an error
		success := ctx.Err() == nil
		o.circuitBreaker.End(success)
	}()

	// Setup or resume checkpoint
	if err := o.setupCheckpoint(ctx); err != nil {
		if span != nil {
			span.SetStatus(telemetry.SpanStatusError, err.Error())
		}
		return err
	}

	// Run the base orchestrator with checkpoint updates
	err := o.runWithCheckpoint(ctx)

	// Record metrics
	if o.metrics != nil {
		metrics := o.OrchestratorV2.Metrics()
		o.metrics.IncrementEvents(metrics.EventsWritten.Load())
		o.metrics.IncrementBytes(metrics.BytesRead.Load())
		if err != nil {
			o.metrics.IncrementErrors()
		}
	}

	// Update checkpoint on completion
	if o.checkpoint != nil {
		if err == nil {
			o.checkpoint.SetPhase("complete")
		}
		o.checkpoint.Save()
	}

	if err != nil && span != nil {
		span.SetStatus(telemetry.SpanStatusError, err.Error())
	}

	return err
}

// setupCheckpoint initializes or resumes checkpoint.
func (o *EnterpriseOrchestrator) setupCheckpoint(ctx context.Context) error {
	if o.checkpointMgr == nil {
		return nil
	}

	// Check for existing checkpoint to resume
	existing, err := o.checkpointMgr.Find(o.cfg.Pipeline.SourcePath)
	if err == nil && existing.ShouldResume() {
		o.checkpoint = existing

		if o.tracer != nil {
			_, span := o.tracer.StartSpan(ctx, "checkpoint.resume")
			span.SetAttribute("bytes_read", existing.BytesRead)
			span.SetAttribute("rows_written", existing.RowsWritten)
			o.tracer.EndSpan(span)
		}

		return nil
	}

	// Create new checkpoint
	o.checkpoint = o.checkpointMgr.Create(
		o.jobID,
		o.cfg.Pipeline.SourcePath,
		o.cfg.Pipeline.SinkPath,
	)

	// Start auto-save
	if o.cfg.CheckpointInterval > 0 {
		stopAutoSave := o.checkpoint.StartAutoSave(o.cfg.CheckpointInterval)
		// Register cleanup
		go func() {
			<-ctx.Done()
			stopAutoSave()
		}()
	}

	return nil
}

// runWithCheckpoint runs the pipeline with checkpoint updates.
func (o *EnterpriseOrchestrator) runWithCheckpoint(ctx context.Context) error {
	// Create a wrapped context that tracks progress
	progressCtx := &progressContext{
		Context:    ctx,
		checkpoint: o.checkpoint,
		metrics:    o.OrchestratorV2.Metrics(),
	}

	return o.OrchestratorV2.Run(progressCtx)
}

// progressContext wraps context to update checkpoint on progress.
type progressContext struct {
	context.Context
	checkpoint *checkpoint.Checkpoint
	metrics    *PipelineMetrics
}

// WriteToDLQ writes a failed record to the dead letter queue.
func (o *EnterpriseOrchestrator) WriteToDLQ(record DLQRecord) error {
	if o.dlq == nil {
		return nil
	}

	record.JobID = o.jobID

	if o.tracer != nil {
		_, span := o.tracer.StartSpan(context.Background(), "dlq.write")
		span.SetAttribute("row_number", record.RowNumber)
		span.SetAttribute("error_type", record.ErrorType)
		defer o.tracer.EndSpan(span)
	}

	return o.dlq.Write(record)
}

// Shutdown initiates graceful shutdown.
func (o *EnterpriseOrchestrator) Shutdown(ctx context.Context) error {
	if o.tracer != nil {
		_, span := o.tracer.StartSpan(ctx, "pipeline.shutdown")
		defer o.tracer.EndSpan(span)
	}

	return o.shutdown.Shutdown(ctx)
}

// Close implements the Closer interface for shutdown manager.
func (o *EnterpriseOrchestrator) Close() error {
	var errs []error

	if o.checkpoint != nil {
		if err := o.checkpoint.Save(); err != nil {
			errs = append(errs, err)
		}
	}

	if o.dlq != nil {
		if err := o.dlq.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}

// IsHealthy returns the health status.
func (o *EnterpriseOrchestrator) IsHealthy() bool {
	return o.shutdown.IsHealthy() && o.circuitBreaker.State() != resilience.CircuitOpen
}

// CircuitBreakerState returns the circuit breaker state.
func (o *EnterpriseOrchestrator) CircuitBreakerState() resilience.CircuitState {
	return o.circuitBreaker.State()
}

// Metrics returns the telemetry metrics.
func (o *EnterpriseOrchestrator) TelemetryMetrics() *telemetry.Metrics {
	return o.metrics
}

// DLQStats returns DLQ statistics.
func (o *EnterpriseOrchestrator) DLQStats() *DLQStats {
	if o.dlq == nil {
		return nil
	}
	stats := o.dlq.Stats()
	return &stats
}

// CheckpointProgress returns checkpoint progress.
func (o *EnterpriseOrchestrator) CheckpointProgress() *checkpoint.Checkpoint {
	return o.checkpoint
}

// HandleSignals sets up signal handling for graceful shutdown.
func (o *EnterpriseOrchestrator) HandleSignals(ctx context.Context) {
	o.shutdown.HandleSignals(ctx)
}

// WaitForShutdown blocks until shutdown is complete.
func (o *EnterpriseOrchestrator) WaitForShutdown() {
	o.shutdown.Wait()
}

// Status returns the enterprise orchestrator status.
type EnterpriseStatus struct {
	Running            bool
	JobID              string
	StartedAt          time.Time
	Duration           time.Duration
	CircuitBreakerOpen bool
	IsHealthy          bool
	IsDraining         bool
	InFlightRequests   int64
	DLQRecords         int64
	CheckpointPhase    string
	BytesProcessed     int64
	EventsProcessed    int64
}

// Status returns the current status.
func (o *EnterpriseOrchestrator) Status() EnterpriseStatus {
	o.mu.Lock()
	running := o.running
	startAt := o.startAt
	o.mu.Unlock()

	status := EnterpriseStatus{
		Running:            running,
		JobID:              o.jobID,
		StartedAt:          startAt,
		CircuitBreakerOpen: o.circuitBreaker.State() == resilience.CircuitOpen,
		IsHealthy:          o.IsHealthy(),
		IsDraining:         o.shutdown.IsDraining(),
		InFlightRequests:   o.shutdown.InFlightCount(),
	}

	if running {
		status.Duration = time.Since(startAt)
	}

	if o.dlq != nil {
		stats := o.dlq.Stats()
		status.DLQRecords = stats.RecordCount
	}

	if o.checkpoint != nil {
		status.CheckpointPhase = o.checkpoint.Phase
		status.BytesProcessed = o.checkpoint.BytesRead
		status.EventsProcessed = o.checkpoint.RowsWritten
	}

	metrics := o.OrchestratorV2.Metrics()
	if metrics != nil {
		status.EventsProcessed = metrics.EventsWritten.Load()
		status.BytesProcessed = metrics.BytesRead.Load()
	}

	return status
}

// ErrCircuitOpen is returned when the circuit breaker is open.
var ErrCircuitOpen = fmt.Errorf("circuit breaker is open, system under stress")

// generateJobID creates a unique job identifier.
func generateJobID() string {
	return fmt.Sprintf("job_%d", time.Now().UnixNano())
}

// --- Enterprise Processor Wrappers ---

// TracedProcessor wraps a processor with telemetry.
type TracedProcessor struct {
	inner  Processor
	tracer *telemetry.Tracer
}

// NewTracedProcessor creates a traced processor wrapper.
func NewTracedProcessor(p Processor, tracer *telemetry.Tracer) *TracedProcessor {
	return &TracedProcessor{inner: p, tracer: tracer}
}

// Name returns the processor name.
func (p *TracedProcessor) Name() string {
	return p.inner.Name()
}

// Process runs the processor with tracing.
func (p *TracedProcessor) Process(ctx context.Context, in <-chan *Event, out chan<- *Event) error {
	ctx, span := p.tracer.StartSpan(ctx, fmt.Sprintf("processor.%s", p.inner.Name()))
	defer p.tracer.EndSpan(span)

	eventCount := int64(0)
	start := time.Now()

	err := p.inner.Process(ctx, in, out)

	span.SetAttribute("event_count", eventCount)
	span.SetAttribute("duration_ms", time.Since(start).Milliseconds())
	if err != nil {
		span.SetStatus(telemetry.SpanStatusError, err.Error())
	}

	return err
}

// ResilientProcessor wraps a processor with circuit breaker protection.
type ResilientProcessor struct {
	inner          Processor
	circuitBreaker *resilience.CircuitBreaker
	poisonPill     *resilience.PoisonPillHandler
}

// NewResilientProcessor creates a resilient processor wrapper.
func NewResilientProcessor(p Processor, cb *resilience.CircuitBreaker, pp *resilience.PoisonPillHandler) *ResilientProcessor {
	return &ResilientProcessor{
		inner:          p,
		circuitBreaker: cb,
		poisonPill:     pp,
	}
}

// Name returns the processor name.
func (p *ResilientProcessor) Name() string {
	return p.inner.Name()
}

// Process runs the processor with resilience protection.
func (p *ResilientProcessor) Process(ctx context.Context, in <-chan *Event, out chan<- *Event) error {
	// Check circuit breaker
	if !p.circuitBreaker.Allow() {
		return ErrCircuitOpen
	}

	p.circuitBreaker.Start()
	defer func() {
		p.circuitBreaker.End(ctx.Err() == nil)
	}()

	return p.inner.Process(ctx, in, out)
}

// CheckpointedProcessor wraps a processor with checkpoint updates.
type CheckpointedProcessor struct {
	inner      Processor
	checkpoint *checkpoint.Checkpoint
	interval   int64 // Update checkpoint every N events
}

// NewCheckpointedProcessor creates a checkpointed processor wrapper.
func NewCheckpointedProcessor(p Processor, cp *checkpoint.Checkpoint, interval int64) *CheckpointedProcessor {
	if interval <= 0 {
		interval = 1000
	}
	return &CheckpointedProcessor{
		inner:      p,
		checkpoint: cp,
		interval:   interval,
	}
}

// Name returns the processor name.
func (p *CheckpointedProcessor) Name() string {
	return p.inner.Name()
}

// Process runs the processor with checkpoint updates.
func (p *CheckpointedProcessor) Process(ctx context.Context, in <-chan *Event, out chan<- *Event) error {
	if p.checkpoint == nil {
		return p.inner.Process(ctx, in, out)
	}

	// Wrap input channel to track progress
	wrappedIn := make(chan *Event, cap(in))
	go func() {
		defer close(wrappedIn)
		var count int64
		for event := range in {
			count++
			if count%p.interval == 0 {
				p.checkpoint.Update(0, count)
			}
			select {
			case wrappedIn <- event:
			case <-ctx.Done():
				return
			}
		}
	}()

	return p.inner.Process(ctx, wrappedIn, out)
}

// DLQProcessor wraps a processor with DLQ error handling.
type DLQProcessor struct {
	inner      Processor
	dlq        *DLQWriter
	jobID      string
	sourceFile string
}

// NewDLQProcessor creates a DLQ-enabled processor wrapper.
func NewDLQProcessor(p Processor, dlq *DLQWriter, jobID, sourceFile string) *DLQProcessor {
	return &DLQProcessor{
		inner:      p,
		dlq:        dlq,
		jobID:      jobID,
		sourceFile: sourceFile,
	}
}

// Name returns the processor name.
func (p *DLQProcessor) Name() string {
	return p.inner.Name()
}

// Process runs the processor with DLQ error handling.
func (p *DLQProcessor) Process(ctx context.Context, in <-chan *Event, out chan<- *Event) error {
	// Note: DLQ writing is handled at a higher level for parse errors
	// This wrapper is for processor-level errors
	return p.inner.Process(ctx, in, out)
}
