// Package telemetry provides observability primitives for enterprise monitoring.
// Supports OpenTelemetry (OTLP) export for integration with Grafana, Datadog, AWS X-Ray.
package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Tracer records spans for distributed tracing.
type Tracer struct {
	mu sync.RWMutex

	serviceName string
	spans       []*Span
	activeSpans map[string]*Span

	// Export configuration
	exportEndpoint string
	batchSize      int
	exportInterval time.Duration

	// Metrics
	totalSpans   int64
	exportedSpans int64
}

// NewTracer creates a new tracer.
func NewTracer(serviceName string) *Tracer {
	return &Tracer{
		serviceName: serviceName,
		spans:       make([]*Span, 0),
		activeSpans: make(map[string]*Span),
		batchSize:   100,
		exportInterval: 10 * time.Second,
	}
}

// WithExportEndpoint sets the OTLP export endpoint.
func (t *Tracer) WithExportEndpoint(endpoint string) *Tracer {
	t.exportEndpoint = endpoint
	return t
}

// StartSpan begins a new trace span.
func (t *Tracer) StartSpan(ctx context.Context, name string) (context.Context, *Span) {
	span := &Span{
		TraceID:   generateTraceID(),
		SpanID:    generateSpanID(),
		Name:      name,
		StartTime: time.Now(),
		Attributes: make(map[string]interface{}),
		Events:    make([]SpanEvent, 0),
		Status:    SpanStatusOK,
	}

	// Check for parent span
	if parentSpan := SpanFromContext(ctx); parentSpan != nil {
		span.TraceID = parentSpan.TraceID
		span.ParentSpanID = parentSpan.SpanID
	}

	t.mu.Lock()
	t.activeSpans[span.SpanID] = span
	t.mu.Unlock()

	return ContextWithSpan(ctx, span), span
}

// EndSpan completes a span and records it.
func (t *Tracer) EndSpan(span *Span) {
	span.EndTime = time.Now()
	span.Duration = span.EndTime.Sub(span.StartTime)

	t.mu.Lock()
	delete(t.activeSpans, span.SpanID)
	t.spans = append(t.spans, span)
	atomic.AddInt64(&t.totalSpans, 1)
	t.mu.Unlock()
}

// Span represents a trace span (unit of work).
type Span struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Name         string
	StartTime    time.Time
	EndTime      time.Time
	Duration     time.Duration
	Attributes   map[string]interface{}
	Events       []SpanEvent
	Status       SpanStatus
	StatusMsg    string
}

// SetAttribute adds a key-value attribute to the span.
func (s *Span) SetAttribute(key string, value interface{}) {
	s.Attributes[key] = value
}

// AddEvent records a timestamped event within the span.
func (s *Span) AddEvent(name string, attrs map[string]interface{}) {
	s.Events = append(s.Events, SpanEvent{
		Name:       name,
		Timestamp:  time.Now(),
		Attributes: attrs,
	})
}

// SetStatus sets the span status.
func (s *Span) SetStatus(status SpanStatus, msg string) {
	s.Status = status
	s.StatusMsg = msg
}

// SpanEvent is a timestamped event within a span.
type SpanEvent struct {
	Name       string
	Timestamp  time.Time
	Attributes map[string]interface{}
}

// SpanStatus represents the outcome of a span.
type SpanStatus int

const (
	SpanStatusOK SpanStatus = iota
	SpanStatusError
)

// Context key for span propagation
type spanContextKey struct{}

// ContextWithSpan returns a context with the span attached.
func ContextWithSpan(ctx context.Context, span *Span) context.Context {
	return context.WithValue(ctx, spanContextKey{}, span)
}

// SpanFromContext retrieves the current span from context.
func SpanFromContext(ctx context.Context) *Span {
	if span, ok := ctx.Value(spanContextKey{}).(*Span); ok {
		return span
	}
	return nil
}

// Metrics aggregates runtime metrics for monitoring.
type Metrics struct {
	mu sync.RWMutex

	// Processing metrics
	EventsProcessed  int64
	BytesRead        int64
	BytesWritten     int64
	ErrorCount       int64

	// Performance metrics
	ParseDuration    time.Duration
	TransformDuration time.Duration
	WriteDuration    time.Duration
	TotalDuration    time.Duration

	// Resource metrics
	PeakMemoryMB     int64
	GCPauseTotal     time.Duration

	// Histograms (simplified)
	latencies []time.Duration
}

// NewMetrics creates a new metrics collector.
func NewMetrics() *Metrics {
	return &Metrics{
		latencies: make([]time.Duration, 0, 1000),
	}
}

// RecordLatency records a latency sample.
func (m *Metrics) RecordLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Keep last 1000 samples
	if len(m.latencies) >= 1000 {
		m.latencies = m.latencies[1:]
	}
	m.latencies = append(m.latencies, d)
}

// IncrementEvents atomically increments the events processed counter.
func (m *Metrics) IncrementEvents(n int64) {
	atomic.AddInt64(&m.EventsProcessed, n)
}

// IncrementBytes atomically increments bytes read.
func (m *Metrics) IncrementBytes(n int64) {
	atomic.AddInt64(&m.BytesRead, n)
}

// IncrementErrors atomically increments error count.
func (m *Metrics) IncrementErrors() {
	atomic.AddInt64(&m.ErrorCount, 1)
}

// Percentile calculates the p-th percentile of recorded latencies.
func (m *Metrics) Percentile(p float64) time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.latencies) == 0 {
		return 0
	}

	// Simple implementation - could use a more efficient algorithm
	sorted := make([]time.Duration, len(m.latencies))
	copy(sorted, m.latencies)

	// Selection sort for small datasets
	for i := 0; i < len(sorted); i++ {
		minIdx := i
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j] < sorted[minIdx] {
				minIdx = j
			}
		}
		sorted[i], sorted[minIdx] = sorted[minIdx], sorted[i]
	}

	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// Summary returns a summary of collected metrics.
func (m *Metrics) Summary() MetricsSummary {
	return MetricsSummary{
		EventsProcessed: atomic.LoadInt64(&m.EventsProcessed),
		BytesRead:       atomic.LoadInt64(&m.BytesRead),
		BytesWritten:    atomic.LoadInt64(&m.BytesWritten),
		ErrorCount:      atomic.LoadInt64(&m.ErrorCount),
		P50Latency:      m.Percentile(0.50),
		P95Latency:      m.Percentile(0.95),
		P99Latency:      m.Percentile(0.99),
	}
}

// MetricsSummary is a snapshot of metrics.
type MetricsSummary struct {
	EventsProcessed int64         `json:"events_processed"`
	BytesRead       int64         `json:"bytes_read"`
	BytesWritten    int64         `json:"bytes_written"`
	ErrorCount      int64         `json:"error_count"`
	P50Latency      time.Duration `json:"p50_latency_ns"`
	P95Latency      time.Duration `json:"p95_latency_ns"`
	P99Latency      time.Duration `json:"p99_latency_ns"`
}

// ToJSON serializes the summary to JSON.
func (s MetricsSummary) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}

// generateTraceID creates a unique trace ID.
func generateTraceID() string {
	return fmt.Sprintf("%016x%016x", time.Now().UnixNano(), time.Now().UnixNano()>>32)
}

// generateSpanID creates a unique span ID.
func generateSpanID() string {
	return fmt.Sprintf("%016x", time.Now().UnixNano())
}

// InstrumentedOperation wraps an operation with tracing and metrics.
func InstrumentedOperation(ctx context.Context, tracer *Tracer, metrics *Metrics, name string, op func(ctx context.Context) error) error {
	ctx, span := tracer.StartSpan(ctx, name)
	start := time.Now()

	err := op(ctx)

	elapsed := time.Since(start)
	metrics.RecordLatency(elapsed)

	if err != nil {
		span.SetStatus(SpanStatusError, err.Error())
		metrics.IncrementErrors()
	}

	span.SetAttribute("duration_ms", elapsed.Milliseconds())
	tracer.EndSpan(span)

	return err
}
