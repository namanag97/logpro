package interfaces

import (
	"context"
	"time"
)

// Tracer provides distributed tracing capabilities.
type Tracer interface {
	// StartSpan creates a new span with the given name.
	// The span should be ended by calling Span.End().
	StartSpan(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span)

	// Extract extracts span context from carrier (e.g., HTTP headers).
	Extract(ctx context.Context, carrier Carrier) context.Context

	// Inject injects span context into carrier (e.g., HTTP headers).
	Inject(ctx context.Context, carrier Carrier) error

	// Close releases resources.
	Close() error
}

// Span represents a unit of work in a trace.
type Span interface {
	// End completes the span and records its duration.
	End()

	// SetStatus sets the span's status.
	SetStatus(code SpanStatus, message string)

	// SetAttribute adds a key-value attribute to the span.
	SetAttribute(key string, value interface{})

	// SetAttributes adds multiple attributes to the span.
	SetAttributes(attrs map[string]interface{})

	// AddEvent records an event on the span.
	AddEvent(name string, attrs map[string]interface{})

	// RecordError records an error on the span.
	RecordError(err error)

	// SpanContext returns the span's context for propagation.
	SpanContext() SpanContext
}

// SpanContext contains the identifiers for a span.
type SpanContext struct {
	TraceID    string
	SpanID     string
	TraceFlags byte
	TraceState string
	Remote     bool
}

// IsValid returns true if the span context is valid.
func (sc SpanContext) IsValid() bool {
	return sc.TraceID != "" && sc.SpanID != ""
}

// SpanStatus represents the status of a span.
type SpanStatus int

const (
	SpanStatusUnset SpanStatus = iota
	SpanStatusOK
	SpanStatusError
)

// SpanKind represents the type of span.
type SpanKind int

const (
	SpanKindInternal SpanKind = iota
	SpanKindServer
	SpanKindClient
	SpanKindProducer
	SpanKindConsumer
)

// SpanOption configures span creation.
type SpanOption func(*SpanConfig)

// SpanConfig holds span configuration.
type SpanConfig struct {
	Kind       SpanKind
	Attributes map[string]interface{}
	Links      []SpanLink
	StartTime  time.Time
}

// SpanLink links the span to another span.
type SpanLink struct {
	SpanContext SpanContext
	Attributes  map[string]interface{}
}

// WithSpanKind sets the span kind.
func WithSpanKind(kind SpanKind) SpanOption {
	return func(c *SpanConfig) {
		c.Kind = kind
	}
}

// WithSpanAttributes sets initial span attributes.
func WithSpanAttributes(attrs map[string]interface{}) SpanOption {
	return func(c *SpanConfig) {
		c.Attributes = attrs
	}
}

// WithStartTime sets the span's start time.
func WithStartTime(t time.Time) SpanOption {
	return func(c *SpanConfig) {
		c.StartTime = t
	}
}

// Carrier is used for context propagation across process boundaries.
type Carrier interface {
	Get(key string) string
	Set(key, value string)
	Keys() []string
}

// MapCarrier implements Carrier using a map.
type MapCarrier map[string]string

func (m MapCarrier) Get(key string) string      { return m[key] }
func (m MapCarrier) Set(key, value string)      { m[key] = value }
func (m MapCarrier) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Common span names.
const (
	SpanIngestFile     = "logflow.ingest.file"
	SpanIngestBatch    = "logflow.ingest.batch"
	SpanDecodeRecords  = "logflow.decode.records"
	SpanWriteParquet   = "logflow.write.parquet"
	SpanQueryExecute   = "logflow.query.execute"
	SpanQueryFetch     = "logflow.query.fetch"
	SpanStorageRead    = "logflow.storage.read"
	SpanStorageWrite   = "logflow.storage.write"
)
