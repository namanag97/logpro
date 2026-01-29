// Package tracer provides default tracing implementations.
package tracer

import (
	"context"

	"github.com/logflow/logflow/pkg/interfaces"
)

// NoopTracer discards all tracing data.
// Use this when tracing is not needed.
type NoopTracer struct{}

// NewNoopTracer creates a new noop tracer.
func NewNoopTracer() *NoopTracer {
	return &NoopTracer{}
}

// StartSpan returns a context with a noop span.
func (t *NoopTracer) StartSpan(ctx context.Context, name string, opts ...interfaces.SpanOption) (context.Context, interfaces.Span) {
	return ctx, &noopSpan{}
}

// Extract does nothing.
func (t *NoopTracer) Extract(ctx context.Context, carrier interfaces.Carrier) context.Context {
	return ctx
}

// Inject does nothing.
func (t *NoopTracer) Inject(ctx context.Context, carrier interfaces.Carrier) error {
	return nil
}

// Close does nothing.
func (t *NoopTracer) Close() error {
	return nil
}

// noopSpan is a span that does nothing.
type noopSpan struct{}

func (s *noopSpan) End()                                          {}
func (s *noopSpan) SetStatus(code interfaces.SpanStatus, msg string)         {}
func (s *noopSpan) SetAttribute(key string, value interface{})              {}
func (s *noopSpan) SetAttributes(attrs map[string]interface{})              {}
func (s *noopSpan) AddEvent(name string, attrs map[string]interface{})      {}
func (s *noopSpan) RecordError(err error)                                   {}
func (s *noopSpan) SpanContext() interfaces.SpanContext                     { return interfaces.SpanContext{} }

// Verify interface compliance.
var (
	_ interfaces.Tracer = (*NoopTracer)(nil)
	_ interfaces.Span   = (*noopSpan)(nil)
)
