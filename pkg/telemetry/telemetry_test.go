package telemetry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewTracer(t *testing.T) {
	tr := NewTracer("test-service")
	if tr == nil {
		t.Fatal("NewTracer returned nil")
	}
}

func TestStartAndEndSpan(t *testing.T) {
	tr := NewTracer("test-service")
	ctx := context.Background()

	ctx, span := tr.StartSpan(ctx, "test-op")
	if span == nil {
		t.Fatal("StartSpan returned nil span")
	}
	if span.Name != "test-op" {
		t.Errorf("span name = %q, want %q", span.Name, "test-op")
	}
	if span.StartTime.IsZero() {
		t.Error("span start time is zero")
	}
	if span.TraceID == "" {
		t.Error("span has empty trace ID")
	}
	if span.SpanID == "" {
		t.Error("span has empty span ID")
	}

	tr.EndSpan(span)
	if span.EndTime.IsZero() {
		t.Error("span end time is zero after EndSpan")
	}
	if span.Duration == 0 {
		t.Error("span duration is zero after EndSpan")
	}

	// Verify span is in context
	extracted := SpanFromContext(ctx)
	if extracted == nil {
		t.Error("SpanFromContext returned nil")
	}
	if extracted != span {
		t.Error("SpanFromContext returned different span")
	}
}

func TestSpanParentChild(t *testing.T) {
	tr := NewTracer("test-service")
	ctx := context.Background()

	ctx, parent := tr.StartSpan(ctx, "parent")
	_, child := tr.StartSpan(ctx, "child")

	if child.ParentSpanID != parent.SpanID {
		t.Errorf("child.ParentSpanID = %q, want %q", child.ParentSpanID, parent.SpanID)
	}
	if child.TraceID != parent.TraceID {
		t.Errorf("child.TraceID = %q, want parent.TraceID %q", child.TraceID, parent.TraceID)
	}

	tr.EndSpan(child)
	tr.EndSpan(parent)
}

func TestSpanAttributes(t *testing.T) {
	tr := NewTracer("test-service")
	ctx := context.Background()
	_, span := tr.StartSpan(ctx, "op")

	span.SetAttribute("key1", "value1")
	span.SetAttribute("key2", 42)

	if v, ok := span.Attributes["key1"]; !ok || v != "value1" {
		t.Errorf("attribute key1 = %v, want value1", v)
	}
	if v, ok := span.Attributes["key2"]; !ok || v != 42 {
		t.Errorf("attribute key2 = %v, want 42", v)
	}
}

func TestSpanEvents(t *testing.T) {
	tr := NewTracer("test-service")
	ctx := context.Background()
	_, span := tr.StartSpan(ctx, "op")

	span.AddEvent("checkpoint", map[string]interface{}{"row": 100})

	if len(span.Events) != 1 {
		t.Fatalf("span events count = %d, want 1", len(span.Events))
	}
	if span.Events[0].Name != "checkpoint" {
		t.Errorf("event name = %q, want %q", span.Events[0].Name, "checkpoint")
	}
}

func TestSpanStatus(t *testing.T) {
	tr := NewTracer("test-service")
	ctx := context.Background()
	_, span := tr.StartSpan(ctx, "op")

	span.SetStatus(SpanStatusError, "something failed")

	if span.Status != SpanStatusError {
		t.Errorf("span status = %d, want %d", span.Status, SpanStatusError)
	}
	if span.StatusMessage != "something failed" {
		t.Errorf("span status message = %q, want %q", span.StatusMessage, "something failed")
	}
	tr.EndSpan(span)
}

func TestContextWithSpanNil(t *testing.T) {
	ctx := context.Background()
	span := SpanFromContext(ctx)
	if span != nil {
		t.Error("SpanFromContext on empty context should return nil")
	}
}

// --- Metrics tests ---

func TestNewMetrics(t *testing.T) {
	m := NewMetrics()
	if m == nil {
		t.Fatal("NewMetrics returned nil")
	}
}

func TestMetricsIncrementEvents(t *testing.T) {
	m := NewMetrics()
	m.IncrementEvents(10)
	m.IncrementEvents(5)

	s := m.Summary()
	if s.EventsProcessed != 15 {
		t.Errorf("events processed = %d, want 15", s.EventsProcessed)
	}
}

func TestMetricsIncrementBytes(t *testing.T) {
	m := NewMetrics()
	m.IncrementBytes(1024)
	m.IncrementBytes(2048)

	s := m.Summary()
	if s.BytesRead != 3072 {
		t.Errorf("bytes read = %d, want 3072", s.BytesRead)
	}
}

func TestMetricsIncrementErrors(t *testing.T) {
	m := NewMetrics()
	m.IncrementErrors()
	m.IncrementErrors()

	s := m.Summary()
	if s.ErrorCount != 2 {
		t.Errorf("error count = %d, want 2", s.ErrorCount)
	}
}

func TestMetricsRecordLatency(t *testing.T) {
	m := NewMetrics()
	m.RecordLatency(100 * time.Millisecond)
	m.RecordLatency(200 * time.Millisecond)
	m.RecordLatency(300 * time.Millisecond)

	p50 := m.Percentile(0.5)
	if p50 < 100*time.Millisecond || p50 > 300*time.Millisecond {
		t.Errorf("P50 = %v, want between 100ms and 300ms", p50)
	}

	p99 := m.Percentile(0.99)
	if p99 < 200*time.Millisecond {
		t.Errorf("P99 = %v, want >= 200ms", p99)
	}
}

func TestMetricsPercentileEmpty(t *testing.T) {
	m := NewMetrics()
	p := m.Percentile(0.5)
	if p != 0 {
		t.Errorf("percentile on empty metrics = %v, want 0", p)
	}
}

func TestMetricsSummaryToJSON(t *testing.T) {
	m := NewMetrics()
	m.IncrementEvents(42)
	s := m.Summary()
	data, err := s.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON error: %v", err)
	}
	if len(data) == 0 {
		t.Error("ToJSON returned empty data")
	}
}

// --- InstrumentedOperation tests ---

func TestInstrumentedOperationSuccess(t *testing.T) {
	tr := NewTracer("test-service")
	m := NewMetrics()
	ctx := context.Background()
	called := false

	err := InstrumentedOperation(ctx, tr, m, "test-op", func(ctx context.Context) error {
		called = true
		return nil
	})

	if err != nil {
		t.Errorf("InstrumentedOperation error = %v, want nil", err)
	}
	if !called {
		t.Error("operation was not called")
	}
}

func TestInstrumentedOperationError(t *testing.T) {
	tr := NewTracer("test-service")
	m := NewMetrics()
	ctx := context.Background()
	testErr := errors.New("test error")

	err := InstrumentedOperation(ctx, tr, m, "failing-op", func(ctx context.Context) error {
		return testErr
	})

	if err != testErr {
		t.Errorf("InstrumentedOperation error = %v, want %v", err, testErr)
	}

	s := m.Summary()
	if s.ErrorCount != 1 {
		t.Errorf("error count = %d, want 1", s.ErrorCount)
	}
}

func TestTracerWithExportEndpoint(t *testing.T) {
	tr := NewTracer("test-service").WithExportEndpoint("http://localhost:4318")
	if tr == nil {
		t.Fatal("WithExportEndpoint returned nil")
	}
}
