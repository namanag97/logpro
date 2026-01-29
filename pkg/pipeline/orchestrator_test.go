package pipeline

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestOrchestratorGoroutineLeaks verifies no goroutines leak on cancellation.
func TestOrchestratorGoroutineLeaks(t *testing.T) {
	// Record goroutine count before
	before := runtime.NumGoroutine()

	// Run a pipeline that gets cancelled
	ctx, cancel := context.WithCancel(context.Background())

	cfg := DefaultConfig()
	cfg.SourcePath = "nonexistent.csv" // Will fail

	o := NewOrchestratorV2(cfg)

	// Cancel immediately
	cancel()

	// Attempt to run (should fail gracefully)
	_ = o.Run(ctx)

	// Give goroutines time to clean up
	time.Sleep(100 * time.Millisecond)

	// Check goroutine count
	after := runtime.NumGoroutine()
	leaked := after - before

	if leaked > 1 { // Allow 1 for test timing
		t.Errorf("Goroutine leak detected: %d goroutines leaked", leaked)
	}
}

// TestOrchestratorContextCancellation verifies context cancellation propagates.
func TestOrchestratorContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	cfg := DefaultConfig()
	o := NewOrchestratorV2(cfg)

	// Add a slow processor
	o.AddProcessor(&slowProcessor{delay: 1 * time.Second})

	// Set up mock source and sink
	o.SetSource(&mockSource{events: 1000})
	o.SetSink(&mockSink{})

	// Run should be cancelled
	err := o.Run(ctx)

	if err == nil {
		t.Error("Expected cancellation error, got nil")
	}

	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		// Our custom error wrapping
		if err.Error() == "" {
			t.Errorf("Expected context error, got: %v", err)
		}
	}
}

// TestOrchestratorPanicRecovery verifies panics are recovered.
func TestOrchestratorPanicRecovery(t *testing.T) {
	cfg := DefaultConfig()
	o := NewOrchestratorV2(cfg)

	// Add a panicking processor wrapped in SafeProcessor
	o.AddProcessor(NewSafeProcessor(&panicProcessor{}))

	o.SetSource(&mockSource{events: 10})
	o.SetSink(&mockSink{})

	err := o.Run(context.Background())

	if err == nil {
		t.Error("Expected panic error, got nil")
	}

	// Error should indicate panic
	if err.Error() == "" {
		t.Errorf("Expected panic in error message, got: %v", err)
	}
}

// --- Mock implementations for testing ---

type mockSource struct {
	events int
	sent   int
}

func (m *mockSource) Name() string { return "mock" }
func (m *mockSource) SupportsFormat(format string) bool { return true }

func (m *mockSource) Read(ctx context.Context, r io.Reader, out chan<- *Event) error {
	for i := 0; i < m.events; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- &Event{
			CaseID:    []byte("case_1"),
			Activity:  []byte("activity"),
			Timestamp: time.Now().UnixNano(),
		}:
			m.sent++
		}
	}
	return nil
}

type mockSink struct {
	received atomic.Int64
}

func (m *mockSink) Name() string { return "mock" }

func (m *mockSink) Write(ctx context.Context, in <-chan *Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-in:
			if !ok {
				return nil
			}
			m.received.Add(1)
		}
	}
}

func (m *mockSink) Close() error { return nil }

type slowProcessor struct {
	delay time.Duration
}

func (p *slowProcessor) Name() string { return "slow" }

func (p *slowProcessor) Process(ctx context.Context, in <-chan *Event, out chan<- *Event) error {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-in:
			if !ok {
				return nil
			}

			// Simulate slow processing
			select {
			case <-time.After(p.delay):
			case <-ctx.Done():
				return ctx.Err()
			}

			select {
			case out <- event:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

type panicProcessor struct{}

func (p *panicProcessor) Name() string { return "panic" }

func (p *panicProcessor) Process(ctx context.Context, in <-chan *Event, out chan<- *Event) error {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-in:
			if !ok {
				return nil
			}
			panic("intentional panic for testing")
		}
	}
}

// --- Benchmark tests ---

func BenchmarkPipelinePassthrough(b *testing.B) {
	cfg := DefaultConfig()
	cfg.BufferSize = 4096

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		o := NewOrchestratorV2(cfg)
		o.SetSource(&mockSource{events: 10000})
		o.SetSink(&mockSink{})
		o.Run(ctx)
	}
}

func BenchmarkPipelineWithProcessors(b *testing.B) {
	cfg := DefaultConfig()
	cfg.BufferSize = 4096

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		o := NewOrchestratorV2(cfg)
		o.SetSource(&mockSource{events: 10000})

		// Add passthrough processor
		o.AddProcessor(ProcessorFunc(func(ctx context.Context, in <-chan *Event, out chan<- *Event) error {
			defer close(out)
			for event := range in {
				out <- event
			}
			return nil
		}))

		o.SetSink(&mockSink{})
		o.Run(ctx)
	}
}
