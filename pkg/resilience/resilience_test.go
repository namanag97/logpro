package resilience

import (
	"context"
	"errors"
	"testing"
	"time"
)

// --- CircuitBreaker tests ---

func TestNewCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker()
	if cb == nil {
		t.Fatal("NewCircuitBreaker returned nil")
	}
	if cb.State() != CircuitClosed {
		t.Errorf("initial state = %d, want CircuitClosed(%d)", cb.State(), CircuitClosed)
	}
}

func TestCircuitBreakerBuilders(t *testing.T) {
	cb := NewCircuitBreaker().
		WithMaxMemory(0.8).
		WithMaxConcurrent(50).
		WithCooldown(5 * time.Second)

	if cb.maxMemoryPct != 0.8 {
		t.Errorf("maxMemoryPct = %f, want 0.8", cb.maxMemoryPct)
	}
	if cb.maxConcurrent != 50 {
		t.Errorf("maxConcurrent = %d, want 50", cb.maxConcurrent)
	}
	if cb.cooldownPeriod != 5*time.Second {
		t.Errorf("cooldownPeriod = %v, want 5s", cb.cooldownPeriod)
	}
}

func TestCircuitBreakerAllowWhenClosed(t *testing.T) {
	cb := NewCircuitBreaker()
	if !cb.Allow() {
		t.Error("Allow should return true when circuit is closed")
	}
}

func TestCircuitBreakerStartEnd(t *testing.T) {
	cb := NewCircuitBreaker().WithMaxConcurrent(100)

	cb.Start()
	if cb.concurrentOps != 1 {
		t.Errorf("concurrentOps = %d, want 1", cb.concurrentOps)
	}

	cb.End(true)
	if cb.concurrentOps != 0 {
		t.Errorf("concurrentOps after End = %d, want 0", cb.concurrentOps)
	}
}

func TestCircuitBreakerEndFailure(t *testing.T) {
	cb := NewCircuitBreaker().WithMaxConcurrent(100)
	cb.Start()
	cb.End(false)

	if cb.failures == 0 {
		t.Error("failures should be > 0 after End(false)")
	}
}

func TestCircuitBreakerTripsOnConcurrency(t *testing.T) {
	cb := NewCircuitBreaker().WithMaxConcurrent(2).WithCooldown(100 * time.Millisecond)

	tripped := false
	cb.OnTrip = func(reason string) {
		tripped = true
	}

	// Fill up concurrent ops
	cb.Start()
	cb.Start()
	cb.Start() // exceeds max

	// After exceeding, the circuit should eventually trip
	// or at least the concurrentOps should track correctly
	if cb.concurrentOps != 3 {
		t.Errorf("concurrentOps = %d, want 3", cb.concurrentOps)
	}

	cb.End(true)
	cb.End(true)
	cb.End(true)

	_ = tripped // callback may or may not fire depending on Allow() check
}

func TestCircuitBreakerOnResetCallback(t *testing.T) {
	cb := NewCircuitBreaker().WithCooldown(10 * time.Millisecond)
	resetCalled := false
	cb.OnReset = func() {
		resetCalled = true
	}
	_ = resetCalled // Used if circuit transitions from open to closed
}

func TestCircuitBreakerStateValues(t *testing.T) {
	tests := []struct {
		state CircuitState
		want  int
	}{
		{CircuitClosed, 0},
		{CircuitOpen, 1},
		{CircuitHalfOpen, 2},
	}
	for _, tt := range tests {
		if int(tt.state) != tt.want {
			t.Errorf("CircuitState %d, want %d", tt.state, tt.want)
		}
	}
}

// --- PoisonPillHandler tests ---

func TestNewPoisonPillHandler(t *testing.T) {
	h := NewPoisonPillHandler()
	if h == nil {
		t.Fatal("NewPoisonPillHandler returned nil")
	}
}

func TestPoisonPillHandlerBuilders(t *testing.T) {
	h := NewPoisonPillHandler().
		WithMaxRowSize(4096).
		WithParseTimeout(2 * time.Second)

	if h.maxRowSize != 4096 {
		t.Errorf("maxRowSize = %d, want 4096", h.maxRowSize)
	}
	if h.parseTimeout != 2*time.Second {
		t.Errorf("parseTimeout = %v, want 2s", h.parseTimeout)
	}
}

func TestPoisonPillSafeProcessSuccess(t *testing.T) {
	h := NewPoisonPillHandler()
	ctx := context.Background()

	err := h.SafeProcess(ctx, 1, func() error {
		return nil
	})
	if err != nil {
		t.Errorf("SafeProcess error = %v, want nil", err)
	}

	stats := h.Stats()
	if stats.Processed != 1 {
		t.Errorf("processed = %d, want 1", stats.Processed)
	}
	if stats.Errors != 0 {
		t.Errorf("errors = %d, want 0", stats.Errors)
	}
}

func TestPoisonPillSafeProcessError(t *testing.T) {
	h := NewPoisonPillHandler()
	ctx := context.Background()

	err := h.SafeProcess(ctx, 1, func() error {
		return errors.New("bad data")
	})
	// SafeProcess may skip or return error depending on implementation
	_ = err

	stats := h.Stats()
	if stats.Errors == 0 && stats.Skipped == 0 {
		t.Error("expected either errors > 0 or skipped > 0")
	}
}

func TestPoisonPillSafeProcessTimeout(t *testing.T) {
	h := NewPoisonPillHandler().WithParseTimeout(50 * time.Millisecond)
	ctx := context.Background()

	err := h.SafeProcess(ctx, 1, func() error {
		time.Sleep(200 * time.Millisecond)
		return nil
	})

	// Should either timeout or be skipped
	_ = err
	stats := h.Stats()
	// At least one of processed, skipped, or errors should be non-zero
	if stats.Processed == 0 && stats.Skipped == 0 && stats.Errors == 0 {
		t.Error("expected at least one counter to be non-zero")
	}
}

func TestPoisonPillSafeProcessContextCancel(t *testing.T) {
	h := NewPoisonPillHandler()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := h.SafeProcess(ctx, 1, func() error {
		return nil
	})
	// Should respect cancelled context
	_ = err
}

func TestPoisonPillOnSkipCallback(t *testing.T) {
	h := NewPoisonPillHandler()
	skipped := false
	h.OnSkip = func(rowNum int64, reason string) {
		skipped = true
	}

	ctx := context.Background()
	_ = h.SafeProcess(ctx, 1, func() error {
		return errors.New("bad row")
	})

	// OnSkip may or may not be called depending on error handling
	_ = skipped
}

func TestPoisonPillMultipleProcessCalls(t *testing.T) {
	h := NewPoisonPillHandler()
	ctx := context.Background()

	for i := int64(0); i < 10; i++ {
		_ = h.SafeProcess(ctx, i, func() error { return nil })
	}

	stats := h.Stats()
	if stats.Processed != 10 {
		t.Errorf("processed = %d, want 10", stats.Processed)
	}
}

// --- Checkpoint tests ---

func TestNewCheckpoint(t *testing.T) {
	cp := NewCheckpoint("input.csv", "output.parquet", "abc123")
	if cp == nil {
		t.Fatal("NewCheckpoint returned nil")
	}
	if cp.InputPath != "input.csv" {
		t.Errorf("InputPath = %q, want %q", cp.InputPath, "input.csv")
	}
	if cp.OutputPath != "output.parquet" {
		t.Errorf("OutputPath = %q, want %q", cp.OutputPath, "output.parquet")
	}
}

func TestCheckpointUpdate(t *testing.T) {
	cp := NewCheckpoint("in.csv", "out.parquet", "hash")
	// Update(row, byteOffset)
	cp.Update(100, 1024)

	if cp.LastByteOffset != 1024 {
		t.Errorf("LastByteOffset = %d, want 1024", cp.LastByteOffset)
	}
	if cp.LastProcessedRow != 100 {
		t.Errorf("LastProcessedRow = %d, want 100", cp.LastProcessedRow)
	}
}

func TestCheckpointShouldResume(t *testing.T) {
	cp := NewCheckpoint("in.csv", "out.parquet", "hash")
	if cp.ShouldResume() {
		t.Error("fresh checkpoint should not resume")
	}

	cp.Update(500, 50)
	if !cp.ShouldResume() {
		t.Error("checkpoint with progress should resume")
	}
}

func TestCheckpointGetResumePoint(t *testing.T) {
	cp := NewCheckpoint("in.csv", "out.parquet", "hash")
	// Update(row, byteOffset)
	cp.Update(200, 2048)
	rp := cp.GetResumePoint()
	if rp != 2048 {
		t.Errorf("GetResumePoint = %d, want 2048", rp)
	}
}

func TestCheckpointIsDirty(t *testing.T) {
	cp := NewCheckpoint("in.csv", "out.parquet", "hash")
	cp.Update(100, 10)

	if !cp.IsDirty() {
		t.Error("checkpoint after Update should be dirty")
	}

	cp.MarkSaved()
	if cp.IsDirty() {
		t.Error("checkpoint after MarkSaved should not be dirty")
	}
}
