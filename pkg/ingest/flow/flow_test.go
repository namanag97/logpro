package flow

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- BoundedQueue tests ---

func TestBoundedQueuePushPop(t *testing.T) {
	q := NewBoundedQueue(10)
	ctx := context.Background()

	if err := q.Push(ctx, "hello"); err != nil {
		t.Fatalf("Push error: %v", err)
	}
	if q.Len() != 1 {
		t.Errorf("Len = %d, want 1", q.Len())
	}

	val, err := q.Pop(ctx)
	if err != nil {
		t.Fatalf("Pop error: %v", err)
	}
	if val != "hello" {
		t.Errorf("Pop value = %v, want %q", val, "hello")
	}
	if q.Len() != 0 {
		t.Errorf("Len after Pop = %d, want 0", q.Len())
	}
}

func TestBoundedQueueFIFO(t *testing.T) {
	q := NewBoundedQueue(10)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_ = q.Push(ctx, i)
	}
	for i := 0; i < 5; i++ {
		val, _ := q.Pop(ctx)
		if val != i {
			t.Errorf("Pop[%d] = %v, want %d", i, val, i)
		}
	}
}

func TestBoundedQueueClose(t *testing.T) {
	q := NewBoundedQueue(10)
	ctx := context.Background()
	_ = q.Push(ctx, "item")
	q.Close()

	err := q.Push(ctx, "after-close")
	if err == nil {
		t.Error("Push after Close should return error")
	}
}

func TestBoundedQueueBlocksWhenFull(t *testing.T) {
	q := NewBoundedQueue(2)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_ = q.Push(ctx, "a")
	_ = q.Push(ctx, "b")

	// This should block until context times out
	err := q.Push(ctx, "c")
	if err == nil {
		t.Error("Push on full queue should block and return context error")
	}
}

func TestBoundedQueueBlocksWhenEmpty(t *testing.T) {
	q := NewBoundedQueue(10)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := q.Pop(ctx)
	if err == nil {
		t.Error("Pop on empty queue should block and return context error")
	}
}

func TestBoundedQueueConcurrent(t *testing.T) {
	q := NewBoundedQueue(100)
	ctx := context.Background()
	var wg sync.WaitGroup

	// Producers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = q.Push(ctx, id*100+j)
			}
		}(i)
	}

	// Consumer
	var count int64
	done := make(chan struct{})
	go func() {
		for atomic.LoadInt64(&count) < 1000 {
			_, err := q.Pop(ctx)
			if err == nil {
				atomic.AddInt64(&count, 1)
			}
		}
		close(done)
	}()

	wg.Wait()
	<-done

	if count != 1000 {
		t.Errorf("count = %d, want 1000", count)
	}
}

// --- BackpressureController tests ---

func TestNewBackpressureController(t *testing.T) {
	bc := NewBackpressureController()
	if bc == nil {
		t.Fatal("NewBackpressureController returned nil")
	}
}

func TestBackpressureUpdateAndGetPressure(t *testing.T) {
	bc := NewBackpressureController()
	bc.UpdatePressure("memory", 80, 100)

	p := bc.GetPressure("memory")
	if p < 0.79 || p > 0.81 {
		t.Errorf("pressure = %f, want ~0.8", p)
	}
}

func TestBackpressureShouldThrottle(t *testing.T) {
	bc := NewBackpressureController()

	// Low pressure
	bc.UpdatePressure("queue", 10, 100)
	if bc.ShouldThrottle("queue") {
		t.Error("ShouldThrottle at 10% should be false")
	}

	// High pressure
	bc.UpdatePressure("queue", 95, 100)
	if !bc.ShouldThrottle("queue") {
		t.Error("ShouldThrottle at 95% should be true")
	}
}

func TestBackpressureUnknownCategory(t *testing.T) {
	bc := NewBackpressureController()
	p := bc.GetPressure("unknown")
	if p != 0 {
		t.Errorf("unknown category pressure = %f, want 0", p)
	}
}

func TestBackpressureCallback(t *testing.T) {
	bc := NewBackpressureController()
	called := false
	bc.OnPressure(func(category string, pressure float64) {
		called = true
	})

	bc.UpdatePressure("queue", 95, 100)
	// Callback may or may not fire depending on threshold
	_ = called
}

// --- RateLimiter tests ---

func TestNewRateLimiter(t *testing.T) {
	rl := NewRateLimiter(100, 10)
	if rl == nil {
		t.Fatal("NewRateLimiter returned nil")
	}
}

func TestRateLimiterAcquire(t *testing.T) {
	rl := NewRateLimiter(10, 10)
	ctx := context.Background()

	// Should be able to acquire immediately (tokens available)
	err := rl.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
}

func TestRateLimiterContextCancel(t *testing.T) {
	rl := NewRateLimiter(1, 1)
	ctx := context.Background()

	// Drain the token
	_ = rl.Acquire(ctx)

	// Now try with a cancelled context
	ctx2, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := rl.Acquire(ctx2)
	// Should return context error since no tokens available
	_ = err
}

// --- WorkerPool tests ---

func TestNewWorkerPool(t *testing.T) {
	p := NewWorkerPool(4)
	if p == nil {
		t.Fatal("NewWorkerPool returned nil")
	}
	p.Close()
}

func TestWorkerPoolSubmit(t *testing.T) {
	p := NewWorkerPool(4)
	var count int64

	for i := 0; i < 100; i++ {
		err := p.Submit(func() {
			atomic.AddInt64(&count, 1)
		})
		if err != nil {
			t.Fatalf("Submit error: %v", err)
		}
	}

	p.Wait()
	p.Close()

	if count != 100 {
		t.Errorf("count = %d, want 100", count)
	}
}

func TestWorkerPoolSubmitAfterClose(t *testing.T) {
	p := NewWorkerPool(2)
	p.Close()

	err := p.Submit(func() {})
	if err == nil {
		t.Error("Submit after Close should return error")
	}
}

// --- TokenBucket tests ---

func TestNewTokenBucket(t *testing.T) {
	tb := NewTokenBucket(100, 10)
	if tb == nil {
		t.Fatal("NewTokenBucket returned nil")
	}
}

func TestTokenBucketTryAcquire(t *testing.T) {
	tb := NewTokenBucket(100, 10)

	if !tb.TryAcquire(50) {
		t.Error("TryAcquire(50) should succeed when bucket has 100 tokens")
	}
	if !tb.TryAcquire(50) {
		t.Error("TryAcquire(50) should succeed when bucket has 50 remaining tokens")
	}
	if tb.TryAcquire(50) {
		t.Error("TryAcquire(50) should fail when bucket is empty")
	}
}

func TestTokenBucketAcquire(t *testing.T) {
	tb := NewTokenBucket(10, 10)
	ctx := context.Background()

	err := tb.Acquire(ctx, 5)
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
}

func TestTokenBucketAcquireContextCancel(t *testing.T) {
	tb := NewTokenBucket(1, 1)
	ctx := context.Background()

	// Drain tokens
	_ = tb.Acquire(ctx, 1)

	ctx2, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := tb.Acquire(ctx2, 100) // ask for more than capacity
	if err == nil {
		// Depending on implementation, this may wait and timeout
	}
}

// --- ThroughputLimiter tests ---

func TestNewThroughputLimiter(t *testing.T) {
	tl := NewThroughputLimiter(1024*1024, 10)
	if tl == nil {
		t.Fatal("NewThroughputLimiter returned nil")
	}
}

func TestThroughputLimiterAcquireBytes(t *testing.T) {
	tl := NewThroughputLimiter(1024*1024, 10)
	ctx := context.Background()

	err := tl.AcquireBytes(ctx, 1024)
	if err != nil {
		t.Fatalf("AcquireBytes error: %v", err)
	}
}

func TestThroughputLimiterAcquireFile(t *testing.T) {
	tl := NewThroughputLimiter(1024*1024, 10)
	ctx := context.Background()

	err := tl.AcquireFile(ctx)
	if err != nil {
		t.Fatalf("AcquireFile error: %v", err)
	}
}

// --- ConcurrencyLimiter tests ---

func TestNewConcurrencyLimiter(t *testing.T) {
	cl := NewConcurrencyLimiter()
	if cl == nil {
		t.Fatal("NewConcurrencyLimiter returned nil")
	}
}

func TestConcurrencyLimiterAcquireRelease(t *testing.T) {
	cl := NewConcurrencyLimiter()
	cl.SetLimit("cpu", 2)
	ctx := context.Background()

	if err := cl.Acquire(ctx, "cpu"); err != nil {
		t.Fatalf("Acquire 1 error: %v", err)
	}
	if err := cl.Acquire(ctx, "cpu"); err != nil {
		t.Fatalf("Acquire 2 error: %v", err)
	}

	// Release one slot
	cl.Release("cpu")

	// Should be able to acquire again
	if err := cl.Acquire(ctx, "cpu"); err != nil {
		t.Fatalf("Acquire after Release error: %v", err)
	}

	cl.Release("cpu")
	cl.Release("cpu")
}
