package flow

import (
	"context"
	"sync"
	"time"
)

// ThroughputLimiter limits throughput by bytes or files per second.
type ThroughputLimiter struct {
	mu sync.Mutex

	// Limits
	maxBytesPerSec int64
	maxFilesPerSec int64

	// Tracking
	bytesThisSecond int64
	filesThisSecond int64
	currentSecond   int64

	// For smooth rate limiting
	lastAcquire time.Time
}

// NewThroughputLimiter creates a throughput limiter.
func NewThroughputLimiter(maxBytesPerSec, maxFilesPerSec int64) *ThroughputLimiter {
	return &ThroughputLimiter{
		maxBytesPerSec: maxBytesPerSec,
		maxFilesPerSec: maxFilesPerSec,
		lastAcquire:    time.Now(),
	}
}

// AcquireBytes waits until bytes can be processed.
func (r *ThroughputLimiter) AcquireBytes(ctx context.Context, bytes int64) error {
	if r.maxBytesPerSec <= 0 {
		return nil // No limit
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().Unix()
	if now != r.currentSecond {
		r.currentSecond = now
		r.bytesThisSecond = 0
		r.filesThisSecond = 0
	}

	// Check if we need to wait
	if r.bytesThisSecond+bytes > r.maxBytesPerSec {
		// Calculate wait time
		waitTime := time.Duration(now+1)*time.Second - time.Duration(time.Now().UnixNano())
		if waitTime > 0 {
			r.mu.Unlock()
			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				r.mu.Lock()
				return ctx.Err()
			}
			r.mu.Lock()
			r.bytesThisSecond = 0
		}
	}

	r.bytesThisSecond += bytes
	return nil
}

// AcquireFile waits until a file can be processed.
func (r *ThroughputLimiter) AcquireFile(ctx context.Context) error {
	if r.maxFilesPerSec <= 0 {
		return nil // No limit
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().Unix()
	if now != r.currentSecond {
		r.currentSecond = now
		r.bytesThisSecond = 0
		r.filesThisSecond = 0
	}

	if r.filesThisSecond >= r.maxFilesPerSec {
		waitTime := time.Duration(now+1)*time.Second - time.Duration(time.Now().UnixNano())
		if waitTime > 0 {
			r.mu.Unlock()
			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				r.mu.Lock()
				return ctx.Err()
			}
			r.mu.Lock()
			r.filesThisSecond = 0
		}
	}

	r.filesThisSecond++
	return nil
}

// TokenBucket implements token bucket rate limiting.
type TokenBucket struct {
	mu sync.Mutex

	capacity   int64
	tokens     int64
	refillRate int64 // tokens per second
	lastRefill time.Time
}

// NewTokenBucket creates a token bucket rate limiter.
func NewTokenBucket(capacity, refillRate int64) *TokenBucket {
	return &TokenBucket{
		capacity:   capacity,
		tokens:     capacity,
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

// Acquire attempts to acquire tokens, blocking if necessary.
func (tb *TokenBucket) Acquire(ctx context.Context, tokens int64) error {
	for {
		tb.mu.Lock()
		tb.refill()

		if tb.tokens >= tokens {
			tb.tokens -= tokens
			tb.mu.Unlock()
			return nil
		}

		// Calculate wait time for enough tokens
		needed := tokens - tb.tokens
		waitDuration := time.Duration(needed*int64(time.Second)) / time.Duration(tb.refillRate)
		tb.mu.Unlock()

		select {
		case <-time.After(waitDuration):
			// Continue loop to try again
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// TryAcquire attempts to acquire tokens without blocking.
func (tb *TokenBucket) TryAcquire(tokens int64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= tokens {
		tb.tokens -= tokens
		return true
	}
	return false
}

func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill)
	tb.lastRefill = now

	newTokens := int64(elapsed.Seconds() * float64(tb.refillRate))
	tb.tokens = min64(tb.capacity, tb.tokens+newTokens)
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
