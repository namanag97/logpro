// Package flow provides flow control and backpressure management.
package flow

import (
	"context"
	"sync"
	"time"
)

// BoundedQueue implements a bounded queue with backpressure.
type BoundedQueue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	items    []interface{}
	capacity int
	closed   bool
}

// NewBoundedQueue creates a queue with the given capacity.
func NewBoundedQueue(capacity int) *BoundedQueue {
	q := &BoundedQueue{
		items:    make([]interface{}, 0, capacity),
		capacity: capacity,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Push adds an item, blocking if full.
func (q *BoundedQueue) Push(ctx context.Context, item interface{}) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.items) >= q.capacity && !q.closed {
		// Check context
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		q.cond.Wait()
	}

	if q.closed {
		return ErrQueueClosed
	}

	q.items = append(q.items, item)
	q.cond.Signal()
	return nil
}

// Pop removes and returns an item, blocking if empty.
func (q *BoundedQueue) Pop(ctx context.Context) (interface{}, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.items) == 0 && !q.closed {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		q.cond.Wait()
	}

	if len(q.items) == 0 {
		return nil, ErrQueueClosed
	}

	item := q.items[0]
	q.items = q.items[1:]
	q.cond.Signal()
	return item, nil
}

// Close closes the queue.
func (q *BoundedQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}

// Len returns current queue length.
func (q *BoundedQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// BackpressureController manages backpressure across multiple queues.
type BackpressureController struct {
	mu sync.RWMutex

	// Per-category pressure levels
	pressure map[string]float64

	// Thresholds
	lowWatermark  float64
	highWatermark float64

	// Callbacks
	onPressure func(category string, level float64)
}

// NewBackpressureController creates a new controller.
func NewBackpressureController() *BackpressureController {
	return &BackpressureController{
		pressure:      make(map[string]float64),
		lowWatermark:  0.3,
		highWatermark: 0.8,
	}
}

// UpdatePressure updates the pressure level for a category.
func (c *BackpressureController) UpdatePressure(category string, current, capacity int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	level := float64(current) / float64(capacity)
	c.pressure[category] = level

	if c.onPressure != nil && level > c.highWatermark {
		c.onPressure(category, level)
	}
}

// GetPressure returns the pressure level for a category.
func (c *BackpressureController) GetPressure(category string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.pressure[category]
}

// ShouldThrottle returns true if the category should be throttled.
func (c *BackpressureController) ShouldThrottle(category string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.pressure[category] > c.highWatermark
}

// OnPressure sets a callback for pressure events.
func (c *BackpressureController) OnPressure(fn func(string, float64)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onPressure = fn
}

// RateLimiter limits operations per time window.
type RateLimiter struct {
	mu       sync.Mutex
	tokens   int
	maxTokens int
	refillRate int
	lastRefill time.Time
}

// NewRateLimiter creates a rate limiter.
func NewRateLimiter(maxTokens, refillPerSec int) *RateLimiter {
	return &RateLimiter{
		tokens:     maxTokens,
		maxTokens:  maxTokens,
		refillRate: refillPerSec,
		lastRefill: time.Now(),
	}
}

// Acquire acquires a token, blocking if none available.
func (r *RateLimiter) Acquire(ctx context.Context) error {
	for {
		r.mu.Lock()
		r.refill()
		if r.tokens > 0 {
			r.tokens--
			r.mu.Unlock()
			return nil
		}
		r.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond * 10):
		}
	}
}

func (r *RateLimiter) refill() {
	now := time.Now()
	elapsed := now.Sub(r.lastRefill)
	tokensToAdd := int(elapsed.Seconds() * float64(r.refillRate))
	if tokensToAdd > 0 {
		r.tokens += tokensToAdd
		if r.tokens > r.maxTokens {
			r.tokens = r.maxTokens
		}
		r.lastRefill = now
	}
}

// Error types
type queueError string

func (e queueError) Error() string { return string(e) }

const ErrQueueClosed = queueError("queue closed")
