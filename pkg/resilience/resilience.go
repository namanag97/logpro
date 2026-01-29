// Package resilience provides fault-tolerance primitives for enterprise-grade processing.
package resilience

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// CircuitBreaker prevents overload by temporarily rejecting requests when the system is stressed.
type CircuitBreaker struct {
	mu sync.RWMutex

	// Configuration
	maxMemoryPct   float64       // Max memory usage before tripping (e.g., 0.90 = 90%)
	maxConcurrent  int           // Max concurrent operations
	cooldownPeriod time.Duration // Time to wait before retrying after trip

	// State
	state          CircuitState
	failures       int64
	lastFailure    time.Time
	tripTime       time.Time
	concurrentOps  int64

	// Callbacks
	OnTrip   func(reason string)
	OnReset  func()
}

// CircuitState represents the state of a circuit breaker.
type CircuitState int

const (
	CircuitClosed CircuitState = iota // Normal operation
	CircuitOpen                       // Rejecting requests
	CircuitHalfOpen                   // Testing if system recovered
)

// NewCircuitBreaker creates a circuit breaker with sensible defaults.
func NewCircuitBreaker() *CircuitBreaker {
	return &CircuitBreaker{
		maxMemoryPct:   0.90,
		maxConcurrent:  1000,
		cooldownPeriod: 30 * time.Second,
		state:          CircuitClosed,
	}
}

// WithMaxMemory sets the maximum memory usage threshold.
func (cb *CircuitBreaker) WithMaxMemory(pct float64) *CircuitBreaker {
	cb.maxMemoryPct = pct
	return cb
}

// WithMaxConcurrent sets the maximum concurrent operations.
func (cb *CircuitBreaker) WithMaxConcurrent(n int) *CircuitBreaker {
	cb.maxConcurrent = n
	return cb
}

// WithCooldown sets the cooldown period after tripping.
func (cb *CircuitBreaker) WithCooldown(d time.Duration) *CircuitBreaker {
	cb.cooldownPeriod = d
	return cb
}

// Allow checks if an operation should be allowed.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.RLock()
	state := cb.state
	cb.mu.RUnlock()

	switch state {
	case CircuitOpen:
		// Check if cooldown has passed
		cb.mu.Lock()
		defer cb.mu.Unlock()

		if time.Since(cb.tripTime) > cb.cooldownPeriod {
			cb.state = CircuitHalfOpen
			return true
		}
		return false

	case CircuitHalfOpen:
		return true

	case CircuitClosed:
		// Check memory usage
		if cb.memoryUsagePct() > cb.maxMemoryPct {
			cb.trip("memory threshold exceeded")
			return false
		}

		// Check concurrent operations
		if atomic.LoadInt64(&cb.concurrentOps) >= int64(cb.maxConcurrent) {
			cb.trip("concurrent operations limit exceeded")
			return false
		}

		return true
	}

	return true
}

// Start marks the beginning of an operation.
func (cb *CircuitBreaker) Start() {
	atomic.AddInt64(&cb.concurrentOps, 1)
}

// End marks the end of an operation.
func (cb *CircuitBreaker) End(success bool) {
	atomic.AddInt64(&cb.concurrentOps, -1)

	if !success {
		atomic.AddInt64(&cb.failures, 1)
		cb.mu.Lock()
		cb.lastFailure = time.Now()
		cb.mu.Unlock()
	} else if cb.state == CircuitHalfOpen {
		cb.reset()
	}
}

func (cb *CircuitBreaker) trip(reason string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == CircuitOpen {
		return
	}

	cb.state = CircuitOpen
	cb.tripTime = time.Now()

	if cb.OnTrip != nil {
		go cb.OnTrip(reason)
	}
}

func (cb *CircuitBreaker) reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state = CircuitClosed
	cb.failures = 0

	if cb.OnReset != nil {
		go cb.OnReset()
	}
}

func (cb *CircuitBreaker) memoryUsagePct() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Estimate total available memory (simplified)
	totalMem := m.Sys
	usedMem := m.Alloc

	if totalMem == 0 {
		return 0
	}

	return float64(usedMem) / float64(totalMem)
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// PoisonPillHandler safely processes potentially malicious or malformed data.
type PoisonPillHandler struct {
	// Configuration
	maxRowSize     int           // Max bytes per row before skipping
	parseTimeout   time.Duration // Max time to parse a single row
	maxErrorsInRow int           // Max consecutive errors before pausing

	// Stats
	processed      int64
	skipped        int64
	errors         int64
	consecutiveErr int64

	// Callbacks
	OnSkip  func(rowNum int64, reason string)
	OnPause func(reason string)
}

// NewPoisonPillHandler creates a handler for malicious data.
func NewPoisonPillHandler() *PoisonPillHandler {
	return &PoisonPillHandler{
		maxRowSize:     10 * 1024 * 1024, // 10MB max per row
		parseTimeout:   5 * time.Second,
		maxErrorsInRow: 100,
	}
}

// WithMaxRowSize sets the maximum row size in bytes.
func (h *PoisonPillHandler) WithMaxRowSize(bytes int) *PoisonPillHandler {
	h.maxRowSize = bytes
	return h
}

// WithParseTimeout sets the maximum time to parse a single row.
func (h *PoisonPillHandler) WithParseTimeout(d time.Duration) *PoisonPillHandler {
	h.parseTimeout = d
	return h
}

// SafeProcess wraps a processing function with poison pill protection.
func (h *PoisonPillHandler) SafeProcess(ctx context.Context, rowNum int64, process func() error) (err error) {
	// Check for too many consecutive errors
	if atomic.LoadInt64(&h.consecutiveErr) >= int64(h.maxErrorsInRow) {
		if h.OnPause != nil {
			h.OnPause("too many consecutive errors")
		}
		// Reset counter after pause
		atomic.StoreInt64(&h.consecutiveErr, 0)
		return fmt.Errorf("processing paused due to error rate")
	}

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, h.parseTimeout)
	defer cancel()

	// Recover from panics
	defer func() {
		if r := recover(); r != nil {
			atomic.AddInt64(&h.errors, 1)
			atomic.AddInt64(&h.consecutiveErr, 1)
			atomic.AddInt64(&h.skipped, 1)

			err = fmt.Errorf("panic recovered: %v", r)
			if h.OnSkip != nil {
				h.OnSkip(rowNum, fmt.Sprintf("panic: %v", r))
			}
		}
	}()

	// Run with timeout
	done := make(chan error, 1)
	go func() {
		done <- process()
	}()

	select {
	case <-timeoutCtx.Done():
		atomic.AddInt64(&h.errors, 1)
		atomic.AddInt64(&h.consecutiveErr, 1)
		atomic.AddInt64(&h.skipped, 1)

		if h.OnSkip != nil {
			h.OnSkip(rowNum, "parse timeout")
		}
		return fmt.Errorf("row %d: parse timeout", rowNum)

	case err := <-done:
		if err != nil {
			atomic.AddInt64(&h.errors, 1)
			atomic.AddInt64(&h.consecutiveErr, 1)
			atomic.AddInt64(&h.skipped, 1)

			if h.OnSkip != nil {
				h.OnSkip(rowNum, err.Error())
			}
			return err
		}

		// Success - reset consecutive error counter
		atomic.StoreInt64(&h.consecutiveErr, 0)
		atomic.AddInt64(&h.processed, 1)
		return nil
	}
}

// Stats returns processing statistics.
func (h *PoisonPillHandler) Stats() PoisonPillStats {
	return PoisonPillStats{
		Processed: atomic.LoadInt64(&h.processed),
		Skipped:   atomic.LoadInt64(&h.skipped),
		Errors:    atomic.LoadInt64(&h.errors),
	}
}

// PoisonPillStats contains processing statistics.
type PoisonPillStats struct {
	Processed int64
	Skipped   int64
	Errors    int64
}

// Checkpoint represents a resumable processing state.
type Checkpoint struct {
	mu sync.Mutex

	// Processing state
	InputPath        string `json:"input_path"`
	OutputPath       string `json:"output_path"`
	LastProcessedRow int64  `json:"last_processed_row"`
	LastByteOffset   int64  `json:"last_byte_offset"`
	StateHash        string `json:"state_hash"`
	UpdatedAt        int64  `json:"updated_at"`

	// Statistics
	TotalRowsProcessed int64 `json:"total_rows_processed"`
	TotalBytesRead     int64 `json:"total_bytes_read"`

	// Persistence
	savePath string
	dirty    bool
}

// NewCheckpoint creates a new checkpoint tracker.
func NewCheckpoint(inputPath, outputPath, savePath string) *Checkpoint {
	return &Checkpoint{
		InputPath:  inputPath,
		OutputPath: outputPath,
		savePath:   savePath,
	}
}

// Update records progress.
func (c *Checkpoint) Update(row, byteOffset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.LastProcessedRow = row
	c.LastByteOffset = byteOffset
	c.TotalRowsProcessed++
	c.TotalBytesRead = byteOffset
	c.UpdatedAt = time.Now().UnixNano()
	c.dirty = true
}

// ShouldResume returns true if there's a valid checkpoint to resume from.
func (c *Checkpoint) ShouldResume() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.LastProcessedRow > 0
}

// GetResumePoint returns the byte offset to resume from.
func (c *Checkpoint) GetResumePoint() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.LastByteOffset
}

// IsDirty returns true if checkpoint has unsaved changes.
func (c *Checkpoint) IsDirty() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dirty
}

// MarkSaved marks the checkpoint as saved.
func (c *Checkpoint) MarkSaved() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dirty = false
}
