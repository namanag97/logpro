package flow

import (
	"context"
	"runtime"
	"sync"
)

// ConcurrencyLimiter limits concurrent operations per category.
type ConcurrencyLimiter struct {
	mu         sync.Mutex
	semaphores map[string]chan struct{}
	limits     map[string]int
}

// NewConcurrencyLimiter creates a new limiter.
func NewConcurrencyLimiter() *ConcurrencyLimiter {
	return &ConcurrencyLimiter{
		semaphores: make(map[string]chan struct{}),
		limits:     make(map[string]int),
	}
}

// SetLimit sets the concurrency limit for a category.
func (c *ConcurrencyLimiter) SetLimit(category string, limit int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.limits[category] = limit
	c.semaphores[category] = make(chan struct{}, limit)
}

// Acquire acquires a slot for the category.
func (c *ConcurrencyLimiter) Acquire(ctx context.Context, category string) error {
	c.mu.Lock()
	sem, ok := c.semaphores[category]
	if !ok {
		// Default limit
		limit := runtime.NumCPU()
		c.limits[category] = limit
		sem = make(chan struct{}, limit)
		c.semaphores[category] = sem
	}
	c.mu.Unlock()

	select {
	case sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release releases a slot for the category.
func (c *ConcurrencyLimiter) Release(category string) {
	c.mu.Lock()
	sem, ok := c.semaphores[category]
	c.mu.Unlock()

	if ok {
		select {
		case <-sem:
		default:
		}
	}
}

// WorkerPool manages a pool of workers.
type WorkerPool struct {
	workers int
	tasks   chan func()
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewWorkerPool creates a worker pool.
func NewWorkerPool(workers int) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &WorkerPool{
		workers: workers,
		tasks:   make(chan func(), workers*10),
		ctx:     ctx,
		cancel:  cancel,
	}
	p.start()
	return p
}

func (p *WorkerPool) start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *WorkerPool) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case task, ok := <-p.tasks:
			if !ok {
				return
			}
			task()
		}
	}
}

// Submit submits a task to the pool.
func (p *WorkerPool) Submit(task func()) error {
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case p.tasks <- task:
		return nil
	}
}

// Close shuts down the pool.
func (p *WorkerPool) Close() {
	p.cancel()
	close(p.tasks)
	p.wg.Wait()
}

// Wait waits for all tasks to complete.
func (p *WorkerPool) Wait() {
	p.wg.Wait()
}
