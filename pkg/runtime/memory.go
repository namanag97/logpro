// Package runtime provides runtime management for memory, CPU, and resources.
package runtime

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryManager monitors and limits memory usage.
// Critical for Lambda/serverless where OOM = failed request.
type MemoryManager struct {
	limit      int64         // Hard limit in bytes
	softLimit  int64         // Soft limit (trigger GC)
	current    atomic.Int64  // Current tracked allocation
	peak       atomic.Int64  // Peak usage
	gcCount    atomic.Int64  // Manual GC triggers
	panicOnOOM bool          // Panic instead of returning error
	mu         sync.RWMutex
	callbacks  []func(used, limit int64)
}

// MemoryConfig configures the memory manager.
type MemoryConfig struct {
	HardLimit  int64 // Max memory (0 = no limit)
	SoftLimit  int64 // GC trigger point (default: 80% of hard)
	PanicOnOOM bool  // Panic instead of error
}

// DefaultMemoryConfig returns sensible defaults.
// Defaults to 80% of system memory or 4GB, whichever is smaller.
func DefaultMemoryConfig() MemoryConfig {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Use 80% of available memory, max 4GB
	systemMem := int64(m.Sys)
	limit := systemMem * 80 / 100
	if limit > 4*1024*1024*1024 {
		limit = 4 * 1024 * 1024 * 1024
	}

	return MemoryConfig{
		HardLimit:  limit,
		SoftLimit:  limit * 80 / 100,
		PanicOnOOM: false,
	}
}

// LambdaMemoryConfig returns config for AWS Lambda.
func LambdaMemoryConfig(lambdaMemoryMB int64) MemoryConfig {
	limit := lambdaMemoryMB * 1024 * 1024
	return MemoryConfig{
		HardLimit:  limit * 85 / 100, // Leave 15% for runtime
		SoftLimit:  limit * 70 / 100,
		PanicOnOOM: false,
	}
}

// NewMemoryManager creates a new memory manager.
func NewMemoryManager(cfg MemoryConfig) *MemoryManager {
	mm := &MemoryManager{
		limit:      cfg.HardLimit,
		softLimit:  cfg.SoftLimit,
		panicOnOOM: cfg.PanicOnOOM,
	}

	if mm.softLimit == 0 && mm.limit > 0 {
		mm.softLimit = mm.limit * 80 / 100
	}

	// Set Go's memory limit if available (Go 1.19+)
	if mm.limit > 0 {
		debug.SetMemoryLimit(mm.limit)
	}

	return mm
}

// Acquire requests memory allocation.
// Returns error if allocation would exceed limit.
func (m *MemoryManager) Acquire(size int64) error {
	if m.limit == 0 {
		return nil // No limit
	}

	current := m.current.Load()
	newUsage := current + size

	if newUsage > m.limit {
		if m.panicOnOOM {
			panic(fmt.Sprintf("OOM: requested %d bytes, current %d, limit %d",
				size, current, m.limit))
		}
		return fmt.Errorf("memory limit exceeded: requested %d bytes, current %d, limit %d",
			size, current, m.limit)
	}

	// Check soft limit, trigger GC if needed
	if newUsage > m.softLimit {
		m.gcCount.Add(1)
		runtime.GC()
	}

	m.current.Add(size)

	// Update peak
	for {
		peak := m.peak.Load()
		if newUsage <= peak || m.peak.CompareAndSwap(peak, newUsage) {
			break
		}
	}

	return nil
}

// Release returns memory to the pool.
func (m *MemoryManager) Release(size int64) {
	m.current.Add(-size)
}

// Usage returns current memory statistics.
func (m *MemoryManager) Usage() MemoryUsage {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return MemoryUsage{
		TrackedBytes:  m.current.Load(),
		PeakBytes:     m.peak.Load(),
		LimitBytes:    m.limit,
		SoftLimitBytes: m.softLimit,
		HeapAlloc:     int64(ms.HeapAlloc),
		HeapSys:       int64(ms.HeapSys),
		HeapInuse:     int64(ms.HeapInuse),
		GCCount:       m.gcCount.Load(),
		SystemGCCount: int64(ms.NumGC),
	}
}

// MemoryUsage holds memory statistics.
type MemoryUsage struct {
	TrackedBytes   int64 `json:"tracked_bytes"`
	PeakBytes      int64 `json:"peak_bytes"`
	LimitBytes     int64 `json:"limit_bytes"`
	SoftLimitBytes int64 `json:"soft_limit_bytes"`
	HeapAlloc      int64 `json:"heap_alloc"`
	HeapSys        int64 `json:"heap_sys"`
	HeapInuse      int64 `json:"heap_inuse"`
	GCCount        int64 `json:"gc_count"`
	SystemGCCount  int64 `json:"system_gc_count"`
}

// AvailableBytes returns how much memory can still be allocated.
func (m *MemoryManager) AvailableBytes() int64 {
	if m.limit == 0 {
		return 1 << 62 // Effectively unlimited
	}
	return m.limit - m.current.Load()
}

// OnLowMemory registers a callback for low memory conditions.
func (m *MemoryManager) OnLowMemory(fn func(used, limit int64)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callbacks = append(m.callbacks, fn)
}

// StartMonitor starts background memory monitoring.
func (m *MemoryManager) StartMonitor(interval time.Duration) func() {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				usage := m.Usage()
				if m.softLimit > 0 && usage.HeapAlloc > m.softLimit {
					m.mu.RLock()
					for _, fn := range m.callbacks {
						fn(usage.HeapAlloc, m.limit)
					}
					m.mu.RUnlock()
				}
			}
		}
	}()
	return func() { close(done) }
}

// --- Resource Pool ---

// ResourcePool manages a pool of reusable resources.
type ResourcePool[T any] struct {
	pool   sync.Pool
	create func() T
	reset  func(T)
	mm     *MemoryManager
	size   int64 // Approximate size of each item
}

// NewResourcePool creates a new resource pool.
func NewResourcePool[T any](create func() T, reset func(T), mm *MemoryManager, sizeBytes int64) *ResourcePool[T] {
	return &ResourcePool[T]{
		pool: sync.Pool{
			New: func() interface{} {
				if mm != nil {
					mm.Acquire(sizeBytes)
				}
				return create()
			},
		},
		create: create,
		reset:  reset,
		mm:     mm,
		size:   sizeBytes,
	}
}

// Get retrieves an item from the pool.
func (p *ResourcePool[T]) Get() T {
	return p.pool.Get().(T)
}

// Put returns an item to the pool.
func (p *ResourcePool[T]) Put(item T) {
	if p.reset != nil {
		p.reset(item)
	}
	p.pool.Put(item)
}

// --- Batch Size Calculator ---

// OptimalBatchSize calculates optimal batch size based on available memory.
func OptimalBatchSize(availableBytes, rowSizeEstimate int64, minBatch, maxBatch int) int {
	if rowSizeEstimate <= 0 {
		rowSizeEstimate = 1024 // Default 1KB per row estimate
	}

	// Use 50% of available memory for batch
	batchMemory := availableBytes / 2
	calculated := int(batchMemory / rowSizeEstimate)

	if calculated < minBatch {
		return minBatch
	}
	if calculated > maxBatch {
		return maxBatch
	}
	return calculated
}

// --- Global Instance ---

var (
	globalMM   *MemoryManager
	globalOnce sync.Once
)

// GlobalMemoryManager returns the global memory manager.
func GlobalMemoryManager() *MemoryManager {
	globalOnce.Do(func() {
		globalMM = NewMemoryManager(DefaultMemoryConfig())
	})
	return globalMM
}

// SetGlobalMemoryLimit sets the global memory limit.
func SetGlobalMemoryLimit(bytes int64) {
	cfg := MemoryConfig{
		HardLimit: bytes,
		SoftLimit: bytes * 80 / 100,
	}
	globalOnce.Do(func() {})
	globalMM = NewMemoryManager(cfg)
}
