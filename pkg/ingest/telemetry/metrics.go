// Package telemetry provides built-in observability for LogFlow.
package telemetry

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects runtime metrics for the ingestion pipeline.
type Metrics struct {
	mu sync.RWMutex

	// Row counters
	RowsRead      int64
	RowsWritten   int64
	RowsSkipped   int64
	RowsQuarantined int64

	// Byte counters
	BytesRead     int64
	BytesWritten  int64

	// Batch counters
	BatchesRead   int64
	BatchesWritten int64

	// Timing
	DecodeTime    time.Duration
	WriteTime     time.Duration
	TotalTime     time.Duration
	StartTime     time.Time

	// Errors
	ErrorCount    int64
	RetryCount    int64

	// Format-specific
	FormatMetrics map[string]*FormatMetrics

	// Per-phase timing
	PhaseMetrics  map[string]*PhaseMetrics
}

// FormatMetrics tracks metrics per format.
type FormatMetrics struct {
	RowsProcessed int64
	BytesProcessed int64
	Duration      time.Duration
	ErrorCount    int64
}

// PhaseMetrics tracks metrics per pipeline phase.
type PhaseMetrics struct {
	Name      string
	Calls     int64
	TotalTime time.Duration
	MinTime   time.Duration
	MaxTime   time.Duration
	AvgTime   time.Duration
}

// NewMetrics creates a new metrics collector.
func NewMetrics() *Metrics {
	return &Metrics{
		FormatMetrics: make(map[string]*FormatMetrics),
		PhaseMetrics:  make(map[string]*PhaseMetrics),
		StartTime:     time.Now(),
	}
}

// AddRowsRead increments rows read counter.
func (m *Metrics) AddRowsRead(n int64) {
	atomic.AddInt64(&m.RowsRead, n)
}

// AddRowsWritten increments rows written counter.
func (m *Metrics) AddRowsWritten(n int64) {
	atomic.AddInt64(&m.RowsWritten, n)
}

// AddRowsSkipped increments rows skipped counter.
func (m *Metrics) AddRowsSkipped(n int64) {
	atomic.AddInt64(&m.RowsSkipped, n)
}

// AddBytesRead increments bytes read counter.
func (m *Metrics) AddBytesRead(n int64) {
	atomic.AddInt64(&m.BytesRead, n)
}

// AddBytesWritten increments bytes written counter.
func (m *Metrics) AddBytesWritten(n int64) {
	atomic.AddInt64(&m.BytesWritten, n)
}

// AddError increments error counter.
func (m *Metrics) AddError() {
	atomic.AddInt64(&m.ErrorCount, 1)
}

// RecordPhase records timing for a pipeline phase.
func (m *Metrics) RecordPhase(name string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pm, ok := m.PhaseMetrics[name]
	if !ok {
		pm = &PhaseMetrics{Name: name, MinTime: duration}
		m.PhaseMetrics[name] = pm
	}

	pm.Calls++
	pm.TotalTime += duration

	if duration < pm.MinTime || pm.MinTime == 0 {
		pm.MinTime = duration
	}
	if duration > pm.MaxTime {
		pm.MaxTime = duration
	}
	pm.AvgTime = pm.TotalTime / time.Duration(pm.Calls)
}

// RecordFormat records metrics for a specific format.
func (m *Metrics) RecordFormat(format string, rows, bytes int64, duration time.Duration, errors int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fm, ok := m.FormatMetrics[format]
	if !ok {
		fm = &FormatMetrics{}
		m.FormatMetrics[format] = fm
	}

	fm.RowsProcessed += rows
	fm.BytesProcessed += bytes
	fm.Duration += duration
	fm.ErrorCount += errors
}

// Snapshot returns a copy of current metrics.
func (m *Metrics) Snapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	elapsed := time.Since(m.StartTime)

	return MetricsSnapshot{
		RowsRead:        atomic.LoadInt64(&m.RowsRead),
		RowsWritten:     atomic.LoadInt64(&m.RowsWritten),
		RowsSkipped:     atomic.LoadInt64(&m.RowsSkipped),
		RowsQuarantined: atomic.LoadInt64(&m.RowsQuarantined),
		BytesRead:       atomic.LoadInt64(&m.BytesRead),
		BytesWritten:    atomic.LoadInt64(&m.BytesWritten),
		BatchesRead:     atomic.LoadInt64(&m.BatchesRead),
		BatchesWritten:  atomic.LoadInt64(&m.BatchesWritten),
		ErrorCount:      atomic.LoadInt64(&m.ErrorCount),
		Elapsed:         elapsed,
		RowsPerSecond:   float64(atomic.LoadInt64(&m.RowsRead)) / elapsed.Seconds(),
		BytesPerSecond:  float64(atomic.LoadInt64(&m.BytesRead)) / elapsed.Seconds(),
	}
}

// MetricsSnapshot is a point-in-time copy of metrics.
type MetricsSnapshot struct {
	RowsRead        int64
	RowsWritten     int64
	RowsSkipped     int64
	RowsQuarantined int64
	BytesRead       int64
	BytesWritten    int64
	BatchesRead     int64
	BatchesWritten  int64
	ErrorCount      int64
	Elapsed         time.Duration
	RowsPerSecond   float64
	BytesPerSecond  float64
}

// Reset resets all metrics.
func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	atomic.StoreInt64(&m.RowsRead, 0)
	atomic.StoreInt64(&m.RowsWritten, 0)
	atomic.StoreInt64(&m.RowsSkipped, 0)
	atomic.StoreInt64(&m.RowsQuarantined, 0)
	atomic.StoreInt64(&m.BytesRead, 0)
	atomic.StoreInt64(&m.BytesWritten, 0)
	atomic.StoreInt64(&m.BatchesRead, 0)
	atomic.StoreInt64(&m.BatchesWritten, 0)
	atomic.StoreInt64(&m.ErrorCount, 0)
	atomic.StoreInt64(&m.RetryCount, 0)
	m.FormatMetrics = make(map[string]*FormatMetrics)
	m.PhaseMetrics = make(map[string]*PhaseMetrics)
	m.StartTime = time.Now()
}

// Global metrics instance
var globalMetrics = NewMetrics()

// Global returns the global metrics instance.
func Global() *Metrics {
	return globalMetrics
}
