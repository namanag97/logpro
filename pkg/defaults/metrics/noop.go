// Package metrics provides default metrics implementations.
package metrics

import (
	"time"

	"github.com/logflow/logflow/pkg/interfaces"
)

// NoopMetrics discards all metrics.
// Use this when metrics collection is not needed.
type NoopMetrics struct{}

// NewNoopMetrics creates a new noop metrics exporter.
func NewNoopMetrics() *NoopMetrics {
	return &NoopMetrics{}
}

// Counter does nothing.
func (n *NoopMetrics) Counter(name string, value int64, tags map[string]string) {}

// Gauge does nothing.
func (n *NoopMetrics) Gauge(name string, value float64, tags map[string]string) {}

// Histogram does nothing.
func (n *NoopMetrics) Histogram(name string, value float64, tags map[string]string) {}

// Timer does nothing.
func (n *NoopMetrics) Timer(name string, duration time.Duration, tags map[string]string) {}

// Flush does nothing.
func (n *NoopMetrics) Flush() error {
	return nil
}

// Close does nothing.
func (n *NoopMetrics) Close() error {
	return nil
}

// Verify interface compliance.
var _ interfaces.MetricsExporter = (*NoopMetrics)(nil)
