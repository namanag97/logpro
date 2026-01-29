package metrics

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/logflow/logflow/pkg/interfaces"
)

// LogMetrics writes metrics to the standard logger.
// Useful for debugging and development.
type LogMetrics struct {
	mu      sync.Mutex
	prefix  string
	minLevel LogLevel
	buffer  []string
	bufferSize int
}

// LogLevel controls which metrics are logged.
type LogLevel int

const (
	LogLevelAll LogLevel = iota
	LogLevelTimers
	LogLevelNone
)

// LogMetricsOption configures LogMetrics.
type LogMetricsOption func(*LogMetrics)

// WithPrefix sets the log prefix.
func WithPrefix(prefix string) LogMetricsOption {
	return func(m *LogMetrics) {
		m.prefix = prefix
	}
}

// WithMinLevel sets the minimum log level.
func WithMinLevel(level LogLevel) LogMetricsOption {
	return func(m *LogMetrics) {
		m.minLevel = level
	}
}

// WithBufferSize sets the buffer size for batched logging.
func WithBufferSize(size int) LogMetricsOption {
	return func(m *LogMetrics) {
		m.bufferSize = size
	}
}

// NewLogMetrics creates a new log-based metrics exporter.
func NewLogMetrics(opts ...LogMetricsOption) *LogMetrics {
	m := &LogMetrics{
		prefix:     "[metrics]",
		minLevel:   LogLevelAll,
		bufferSize: 0, // Default: immediate logging
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Counter logs a counter metric.
func (m *LogMetrics) Counter(name string, value int64, tags map[string]string) {
	if m.minLevel >= LogLevelTimers {
		return
	}
	m.log("counter", name, fmt.Sprintf("%d", value), tags)
}

// Gauge logs a gauge metric.
func (m *LogMetrics) Gauge(name string, value float64, tags map[string]string) {
	if m.minLevel >= LogLevelTimers {
		return
	}
	m.log("gauge", name, fmt.Sprintf("%.4f", value), tags)
}

// Histogram logs a histogram metric.
func (m *LogMetrics) Histogram(name string, value float64, tags map[string]string) {
	if m.minLevel >= LogLevelTimers {
		return
	}
	m.log("histogram", name, fmt.Sprintf("%.4f", value), tags)
}

// Timer logs a timer metric.
func (m *LogMetrics) Timer(name string, duration time.Duration, tags map[string]string) {
	if m.minLevel >= LogLevelNone {
		return
	}
	m.log("timer", name, duration.String(), tags)
}

// Flush outputs any buffered metrics.
func (m *LogMetrics) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.buffer) > 0 {
		for _, line := range m.buffer {
			log.Println(line)
		}
		m.buffer = nil
	}
	return nil
}

// Close flushes and closes the exporter.
func (m *LogMetrics) Close() error {
	return m.Flush()
}

func (m *LogMetrics) log(metricType, name, value string, tags map[string]string) {
	tagStr := formatTags(tags)
	line := fmt.Sprintf("%s %s %s=%s%s", m.prefix, metricType, name, value, tagStr)

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.bufferSize > 0 {
		m.buffer = append(m.buffer, line)
		if len(m.buffer) >= m.bufferSize {
			for _, l := range m.buffer {
				log.Println(l)
			}
			m.buffer = nil
		}
	} else {
		log.Println(line)
	}
}

func formatTags(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}

	// Sort keys for consistent output
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%s", k, tags[k])
	}
	return " {" + strings.Join(parts, ", ") + "}"
}

// Verify interface compliance.
var _ interfaces.MetricsExporter = (*LogMetrics)(nil)
