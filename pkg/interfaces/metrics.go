package interfaces

import "time"

// MetricsExporter exports metrics to a monitoring backend.
type MetricsExporter interface {
	// Counter increments a counter metric.
	Counter(name string, value int64, tags map[string]string)

	// Gauge sets a gauge metric to the specified value.
	Gauge(name string, value float64, tags map[string]string)

	// Histogram records a value in a histogram.
	Histogram(name string, value float64, tags map[string]string)

	// Timer records a duration.
	Timer(name string, duration time.Duration, tags map[string]string)

	// Flush sends any buffered metrics to the backend.
	Flush() error

	// Close releases resources.
	Close() error
}

// Common metric names used throughout the system.
const (
	// Ingest metrics
	MetricIngestRowsTotal      = "logflow.ingest.rows.total"
	MetricIngestBytesTotal     = "logflow.ingest.bytes.total"
	MetricIngestDuration       = "logflow.ingest.duration"
	MetricIngestErrors         = "logflow.ingest.errors"
	MetricIngestBatchSize      = "logflow.ingest.batch_size"
	MetricIngestThroughput     = "logflow.ingest.throughput"

	// Query metrics
	MetricQueryDuration        = "logflow.query.duration"
	MetricQueryRowsScanned     = "logflow.query.rows_scanned"
	MetricQueryRowsReturned    = "logflow.query.rows_returned"
	MetricQueryErrors          = "logflow.query.errors"
	MetricQueryCacheHits       = "logflow.query.cache_hits"
	MetricQueryCacheMisses     = "logflow.query.cache_misses"

	// Storage metrics
	MetricStorageFilesTotal    = "logflow.storage.files.total"
	MetricStorageBytesTotal    = "logflow.storage.bytes.total"
	MetricStorageWriteLatency  = "logflow.storage.write_latency"
	MetricStorageReadLatency   = "logflow.storage.read_latency"

	// Job metrics
	MetricJobsSubmitted        = "logflow.jobs.submitted"
	MetricJobsCompleted        = "logflow.jobs.completed"
	MetricJobsFailed           = "logflow.jobs.failed"
	MetricJobDuration          = "logflow.job.duration"

	// System metrics
	MetricMemoryUsage          = "logflow.system.memory_usage"
	MetricCPUUsage             = "logflow.system.cpu_usage"
	MetricGoroutines           = "logflow.system.goroutines"
)

// Common tag names.
const (
	TagFormat    = "format"
	TagTable     = "table"
	TagDatabase  = "database"
	TagOperation = "operation"
	TagStatus    = "status"
	TagSource    = "source"
	TagJobType   = "job_type"
	TagJobID     = "job_id"
)
