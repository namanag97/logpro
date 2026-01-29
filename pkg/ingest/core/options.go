package core

import (
	"time"

	"github.com/logflow/logflow/pkg/interfaces"
)

// IngestOptions configures the complete ingestion pipeline.
type IngestOptions struct {
	// Source options
	SourceID string
	Format   Format

	// Decode options
	DecodeOptions DecodeOptions

	// Sink options
	SinkOptions SinkOptions

	// Pipeline options
	Workers        int
	QueueSize      int
	FlushInterval  time.Duration
	FlushThreshold int64

	// Quality options
	EnableQuality  bool
	EnableChecksum bool
	SampleRate     float64

	// Error handling
	ErrorPolicy ErrorPolicy
	MaxErrors   int
	Quarantine  string

	// Observability
	Metrics interfaces.MetricsExporter
	Tracer  interfaces.Tracer
	Alerter interfaces.Alerter

	// Progress callback
	OnProgress func(Progress)
}

// DefaultIngestOptions returns sensible defaults.
func DefaultIngestOptions() IngestOptions {
	return IngestOptions{
		DecodeOptions:  DefaultDecodeOptions(),
		SinkOptions:    DefaultSinkOptions(),
		Workers:        4,
		QueueSize:      100,
		FlushInterval:  time.Second * 5,
		FlushThreshold: 64 * 1024 * 1024,
		EnableQuality:  true,
		EnableChecksum: true,
		SampleRate:     0.01,
		ErrorPolicy:    ErrorPolicySkip,
		MaxErrors:      1000,
	}
}

// Progress reports ingestion progress.
type Progress struct {
	// Source info
	SourceID string
	Format   Format

	// Progress metrics
	BytesRead    int64
	BytesWritten int64
	RowsRead     int64
	RowsWritten  int64
	BatchesRead  int
	BatchesWrite int

	// Timing
	StartTime   time.Time
	CurrentTime time.Time
	Duration    time.Duration

	// Rates
	BytesPerSec float64
	RowsPerSec  float64

	// Estimated completion
	PercentComplete float64
	EstimatedETA    time.Duration

	// Errors
	ErrorCount int
	Errors     []RowError
}

// Option is a functional option for IngestOptions.
type Option func(*IngestOptions)

// WithWorkers sets the number of workers.
func WithWorkers(n int) Option {
	return func(o *IngestOptions) {
		o.Workers = n
	}
}

// WithCompression sets the output compression.
func WithCompression(c Compression) Option {
	return func(o *IngestOptions) {
		o.SinkOptions.Compression = c
	}
}

// WithRowGroupSize sets the Parquet row group size.
func WithRowGroupSize(size int) Option {
	return func(o *IngestOptions) {
		o.SinkOptions.RowGroupSize = size
	}
}

// WithBatchSize sets the decode batch size.
func WithBatchSize(size int) Option {
	return func(o *IngestOptions) {
		o.DecodeOptions.BatchSize = size
	}
}

// WithQualityValidation enables/disables quality checks.
func WithQualityValidation(enabled bool) Option {
	return func(o *IngestOptions) {
		o.EnableQuality = enabled
	}
}

// WithChecksum enables/disables checksum computation.
func WithChecksum(enabled bool) Option {
	return func(o *IngestOptions) {
		o.EnableChecksum = enabled
	}
}

// WithErrorPolicy sets the error handling policy.
func WithErrorPolicy(policy ErrorPolicy) Option {
	return func(o *IngestOptions) {
		o.ErrorPolicy = policy
	}
}

// WithMaxErrors sets the maximum errors before abort.
func WithMaxErrors(n int) Option {
	return func(o *IngestOptions) {
		o.MaxErrors = n
	}
}

// WithMetrics sets the metrics exporter.
func WithMetrics(m interfaces.MetricsExporter) Option {
	return func(o *IngestOptions) {
		o.Metrics = m
	}
}

// WithTracer sets the tracer.
func WithTracer(t interfaces.Tracer) Option {
	return func(o *IngestOptions) {
		o.Tracer = t
	}
}

// WithProgress sets the progress callback.
func WithProgress(fn func(Progress)) Option {
	return func(o *IngestOptions) {
		o.OnProgress = fn
	}
}

// Apply applies functional options to IngestOptions.
func (o *IngestOptions) Apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}
