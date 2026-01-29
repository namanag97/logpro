// Package pkg provides the main entry point for the LogFlow library.
//
// LogFlow is a high-performance data ingestion and analytics library
// that converts any data format to analysis-ready Parquet files.
//
// Basic usage:
//
//	// Simple file conversion
//	result, err := logflow.Convert(ctx, "data.csv", "data.parquet")
//
//	// With options
//	result, err := logflow.Convert(ctx, "data.csv", "data.parquet",
//	    logflow.WithCompression("zstd"),
//	)
//
//	// Full engine usage
//	engine, err := logflow.NewEngine()
//	defer engine.Close()
//	engine.Ingest(ctx, "events.csv", "events")
//	results, err := engine.Query(ctx, "SELECT * FROM events LIMIT 10")
package pkg

import (
	"context"
	"fmt"

	"github.com/logflow/logflow/pkg/defaults/alerting"
	"github.com/logflow/logflow/pkg/defaults/auth"
	"github.com/logflow/logflow/pkg/defaults/metrics"
	"github.com/logflow/logflow/pkg/defaults/scheduler"
	"github.com/logflow/logflow/pkg/defaults/tracer"
	"github.com/logflow/logflow/pkg/ingest"
	"github.com/logflow/logflow/pkg/interfaces"
	"github.com/logflow/logflow/pkg/query/engine"
	"github.com/logflow/logflow/pkg/storage/catalog"
	"github.com/logflow/logflow/pkg/storage/object"
)

// Engine is the main LogFlow engine combining ingestion, storage, and query.
type Engine struct {
	ingestEngine *ingest.Engine
	queryEngine  *engine.Engine
	catalog      interfaces.Catalog
	storage      interfaces.ObjectStorage
	auth         interfaces.Authenticator
	metrics      interfaces.MetricsExporter
	tracer       interfaces.Tracer
	alerter      interfaces.Alerter
	scheduler    interfaces.Scheduler
}

// EngineOption configures the engine.
type EngineOption func(*engineConfig)

type engineConfig struct {
	catalogPath string
	storagePath string
	auth        interfaces.Authenticator
	metrics     interfaces.MetricsExporter
	tracer      interfaces.Tracer
	alerter     interfaces.Alerter
	scheduler   interfaces.Scheduler
}

// WithCatalogPath sets the catalog storage path.
func WithCatalogPath(path string) EngineOption {
	return func(c *engineConfig) { c.catalogPath = path }
}

// WithStoragePath sets the data storage path.
func WithStoragePath(path string) EngineOption {
	return func(c *engineConfig) { c.storagePath = path }
}

// WithAuthenticator sets the authenticator.
func WithAuthenticator(auth interfaces.Authenticator) EngineOption {
	return func(c *engineConfig) { c.auth = auth }
}

// WithMetricsExporter sets the metrics exporter.
func WithMetricsExporter(m interfaces.MetricsExporter) EngineOption {
	return func(c *engineConfig) { c.metrics = m }
}

// WithTracer sets the tracer.
func WithTracer(t interfaces.Tracer) EngineOption {
	return func(c *engineConfig) { c.tracer = t }
}

// WithAlerter sets the alerter.
func WithAlerter(a interfaces.Alerter) EngineOption {
	return func(c *engineConfig) { c.alerter = a }
}

// WithScheduler sets the scheduler.
func WithScheduler(s interfaces.Scheduler) EngineOption {
	return func(c *engineConfig) { c.scheduler = s }
}

// NewEngine creates a new LogFlow engine.
func NewEngine(opts ...EngineOption) (*Engine, error) {
	cfg := &engineConfig{
		catalogPath: "./catalog",
		storagePath: "./data",
	}
	for _, opt := range opts {
		opt(cfg)
	}

	// Create ingest engine
	ingestEng, err := ingest.NewEngine()
	if err != nil {
		return nil, fmt.Errorf("failed to create ingest engine: %w", err)
	}

	// Create query engine
	queryEng, err := engine.NewEngine()
	if err != nil {
		ingestEng.Close()
		return nil, fmt.Errorf("failed to create query engine: %w", err)
	}

	// Create catalog
	cat, err := catalog.NewFileCatalog(cfg.catalogPath)
	if err != nil {
		ingestEng.Close()
		queryEng.Close()
		return nil, fmt.Errorf("failed to create catalog: %w", err)
	}

	// Create storage
	store, err := object.NewLocalStorage(cfg.storagePath)
	if err != nil {
		ingestEng.Close()
		queryEng.Close()
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	// Use defaults for optional components
	authImpl := cfg.auth
	if authImpl == nil {
		authImpl = auth.NewNoopAuthenticator()
	}
	metricsImpl := cfg.metrics
	if metricsImpl == nil {
		metricsImpl = metrics.NewNoopMetrics()
	}
	tracerImpl := cfg.tracer
	if tracerImpl == nil {
		tracerImpl = tracer.NewNoopTracer()
	}
	alerterImpl := cfg.alerter
	if alerterImpl == nil {
		alerterImpl = alerting.NewNoopAlerter()
	}
	schedulerImpl := cfg.scheduler
	if schedulerImpl == nil {
		schedulerImpl = scheduler.NewNoopScheduler()
	}

	return &Engine{
		ingestEngine: ingestEng,
		queryEngine:  queryEng,
		catalog:      cat,
		storage:      store,
		auth:         authImpl,
		metrics:      metricsImpl,
		tracer:       tracerImpl,
		alerter:      alerterImpl,
		scheduler:    schedulerImpl,
	}, nil
}

// Close releases all resources.
func (e *Engine) Close() error {
	var firstErr error
	if err := e.ingestEngine.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := e.queryEngine.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := e.metrics.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := e.tracer.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := e.alerter.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// IngestOption configures ingestion.
type IngestOption func(*ingest.Options)

// WithCompression sets compression algorithm.
func WithCompression(c string) IngestOption {
	return func(o *ingest.Options) { o.Compression = c }
}

// WithQuality enables quality validation.
func WithQuality(enabled bool) IngestOption {
	return func(o *ingest.Options) { o.EnableQuality = enabled }
}

// WithChecksum enables checksum calculation.
func WithChecksum(enabled bool) IngestOption {
	return func(o *ingest.Options) { o.EnableChecksum = enabled }
}

// WithWorkers sets parallelism.
func WithWorkers(n int) IngestOption {
	return func(o *ingest.Options) { o.Workers = n }
}

// WithRowGroupSize sets Parquet row group size.
func WithRowGroupSize(size int) IngestOption {
	return func(o *ingest.Options) { o.RowGroupSize = size }
}

// WithMaxErrors sets max errors before abort.
func WithMaxErrors(n int) IngestOption {
	return func(o *ingest.Options) { o.MaxErrors = n }
}

// Ingest processes a file and stores it as a table.
func (e *Engine) Ingest(ctx context.Context, inputPath, tableName string, opts ...IngestOption) (*ingest.Result, error) {
	options := ingest.DefaultOptions()
	for _, opt := range opts {
		opt(&options)
	}
	return e.ingestEngine.Ingest(ctx, inputPath, options)
}

// Query executes a SQL query and returns results.
func (e *Engine) Query(ctx context.Context, sql string) (*engine.Result, error) {
	return e.queryEngine.Query(ctx, sql)
}

// RegisterTable registers a Parquet file as a queryable table.
func (e *Engine) RegisterTable(ctx context.Context, name, path string) error {
	return e.queryEngine.RegisterTable(ctx, name, path)
}

// Catalog returns the catalog.
func (e *Engine) Catalog() interfaces.Catalog { return e.catalog }

// Storage returns the storage.
func (e *Engine) Storage() interfaces.ObjectStorage { return e.storage }

// Convert is a convenience function that converts a file to Parquet.
func Convert(ctx context.Context, inputPath, outputPath string, opts ...IngestOption) (*ingest.Result, error) {
	eng, err := ingest.NewEngine()
	if err != nil {
		return nil, err
	}
	defer eng.Close()

	options := ingest.DefaultOptions()
	options.OutputPath = outputPath
	for _, opt := range opts {
		opt(&options)
	}

	return eng.Ingest(ctx, inputPath, options)
}

// QuickConvert is the simplest way to convert a file.
func QuickConvert(ctx context.Context, inputPath, outputPath string) (*ingest.Result, error) {
	return ingest.QuickIngest(ctx, inputPath, outputPath)
}

// Analyze analyzes a file without processing it.
func Analyze(path string) (*ingest.FileAnalysis, error) {
	eng, err := ingest.NewEngine()
	if err != nil {
		return nil, err
	}
	defer eng.Close()
	return eng.Analyze(path)
}

// Version information
const (
	Version   = "0.1.0"
	GitCommit = "dev"
)
