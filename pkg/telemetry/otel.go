// Package telemetry provides OpenTelemetry OTLP gRPC export integration.
package telemetry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// OTLPConfig configures the OpenTelemetry OTLP gRPC exporter.
type OTLPConfig struct {
	// Endpoint is the OTLP gRPC endpoint (e.g., "localhost:4317")
	Endpoint string

	// ServiceName identifies this service in traces
	ServiceName string

	// ServiceVersion is the version of this service
	ServiceVersion string

	// Environment is the deployment environment (e.g., "production", "staging")
	Environment string

	// InsecureTLS disables TLS for the gRPC connection (use for local dev)
	InsecureTLS bool

	// Headers are additional headers to send with each request (e.g., auth tokens)
	Headers map[string]string

	// BatchTimeout is how long to wait before sending a batch of spans
	BatchTimeout time.Duration

	// MaxBatchSize is the maximum number of spans per batch
	MaxBatchSize int

	// MaxQueueSize is the maximum number of spans to queue before dropping
	MaxQueueSize int

	// ExportTimeout is the timeout for exporting a batch
	ExportTimeout time.Duration

	// SamplingRatio is the fraction of traces to sample (0.0 to 1.0)
	// 1.0 means sample all traces, 0.1 means sample 10%
	SamplingRatio float64
}

// DefaultOTLPConfig returns sensible defaults for OTLP configuration.
func DefaultOTLPConfig(serviceName string) OTLPConfig {
	return OTLPConfig{
		Endpoint:       "localhost:4317",
		ServiceName:    serviceName,
		ServiceVersion: "1.0.0",
		Environment:    "development",
		InsecureTLS:    true,
		BatchTimeout:   5 * time.Second,
		MaxBatchSize:   512,
		MaxQueueSize:   2048,
		ExportTimeout:  30 * time.Second,
		SamplingRatio:  1.0, // Sample all traces by default
	}
}

// OTLPExporter manages the OpenTelemetry OTLP gRPC exporter lifecycle.
type OTLPExporter struct {
	mu sync.Mutex

	cfg            OTLPConfig
	tracerProvider *sdktrace.TracerProvider
	tracer         trace.Tracer
	shutdown       func(context.Context) error
	initialized    bool
}

// NewOTLPExporter creates a new OTLP gRPC exporter.
func NewOTLPExporter(cfg OTLPConfig) *OTLPExporter {
	return &OTLPExporter{cfg: cfg}
}

// Init initializes the OTLP exporter and sets up the global tracer provider.
// Returns a shutdown function that should be called to flush and close the exporter.
func (e *OTLPExporter) Init(ctx context.Context) (func(context.Context) error, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.initialized {
		return e.shutdown, nil
	}

	// Create gRPC connection options
	opts := []grpc.DialOption{}
	if e.cfg.InsecureTLS {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Create OTLP exporter options
	exporterOpts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(e.cfg.Endpoint),
		otlptracegrpc.WithDialOption(opts...),
		otlptracegrpc.WithTimeout(e.cfg.ExportTimeout),
	}

	if e.cfg.InsecureTLS {
		exporterOpts = append(exporterOpts, otlptracegrpc.WithInsecure())
	}

	if len(e.cfg.Headers) > 0 {
		exporterOpts = append(exporterOpts, otlptracegrpc.WithHeaders(e.cfg.Headers))
	}

	// Create the exporter
	exporter, err := otlptracegrpc.New(ctx, exporterOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	// Create resource with service information
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(e.cfg.ServiceName),
			semconv.ServiceVersion(e.cfg.ServiceVersion),
			semconv.DeploymentEnvironment(e.cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create sampler
	var sampler sdktrace.Sampler
	if e.cfg.SamplingRatio >= 1.0 {
		sampler = sdktrace.AlwaysSample()
	} else if e.cfg.SamplingRatio <= 0 {
		sampler = sdktrace.NeverSample()
	} else {
		sampler = sdktrace.TraceIDRatioBased(e.cfg.SamplingRatio)
	}

	// Create batch span processor options
	bspOpts := []sdktrace.BatchSpanProcessorOption{
		sdktrace.WithBatchTimeout(e.cfg.BatchTimeout),
		sdktrace.WithMaxExportBatchSize(e.cfg.MaxBatchSize),
		sdktrace.WithMaxQueueSize(e.cfg.MaxQueueSize),
		sdktrace.WithExportTimeout(e.cfg.ExportTimeout),
	}

	// Create tracer provider
	e.tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, bspOpts...),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	// Set global tracer provider
	otel.SetTracerProvider(e.tracerProvider)

	// Set up propagators for distributed tracing
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Get tracer for this service
	e.tracer = e.tracerProvider.Tracer(e.cfg.ServiceName)

	// Create shutdown function
	e.shutdown = func(ctx context.Context) error {
		e.mu.Lock()
		defer e.mu.Unlock()

		if !e.initialized {
			return nil
		}

		e.initialized = false
		return e.tracerProvider.Shutdown(ctx)
	}

	e.initialized = true
	return e.shutdown, nil
}

// Tracer returns the OpenTelemetry tracer.
func (e *OTLPExporter) Tracer() trace.Tracer {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.tracer
}

// TracerProvider returns the tracer provider.
func (e *OTLPExporter) TracerProvider() *sdktrace.TracerProvider {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.tracerProvider
}

// IsInitialized returns whether the exporter has been initialized.
func (e *OTLPExporter) IsInitialized() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.initialized
}

// --- Integration with existing Tracer ---

// OTLPTracer wraps the existing Tracer with OTLP export capability.
type OTLPTracer struct {
	*Tracer
	exporter *OTLPExporter
	otelSpans sync.Map // spanID -> trace.Span
}

// NewOTLPTracer creates a new tracer with OTLP export.
func NewOTLPTracer(cfg OTLPConfig) (*OTLPTracer, error) {
	exporter := NewOTLPExporter(cfg)
	_, err := exporter.Init(context.Background())
	if err != nil {
		return nil, err
	}

	return &OTLPTracer{
		Tracer:   NewTracer(cfg.ServiceName),
		exporter: exporter,
	}, nil
}

// StartSpan starts a new span with OTLP export.
func (t *OTLPTracer) StartSpan(ctx context.Context, name string, opts ...SpanOption) (context.Context, *Span) {
	// Start the native span
	ctx, span := t.Tracer.StartSpan(ctx, name)

	// Also start an OTEL span if exporter is initialized
	if t.exporter.IsInitialized() {
		otelTracer := t.exporter.Tracer()
		if otelTracer != nil {
			var otelOpts []trace.SpanStartOption
			for _, opt := range opts {
				otelOpts = append(otelOpts, opt.toOTEL()...)
			}
			var otelSpan trace.Span
			ctx, otelSpan = otelTracer.Start(ctx, name, otelOpts...)
			t.otelSpans.Store(span.SpanID, otelSpan)
		}
	}

	return ctx, span
}

// EndSpan ends a span and exports to OTLP.
func (t *OTLPTracer) EndSpan(span *Span) {
	// End the native span
	t.Tracer.EndSpan(span)

	// End the OTEL span if it exists
	if otelSpan, ok := t.otelSpans.LoadAndDelete(span.SpanID); ok {
		s := otelSpan.(trace.Span)

		// Transfer attributes
		for k, v := range span.Attributes {
			s.SetAttributes(toOTELAttribute(k, v))
		}

		// Transfer status
		if span.Status == SpanStatusError {
			s.SetStatus(1, span.StatusMsg) // codes.Error = 1
		}

		s.End()
	}
}

// Shutdown shuts down the OTLP exporter.
func (t *OTLPTracer) Shutdown(ctx context.Context) error {
	if t.exporter.shutdown != nil {
		return t.exporter.shutdown(ctx)
	}
	return nil
}

// SpanOption configures span creation.
type SpanOption struct {
	key   string
	value interface{}
}

// WithAttribute adds an attribute to the span.
func WithAttribute(key string, value interface{}) SpanOption {
	return SpanOption{key: key, value: value}
}

func (o SpanOption) toOTEL() []trace.SpanStartOption {
	return []trace.SpanStartOption{
		trace.WithAttributes(toOTELAttribute(o.key, o.value)),
	}
}

// toOTELAttribute converts a key-value pair to an OTEL attribute.
func toOTELAttribute(key string, value interface{}) attribute.KeyValue {
	switch v := value.(type) {
	case string:
		return attribute.String(key, v)
	case int:
		return attribute.Int(key, v)
	case int64:
		return attribute.Int64(key, v)
	case float64:
		return attribute.Float64(key, v)
	case bool:
		return attribute.Bool(key, v)
	default:
		return attribute.String(key, fmt.Sprintf("%v", v))
	}
}

// --- Convenience Functions ---

// InitOTLP initializes OpenTelemetry with gRPC exporter and returns shutdown function.
// This is the main entry point for enabling OTLP export.
func InitOTLP(cfg OTLPConfig) (func(context.Context) error, error) {
	exporter := NewOTLPExporter(cfg)
	return exporter.Init(context.Background())
}

// MustInitOTLP initializes OTLP or panics on error.
func MustInitOTLP(cfg OTLPConfig) func(context.Context) error {
	shutdown, err := InitOTLP(cfg)
	if err != nil {
		panic(fmt.Sprintf("failed to initialize OTLP: %v", err))
	}
	return shutdown
}

// GlobalTracer returns the global OTEL tracer.
func GlobalTracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

// StartSpanFromContext starts a span using the global tracer.
func StartSpanFromContext(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer("").Start(ctx, name, opts...)
}

// --- Tracer Enhancement ---

// WithOTLP enhances an existing Tracer with OTLP export capability.
func (t *Tracer) WithOTLP(cfg OTLPConfig) (*OTLPTracer, error) {
	exporter := NewOTLPExporter(cfg)
	_, err := exporter.Init(context.Background())
	if err != nil {
		return nil, err
	}

	return &OTLPTracer{
		Tracer:   t,
		exporter: exporter,
	}, nil
}

// --- Span Helpers ---

// RecordError records an error on the current span from context.
func RecordError(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.RecordError(err)
	}
}

// SetSpanStatus sets the status on the current span from context.
func SetSpanStatus(ctx context.Context, code int, description string) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.SetStatus(1, description)
	}
}

// AddSpanEvent adds an event to the current span from context.
func AddSpanEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.AddEvent(name, trace.WithAttributes(attrs...))
	}
}

// SetSpanAttributes sets attributes on the current span from context.
func SetSpanAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.SetAttributes(attrs...)
	}
}
