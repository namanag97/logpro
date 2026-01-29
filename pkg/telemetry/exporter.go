// Package telemetry provides observability for LogFlow.
// This file implements OTLP (OpenTelemetry Protocol) export for metrics and traces.
package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Exporter sends telemetry data to external systems.
type Exporter struct {
	mu sync.RWMutex

	endpoint   string
	headers    map[string]string
	client     *http.Client
	batchSize  int
	flushInterval time.Duration

	// Buffered data
	spans   []SpanData
	metrics []MetricData

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed bool
}

// SpanData represents a trace span for export.
type SpanData struct {
	TraceID     string            `json:"traceId"`
	SpanID      string            `json:"spanId"`
	ParentID    string            `json:"parentSpanId,omitempty"`
	Name        string            `json:"name"`
	Kind        string            `json:"kind"`
	StartTime   int64             `json:"startTimeUnixNano"`
	EndTime     int64             `json:"endTimeUnixNano"`
	Attributes  map[string]interface{} `json:"attributes,omitempty"`
	Status      ExportSpanStatus        `json:"status"`
	ServiceName string            `json:"serviceName"`
}

// ExportSpanStatus represents span status.
type ExportSpanStatus struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

// MetricData represents a metric for export.
type MetricData struct {
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Unit        string            `json:"unit,omitempty"`
	Type        string            `json:"type"` // gauge, counter, histogram
	Value       interface{}       `json:"value"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	Timestamp   int64             `json:"timestampUnixNano"`
}

// ExporterConfig configures the exporter.
type ExporterConfig struct {
	Endpoint      string            // OTLP endpoint (e.g., "https://otlp.example.com/v1/traces")
	Headers       map[string]string // Auth headers (e.g., {"Authorization": "Bearer xxx"})
	BatchSize     int               // Flush when this many items buffered (default: 100)
	FlushInterval time.Duration     // Flush interval (default: 10s)
	ServiceName   string            // Service name for traces
	Timeout       time.Duration     // HTTP timeout (default: 30s)
}

// DefaultExporterConfig returns sensible defaults.
func DefaultExporterConfig() ExporterConfig {
	return ExporterConfig{
		BatchSize:     100,
		FlushInterval: 10 * time.Second,
		ServiceName:   "logflow",
		Timeout:       30 * time.Second,
	}
}

// NewExporter creates a new telemetry exporter.
func NewExporter(cfg ExporterConfig) *Exporter {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 10 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "logflow"
	}

	ctx, cancel := context.WithCancel(context.Background())

	e := &Exporter{
		endpoint:      cfg.Endpoint,
		headers:       cfg.Headers,
		client:        &http.Client{Timeout: cfg.Timeout},
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		spans:         make([]SpanData, 0, cfg.BatchSize),
		metrics:       make([]MetricData, 0, cfg.BatchSize),
		ctx:           ctx,
		cancel:        cancel,
	}

	// Start background flush goroutine
	if cfg.Endpoint != "" {
		e.wg.Add(1)
		go e.flushLoop()
	}

	return e
}

// RecordSpan records a span for export.
func (e *Exporter) RecordSpan(span SpanData) {
	if e.endpoint == "" {
		return
	}

	e.mu.Lock()
	e.spans = append(e.spans, span)
	shouldFlush := len(e.spans) >= e.batchSize
	e.mu.Unlock()

	if shouldFlush {
		go e.Flush()
	}
}

// RecordMetric records a metric for export.
func (e *Exporter) RecordMetric(metric MetricData) {
	if e.endpoint == "" {
		return
	}

	e.mu.Lock()
	e.metrics = append(e.metrics, metric)
	shouldFlush := len(e.metrics) >= e.batchSize
	e.mu.Unlock()

	if shouldFlush {
		go e.Flush()
	}
}

// RecordCounter records a counter metric.
func (e *Exporter) RecordCounter(name string, value int64, attrs map[string]string) {
	e.RecordMetric(MetricData{
		Name:       name,
		Type:       "counter",
		Value:      value,
		Attributes: attrs,
		Timestamp:  time.Now().UnixNano(),
	})
}

// RecordGauge records a gauge metric.
func (e *Exporter) RecordGauge(name string, value float64, attrs map[string]string) {
	e.RecordMetric(MetricData{
		Name:       name,
		Type:       "gauge",
		Value:      value,
		Attributes: attrs,
		Timestamp:  time.Now().UnixNano(),
	})
}

// RecordHistogram records a histogram value.
func (e *Exporter) RecordHistogram(name string, value float64, attrs map[string]string) {
	e.RecordMetric(MetricData{
		Name:       name,
		Type:       "histogram",
		Value:      value,
		Attributes: attrs,
		Timestamp:  time.Now().UnixNano(),
	})
}

// Flush sends buffered data to the endpoint.
func (e *Exporter) Flush() error {
	e.mu.Lock()
	spans := e.spans
	metrics := e.metrics
	e.spans = make([]SpanData, 0, e.batchSize)
	e.metrics = make([]MetricData, 0, e.batchSize)
	e.mu.Unlock()

	var errs []error

	if len(spans) > 0 {
		if err := e.exportSpans(spans); err != nil {
			errs = append(errs, err)
		}
	}

	if len(metrics) > 0 {
		if err := e.exportMetrics(metrics); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("export errors: %v", errs)
	}
	return nil
}

func (e *Exporter) exportSpans(spans []SpanData) error {
	// Build OTLP traces payload
	payload := map[string]interface{}{
		"resourceSpans": []map[string]interface{}{
			{
				"resource": map[string]interface{}{
					"attributes": []map[string]interface{}{
						{"key": "service.name", "value": map[string]string{"stringValue": "logflow"}},
					},
				},
				"scopeSpans": []map[string]interface{}{
					{
						"scope": map[string]interface{}{
							"name": "logflow",
						},
						"spans": e.convertSpans(spans),
					},
				},
			},
		},
	}

	return e.send(e.endpoint+"/v1/traces", payload)
}

func (e *Exporter) convertSpans(spans []SpanData) []map[string]interface{} {
	result := make([]map[string]interface{}, len(spans))
	for i, span := range spans {
		s := map[string]interface{}{
			"traceId":           span.TraceID,
			"spanId":            span.SpanID,
			"name":              span.Name,
			"kind":              1, // INTERNAL
			"startTimeUnixNano": fmt.Sprintf("%d", span.StartTime),
			"endTimeUnixNano":   fmt.Sprintf("%d", span.EndTime),
			"status": map[string]interface{}{
				"code": span.Status.Code,
			},
		}
		if span.ParentID != "" {
			s["parentSpanId"] = span.ParentID
		}
		if len(span.Attributes) > 0 {
			attrs := make([]map[string]interface{}, 0, len(span.Attributes))
			for k, v := range span.Attributes {
				attrs = append(attrs, map[string]interface{}{
					"key":   k,
					"value": map[string]interface{}{"stringValue": fmt.Sprintf("%v", v)},
				})
			}
			s["attributes"] = attrs
		}
		result[i] = s
	}
	return result
}

func (e *Exporter) exportMetrics(metrics []MetricData) error {
	// Build OTLP metrics payload
	payload := map[string]interface{}{
		"resourceMetrics": []map[string]interface{}{
			{
				"resource": map[string]interface{}{
					"attributes": []map[string]interface{}{
						{"key": "service.name", "value": map[string]string{"stringValue": "logflow"}},
					},
				},
				"scopeMetrics": []map[string]interface{}{
					{
						"scope": map[string]interface{}{
							"name": "logflow",
						},
						"metrics": e.convertMetrics(metrics),
					},
				},
			},
		},
	}

	return e.send(e.endpoint+"/v1/metrics", payload)
}

func (e *Exporter) convertMetrics(metrics []MetricData) []map[string]interface{} {
	result := make([]map[string]interface{}, len(metrics))
	for i, m := range metrics {
		metric := map[string]interface{}{
			"name": m.Name,
		}
		if m.Description != "" {
			metric["description"] = m.Description
		}
		if m.Unit != "" {
			metric["unit"] = m.Unit
		}

		// Build data points based on type
		dataPoint := map[string]interface{}{
			"timeUnixNano": fmt.Sprintf("%d", m.Timestamp),
		}

		switch v := m.Value.(type) {
		case int64:
			dataPoint["asInt"] = fmt.Sprintf("%d", v)
		case float64:
			dataPoint["asDouble"] = v
		default:
			dataPoint["asDouble"] = 0
		}

		if len(m.Attributes) > 0 {
			attrs := make([]map[string]interface{}, 0, len(m.Attributes))
			for k, v := range m.Attributes {
				attrs = append(attrs, map[string]interface{}{
					"key":   k,
					"value": map[string]string{"stringValue": v},
				})
			}
			dataPoint["attributes"] = attrs
		}

		switch m.Type {
		case "counter":
			metric["sum"] = map[string]interface{}{
				"dataPoints":             []map[string]interface{}{dataPoint},
				"aggregationTemporality": 2, // CUMULATIVE
				"isMonotonic":            true,
			}
		case "gauge":
			metric["gauge"] = map[string]interface{}{
				"dataPoints": []map[string]interface{}{dataPoint},
			}
		case "histogram":
			metric["histogram"] = map[string]interface{}{
				"dataPoints":             []map[string]interface{}{dataPoint},
				"aggregationTemporality": 2,
			}
		default:
			metric["gauge"] = map[string]interface{}{
				"dataPoints": []map[string]interface{}{dataPoint},
			}
		}

		result[i] = metric
	}
	return result
}

func (e *Exporter) send(url string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(e.ctx, "POST", url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range e.headers {
		req.Header.Set(k, v)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("export failed with status %d", resp.StatusCode)
	}

	return nil
}

func (e *Exporter) flushLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			// Final flush before shutdown
			e.Flush()
			return
		case <-ticker.C:
			e.Flush()
		}
	}
}

// Close shuts down the exporter.
func (e *Exporter) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()

	e.cancel()
	e.wg.Wait()
	return nil
}

// Global exporter instance
var globalExporter *Exporter
var exporterOnce sync.Once

// InitGlobalExporter initializes the global exporter.
func InitGlobalExporter(cfg ExporterConfig) {
	exporterOnce.Do(func() {
		globalExporter = NewExporter(cfg)
	})
}

// GlobalExporter returns the global exporter.
func GlobalExporter() *Exporter {
	if globalExporter == nil {
		globalExporter = NewExporter(DefaultExporterConfig())
	}
	return globalExporter
}

// ShutdownExporter closes the global exporter.
func ShutdownExporter() error {
	if globalExporter != nil {
		return globalExporter.Close()
	}
	return nil
}
