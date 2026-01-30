// Package pipeline defines the core interfaces for the Pipeline Pattern architecture.
// This follows the "Filters and Pipes" pattern used in Unix/Linux tools.
package pipeline

import (
	"context"
	"io"

	"github.com/logflow/logflow/internal/model"
)

// Event is the unit of data flowing through the pipeline.
// Re-exported from model for convenience.
type Event = model.Event

// Adapter is the interface for data sources (readers) and sinks (writers).
// Adapters handle I/O - they don't transform data.
type Adapter interface {
	// Name returns the adapter identifier (e.g., "csv", "xes", "parquet")
	Name() string
}

// Source reads data and emits events to a channel.
// Sources are the entry points of a pipeline.
type Source interface {
	Adapter

	// Read opens the source and emits events to the output channel.
	// The source is responsible for closing the channel when done.
	Read(ctx context.Context, r io.Reader, out chan<- *Event) error

	// SupportsFormat returns true if this source can handle the given format.
	SupportsFormat(format string) bool
}

// Sink consumes events from a channel and writes them to output.
// Sinks are the exit points of a pipeline.
type Sink interface {
	Adapter

	// Write consumes events and writes to the destination.
	Write(ctx context.Context, in <-chan *Event) error

	// Close flushes and closes the sink.
	Close() error
}

// Processor transforms events in a pipeline.
// Processors sit between Source and Sink, performing transformations.
type Processor interface {
	// Name returns the processor identifier (e.g., "filter", "sample", "anonymize")
	Name() string

	// Process reads from input channel, transforms, and writes to output channel.
	// The processor is responsible for closing the output channel when done.
	Process(ctx context.Context, in <-chan *Event, out chan<- *Event) error
}

// Inspector analyzes events without modifying them.
// Inspectors are read-only processors for gathering statistics.
type Inspector interface {
	// Name returns the inspector identifier
	Name() string

	// Inspect processes an event and updates internal state.
	Inspect(event *Event)

	// Report returns the inspection results.
	Report() interface{}
}

// PassthroughInspector wraps an Inspector to work as a Processor.
// Events pass through unchanged while being inspected.
type PassthroughInspector struct {
	inspector Inspector
}

// NewPassthroughInspector creates a processor that inspects without modifying.
func NewPassthroughInspector(i Inspector) *PassthroughInspector {
	return &PassthroughInspector{inspector: i}
}

// Name returns the wrapped inspector's name.
func (p *PassthroughInspector) Name() string {
	return p.inspector.Name()
}

// Process passes events through while inspecting them.
func (p *PassthroughInspector) Process(ctx context.Context, in <-chan *Event, out chan<- *Event) error {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-in:
			if !ok {
				return nil
			}
			p.inspector.Inspect(event)
			select {
			case out <- event:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// Report returns the inspector's report.
func (p *PassthroughInspector) Report() interface{} {
	return p.inspector.Report()
}

// ProcessorFunc is a function type that implements Processor.
// Useful for simple inline processors.
type ProcessorFunc func(ctx context.Context, in <-chan *Event, out chan<- *Event) error

// Process implements Processor.
func (f ProcessorFunc) Process(ctx context.Context, in <-chan *Event, out chan<- *Event) error {
	return f(ctx, in, out)
}

// Name returns "func" for anonymous processors.
func (f ProcessorFunc) Name() string {
	return "func"
}

// Config holds pipeline configuration.
type Config struct {
	// Source configuration
	SourceFormat string
	SourcePath   string

	// Sink configuration
	SinkFormat  string
	SinkPath    string
	Compression string

	// Column mapping — process-mining specific (kept for backward compatibility)
	CaseIDColumn    string
	ActivityColumn  string
	TimestampColumn string
	ResourceColumn  string

	// ColumnMapping is a generic mapping from logical role to physical column name.
	// Process-mining columns (case_id, activity, timestamp, resource) are
	// populated by the fields above; additional columns can be added freely.
	ColumnMapping map[string]string

	// Processing options
	BufferSize int
	BatchSize  int
	UseDuckDB  bool

	// Error handling
	ErrorPolicy    ErrorPolicy
	MaxErrors      int64 // Maximum errors before aborting (0 = unlimited)
	QuarantinePath string // Path for quarantined records

	// Callbacks
	OnError    func(ErrorRecord)
	OnSkip     func(rowNum int64, reason string)
	OnProgress func(rowsProcessed, bytesRead int64)

	// Processor-specific options
	ProcessorOptions map[string]interface{}
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() Config {
	return Config{
		CaseIDColumn:     "case:concept:name",
		ActivityColumn:   "concept:name",
		TimestampColumn:  "time:timestamp",
		ResourceColumn:   "org:resource",
		BufferSize:       64 * 1024,
		BatchSize:        1024,
		Compression:      "snappy",
		ErrorPolicy:      ErrorPolicySkip, // Default to skip bad rows
		MaxErrors:        0,               // No limit by default
		ProcessorOptions: make(map[string]interface{}),
	}
}

// Complexity represents the computational complexity of an operation.
type Complexity int

const (
	// ComplexityLight for operations Go handles efficiently (filter, anonymize)
	ComplexityLight Complexity = iota
	// ComplexityHeavy for operations requiring DuckDB (sort, group, join)
	ComplexityHeavy
)

// RequiresDuckDB returns true if the operation needs DuckDB for performance.
func RequiresDuckDB(operations []string) bool {
	heavyOps := map[string]bool{
		"sort":     true,
		"group":    true,
		"join":     true,
		"window":   true,
		"pivot":    true,
		"duration": true,
		"powerbi":  true,
		"aggregate": true,
	}

	for _, op := range operations {
		if heavyOps[op] {
			return true
		}
	}
	return false
}
