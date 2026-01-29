package pipeline

import (
	"context"
	"fmt"
	"io"
	"os"

	"golang.org/x/sync/errgroup"
)

// Orchestrator connects Sources, Processors, and Sinks into a pipeline.
// It manages the lifecycle and data flow between components.
type Orchestrator struct {
	cfg        Config
	source     Source
	sink       Sink
	processors []Processor
	inspectors []*PassthroughInspector

	// Channel configuration
	bufferSize int

	// Error handling
	errors chan error
}

// NewOrchestrator creates a new pipeline orchestrator.
func NewOrchestrator(cfg Config) *Orchestrator {
	bufSize := cfg.BufferSize
	if bufSize <= 0 {
		bufSize = 4096
	}

	return &Orchestrator{
		cfg:        cfg,
		processors: make([]Processor, 0),
		inspectors: make([]*PassthroughInspector, 0),
		bufferSize: bufSize,
		errors:     make(chan error, 10),
	}
}

// SetSource sets the data source.
func (o *Orchestrator) SetSource(s Source) *Orchestrator {
	o.source = s
	return o
}

// SetSink sets the data sink.
func (o *Orchestrator) SetSink(s Sink) *Orchestrator {
	o.sink = s
	return o
}

// AddProcessor adds a processor to the pipeline.
// Processors are applied in the order they are added.
func (o *Orchestrator) AddProcessor(p Processor) *Orchestrator {
	o.processors = append(o.processors, p)
	return o
}

// AddInspector adds an inspector that passes through events while gathering stats.
func (o *Orchestrator) AddInspector(i Inspector) *Orchestrator {
	pt := NewPassthroughInspector(i)
	o.inspectors = append(o.inspectors, pt)
	o.processors = append(o.processors, pt)
	return o
}

// Run executes the pipeline.
// Data flows: Source -> Processor1 -> Processor2 -> ... -> Sink
// Uses errgroup for coordinated shutdown - any error cancels all goroutines.
func (o *Orchestrator) Run(ctx context.Context) error {
	if o.source == nil {
		return fmt.Errorf("no source configured")
	}
	if o.sink == nil {
		return fmt.Errorf("no sink configured")
	}

	// Open source file
	var reader io.Reader
	if o.cfg.SourcePath == "-" {
		reader = os.Stdin
	} else {
		file, err := os.Open(o.cfg.SourcePath)
		if err != nil {
			return fmt.Errorf("failed to open source %q: %w", o.cfg.SourcePath, err)
		}
		defer file.Close()
		reader = file
	}

	// Create channel chain
	// Source -> [chan0] -> Proc1 -> [chan1] -> Proc2 -> [chan2] -> ... -> Sink
	numStages := len(o.processors) + 1 // +1 for source
	channels := make([]chan *Event, numStages)
	for i := range channels {
		channels[i] = make(chan *Event, o.bufferSize)
	}

	// Use errgroup for coordinated shutdown
	g, ctx := errgroup.WithContext(ctx)

	// Start source goroutine
	g.Go(func() error {
		defer close(channels[0])
		if err := o.source.Read(ctx, reader, channels[0]); err != nil {
			return fmt.Errorf("source %s: %w", o.source.Name(), err)
		}
		return nil
	})

	// Start processor goroutines - each closes its output channel when done
	for i, proc := range o.processors {
		inChan := channels[i]
		outChan := channels[i+1]
		processor := proc
		stageNum := i + 1

		g.Go(func() error {
			defer close(outChan)
			if err := processor.Process(ctx, inChan, outChan); err != nil {
				return fmt.Errorf("processor %s (stage %d): %w", processor.Name(), stageNum, err)
			}
			return nil
		})
	}

	// Start sink goroutine
	finalChan := channels[len(channels)-1]
	g.Go(func() error {
		if err := o.sink.Write(ctx, finalChan); err != nil {
			return fmt.Errorf("sink %s: %w", o.sink.Name(), err)
		}
		return nil
	})

	// Wait for all goroutines - errgroup handles cancellation
	err := g.Wait()

	// Always close sink
	if closeErr := o.sink.Close(); closeErr != nil && err == nil {
		err = fmt.Errorf("failed to close sink: %w", closeErr)
	}

	return err
}

// RunWithReader executes the pipeline with a custom reader.
// Uses errgroup for coordinated shutdown - any error cancels all goroutines.
func (o *Orchestrator) RunWithReader(ctx context.Context, reader io.Reader) error {
	if o.source == nil {
		return fmt.Errorf("no source configured")
	}
	if o.sink == nil {
		return fmt.Errorf("no sink configured")
	}

	// Create channel chain
	numStages := len(o.processors) + 1
	channels := make([]chan *Event, numStages)
	for i := range channels {
		channels[i] = make(chan *Event, o.bufferSize)
	}

	// Use errgroup for coordinated shutdown
	g, ctx := errgroup.WithContext(ctx)

	// Start source
	g.Go(func() error {
		defer close(channels[0])
		if err := o.source.Read(ctx, reader, channels[0]); err != nil {
			return fmt.Errorf("source %s: %w", o.source.Name(), err)
		}
		return nil
	})

	// Start processors
	for i, proc := range o.processors {
		inChan := channels[i]
		outChan := channels[i+1]
		processor := proc

		g.Go(func() error {
			defer close(outChan)
			if err := processor.Process(ctx, inChan, outChan); err != nil {
				return fmt.Errorf("processor %s: %w", processor.Name(), err)
			}
			return nil
		})
	}

	// Start sink
	g.Go(func() error {
		if err := o.sink.Write(ctx, channels[len(channels)-1]); err != nil {
			return fmt.Errorf("sink %s: %w", o.sink.Name(), err)
		}
		return nil
	})

	err := g.Wait()

	if closeErr := o.sink.Close(); closeErr != nil && err == nil {
		err = fmt.Errorf("failed to close sink: %w", closeErr)
	}

	return err
}

// GetInspectorReports returns reports from all inspectors.
func (o *Orchestrator) GetInspectorReports() map[string]interface{} {
	reports := make(map[string]interface{})
	for _, inspector := range o.inspectors {
		reports[inspector.Name()] = inspector.Report()
	}
	return reports
}

// Result holds the results of a pipeline execution.
type Result struct {
	EventsProcessed int64
	BytesRead       int64
	BytesWritten    int64
	InspectorReports map[string]interface{}
}

// Builder provides a fluent interface for constructing pipelines.
type Builder struct {
	orchestrator *Orchestrator
	err          error
}

// NewBuilder creates a new pipeline builder.
func NewBuilder(cfg Config) *Builder {
	return &Builder{
		orchestrator: NewOrchestrator(cfg),
	}
}

// Source sets the source by name using the registry.
func (b *Builder) Source(name string, factory func(Config) (Source, error)) *Builder {
	if b.err != nil {
		return b
	}

	source, err := factory(b.orchestrator.cfg)
	if err != nil {
		b.err = fmt.Errorf("failed to create source %s: %w", name, err)
		return b
	}

	b.orchestrator.SetSource(source)
	return b
}

// Sink sets the sink by name using the registry.
func (b *Builder) Sink(name string, factory func(Config) (Sink, error)) *Builder {
	if b.err != nil {
		return b
	}

	sink, err := factory(b.orchestrator.cfg)
	if err != nil {
		b.err = fmt.Errorf("failed to create sink %s: %w", name, err)
		return b
	}

	b.orchestrator.SetSink(sink)
	return b
}

// Process adds a processor by name.
func (b *Builder) Process(name string, factory func(Config) (Processor, error)) *Builder {
	if b.err != nil {
		return b
	}

	proc, err := factory(b.orchestrator.cfg)
	if err != nil {
		b.err = fmt.Errorf("failed to create processor %s: %w", name, err)
		return b
	}

	b.orchestrator.AddProcessor(proc)
	return b
}

// Inspect adds an inspector.
func (b *Builder) Inspect(inspector Inspector) *Builder {
	if b.err != nil {
		return b
	}
	b.orchestrator.AddInspector(inspector)
	return b
}

// Build returns the configured orchestrator.
func (b *Builder) Build() (*Orchestrator, error) {
	if b.err != nil {
		return nil, b.err
	}
	return b.orchestrator, nil
}

// Run builds and runs the pipeline.
func (b *Builder) Run(ctx context.Context) error {
	if b.err != nil {
		return b.err
	}
	return b.orchestrator.Run(ctx)
}
