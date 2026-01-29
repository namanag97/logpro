package sinks

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/ipc"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// ArrowIPCSink writes Arrow RecordBatches to Arrow IPC format.
// This format is ideal for zero-copy streaming via Arrow Flight.
type ArrowIPCSink struct {
	mu sync.Mutex

	writer      *ipc.Writer
	output      io.WriteCloser
	schema      *arrow.Schema
	opts        core.SinkOptions
	rowsWritten int64
	startTime   time.Time
}

// NewArrowIPCSink creates an Arrow IPC sink.
func NewArrowIPCSink() *ArrowIPCSink {
	return &ArrowIPCSink{}
}

// Open prepares the sink for writing.
func (s *ArrowIPCSink) Open(ctx context.Context, schema *arrow.Schema, opts core.SinkOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.schema = schema
	s.opts = opts
	s.startTime = time.Now()
	s.rowsWritten = 0

	// Create output
	var output io.WriteCloser
	var err error

	if opts.Path != "" {
		output, err = os.Create(opts.Path)
		if err != nil {
			return fmt.Errorf("failed to create file: %w", err)
		}
	} else if opts.Writer != nil {
		output = opts.Writer
	} else {
		return fmt.Errorf("no output path or writer specified")
	}

	s.output = output

	// Create IPC writer
	s.writer = ipc.NewWriter(output, ipc.WithSchema(schema))

	return nil
}

// Write writes a batch to the IPC stream.
func (s *ArrowIPCSink) Write(ctx context.Context, batch arrow.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writer == nil {
		return fmt.Errorf("sink not open")
	}

	if err := s.writer.Write(batch); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	s.rowsWritten += batch.NumRows()
	return nil
}

// Flush forces buffered data to be written.
func (s *ArrowIPCSink) Flush(ctx context.Context) error {
	return nil
}

// Close finalizes the IPC stream.
func (s *ArrowIPCSink) Close(ctx context.Context) (*core.SinkResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writer == nil {
		return nil, fmt.Errorf("sink not open")
	}

	if err := s.writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	// Get file size if output is a file
	var bytesWritten int64
	if f, ok := s.output.(*os.File); ok {
		info, _ := f.Stat()
		if info != nil {
			bytesWritten = info.Size()
		}
	}

	if err := s.output.Close(); err != nil {
		return nil, fmt.Errorf("failed to close output: %w", err)
	}

	return &core.SinkResult{
		Path:         s.opts.Path,
		RowsWritten:  s.rowsWritten,
		BytesWritten: bytesWritten,
		FilesWritten: 1,
		Duration:     time.Since(s.startTime),
	}, nil
}

// ArrowIPCStreamSink writes Arrow RecordBatches to a channel for streaming.
// Useful for Arrow Flight DoGet handlers.
type ArrowIPCStreamSink struct {
	mu sync.Mutex

	schema      *arrow.Schema
	batches     chan arrow.Record
	rowsWritten int64
	startTime   time.Time
	closed      bool
}

// NewArrowIPCStreamSink creates a streaming Arrow sink.
func NewArrowIPCStreamSink(bufferSize int) *ArrowIPCStreamSink {
	if bufferSize <= 0 {
		bufferSize = 4
	}
	return &ArrowIPCStreamSink{
		batches: make(chan arrow.Record, bufferSize),
	}
}

// Batches returns the channel of record batches.
func (s *ArrowIPCStreamSink) Batches() <-chan arrow.Record {
	return s.batches
}

// Schema returns the schema.
func (s *ArrowIPCStreamSink) Schema() *arrow.Schema {
	return s.schema
}

// Open prepares the sink for writing.
func (s *ArrowIPCStreamSink) Open(ctx context.Context, schema *arrow.Schema, opts core.SinkOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.schema = schema
	s.startTime = time.Now()
	s.rowsWritten = 0
	s.closed = false

	return nil
}

// Write writes a batch to the stream.
func (s *ArrowIPCStreamSink) Write(ctx context.Context, batch arrow.Record) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("sink closed")
	}
	s.mu.Unlock()

	// Retain the batch for the consumer
	batch.Retain()

	select {
	case s.batches <- batch:
		s.mu.Lock()
		s.rowsWritten += batch.NumRows()
		s.mu.Unlock()
		return nil
	case <-ctx.Done():
		batch.Release()
		return ctx.Err()
	}
}

// Flush is a no-op for streaming.
func (s *ArrowIPCStreamSink) Flush(ctx context.Context) error {
	return nil
}

// Close closes the batch channel.
func (s *ArrowIPCStreamSink) Close(ctx context.Context) (*core.SinkResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("already closed")
	}

	s.closed = true
	close(s.batches)

	return &core.SinkResult{
		RowsWritten: s.rowsWritten,
		Duration:    time.Since(s.startTime),
	}, nil
}

// Verify interface compliance
var (
	_ core.Sink = (*ArrowIPCSink)(nil)
	_ core.Sink = (*ArrowIPCStreamSink)(nil)
)
