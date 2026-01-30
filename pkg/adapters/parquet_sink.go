package adapters

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/logflow/logflow/pkg/pipeline"
	"github.com/logflow/logflow/pkg/writer"
)

// ParquetSink writes events to Parquet format by delegating to writer.ParquetWriter.
type ParquetSink struct {
	cfg    pipeline.Config
	file   *os.File // Only set if we opened the file
	inner  *writer.ParquetWriter
	closed bool
}

// NewParquetSink creates a new Parquet sink.
func NewParquetSink(cfg pipeline.Config) (*ParquetSink, error) {
	var output io.Writer
	var file *os.File
	var err error

	if cfg.SinkPath == "-" {
		output = os.Stdout
	} else {
		file, err = os.Create(cfg.SinkPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create output file: %w", err)
		}
		output = file
	}

	return newParquetSinkWithWriter(cfg, output, file)
}

// NewParquetSinkWithWriter creates a Parquet sink with a custom writer.
func NewParquetSinkWithWriter(cfg pipeline.Config, w io.Writer) (*ParquetSink, error) {
	return newParquetSinkWithWriter(cfg, w, nil)
}

func newParquetSinkWithWriter(cfg pipeline.Config, output io.Writer, file *os.File) (*ParquetSink, error) {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1024
	}

	writerCfg := writer.Config{
		BatchSize:   batchSize,
		Compression: writer.ParseCompression(cfg.Compression),
	}

	inner, err := writer.NewParquetWriter(output, writerCfg)
	if err != nil {
		if file != nil {
			file.Close()
		}
		return nil, err
	}

	return &ParquetSink{
		cfg:   cfg,
		file:  file,
		inner: inner,
	}, nil
}

// Name returns the adapter name.
func (s *ParquetSink) Name() string {
	return "parquet"
}

// Write implements Sink.Write.
func (s *ParquetSink) Write(ctx context.Context, in <-chan *pipeline.Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-in:
			if !ok {
				// Channel closed, flush remaining
				return s.inner.Flush()
			}

			if err := s.inner.WriteEvent(ctx, event); err != nil {
				return err
			}
		}
	}
}

// Close flushes and closes the sink.
func (s *ParquetSink) Close() error {
	if s.closed {
		return nil
	}

	err := s.inner.Close()

	// Close file if we opened it
	if s.file != nil {
		s.file.Close()
	}

	s.closed = true
	return err
}

// RowsWritten returns total rows written.
func (s *ParquetSink) RowsWritten() int64 {
	return s.inner.RowsWritten()
}

// --- Factory function for registry ---

// ParquetSinkFactory creates a ParquetSink from config.
func ParquetSinkFactory(cfg pipeline.Config) (pipeline.Sink, error) {
	return NewParquetSink(cfg)
}
