// Package sinks provides output sink implementations.
package sinks

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// ParquetSink writes Arrow batches to Parquet files.
type ParquetSink struct {
	mu sync.Mutex

	path       string
	schema     *arrow.Schema
	opts       core.SinkOptions
	writer     *pqarrow.FileWriter
	file       *os.File
	rowsWritten int64
	startTime   time.Time
}

// NewParquetSink creates a Parquet sink.
func NewParquetSink() *ParquetSink {
	return &ParquetSink{}
}

// Open prepares the sink for writing.
func (s *ParquetSink) Open(ctx context.Context, schema *arrow.Schema, opts core.SinkOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.path = opts.Path
	s.schema = schema
	s.opts = opts
	s.startTime = time.Now()

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(s.path), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Open file
	file, err := os.Create(s.path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	s.file = file

	// Configure writer
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(getCompression(opts.Compression)),
		parquet.WithDictionaryDefault(opts.DictionaryEncoding),
		parquet.WithStats(opts.Statistics),
	)

	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)

	writer, err := pqarrow.NewFileWriter(schema, file, writerProps, arrowProps)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	s.writer = writer

	return nil
}

// Write writes a batch to the sink.
func (s *ParquetSink) Write(ctx context.Context, batch arrow.Record) error {
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
func (s *ParquetSink) Flush(ctx context.Context) error {
	// Parquet writer doesn't have explicit flush
	return nil
}

// Close finalizes and closes the sink.
func (s *ParquetSink) Close(ctx context.Context) (*core.SinkResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writer == nil {
		return nil, fmt.Errorf("sink not open")
	}

	// Close writer (this also closes the underlying file)
	if err := s.writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}
	s.writer = nil
	s.file = nil

	// Get file size
	info, _ := os.Stat(s.path)
	bytesWritten := int64(0)
	if info != nil {
		bytesWritten = info.Size()
	}

	return &core.SinkResult{
		Path:         s.path,
		RowsWritten:  s.rowsWritten,
		BytesWritten: bytesWritten,
		FilesWritten: 1,
		Duration:     time.Since(s.startTime),
	}, nil
}

// getCompression converts compression enum to Parquet codec.
func getCompression(c core.Compression) compress.Compression {
	switch c {
	case core.CompressionSnappy:
		return compress.Codecs.Snappy
	case core.CompressionGzip:
		return compress.Codecs.Gzip
	case core.CompressionLZ4:
		return compress.Codecs.Lz4
	case core.CompressionZstd:
		return compress.Codecs.Zstd
	case core.CompressionBrotli:
		return compress.Codecs.Brotli
	default:
		return compress.Codecs.Uncompressed
	}
}

// NullSink discards all data (for testing/benchmarking).
type NullSink struct {
	rowsWritten int64
	startTime   time.Time
}

// NewNullSink creates a null sink.
func NewNullSink() *NullSink {
	return &NullSink{}
}

func (s *NullSink) Open(ctx context.Context, schema *arrow.Schema, opts core.SinkOptions) error {
	s.startTime = time.Now()
	return nil
}

func (s *NullSink) Write(ctx context.Context, batch arrow.Record) error {
	s.rowsWritten += batch.NumRows()
	return nil
}

func (s *NullSink) Flush(ctx context.Context) error {
	return nil
}

func (s *NullSink) Close(ctx context.Context) (*core.SinkResult, error) {
	return &core.SinkResult{
		RowsWritten: s.rowsWritten,
		Duration:    time.Since(s.startTime),
	}, nil
}
