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

// Version info for metadata
const logflowVersion = "1.0.0"

// ParquetSink writes Arrow batches to Parquet files.
// Uses atomic writes (write to temp file, rename on success) to prevent corruption.
type ParquetSink struct {
	mu sync.Mutex

	path         string
	tempPath     string // Temp file path for atomic writes
	schema       *arrow.Schema
	opts         core.SinkOptions
	writer       *pqarrow.FileWriter
	file         *os.File
	rowsWritten  int64
	startTime    time.Time
	sourceFile   string // Original source file path
	sourceFormat string // Original format (csv, json, xes, etc.)
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
	s.opts = opts
	s.startTime = time.Now()

	// Extract source info from metadata if provided
	if opts.Metadata != nil {
		s.sourceFile = opts.Metadata["source_file"]
		s.sourceFormat = opts.Metadata["source_format"]
	}

	// Add lineage metadata to schema
	metaKeys := []string{
		"logflow.version",
		"logflow.created_at",
		"logflow.schema_fields",
	}
	metaValues := []string{
		logflowVersion,
		s.startTime.Format(time.RFC3339),
		fmt.Sprintf("%d", schema.NumFields()),
	}
	if s.sourceFile != "" {
		metaKeys = append(metaKeys, "logflow.source_file")
		metaValues = append(metaValues, s.sourceFile)
	}
	if s.sourceFormat != "" {
		metaKeys = append(metaKeys, "logflow.source_format")
		metaValues = append(metaValues, s.sourceFormat)
	}
	// Add user-provided metadata
	for k, v := range opts.Metadata {
		if k != "source_file" && k != "source_format" {
			metaKeys = append(metaKeys, "logflow.user."+k)
			metaValues = append(metaValues, v)
		}
	}

	// Create schema with metadata
	schemaMeta := arrow.NewMetadata(metaKeys, metaValues)
	schemaWithMeta := arrow.NewSchema(schema.Fields(), &schemaMeta)
	s.schema = schemaWithMeta

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(s.path), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// ATOMIC WRITE: Create temp file, rename on successful close
	s.tempPath = s.path + ".tmp." + fmt.Sprintf("%d", time.Now().UnixNano())
	file, err := os.Create(s.tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	s.file = file

	// Configure writer
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(getCompression(opts.Compression)),
		parquet.WithDictionaryDefault(opts.DictionaryEncoding),
		parquet.WithStats(opts.Statistics),
		parquet.WithCreatedBy("LogFlow "+logflowVersion),
	)

	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)

	writer, err := pqarrow.NewFileWriter(schemaWithMeta, file, writerProps, arrowProps)
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
// Uses atomic rename: temp file is renamed to final path only on success.
func (s *ParquetSink) Close(ctx context.Context) (*core.SinkResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writer == nil {
		return nil, fmt.Errorf("sink not open")
	}

	// Close writer (this also closes the underlying file)
	if err := s.writer.Close(); err != nil {
		// ATOMIC WRITE: Clean up temp file on failure
		os.Remove(s.tempPath)
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}
	s.writer = nil
	s.file = nil

	// ATOMIC WRITE: Rename temp file to final path
	if err := os.Rename(s.tempPath, s.path); err != nil {
		// Clean up temp file on rename failure
		os.Remove(s.tempPath)
		return nil, fmt.Errorf("failed to rename temp file to final path: %w", err)
	}

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

// Abort cancels the write and cleans up the temp file.
func (s *ParquetSink) Abort() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writer != nil {
		s.writer.Close()
		s.writer = nil
	}
	if s.file != nil {
		s.file.Close()
		s.file = nil
	}
	if s.tempPath != "" {
		os.Remove(s.tempPath)
	}
	return nil
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
