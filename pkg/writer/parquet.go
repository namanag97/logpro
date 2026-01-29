package writer

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"

	"github.com/logflow/logflow/internal/model"
	"github.com/logflow/logflow/internal/pool"
)

// ParquetWriter writes events to Parquet format using Apache Arrow.
type ParquetWriter struct {
	cfg    Config
	output io.Writer

	allocator memory.Allocator
	schema    *arrow.Schema
	writer    *pqarrow.FileWriter

	// Arrow builders for each column
	caseIDBuilder    *array.StringBuilder
	activityBuilder  *array.StringBuilder
	timestampBuilder *array.Int64Builder
	resourceBuilder  *array.StringBuilder

	// Buffer pool for attribute handling
	bufferPool *pool.BufferPool

	mu              sync.Mutex
	rowCount        int
	totalRowsWritten int64
	closed          bool
	eventPool       *pool.EventPool
}

// eventSchema returns the Arrow schema for events.
func eventSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "case_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "activity", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "resource", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

// NewParquetWriter creates a new Parquet writer.
func NewParquetWriter(output io.Writer, cfg Config) (*ParquetWriter, error) {
	allocator := memory.NewGoAllocator()
	schema := eventSchema()

	// Map compression type
	var codec compress.Compression
	switch cfg.Compression {
	case CompressionSnappy:
		codec = compress.Codecs.Snappy
	case CompressionGzip:
		codec = compress.Codecs.Gzip
	case CompressionZstd:
		codec = compress.Codecs.Zstd
	case CompressionLZ4:
		codec = compress.Codecs.Lz4
	default:
		codec = compress.Codecs.Uncompressed
	}

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(codec),
		parquet.WithDictionaryDefault(true),
		parquet.WithDataPageSize(1024*1024), // 1MB
	)

	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)

	// Create file writer
	writer, err := pqarrow.NewFileWriter(schema, output, writerProps, arrowProps)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	pw := &ParquetWriter{
		cfg:              cfg,
		output:           output,
		allocator:        allocator,
		schema:           schema,
		writer:           writer,
		caseIDBuilder:    array.NewStringBuilder(allocator),
		activityBuilder:  array.NewStringBuilder(allocator),
		timestampBuilder: array.NewInt64Builder(allocator),
		resourceBuilder:  array.NewStringBuilder(allocator),
		bufferPool:       pool.NewBufferPool(pool.DefaultBufferSize),
		eventPool:        pool.NewEventPool(),
	}

	// Reserve initial capacity
	pw.caseIDBuilder.Reserve(cfg.BatchSize)
	pw.activityBuilder.Reserve(cfg.BatchSize)
	pw.timestampBuilder.Reserve(cfg.BatchSize)
	pw.resourceBuilder.Reserve(cfg.BatchSize)

	return pw, nil
}

// Write implements the Writer interface.
func (w *ParquetWriter) Write(ctx context.Context, events <-chan *model.Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-events:
			if !ok {
				// Channel closed, flush remaining data
				return w.flushBatch()
			}

			if err := w.WriteEvent(ctx, event); err != nil {
				return err
			}
		}
	}
}

// WriteEvent writes a single event to the Parquet file.
// This is useful for integration with streaming pipelines.
func (w *ParquetWriter) WriteEvent(ctx context.Context, event *model.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.appendEvent(event)
	w.rowCount++

	// Write batch if we've accumulated enough rows
	if w.rowCount >= w.cfg.BatchSize {
		if err := w.flushBatch(); err != nil {
			return err
		}
	}

	// Return event to pool
	w.eventPool.Put(event)
	return nil
}

// appendEvent adds an event to the Arrow builders.
func (w *ParquetWriter) appendEvent(event *model.Event) {
	// Append values to builders (convert bytes to string at write time)
	w.caseIDBuilder.Append(string(event.CaseID))
	w.activityBuilder.Append(string(event.Activity))
	w.timestampBuilder.Append(event.Timestamp)

	if len(event.Resource) > 0 {
		w.resourceBuilder.Append(string(event.Resource))
	} else {
		w.resourceBuilder.AppendNull()
	}
}

// flushBatch writes the current batch to Parquet.
func (w *ParquetWriter) flushBatch() error {
	if w.rowCount == 0 {
		return nil
	}

	// Build arrays
	caseIDArray := w.caseIDBuilder.NewArray()
	activityArray := w.activityBuilder.NewArray()
	timestampArray := w.timestampBuilder.NewArray()
	resourceArray := w.resourceBuilder.NewArray()

	defer caseIDArray.Release()
	defer activityArray.Release()
	defer timestampArray.Release()
	defer resourceArray.Release()

	// Create record batch
	batch := array.NewRecord(w.schema, []arrow.Array{
		caseIDArray,
		activityArray,
		timestampArray,
		resourceArray,
	}, int64(w.rowCount))
	defer batch.Release()

	// Write to Parquet
	if err := w.writer.Write(batch); err != nil {
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	w.totalRowsWritten += int64(w.rowCount)
	w.rowCount = 0
	return nil
}

// Flush flushes any buffered data.
func (w *ParquetWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushBatch()
}

// Close closes the writer and releases resources.
func (w *ParquetWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	// Flush any remaining data
	if err := w.flushBatch(); err != nil {
		return err
	}

	// Close Parquet writer
	if err := w.writer.Close(); err != nil {
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}

	// Release builders
	w.caseIDBuilder.Release()
	w.activityBuilder.Release()
	w.timestampBuilder.Release()
	w.resourceBuilder.Release()

	w.closed = true
	return nil
}

// RowsWritten returns the total number of rows written.
func (w *ParquetWriter) RowsWritten() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.totalRowsWritten
}
