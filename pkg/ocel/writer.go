package ocel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
)

// Writer writes OCEL events to Parquet with nested object lists.
type Writer struct {
	output    io.Writer
	allocator memory.Allocator
	schema    *arrow.Schema
	writer    *pqarrow.FileWriter

	// Builders for nested structure
	eventIDBuilder   *array.StringBuilder
	activityBuilder  *array.StringBuilder
	timestampBuilder *array.Int64Builder
	objectsBuilder   *array.ListBuilder // List of structs

	// Metadata
	objectTypes map[string]struct{}
	eventCount  int64

	// Config
	batchSize int
	rowCount  int
}

// WriterConfig holds writer configuration.
type WriterConfig struct {
	Compression string
	BatchSize   int
}

// DefaultWriterConfig returns sensible defaults.
func DefaultWriterConfig() WriterConfig {
	return WriterConfig{
		Compression: "zstd", // Better compression for nested data
		BatchSize:   8192,
	}
}

// NewWriter creates a new OCEL Parquet writer.
func NewWriter(output io.Writer, cfg WriterConfig) (*Writer, error) {
	allocator := memory.NewGoAllocator()
	schema := ArrowSchema()

	// Map compression
	var codec compress.Compression
	switch cfg.Compression {
	case "zstd":
		codec = compress.Codecs.Zstd
	case "snappy":
		codec = compress.Codecs.Snappy
	case "gzip":
		codec = compress.Codecs.Gzip
	default:
		codec = compress.Codecs.Zstd
	}

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(codec),
		parquet.WithDictionaryDefault(true),
		parquet.WithDataPageSize(1024*1024),
	)

	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)

	writer, err := pqarrow.NewFileWriter(schema, output, writerProps, arrowProps)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// Create builders
	// Objects list builder contains a struct builder
	objectRefStruct := arrow.StructOf(
		arrow.Field{Name: "object_id", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "object_type", Type: arrow.BinaryTypes.String, Nullable: false},
	)
	objectsBuilder := array.NewListBuilder(allocator, objectRefStruct)

	w := &Writer{
		output:           output,
		allocator:        allocator,
		schema:           schema,
		writer:           writer,
		eventIDBuilder:   array.NewStringBuilder(allocator),
		activityBuilder:  array.NewStringBuilder(allocator),
		timestampBuilder: array.NewInt64Builder(allocator),
		objectsBuilder:   objectsBuilder,
		objectTypes:      make(map[string]struct{}),
		batchSize:        cfg.BatchSize,
	}

	return w, nil
}

// Write writes a single OCEL event.
func (w *Writer) Write(event *Event) error {
	w.eventIDBuilder.Append(event.EventID)
	w.activityBuilder.Append(event.Activity)
	w.timestampBuilder.Append(event.Timestamp)

	// Build nested objects list
	w.objectsBuilder.Append(true) // Start list
	structBuilder := w.objectsBuilder.ValueBuilder().(*array.StructBuilder)

	for _, obj := range event.Objects {
		structBuilder.Append(true)
		structBuilder.FieldBuilder(0).(*array.StringBuilder).Append(obj.ObjectID)
		structBuilder.FieldBuilder(1).(*array.StringBuilder).Append(obj.ObjectType)

		// Track object types
		w.objectTypes[obj.ObjectType] = struct{}{}
	}

	w.rowCount++
	w.eventCount++

	// Flush batch if needed
	if w.rowCount >= w.batchSize {
		return w.flushBatch()
	}

	return nil
}

// WriteEvents writes multiple events from a channel.
func (w *Writer) WriteEvents(ctx context.Context, events <-chan *Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-events:
			if !ok {
				return w.flushBatch()
			}
			if err := w.Write(event); err != nil {
				return err
			}
		}
	}
}

// flushBatch writes the current batch to Parquet.
func (w *Writer) flushBatch() error {
	if w.rowCount == 0 {
		return nil
	}

	// Build arrays
	eventIDArray := w.eventIDBuilder.NewArray()
	activityArray := w.activityBuilder.NewArray()
	timestampArray := w.timestampBuilder.NewArray()
	objectsArray := w.objectsBuilder.NewArray()

	defer eventIDArray.Release()
	defer activityArray.Release()
	defer timestampArray.Release()
	defer objectsArray.Release()

	// Create record batch
	batch := array.NewRecord(w.schema, []arrow.Array{
		eventIDArray,
		activityArray,
		timestampArray,
		objectsArray,
	}, int64(w.rowCount))
	defer batch.Release()

	// Write to Parquet
	if err := w.writer.Write(batch); err != nil {
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	w.rowCount = 0
	return nil
}

// Close closes the writer and writes metadata.
func (w *Writer) Close() error {
	// Flush remaining data
	if err := w.flushBatch(); err != nil {
		return err
	}

	// Close Parquet writer
	if err := w.writer.Close(); err != nil {
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}

	// Release builders
	w.eventIDBuilder.Release()
	w.activityBuilder.Release()
	w.timestampBuilder.Release()
	w.objectsBuilder.Release()

	return nil
}

// ObjectTypes returns the discovered object types.
func (w *Writer) ObjectTypes() []string {
	types := make([]string, 0, len(w.objectTypes))
	for t := range w.objectTypes {
		types = append(types, t)
	}
	return types
}

// EventCount returns the number of events written.
func (w *Writer) EventCount() int64 {
	return w.eventCount
}

// Metadata returns the OCEL metadata for the Parquet footer.
func (w *Writer) Metadata() map[string]string {
	objectTypesJSON, _ := json.Marshal(w.ObjectTypes())
	return map[string]string{
		MetaKeyVersion:     "2.0",
		MetaKeyObjectTypes: string(objectTypesJSON),
		MetaKeyEventCount:  fmt.Sprintf("%d", w.eventCount),
	}
}

// --- Convenience Functions ---

// WriteLogToFile writes a complete OCEL log to a Parquet file.
func WriteLogToFile(log *Log, outputPath string, cfg WriterConfig) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer, err := NewWriter(file, cfg)
	if err != nil {
		return err
	}

	for i := range log.Events {
		if err := writer.Write(&log.Events[i]); err != nil {
			writer.Close()
			return err
		}
	}

	return writer.Close()
}
