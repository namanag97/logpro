package adapters

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"

	"github.com/logflow/logflow/pkg/pipeline"
)

// ParquetSink writes events to Parquet format using Apache Arrow.
type ParquetSink struct {
	cfg    pipeline.Config
	output io.Writer
	file   *os.File // Only set if we opened the file

	allocator memory.Allocator
	schema    *arrow.Schema
	writer    *pqarrow.FileWriter

	// Arrow builders
	caseIDBuilder    *array.StringBuilder
	activityBuilder  *array.StringBuilder
	timestampBuilder *array.Int64Builder
	resourceBuilder  *array.StringBuilder

	mu               sync.Mutex
	rowCount         int
	totalRowsWritten int64
	closed           bool
}

// NewParquetSink creates a new Parquet sink.
func NewParquetSink(cfg pipeline.Config) (*ParquetSink, error) {
	// Open output file
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
	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "case_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "activity", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "resource", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// Map compression
	codec := mapCompression(cfg.Compression)

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
		if file != nil {
			file.Close()
		}
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1024
	}

	sink := &ParquetSink{
		cfg:              cfg,
		output:           output,
		file:             file,
		allocator:        allocator,
		schema:           schema,
		writer:           writer,
		caseIDBuilder:    array.NewStringBuilder(allocator),
		activityBuilder:  array.NewStringBuilder(allocator),
		timestampBuilder: array.NewInt64Builder(allocator),
		resourceBuilder:  array.NewStringBuilder(allocator),
	}

	// Reserve capacity
	sink.caseIDBuilder.Reserve(batchSize)
	sink.activityBuilder.Reserve(batchSize)
	sink.timestampBuilder.Reserve(batchSize)
	sink.resourceBuilder.Reserve(batchSize)

	return sink, nil
}

// Name returns the adapter name.
func (s *ParquetSink) Name() string {
	return "parquet"
}

// Write implements Sink.Write.
func (s *ParquetSink) Write(ctx context.Context, in <-chan *pipeline.Event) error {
	batchSize := s.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1024
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-in:
			if !ok {
				// Channel closed, flush remaining
				return s.flushBatch()
			}

			s.mu.Lock()
			s.appendEvent(event)
			s.rowCount++

			if s.rowCount >= batchSize {
				if err := s.flushBatch(); err != nil {
					s.mu.Unlock()
					return err
				}
			}
			s.mu.Unlock()
		}
	}
}

// appendEvent adds an event to Arrow builders.
func (s *ParquetSink) appendEvent(event *pipeline.Event) {
	s.caseIDBuilder.Append(string(event.CaseID))
	s.activityBuilder.Append(string(event.Activity))
	s.timestampBuilder.Append(event.Timestamp)

	if len(event.Resource) > 0 {
		s.resourceBuilder.Append(string(event.Resource))
	} else {
		s.resourceBuilder.AppendNull()
	}
}

// flushBatch writes the current batch to Parquet.
func (s *ParquetSink) flushBatch() error {
	if s.rowCount == 0 {
		return nil
	}

	caseIDArray := s.caseIDBuilder.NewArray()
	activityArray := s.activityBuilder.NewArray()
	timestampArray := s.timestampBuilder.NewArray()
	resourceArray := s.resourceBuilder.NewArray()

	defer caseIDArray.Release()
	defer activityArray.Release()
	defer timestampArray.Release()
	defer resourceArray.Release()

	batch := array.NewRecord(s.schema, []arrow.Array{
		caseIDArray,
		activityArray,
		timestampArray,
		resourceArray,
	}, int64(s.rowCount))
	defer batch.Release()

	if err := s.writer.Write(batch); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	s.totalRowsWritten += int64(s.rowCount)
	s.rowCount = 0
	return nil
}

// Close flushes and closes the sink.
func (s *ParquetSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	// Flush remaining
	if err := s.flushBatch(); err != nil {
		return err
	}

	// Close parquet writer
	if err := s.writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	// Release builders
	s.caseIDBuilder.Release()
	s.activityBuilder.Release()
	s.timestampBuilder.Release()
	s.resourceBuilder.Release()

	// Close file if we opened it
	if s.file != nil {
		s.file.Close()
	}

	s.closed = true
	return nil
}

// RowsWritten returns total rows written.
func (s *ParquetSink) RowsWritten() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalRowsWritten
}

// mapCompression maps compression string to Arrow compression codec.
func mapCompression(name string) compress.Compression {
	switch name {
	case "snappy":
		return compress.Codecs.Snappy
	case "gzip":
		return compress.Codecs.Gzip
	case "zstd":
		return compress.Codecs.Zstd
	case "lz4":
		return compress.Codecs.Lz4
	default:
		return compress.Codecs.Uncompressed
	}
}

// --- Factory function for registry ---

// ParquetSinkFactory creates a ParquetSink from config.
func ParquetSinkFactory(cfg pipeline.Config) (pipeline.Sink, error) {
	return NewParquetSink(cfg)
}
