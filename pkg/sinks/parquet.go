package sinks

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"

	"github.com/logflow/logflow/pkg/pipeline"
)

// ParquetSink writes events directly to a Parquet file.
type ParquetSink struct {
	cfg    pipeline.Config
	path   string
	schema *arrow.Schema
}

// NewParquetSink creates a Parquet sink.
func NewParquetSink(cfg pipeline.Config) (*ParquetSink, error) {
	path := cfg.SinkPath
	if path == "" {
		return nil, fmt.Errorf("sink path required for Parquet sink")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}
	// Default schema: all events mapped to string columns with standard fields
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "case_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "activity", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "resource", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	return &ParquetSink{cfg: cfg, path: path, schema: schema}, nil
}

func (s *ParquetSink) Name() string { return "parquet" }

func (s *ParquetSink) Write(ctx context.Context, in <-chan *pipeline.Event) error {
	file, err := os.Create(s.path)
	if err != nil {
		return fmt.Errorf("failed to create parquet file: %w", err)
	}
	defer file.Close()

	codec := mapCompressionCodec(s.cfg.Compression)
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(codec),
		parquet.WithDictionaryDefault(true),
	)
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	writer, err := pqarrow.NewFileWriter(s.schema, file, writerProps, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer writer.Close()

	allocator := memory.NewGoAllocator()
	batchSize := s.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 8192
	}

	caseIDs := array.NewStringBuilder(allocator)
	activities := array.NewStringBuilder(allocator)
	timestamps := array.NewInt64Builder(allocator)
	resources := array.NewStringBuilder(allocator)
	defer caseIDs.Release()
	defer activities.Release()
	defer timestamps.Release()
	defer resources.Release()

	rowCount := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-in:
			if !ok {
				// Flush remaining
				if rowCount > 0 {
					return flushParquetBatch(writer, s.schema, caseIDs, activities, timestamps, resources, rowCount)
				}
				return nil
			}
			caseIDs.Append(string(event.CaseID))
			activities.Append(string(event.Activity))
			timestamps.Append(event.Timestamp)
			if len(event.Resource) > 0 {
				resources.Append(string(event.Resource))
			} else {
				resources.AppendNull()
			}
			rowCount++

			if rowCount >= batchSize {
				if err := flushParquetBatch(writer, s.schema, caseIDs, activities, timestamps, resources, rowCount); err != nil {
					return err
				}
				rowCount = 0
			}
		}
	}
}

func flushParquetBatch(writer *pqarrow.FileWriter, schema *arrow.Schema, caseIDs, activities *array.StringBuilder, timestamps *array.Int64Builder, resources *array.StringBuilder, count int) error {
	cols := []arrow.Array{caseIDs.NewArray(), activities.NewArray(), timestamps.NewArray(), resources.NewArray()}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()
	batch := array.NewRecord(schema, cols, int64(count))
	defer batch.Release()
	return writer.Write(batch)
}

func (s *ParquetSink) Close() error { return nil }

func ParquetSinkFactory(cfg pipeline.Config) (pipeline.Sink, error) {
	return NewParquetSink(cfg)
}
