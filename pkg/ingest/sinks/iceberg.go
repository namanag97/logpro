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
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/google/uuid"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// IcebergSink writes Arrow batches to an Iceberg table.
// This is a simplified implementation that writes Parquet data files
// and generates Iceberg metadata. For full Iceberg support, integrate with iceberg-go.
type IcebergSink struct {
	mu sync.Mutex

	tablePath     string
	schema        *arrow.Schema
	opts          core.SinkOptions
	partitionSpec *PartitionSpec

	// Current data file being written
	currentFile   *os.File
	currentWriter *pqarrow.FileWriter
	currentPath   string
	currentRows   int64

	// Accumulated data files
	dataFiles   []DataFileInfo
	rowsWritten int64
	startTime   time.Time

	// Target file size
	targetFileSize int64
}

// DataFileInfo contains metadata about a written data file.
type DataFileInfo struct {
	Path        string
	Size        int64
	RowCount    int64
	PartitionID string
}

// PartitionSpec defines how data is partitioned.
type PartitionSpec struct {
	Fields []PartitionField
}

// PartitionField defines a single partition field.
type PartitionField struct {
	SourceColumn string
	Transform    string // identity, year, month, day, hour, bucket, truncate
	Name         string
}

// NewIcebergSink creates an Iceberg sink.
func NewIcebergSink() *IcebergSink {
	return &IcebergSink{
		targetFileSize: 128 * 1024 * 1024, // 128MB default
	}
}

// Open prepares the sink for writing.
func (s *IcebergSink) Open(ctx context.Context, schema *arrow.Schema, opts core.SinkOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.tablePath = opts.Path
	s.schema = schema
	s.opts = opts
	s.startTime = time.Now()
	s.dataFiles = nil
	s.rowsWritten = 0

	if opts.TargetFileSize > 0 {
		s.targetFileSize = opts.TargetFileSize
	}

	// Parse partition spec from options
	if len(opts.PartitionBy) > 0 {
		s.partitionSpec = &PartitionSpec{
			Fields: make([]PartitionField, len(opts.PartitionBy)),
		}
		for i, col := range opts.PartitionBy {
			s.partitionSpec.Fields[i] = PartitionField{
				SourceColumn: col,
				Transform:    "identity",
				Name:         col,
			}
		}
	}

	// Create table directory structure
	dataDir := filepath.Join(s.tablePath, "data")
	metadataDir := filepath.Join(s.tablePath, "metadata")

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Start first data file
	return s.startNewDataFile()
}

func (s *IcebergSink) startNewDataFile() error {
	// Generate unique filename
	filename := fmt.Sprintf("%s-%s.parquet",
		time.Now().Format("20060102-150405"),
		uuid.New().String()[:8])

	s.currentPath = filepath.Join(s.tablePath, "data", filename)

	file, err := os.Create(s.currentPath)
	if err != nil {
		return fmt.Errorf("failed to create data file: %w", err)
	}
	s.currentFile = file

	// Configure writer
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(getCompression(s.opts.Compression)),
		parquet.WithDictionaryDefault(s.opts.DictionaryEncoding),
		parquet.WithStats(s.opts.Statistics),
	)

	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)

	writer, err := pqarrow.NewFileWriter(s.schema, file, writerProps, arrowProps)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	s.currentWriter = writer
	s.currentRows = 0

	return nil
}

func (s *IcebergSink) closeCurrentDataFile() error {
	if s.currentWriter == nil {
		return nil
	}

	// Close writer (this also closes the underlying file)
	if err := s.currentWriter.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	// Get file size
	info, _ := os.Stat(s.currentPath)
	size := int64(0)
	if info != nil {
		size = info.Size()
	}

	s.dataFiles = append(s.dataFiles, DataFileInfo{
		Path:     s.currentPath,
		Size:     size,
		RowCount: s.currentRows,
	})

	s.currentWriter = nil
	s.currentFile = nil
	s.currentPath = ""

	return nil
}

// Write writes a batch to the sink.
func (s *IcebergSink) Write(ctx context.Context, batch arrow.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.currentWriter == nil {
		return fmt.Errorf("sink not open")
	}

	if err := s.currentWriter.Write(batch); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	s.currentRows += batch.NumRows()
	s.rowsWritten += batch.NumRows()

	// Check if we need to roll to a new file
	info, _ := s.currentFile.Stat()
	if info != nil && info.Size() >= s.targetFileSize {
		if err := s.closeCurrentDataFile(); err != nil {
			return err
		}
		if err := s.startNewDataFile(); err != nil {
			return err
		}
	}

	return nil
}

// Flush forces buffered data to be written.
func (s *IcebergSink) Flush(ctx context.Context) error {
	return nil
}

// Close finalizes the Iceberg table.
func (s *IcebergSink) Close(ctx context.Context) (*core.SinkResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close current data file
	if err := s.closeCurrentDataFile(); err != nil {
		return nil, err
	}

	// Write Iceberg metadata
	if err := s.writeMetadata(); err != nil {
		return nil, err
	}

	// Calculate total bytes
	var totalBytes int64
	var paths []string
	for _, df := range s.dataFiles {
		totalBytes += df.Size
		paths = append(paths, df.Path)
	}

	return &core.SinkResult{
		Path:         s.tablePath,
		RowsWritten:  s.rowsWritten,
		BytesWritten: totalBytes,
		FilesWritten: len(s.dataFiles),
		Paths:        paths,
		Duration:     time.Since(s.startTime),
	}, nil
}

func (s *IcebergSink) writeMetadata() error {
	// Write a simplified version-hint.text
	versionPath := filepath.Join(s.tablePath, "metadata", "version-hint.text")
	if err := os.WriteFile(versionPath, []byte("1"), 0644); err != nil {
		return err
	}

	// Write a manifest list (simplified JSON)
	manifestPath := filepath.Join(s.tablePath, "metadata", "snap-1.json")
	manifest := s.generateManifest()
	if err := os.WriteFile(manifestPath, []byte(manifest), 0644); err != nil {
		return err
	}

	// Write table metadata
	metadataPath := filepath.Join(s.tablePath, "metadata", "v1.metadata.json")
	metadata := s.generateTableMetadata()
	return os.WriteFile(metadataPath, []byte(metadata), 0644)
}

func (s *IcebergSink) generateManifest() string {
	// Simplified manifest format
	var files string
	for i, df := range s.dataFiles {
		if i > 0 {
			files += ","
		}
		files += fmt.Sprintf(`{"path":"%s","size":%d,"row_count":%d}`,
			df.Path, df.Size, df.RowCount)
	}
	return fmt.Sprintf(`{"data_files":[%s]}`, files)
}

func (s *IcebergSink) generateTableMetadata() string {
	// Simplified Iceberg table metadata
	schemaJSON := s.schemaToJSON()
	return fmt.Sprintf(`{
  "format-version": 2,
  "table-uuid": "%s",
  "location": "%s",
  "last-sequence-number": 1,
  "last-updated-ms": %d,
  "schema": %s,
  "current-schema-id": 0,
  "partition-spec": [],
  "current-snapshot-id": 1,
  "snapshots": [
    {
      "snapshot-id": 1,
      "sequence-number": 1,
      "timestamp-ms": %d,
      "manifest-list": "%s"
    }
  ]
}`,
		uuid.New().String(),
		s.tablePath,
		time.Now().UnixMilli(),
		schemaJSON,
		time.Now().UnixMilli(),
		filepath.Join(s.tablePath, "metadata", "snap-1.json"))
}

func (s *IcebergSink) schemaToJSON() string {
	var fields string
	for i, field := range s.schema.Fields() {
		if i > 0 {
			fields += ","
		}
		fields += fmt.Sprintf(`{"id":%d,"name":"%s","type":"%s","required":false}`,
			i+1, field.Name, arrowTypeToIceberg(field.Type))
	}
	return fmt.Sprintf(`{"type":"struct","fields":[%s]}`, fields)
}

func arrowTypeToIceberg(t arrow.DataType) string {
	switch t.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32:
		return "int"
	case arrow.INT64:
		return "long"
	case arrow.FLOAT32:
		return "float"
	case arrow.FLOAT64:
		return "double"
	case arrow.BOOL:
		return "boolean"
	case arrow.STRING, arrow.LARGE_STRING:
		return "string"
	case arrow.BINARY, arrow.LARGE_BINARY:
		return "binary"
	case arrow.TIMESTAMP:
		return "timestamp"
	case arrow.DATE32, arrow.DATE64:
		return "date"
	default:
		return "string"
	}
}

// Verify interface compliance
var _ core.Sink = (*IcebergSink)(nil)
