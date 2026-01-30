// Package sinks provides data sink implementations.
// This file implements a minimal Apache Iceberg table format writer.
// It writes Parquet data files and maintains Iceberg-compatible metadata.
package sinks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"

	"github.com/logflow/logflow/pkg/pipeline"
)

// IcebergSink writes events to an Apache Iceberg table.
// It supports both fixed process-mining schemas and dynamic schemas passed via
// SetSchema(). When no schema is set, it defaults to the legacy 4-column layout.
type IcebergSink struct {
	cfg       pipeline.Config
	tableDir  string
	allocator memory.Allocator

	mu            sync.Mutex
	schema        *arrow.Schema
	currentWriter *pqarrow.FileWriter
	currentFile   *os.File
	currentPath   string

	// Dynamic builders (one per schema field)
	builders []array.Builder

	// Tracking
	rowCount      int
	totalRows     int64 // actual total rows written across all files
	batchCount    int
	dataFiles     []DataFile
	snapshotID    int64
	schemaID      int
	closed        bool
}

// DataFile represents an Iceberg data file entry.
type DataFile struct {
	FilePath      string            `json:"file_path"`
	FileFormat    string            `json:"file_format"`
	RecordCount   int64             `json:"record_count"`
	FileSizeBytes int64             `json:"file_size_in_bytes"`
	ColumnSizes   map[string]int64  `json:"column_sizes,omitempty"`
	ValueCounts   map[string]int64  `json:"value_counts,omitempty"`
	NullCounts    map[string]int64  `json:"null_value_counts,omitempty"`
	Partition     map[string]string `json:"partition,omitempty"`
}

// IcebergMetadata represents the table metadata file.
type IcebergMetadata struct {
	FormatVersion    int             `json:"format-version"`
	TableUUID        string          `json:"table-uuid"`
	Location         string          `json:"location"`
	LastUpdatedMs    int64           `json:"last-updated-ms"`
	LastColumnID     int             `json:"last-column-id"`
	Schemas          []SchemaSpec    `json:"schemas"`
	CurrentSchemaID  int             `json:"current-schema-id"`
	PartitionSpecs   []PartitionSpec `json:"partition-specs"`
	DefaultSpecID    int             `json:"default-spec-id"`
	LastPartitionID  int             `json:"last-partition-id"`
	Properties       map[string]string `json:"properties"`
	CurrentSnapshotID *int64          `json:"current-snapshot-id"`
	Snapshots        []Snapshot      `json:"snapshots"`
	SnapshotLog      []SnapshotLog   `json:"snapshot-log"`
}

// SchemaSpec represents an Iceberg schema.
type SchemaSpec struct {
	SchemaID int     `json:"schema-id"`
	Type     string  `json:"type"`
	Fields   []Field `json:"fields"`
}

// Field represents a schema field.
type Field struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Type     string `json:"type"`
}

// PartitionSpec represents a partition specification.
type PartitionSpec struct {
	SpecID int           `json:"spec-id"`
	Fields []interface{} `json:"fields"`
}

// Snapshot represents an Iceberg snapshot.
type Snapshot struct {
	SnapshotID   int64             `json:"snapshot-id"`
	ParentID     *int64            `json:"parent-snapshot-id,omitempty"`
	TimestampMs  int64             `json:"timestamp-ms"`
	Summary      map[string]string `json:"summary"`
	ManifestList string            `json:"manifest-list"`
	SchemaID     int               `json:"schema-id"`
}

// SnapshotLog represents a snapshot log entry.
type SnapshotLog struct {
	TimestampMs int64 `json:"timestamp-ms"`
	SnapshotID  int64 `json:"snapshot-id"`
}

// Manifest represents an Iceberg manifest file.
type Manifest struct {
	Entries []ManifestEntry `json:"entries"`
}

// ManifestEntry represents a manifest entry.
type ManifestEntry struct {
	Status   int      `json:"status"` // 0=EXISTING, 1=ADDED, 2=DELETED
	DataFile DataFile `json:"data_file"`
}

// NewIcebergSink creates a new Iceberg table sink with the default
// process-mining schema (case_id, activity, timestamp, resource).
func NewIcebergSink(cfg pipeline.Config) (*IcebergSink, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "case_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "activity", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "resource", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	return NewIcebergSinkWithSchema(cfg, schema)
}

// NewIcebergSinkWithSchema creates an Iceberg sink with a caller-provided
// Arrow schema. This allows writing any set of columns, not just the
// process-mining defaults.
func NewIcebergSinkWithSchema(cfg pipeline.Config, schema *arrow.Schema) (*IcebergSink, error) {
	tableDir := cfg.SinkPath
	if tableDir == "" {
		return nil, fmt.Errorf("sink path (table directory) required for Iceberg")
	}

	// Create table directory structure
	dataDir := filepath.Join(tableDir, "data")
	metadataDir := filepath.Join(tableDir, "metadata")

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create metadata directory: %w", err)
	}

	allocator := memory.NewGoAllocator()

	// Build dynamic builders from schema
	builders := make([]array.Builder, schema.NumFields())
	for i, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.INT64:
			builders[i] = array.NewInt64Builder(allocator)
		case arrow.FLOAT64:
			builders[i] = array.NewFloat64Builder(allocator)
		case arrow.BOOL:
			builders[i] = array.NewBooleanBuilder(allocator)
		case arrow.TIMESTAMP:
			builders[i] = array.NewTimestampBuilder(allocator, &arrow.TimestampType{Unit: arrow.Microsecond})
		default:
			builders[i] = array.NewStringBuilder(allocator)
		}
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 8192
	}
	for _, b := range builders {
		b.Reserve(batchSize)
	}

	return &IcebergSink{
		cfg:        cfg,
		tableDir:   tableDir,
		allocator:  allocator,
		schema:     schema,
		builders:   builders,
		snapshotID: time.Now().UnixMilli(),
	}, nil
}

// Name returns the adapter name.
func (s *IcebergSink) Name() string {
	return "iceberg"
}

// Write implements Sink.Write.
func (s *IcebergSink) Write(ctx context.Context, in <-chan *pipeline.Event) error {
	batchSize := s.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 8192
	}

	// Target file size: 128MB worth of rows (estimate)
	targetRowsPerFile := 1000000

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-in:
			if !ok {
				return s.finalize()
			}

			s.mu.Lock()

			// Ensure we have a writer
			if s.currentWriter == nil {
				if err := s.startNewFile(); err != nil {
					s.mu.Unlock()
					return err
				}
			}

			s.appendEvent(event)
			s.rowCount++

			// Flush batch if full
			if s.rowCount >= batchSize {
				if err := s.flushBatch(); err != nil {
					s.mu.Unlock()
					return err
				}
			}

			// Start new file if current is large enough
			if s.batchCount*batchSize >= targetRowsPerFile {
				if err := s.rotateFile(); err != nil {
					s.mu.Unlock()
					return err
				}
			}

			s.mu.Unlock()
		}
	}
}

func (s *IcebergSink) appendEvent(event *pipeline.Event) {
	// Map the event's fields to the schema columns by name.
	// The first 4 columns (if named case_id, activity, timestamp, resource)
	// use the structured event fields; others come from Attributes.
	for i, field := range s.schema.Fields() {
		switch field.Name {
		case "case_id":
			s.builders[i].(*array.StringBuilder).Append(string(event.CaseID))
		case "activity":
			s.builders[i].(*array.StringBuilder).Append(string(event.Activity))
		case "timestamp":
			s.builders[i].(*array.Int64Builder).Append(event.Timestamp)
		case "resource":
			if len(event.Resource) > 0 {
				s.builders[i].(*array.StringBuilder).Append(string(event.Resource))
			} else {
				s.builders[i].AppendNull()
			}
		default:
			// Look up in event Attributes
			found := false
			for _, attr := range event.Attributes {
				if string(attr.Key) == field.Name {
					s.builders[i].(*array.StringBuilder).Append(string(attr.Value))
					found = true
					break
				}
			}
			if !found {
				s.builders[i].AppendNull()
			}
		}
	}
}

func (s *IcebergSink) flushBatch() error {
	if s.rowCount == 0 {
		return nil
	}

	arrays := make([]arrow.Array, len(s.builders))
	for i, b := range s.builders {
		arrays[i] = b.NewArray()
		defer arrays[i].Release()
	}

	batch := array.NewRecord(s.schema, arrays, int64(s.rowCount))
	defer batch.Release()

	if err := s.currentWriter.Write(batch); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	s.totalRows += int64(s.rowCount)
	s.batchCount++
	s.rowCount = 0
	return nil
}

func (s *IcebergSink) startNewFile() error {
	dataDir := filepath.Join(s.tableDir, "data")
	filename := fmt.Sprintf("%016x-%d.parquet", time.Now().UnixNano(), len(s.dataFiles))
	s.currentPath = filepath.Join(dataDir, filename)

	file, err := os.Create(s.currentPath)
	if err != nil {
		return fmt.Errorf("failed to create data file: %w", err)
	}
	s.currentFile = file

	codec := mapCompressionCodec(s.cfg.Compression)
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(codec),
		parquet.WithDictionaryDefault(true),
	)
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	writer, err := pqarrow.NewFileWriter(s.schema, file, writerProps, arrowProps)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	s.currentWriter = writer
	s.batchCount = 0
	return nil
}

func (s *IcebergSink) rotateFile() error {
	// Flush current batch
	if err := s.flushBatch(); err != nil {
		return err
	}

	// Close current file
	if err := s.closeCurrentFile(); err != nil {
		return err
	}

	// Start new file
	return s.startNewFile()
}

func (s *IcebergSink) closeCurrentFile() error {
	if s.currentWriter == nil {
		return nil
	}

	if err := s.currentWriter.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	if s.currentFile != nil {
		s.currentFile.Close()
	}

	// Get file info
	info, err := os.Stat(s.currentPath)
	if err != nil {
		return err
	}

	// Add to data files list — use actual totalRows, not estimated batchCount * batchSize
	s.dataFiles = append(s.dataFiles, DataFile{
		FilePath:      "data/" + filepath.Base(s.currentPath),
		FileFormat:    "PARQUET",
		RecordCount:   s.totalRows,
		FileSizeBytes: info.Size(),
	})

	s.currentWriter = nil
	s.currentFile = nil
	return nil
}

func (s *IcebergSink) finalize() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush remaining
	if err := s.flushBatch(); err != nil {
		return err
	}

	// Close current file
	if err := s.closeCurrentFile(); err != nil {
		return err
	}

	// Write manifest
	if err := s.writeManifest(); err != nil {
		return err
	}

	// Write metadata
	return s.writeMetadata()
}

func (s *IcebergSink) writeManifest() error {
	manifestDir := filepath.Join(s.tableDir, "metadata")
	manifestPath := filepath.Join(manifestDir, fmt.Sprintf("snap-%d-manifest.avro", s.snapshotID))

	// Create manifest entries
	manifest := Manifest{
		Entries: make([]ManifestEntry, len(s.dataFiles)),
	}
	for i, df := range s.dataFiles {
		manifest.Entries[i] = ManifestEntry{
			Status:   1, // ADDED
			DataFile: df,
		}
	}

	// Write as JSON (simplified - real Iceberg uses Avro)
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(manifestPath, data, 0644)
}

func (s *IcebergSink) writeMetadata() error {
	metadataDir := filepath.Join(s.tableDir, "metadata")

	// Generate table UUID from path
	hash := sha256.Sum256([]byte(s.tableDir))
	tableUUID := hex.EncodeToString(hash[:16])
	tableUUID = fmt.Sprintf("%s-%s-%s-%s-%s",
		tableUUID[0:8], tableUUID[8:12], tableUUID[12:16], tableUUID[16:20], tableUUID[20:32])

	// Calculate total rows
	var totalRows int64
	for _, df := range s.dataFiles {
		totalRows += df.RecordCount
	}

	metadata := IcebergMetadata{
		FormatVersion:   2,
		TableUUID:       tableUUID,
		Location:        s.tableDir,
		LastUpdatedMs:   time.Now().UnixMilli(),
		LastColumnID:    4,
		CurrentSchemaID: 0,
		Schemas: []SchemaSpec{
			{
				SchemaID: 0,
				Type:     "struct",
				Fields: []Field{
					{ID: 1, Name: "case_id", Required: true, Type: "string"},
					{ID: 2, Name: "activity", Required: true, Type: "string"},
					{ID: 3, Name: "timestamp", Required: true, Type: "long"},
					{ID: 4, Name: "resource", Required: false, Type: "string"},
				},
			},
		},
		PartitionSpecs: []PartitionSpec{
			{SpecID: 0, Fields: []interface{}{}},
		},
		DefaultSpecID:    0,
		LastPartitionID:  999,
		Properties:       map[string]string{"write.format.default": "parquet"},
		CurrentSnapshotID: &s.snapshotID,
		Snapshots: []Snapshot{
			{
				SnapshotID:  s.snapshotID,
				TimestampMs: time.Now().UnixMilli(),
				Summary: map[string]string{
					"operation":      "append",
					"added-files":    fmt.Sprintf("%d", len(s.dataFiles)),
					"added-records":  fmt.Sprintf("%d", totalRows),
					"total-records":  fmt.Sprintf("%d", totalRows),
					"total-files":    fmt.Sprintf("%d", len(s.dataFiles)),
				},
				ManifestList: fmt.Sprintf("metadata/snap-%d-manifest.avro", s.snapshotID),
				SchemaID:     0,
			},
		},
		SnapshotLog: []SnapshotLog{
			{TimestampMs: time.Now().UnixMilli(), SnapshotID: s.snapshotID},
		},
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}

	// Write versioned metadata file
	metadataPath := filepath.Join(metadataDir, fmt.Sprintf("v%d.metadata.json", 1))
	if err := os.WriteFile(metadataPath, data, 0644); err != nil {
		return err
	}

	// Write version hint
	versionHint := filepath.Join(metadataDir, "version-hint.text")
	return os.WriteFile(versionHint, []byte("1"), 0644)
}

// Close flushes and closes the sink.
func (s *IcebergSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	// Release builders
	s.caseIDBuilder.Release()
	s.activityBuilder.Release()
	s.timestampBuilder.Release()
	s.resourceBuilder.Release()

	s.closed = true
	return nil
}

func mapCompressionCodec(name string) compress.Compression {
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
		return compress.Codecs.Snappy
	}
}

// IcebergSinkFactory creates an IcebergSink from config.
func IcebergSinkFactory(cfg pipeline.Config) (pipeline.Sink, error) {
	return NewIcebergSink(cfg)
}
