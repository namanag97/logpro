// Package table provides table abstraction for data storage.
package table

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/logflow/logflow/pkg/interfaces"
)

// Table represents a data table with schema and statistics.
type Table struct {
	database     string
	name         string
	schema       *arrow.Schema
	location     string
	format       interfaces.TableFormat
	partitioning interfaces.PartitionSpec
	properties   map[string]string
	rowCount     int64
	sizeBytes    int64
	fileCount    int
	snapshots    []interfaces.Snapshot
	dataFiles    []interfaces.DataFile
}

// NewTable creates a new table.
func NewTable(database, name string, schema *arrow.Schema, location string) *Table {
	return &Table{
		database:   database,
		name:       name,
		schema:     schema,
		location:   location,
		format:     interfaces.FormatParquetFiles,
		properties: make(map[string]string),
	}
}

func (t *Table) Name() string                           { return t.name }
func (t *Table) Database() string                       { return t.database }
func (t *Table) Schema() *arrow.Schema                  { return t.schema }
func (t *Table) Location() string                       { return t.location }
func (t *Table) Format() interfaces.TableFormat         { return t.format }
func (t *Table) Partitioning() interfaces.PartitionSpec { return t.partitioning }
func (t *Table) Properties() map[string]string          { return t.properties }
func (t *Table) RowCount() int64                        { return t.rowCount }
func (t *Table) SizeBytes() int64                       { return t.sizeBytes }
func (t *Table) FileCount() int                         { return t.fileCount }

func (t *Table) CurrentSnapshot() interfaces.Snapshot {
	if len(t.snapshots) == 0 {
		return interfaces.Snapshot{}
	}
	return t.snapshots[len(t.snapshots)-1]
}

func (t *Table) Snapshots() []interfaces.Snapshot {
	return t.snapshots
}

func (t *Table) AsOf(version int64) (interfaces.Table, error) {
	for _, snap := range t.snapshots {
		if snap.ID == version {
			// Return table view at this snapshot
			return t, nil
		}
	}
	return nil, fmt.Errorf("snapshot not found: %d", version)
}

func (t *Table) AsOfTime(timestamp time.Time) (interfaces.Table, error) {
	for i := len(t.snapshots) - 1; i >= 0; i-- {
		if t.snapshots[i].Timestamp.Before(timestamp) || t.snapshots[i].Timestamp.Equal(timestamp) {
			return t, nil
		}
	}
	return nil, fmt.Errorf("no snapshot found before: %v", timestamp)
}

func (t *Table) DataFiles() []interfaces.DataFile {
	return t.dataFiles
}

func (t *Table) ManifestFiles() []interfaces.ManifestFile {
	return nil
}

// SetPartitioning sets the partition specification.
func (t *Table) SetPartitioning(spec interfaces.PartitionSpec) {
	t.partitioning = spec
}

// SetProperty sets a table property.
func (t *Table) SetProperty(key, value string) {
	t.properties[key] = value
}

// AddDataFile adds a data file to the table.
func (t *Table) AddDataFile(file interfaces.DataFile) {
	t.dataFiles = append(t.dataFiles, file)
	t.rowCount += file.RecordCount
	t.sizeBytes += file.FileSizeBytes
	t.fileCount++
}

// CreateSnapshot creates a new snapshot.
func (t *Table) CreateSnapshot(operation string) interfaces.Snapshot {
	var parentID int64
	if len(t.snapshots) > 0 {
		parentID = t.snapshots[len(t.snapshots)-1].ID
	}

	snap := interfaces.Snapshot{
		ID:          time.Now().UnixNano(),
		ParentID:    parentID,
		Timestamp:   time.Now(),
		Operation:   operation,
		AddedFiles:  0,
		AddedRows:   0,
		SequenceNum: int64(len(t.snapshots) + 1),
	}

	t.snapshots = append(t.snapshots, snap)
	return snap
}

// Builder builds tables incrementally.
type Builder struct {
	table *Table
}

// NewBuilder creates a new table builder.
func NewBuilder(database, name string) *Builder {
	return &Builder{
		table: &Table{
			database:   database,
			name:       name,
			properties: make(map[string]string),
		},
	}
}

// WithSchema sets the schema.
func (b *Builder) WithSchema(schema *arrow.Schema) *Builder {
	b.table.schema = schema
	return b
}

// WithLocation sets the location.
func (b *Builder) WithLocation(location string) *Builder {
	b.table.location = location
	return b
}

// WithFormat sets the format.
func (b *Builder) WithFormat(format interfaces.TableFormat) *Builder {
	b.table.format = format
	return b
}

// WithPartitioning sets partitioning.
func (b *Builder) WithPartitioning(spec interfaces.PartitionSpec) *Builder {
	b.table.partitioning = spec
	return b
}

// WithProperty sets a property.
func (b *Builder) WithProperty(key, value string) *Builder {
	b.table.properties[key] = value
	return b
}

// Build returns the built table.
func (b *Builder) Build() *Table {
	return b.table
}

// Statistics contains table statistics.
type Statistics struct {
	RowCount     int64
	SizeBytes    int64
	FileCount    int
	ColumnStats  map[string]*ColumnStatistics
	LastModified time.Time
}

// ColumnStatistics contains per-column statistics.
type ColumnStatistics struct {
	NullCount     int64
	DistinctCount int64
	MinValue      interface{}
	MaxValue      interface{}
	TotalBytes    int64
}

// Partitioner generates partition paths.
type Partitioner struct {
	spec interfaces.PartitionSpec
}

// NewPartitioner creates a new partitioner.
func NewPartitioner(spec interfaces.PartitionSpec) *Partitioner {
	return &Partitioner{spec: spec}
}

// PartitionPath generates the partition path for a record.
func (p *Partitioner) PartitionPath(values map[string]interface{}) string {
	if len(p.spec.Fields) == 0 {
		return ""
	}

	path := ""
	for _, field := range p.spec.Fields {
		val, ok := values[field.SourceColumn]
		if !ok {
			continue
		}

		partValue := p.transformValue(val, field.Transform)
		path = filepath.Join(path, fmt.Sprintf("%s=%v", field.Name, partValue))
	}

	return path
}

func (p *Partitioner) transformValue(val interface{}, transform interfaces.PartitionTransform) interface{} {
	switch transform {
	case interfaces.TransformYear:
		if t, ok := val.(time.Time); ok {
			return t.Year()
		}
	case interfaces.TransformMonth:
		if t, ok := val.(time.Time); ok {
			return t.Format("2006-01")
		}
	case interfaces.TransformDay:
		if t, ok := val.(time.Time); ok {
			return t.Format("2006-01-02")
		}
	case interfaces.TransformHour:
		if t, ok := val.(time.Time); ok {
			return t.Format("2006-01-02-15")
		}
	}
	return val
}

// Writer writes data to a table.
type Writer struct {
	table   *Table
	ctx     context.Context
	storage interfaces.ObjectStorage
}

// NewWriter creates a new table writer.
func NewWriter(ctx context.Context, table *Table, storage interfaces.ObjectStorage) *Writer {
	return &Writer{
		ctx:     ctx,
		table:   table,
		storage: storage,
	}
}

// Verify interface compliance
var _ interfaces.Table = (*Table)(nil)
