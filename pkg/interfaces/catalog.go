package interfaces

import (
	"context"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
)

// Catalog manages table metadata and provides table discovery.
type Catalog interface {
	// Table operations
	CreateTable(ctx context.Context, spec TableSpec) error
	GetTable(ctx context.Context, database, table string) (Table, error)
	ListTables(ctx context.Context, database string) ([]TableInfo, error)
	DropTable(ctx context.Context, database, table string) error
	TableExists(ctx context.Context, database, table string) (bool, error)

	// Database operations
	CreateDatabase(ctx context.Context, name string) error
	ListDatabases(ctx context.Context) ([]string, error)
	DropDatabase(ctx context.Context, name string) error
	DatabaseExists(ctx context.Context, name string) (bool, error)

	// Refresh metadata from storage
	Refresh(ctx context.Context) error
}

// Table represents a logical table with metadata, schema, and data files.
type Table interface {
	// Metadata
	Name() string
	Database() string
	Schema() *arrow.Schema
	Location() string
	Format() TableFormat
	Partitioning() PartitionSpec
	Properties() map[string]string

	// Statistics
	RowCount() int64
	SizeBytes() int64
	FileCount() int

	// Snapshots (for time travel)
	CurrentSnapshot() Snapshot
	Snapshots() []Snapshot
	AsOf(version int64) (Table, error)
	AsOfTime(timestamp time.Time) (Table, error)

	// Files
	DataFiles() []DataFile
	ManifestFiles() []ManifestFile
}

// TableSpec defines parameters for creating a new table.
type TableSpec struct {
	Database     string
	Name         string
	Schema       *arrow.Schema
	Location     string
	Format       TableFormat
	Partitioning PartitionSpec
	Properties   map[string]string
}

// TableInfo provides summary information about a table.
type TableInfo struct {
	Database   string
	Name       string
	Format     TableFormat
	RowCount   int64
	SizeBytes  int64
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Properties map[string]string
}

// TableFormat specifies the underlying table format.
type TableFormat int

const (
	FormatParquetFiles TableFormat = iota
	FormatIceberg
	FormatDelta
	FormatHudi
)

func (f TableFormat) String() string {
	switch f {
	case FormatParquetFiles:
		return "parquet"
	case FormatIceberg:
		return "iceberg"
	case FormatDelta:
		return "delta"
	case FormatHudi:
		return "hudi"
	default:
		return "unknown"
	}
}

// PartitionSpec defines how a table is partitioned.
type PartitionSpec struct {
	Fields []PartitionField
}

// PartitionField defines a single partition column and transform.
type PartitionField struct {
	SourceColumn string
	Name         string
	Transform    PartitionTransform
}

// PartitionTransform defines how to transform a column for partitioning.
type PartitionTransform int

const (
	TransformIdentity PartitionTransform = iota
	TransformYear
	TransformMonth
	TransformDay
	TransformHour
	TransformBucket
	TransformTruncate
)

func (t PartitionTransform) String() string {
	switch t {
	case TransformIdentity:
		return "identity"
	case TransformYear:
		return "year"
	case TransformMonth:
		return "month"
	case TransformDay:
		return "day"
	case TransformHour:
		return "hour"
	case TransformBucket:
		return "bucket"
	case TransformTruncate:
		return "truncate"
	default:
		return "identity"
	}
}

// Snapshot represents a point-in-time view of the table.
type Snapshot struct {
	ID            int64
	ParentID      int64
	Timestamp     time.Time
	Operation     string
	Summary       map[string]string
	ManifestList  string
	SchemaID      int
	SequenceNum   int64
	AddedFiles    int
	DeletedFiles  int
	AddedRows     int64
	DeletedRows   int64
}

// DataFile represents a data file in the table.
type DataFile struct {
	Path          string
	Format        string
	PartitionData map[string]string
	RecordCount   int64
	FileSizeBytes int64
	ColumnSizes   map[string]int64
	ValueCounts   map[string]int64
	NullCounts    map[string]int64
	LowerBounds   map[string][]byte
	UpperBounds   map[string][]byte
	SortOrder     int
}

// ManifestFile represents a manifest file that lists data files.
type ManifestFile struct {
	Path                  string
	Length                int64
	PartitionSpecID       int
	AddedSnapshotID       int64
	AddedDataFilesCount   int
	ExistingDataFilesCount int
	DeletedDataFilesCount int
	AddedRowsCount        int64
	ExistingRowsCount     int64
	DeletedRowsCount      int64
}
