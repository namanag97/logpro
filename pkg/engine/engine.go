package engine

import "context"

// RecordSet represents query results (simplified).
type RecordSet struct {
	Columns []string
	Rows    [][]interface{}
}

// ReadOpts configures reading.
type ReadOpts struct {
	Header      bool
	AllVarchar  bool
	Compression string
	Format      string // "auto", "newline_delimited"
}

// WriteOpts configures writing.
type WriteOpts struct {
	Compression string
	Format      string // "parquet", "csv", etc.
}

// Engine abstracts a query/conversion engine.
type Engine interface {
	// Convert reads a file and writes to output format.
	Convert(ctx context.Context, inputPath, outputPath string, readOpts ReadOpts, writeOpts WriteOpts) error

	// Query executes a SQL query and returns results.
	Query(ctx context.Context, sql string) (*RecordSet, error)

	// QueryRow executes a query expected to return one row.
	QueryRow(ctx context.Context, sql string, dest ...interface{}) error

	// Exec executes a statement without returning rows.
	Exec(ctx context.Context, sql string) error

	// Close releases resources.
	Close() error
}
