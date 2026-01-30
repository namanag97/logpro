package engine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
)

// DuckDBEngine implements Engine using DuckDB.
type DuckDBEngine struct {
	db *sql.DB
}

// NewDuckDBEngine creates a new DuckDB engine.
func NewDuckDBEngine() (*DuckDBEngine, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to initialize DuckDB: %w", err)
	}
	return &DuckDBEngine{db: db}, nil
}

// DB returns the underlying *sql.DB for direct access when needed.
func (e *DuckDBEngine) DB() *sql.DB {
	return e.db
}

// Convert reads a file and writes to output format using DuckDB's COPY.
func (e *DuckDBEngine) Convert(ctx context.Context, inputPath, outputPath string, readOpts ReadOpts, writeOpts WriteOpts) error {
	compression := writeOpts.Compression
	if compression == "" {
		compression = "snappy"
	}

	// Use generic COPY ... TO pattern
	query := fmt.Sprintf(
		`COPY (SELECT * FROM '%s') TO '%s' (FORMAT PARQUET, COMPRESSION '%s')`,
		escapePath(inputPath), escapePath(outputPath), compression,
	)
	_, err := e.db.ExecContext(ctx, query)
	return err
}

// Query executes a SQL query and returns all results as a RecordSet.
func (e *DuckDBEngine) Query(ctx context.Context, sqlStr string) (*RecordSet, error) {
	rows, err := e.db.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	rs := &RecordSet{Columns: cols}
	for rows.Next() {
		values := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		rs.Rows = append(rs.Rows, values)
	}
	return rs, rows.Err()
}

// QueryRow executes a query expected to return one row.
func (e *DuckDBEngine) QueryRow(ctx context.Context, sqlStr string, dest ...interface{}) error {
	return e.db.QueryRowContext(ctx, sqlStr).Scan(dest...)
}

// Exec executes a statement without returning rows.
func (e *DuckDBEngine) Exec(ctx context.Context, sqlStr string) error {
	_, err := e.db.ExecContext(ctx, sqlStr)
	return err
}

// Close releases all DuckDB resources.
func (e *DuckDBEngine) Close() error {
	return e.db.Close()
}

func escapePath(path string) string {
	return strings.ReplaceAll(path, "'", "''")
}
