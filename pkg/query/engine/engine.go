// Package engine provides the query execution engine.
package engine

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// Engine executes SQL queries using DuckDB.
type Engine struct {
	db      *sql.DB
	threads int
}

// NewEngine creates a new query engine.
func NewEngine() (*Engine, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to initialize DuckDB: %w", err)
	}

	e := &Engine{
		db:      db,
		threads: runtime.NumCPU(),
	}

	// Configure DuckDB
	e.db.Exec(fmt.Sprintf("SET threads=%d", e.threads))

	return e, nil
}

// NewEngineWithDB creates an engine with an existing connection.
func NewEngineWithDB(db *sql.DB) *Engine {
	return &Engine{
		db:      db,
		threads: runtime.NumCPU(),
	}
}

// Close closes the engine.
func (e *Engine) Close() error {
	return e.db.Close()
}

// Query executes a SQL query and returns results.
func (e *Engine) Query(ctx context.Context, sql string, args ...interface{}) (*Result, error) {
	start := time.Now()

	rows, err := e.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	result := &Result{
		rows:     rows,
		duration: time.Since(start),
	}

	// Get column info
	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	result.columns = cols

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}
	result.columnTypes = colTypes

	return result, nil
}

// Exec executes a SQL statement.
func (e *Engine) Exec(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return e.db.ExecContext(ctx, sql, args...)
}

// QueryRow executes a query that returns a single row.
func (e *Engine) QueryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return e.db.QueryRowContext(ctx, sql, args...)
}

// RegisterTable registers a Parquet file as a table.
func (e *Engine) RegisterTable(ctx context.Context, name, path string) error {
	query := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet('%s')",
		name, strings.ReplaceAll(path, "'", "''"))
	_, err := e.db.ExecContext(ctx, query)
	return err
}

// RegisterParquetGlob registers multiple Parquet files as a table.
func (e *Engine) RegisterParquetGlob(ctx context.Context, name, pattern string) error {
	query := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet('%s')",
		name, strings.ReplaceAll(pattern, "'", "''"))
	_, err := e.db.ExecContext(ctx, query)
	return err
}

// DropTable drops a registered table.
func (e *Engine) DropTable(ctx context.Context, name string) error {
	_, err := e.db.ExecContext(ctx, fmt.Sprintf("DROP VIEW IF EXISTS %s", name))
	return err
}

// ListTables lists registered tables.
func (e *Engine) ListTables(ctx context.Context) ([]string, error) {
	rows, err := e.db.QueryContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, nil
}

// Describe returns the schema of a table.
func (e *Engine) Describe(ctx context.Context, table string) ([]ColumnInfo, error) {
	rows, err := e.db.QueryContext(ctx, fmt.Sprintf("DESCRIBE %s", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var isNull, key, defaultVal, extra sql.NullString
		if err := rows.Scan(&col.Name, &col.Type, &isNull, &key, &defaultVal, &extra); err != nil {
			return nil, err
		}
		col.Nullable = isNull.String == "YES"
		columns = append(columns, col)
	}
	return columns, nil
}

// ColumnInfo describes a column.
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

// Result represents query results.
type Result struct {
	rows        *sql.Rows
	columns     []string
	columnTypes []*sql.ColumnType
	duration    time.Duration
	rowCount    int64
}

// Columns returns column names.
func (r *Result) Columns() []string {
	return r.columns
}

// ColumnTypes returns column types.
func (r *Result) ColumnTypes() []*sql.ColumnType {
	return r.columnTypes
}

// Duration returns query duration.
func (r *Result) Duration() time.Duration {
	return r.duration
}

// Next advances to the next row.
func (r *Result) Next() bool {
	if r.rows.Next() {
		r.rowCount++
		return true
	}
	return false
}

// Scan scans the current row.
func (r *Result) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

// Close closes the result set.
func (r *Result) Close() error {
	return r.rows.Close()
}

// RowCount returns rows scanned so far.
func (r *Result) RowCount() int64 {
	return r.rowCount
}

// ToMaps reads all rows as maps.
func (r *Result) ToMaps() ([]map[string]interface{}, error) {
	defer r.Close()

	var results []map[string]interface{}
	values := make([]interface{}, len(r.columns))
	valuePtrs := make([]interface{}, len(r.columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for r.Next() {
		if err := r.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range r.columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}

	return results, nil
}

// Context provides query context with limits.
type Context struct {
	context.Context
	Timeout     time.Duration
	MemoryLimit int64
	RowLimit    int64
}

// NewContext creates a new query context.
func NewContext(ctx context.Context) *Context {
	return &Context{
		Context: ctx,
		Timeout: 30 * time.Second,
	}
}

// WithTimeout sets the timeout.
func (c *Context) WithTimeout(d time.Duration) *Context {
	c.Timeout = d
	return c
}

// WithMemoryLimit sets the memory limit.
func (c *Context) WithMemoryLimit(bytes int64) *Context {
	c.MemoryLimit = bytes
	return c
}

// WithRowLimit sets the row limit.
func (c *Context) WithRowLimit(rows int64) *Context {
	c.RowLimit = rows
	return c
}
