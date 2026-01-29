package writer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/logflow/logflow/internal/model"
)

// DuckDBWriter writes events to Parquet format via DuckDB for maximum performance.
type DuckDBWriter struct {
	cfg        Config
	outputPath string
	db         *sql.DB
	stmt       *sql.Stmt

	mu              sync.Mutex
	totalRowsWritten int64
	closed          bool

	// Batch accumulator
	batch     []*model.Event
	batchSize int
}

// NewDuckDBWriter creates a new DuckDB-based Parquet writer.
func NewDuckDBWriter(outputPath string, cfg Config) (*DuckDBWriter, error) {
	// Open in-memory DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Create events table
	_, err = db.Exec(`
		CREATE TABLE events (
			case_id VARCHAR NOT NULL,
			activity VARCHAR NOT NULL,
			timestamp BIGINT NOT NULL,
			resource VARCHAR
		)
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// Prepare insert statement
	stmt, err := db.Prepare(`
		INSERT INTO events (case_id, activity, timestamp, resource)
		VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to prepare insert: %w", err)
	}

	return &DuckDBWriter{
		cfg:        cfg,
		outputPath: outputPath,
		db:         db,
		stmt:       stmt,
		batch:      make([]*model.Event, 0, cfg.BatchSize),
		batchSize:  cfg.BatchSize,
	}, nil
}

// Write implements the Writer interface.
func (w *DuckDBWriter) Write(ctx context.Context, events <-chan *model.Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-events:
			if !ok {
				// Channel closed, flush remaining data
				return w.flushBatch()
			}

			w.mu.Lock()
			w.batch = append(w.batch, event)

			if len(w.batch) >= w.batchSize {
				if err := w.flushBatch(); err != nil {
					w.mu.Unlock()
					return err
				}
			}
			w.mu.Unlock()
		}
	}
}

// flushBatch writes the current batch to DuckDB.
func (w *DuckDBWriter) flushBatch() error {
	if len(w.batch) == 0 {
		return nil
	}

	// Start transaction for batch insert
	tx, err := w.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	stmt := tx.Stmt(w.stmt)
	for _, event := range w.batch {
		var resource interface{}
		if len(event.Resource) > 0 {
			resource = string(event.Resource)
		}

		_, err := stmt.Exec(
			string(event.CaseID),
			string(event.Activity),
			event.Timestamp,
			resource,
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert event: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	w.totalRowsWritten += int64(len(w.batch))
	w.batch = w.batch[:0]
	return nil
}

// Flush flushes any buffered data.
func (w *DuckDBWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushBatch()
}

// Close closes the writer, exports to Parquet, and releases resources.
func (w *DuckDBWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	// Flush remaining data
	if err := w.flushBatch(); err != nil {
		return err
	}

	// Export to Parquet
	compression := "snappy"
	switch w.cfg.Compression {
	case CompressionGzip:
		compression = "gzip"
	case CompressionZstd:
		compression = "zstd"
	case CompressionNone:
		compression = "uncompressed"
	}

	query := fmt.Sprintf(`
		COPY events TO '%s' (FORMAT PARQUET, COMPRESSION '%s')
	`, w.outputPath, compression)

	if _, err := w.db.Exec(query); err != nil {
		return fmt.Errorf("failed to export parquet: %w", err)
	}

	w.stmt.Close()
	w.db.Close()
	w.closed = true
	return nil
}

// RowsWritten returns the total number of rows written.
func (w *DuckDBWriter) RowsWritten() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.totalRowsWritten
}

// DuckDBCSVReader uses DuckDB's read_csv_auto for high-performance CSV parsing.
type DuckDBCSVReader struct {
	db *sql.DB
}

// NewDuckDBCSVReader creates a new DuckDB CSV reader.
func NewDuckDBCSVReader() (*DuckDBCSVReader, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}
	return &DuckDBCSVReader{db: db}, nil
}

// ConversionResult holds the result of a CSV to Parquet conversion.
type ConversionResult struct {
	RowsWritten int64
}

// ConvertCSVToParquet converts a CSV file directly to Parquet using DuckDB.
func (r *DuckDBCSVReader) ConvertCSVToParquet(
	inputPath, outputPath string,
	caseIDCol, activityCol, timestampCol, resourceCol string,
	compression CompressionType,
	metadata map[string]string,
) (*ConversionResult, error) {
	compressionStr := "snappy"
	switch compression {
	case CompressionGzip:
		compressionStr = "gzip"
	case CompressionZstd:
		compressionStr = "zstd"
	case CompressionNone:
		compressionStr = "uncompressed"
	}

	// First, check which columns exist in the CSV
	columns, err := r.GetSchemaInfo(inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV schema: %w", err)
	}

	columnSet := make(map[string]bool)
	for _, col := range columns {
		columnSet[col.Name] = true
	}

	// Validate required columns exist
	if !columnSet[caseIDCol] {
		return nil, fmt.Errorf("case_id column %q not found (available: %v)", caseIDCol, columnNames(columns))
	}
	if !columnSet[activityCol] {
		return nil, fmt.Errorf("activity column %q not found (available: %v)", activityCol, columnNames(columns))
	}
	if !columnSet[timestampCol] {
		return nil, fmt.Errorf("timestamp column %q not found (available: %v)", timestampCol, columnNames(columns))
	}

	// Build resource column expression (NULL if column doesn't exist)
	resourceExpr := "NULL"
	if columnSet[resourceCol] {
		resourceExpr = fmt.Sprintf(`"%s"`, resourceCol)
	}

	// Use DuckDB's read_csv_auto for automatic schema inference
	query := fmt.Sprintf(`
		COPY (
			SELECT
				"%s" as case_id,
				"%s" as activity,
				epoch_ns(TRY_CAST("%s" AS TIMESTAMP)) as timestamp,
				%s as resource
			FROM read_csv_auto('%s', header=true)
		) TO '%s' (FORMAT PARQUET, COMPRESSION '%s')
	`, caseIDCol, activityCol, timestampCol, resourceExpr, inputPath, outputPath, compressionStr)

	if _, err := r.db.Exec(query); err != nil {
		return nil, fmt.Errorf("duckdb csv conversion failed: %w", err)
	}

	// Get row count from output file
	rowCount, err := r.GetRowCount(outputPath)
	if err != nil {
		// Non-fatal - return 0 if we can't get the count
		return &ConversionResult{RowsWritten: 0}, nil
	}

	return &ConversionResult{RowsWritten: rowCount}, nil
}

// columnNames extracts column names from ColumnInfo slice.
func columnNames(columns []ColumnInfo) []string {
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.Name
	}
	return names
}

// GetRowCount returns the number of rows in a Parquet file.
func (r *DuckDBCSVReader) GetRowCount(parquetPath string) (int64, error) {
	query := fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s')`, parquetPath)
	var count int64
	if err := r.db.QueryRow(query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// ConvertCSVToParquetFromStdin converts CSV from stdin to Parquet using a temp file.
func (r *DuckDBCSVReader) ConvertCSVToParquetFromStdin(
	outputPath string,
	caseIDCol, activityCol, timestampCol, resourceCol string,
	compression CompressionType,
) (*ConversionResult, error) {
	// Create temp file for stdin content
	tmpFile, err := os.CreateTemp("", "logflow-stdin-*.csv")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	// Copy stdin to temp file
	if _, err := tmpFile.ReadFrom(os.Stdin); err != nil {
		tmpFile.Close()
		return nil, fmt.Errorf("failed to read stdin: %w", err)
	}
	tmpFile.Close()

	return r.ConvertCSVToParquet(tmpFile.Name(), outputPath, caseIDCol, activityCol, timestampCol, resourceCol, compression, nil)
}

// Close closes the DuckDB connection.
func (r *DuckDBCSVReader) Close() error {
	return r.db.Close()
}

// GetSchemaInfo retrieves schema information from a CSV file.
func (r *DuckDBCSVReader) GetSchemaInfo(csvPath string) ([]ColumnInfo, error) {
	query := fmt.Sprintf(`DESCRIBE SELECT * FROM read_csv_auto('%s', header=true, sample_size=1000)`, csvPath)

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var name, dtype string
		var null, key, dflt, extra interface{}
		if err := rows.Scan(&name, &dtype, &null, &key, &dflt, &extra); err != nil {
			return nil, err
		}
		columns = append(columns, ColumnInfo{Name: name, Type: dtype})
	}

	return columns, rows.Err()
}

// ColumnInfo holds column metadata.
type ColumnInfo struct {
	Name string
	Type string
}

func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}
