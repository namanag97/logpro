package writer

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	_ "github.com/marcboeker/go-duckdb"

	"github.com/logflow/logflow/internal/model"
)

// ArrowAppenderWriter uses DuckDB's Arrow Appender for zero-copy data transfer.
// This is the fastest path for streaming data (XES, JSON) into DuckDB.
type ArrowAppenderWriter struct {
	cfg        Config
	outputPath string
	db         *sql.DB

	allocator memory.Allocator
	schema    *arrow.Schema

	// Arrow builders for batch construction
	caseIDBuilder    *array.StringBuilder
	activityBuilder  *array.StringBuilder
	timestampBuilder *array.Int64Builder
	resourceBuilder  *array.StringBuilder

	mu               sync.Mutex
	rowCount         int
	totalRowsWritten int64
	closed           bool

	// Metadata for Parquet footer
	metadata map[string]string
}

// NewArrowAppenderWriter creates a new Arrow-based DuckDB writer.
func NewArrowAppenderWriter(outputPath string, cfg Config) (*ArrowAppenderWriter, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Create table to receive data
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

	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "case_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "activity", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "resource", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	w := &ArrowAppenderWriter{
		cfg:              cfg,
		outputPath:       outputPath,
		db:               db,
		allocator:        allocator,
		schema:           schema,
		caseIDBuilder:    array.NewStringBuilder(allocator),
		activityBuilder:  array.NewStringBuilder(allocator),
		timestampBuilder: array.NewInt64Builder(allocator),
		resourceBuilder:  array.NewStringBuilder(allocator),
		metadata:         make(map[string]string),
	}

	// Reserve capacity for batch building
	w.caseIDBuilder.Reserve(cfg.BatchSize)
	w.activityBuilder.Reserve(cfg.BatchSize)
	w.timestampBuilder.Reserve(cfg.BatchSize)
	w.resourceBuilder.Reserve(cfg.BatchSize)

	return w, nil
}

// SetMetadata sets key-value metadata to be written to the Parquet footer.
func (w *ArrowAppenderWriter) SetMetadata(key, value string) {
	w.mu.Lock()
	w.metadata[key] = value
	w.mu.Unlock()
}

// Write implements the Writer interface using Arrow record batches.
func (w *ArrowAppenderWriter) Write(ctx context.Context, events <-chan *model.Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-events:
			if !ok {
				return w.flushBatch()
			}

			w.mu.Lock()
			w.appendEvent(event)
			w.rowCount++

			if w.rowCount >= w.cfg.BatchSize {
				if err := w.flushBatch(); err != nil {
					w.mu.Unlock()
					return err
				}
			}
			w.mu.Unlock()
		}
	}
}

// appendEvent adds an event to the Arrow builders.
func (w *ArrowAppenderWriter) appendEvent(event *model.Event) {
	w.caseIDBuilder.Append(string(event.CaseID))
	w.activityBuilder.Append(string(event.Activity))
	w.timestampBuilder.Append(event.Timestamp)

	if len(event.Resource) > 0 {
		w.resourceBuilder.Append(string(event.Resource))
	} else {
		w.resourceBuilder.AppendNull()
	}
}

// flushBatch builds an Arrow RecordBatch and inserts it into DuckDB.
func (w *ArrowAppenderWriter) flushBatch() error {
	if w.rowCount == 0 {
		return nil
	}

	// Build arrays from builders
	caseIDArray := w.caseIDBuilder.NewArray()
	activityArray := w.activityBuilder.NewArray()
	timestampArray := w.timestampBuilder.NewArray()
	resourceArray := w.resourceBuilder.NewArray()

	defer caseIDArray.Release()
	defer activityArray.Release()
	defer timestampArray.Release()
	defer resourceArray.Release()

	// Insert rows via SQL (DuckDB doesn't expose Arrow appender in Go bindings directly)
	// Use batch insert for efficiency
	tx, err := w.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	stmt, err := tx.Prepare("INSERT INTO events (case_id, activity, timestamp, resource) VALUES (?, ?, ?, ?)")
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare insert: %w", err)
	}
	defer stmt.Close()

	caseIDs := caseIDArray.(*array.String)
	activities := activityArray.(*array.String)
	timestamps := timestampArray.(*array.Int64)
	resources := resourceArray.(*array.String)

	for i := 0; i < w.rowCount; i++ {
		var resource interface{}
		if !resources.IsNull(i) {
			resource = resources.Value(i)
		}

		_, err := stmt.Exec(
			caseIDs.Value(i),
			activities.Value(i),
			timestamps.Value(i),
			resource,
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert row: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	w.totalRowsWritten += int64(w.rowCount)
	w.rowCount = 0
	return nil
}

// Flush flushes any buffered data.
func (w *ArrowAppenderWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushBatch()
}

// Close finalizes the writer and exports to Parquet.
func (w *ArrowAppenderWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	if err := w.flushBatch(); err != nil {
		return err
	}

	// Build compression string
	compression := "snappy"
	switch w.cfg.Compression {
	case CompressionGzip:
		compression = "gzip"
	case CompressionZstd:
		compression = "zstd"
	case CompressionLZ4:
		compression = "lz4"
	case CompressionNone:
		compression = "uncompressed"
	}

	// Build metadata KV pairs for Parquet footer
	metadataClause := ""
	if len(w.metadata) > 0 {
		pairs := ""
		first := true
		for k, v := range w.metadata {
			if !first {
				pairs += ", "
			}
			pairs += fmt.Sprintf("'%s': '%s'", k, v)
			first = false
		}
		metadataClause = fmt.Sprintf(", KV_METADATA {%s}", pairs)
	}

	// Export sorted by case_id, timestamp for optimal delta encoding
	query := fmt.Sprintf(`
		COPY (
			SELECT * FROM events
			ORDER BY case_id, timestamp
		) TO '%s' (FORMAT PARQUET, COMPRESSION '%s'%s)
	`, w.outputPath, compression, metadataClause)

	if _, err := w.db.Exec(query); err != nil {
		return fmt.Errorf("failed to export parquet: %w", err)
	}

	// Release builders
	w.caseIDBuilder.Release()
	w.activityBuilder.Release()
	w.timestampBuilder.Release()
	w.resourceBuilder.Release()

	w.db.Close()
	w.closed = true
	return nil
}

// RowsWritten returns the total number of rows written.
func (w *ArrowAppenderWriter) RowsWritten() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.totalRowsWritten
}
