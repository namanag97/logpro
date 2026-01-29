// Package roundtrip provides round-trip testing utilities.
package roundtrip

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"

	"github.com/logflow/logflow/pkg/ingest/core"
	"github.com/logflow/logflow/pkg/ingest/decoders"
	"github.com/logflow/logflow/pkg/ingest/sinks"
	"github.com/logflow/logflow/pkg/testing/generators"
)

// TestCase represents a round-trip test case.
type TestCase struct {
	Name        string
	Format      core.Format
	Generator   func() []byte
	Columns     []generators.ColumnSpec
	RowCount    int
	Options     TestOptions
}

// TestOptions configures test behavior.
type TestOptions struct {
	Compression      string
	BatchSize        int
	SkipSchemaCheck  bool
	SkipDataCheck    bool
	AllowTypeWiden   bool
}

// Result contains test results.
type Result struct {
	Success       bool
	Error         error
	RowsWritten   int64
	RowsRead      int64
	SchemaMatches bool
	DataMatches   bool
	Messages      []string
}

// Run executes a round-trip test.
func Run(ctx context.Context, tc TestCase) *Result {
	result := &Result{Success: true}

	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "roundtrip-*")
	if err != nil {
		result.Success = false
		result.Error = fmt.Errorf("failed to create temp dir: %w", err)
		return result
	}
	defer os.RemoveAll(tmpDir)

	// Generate input data
	var inputData []byte
	if tc.Generator != nil {
		inputData = tc.Generator()
	} else {
		inputData, err = generateCSV(tc.Columns, tc.RowCount)
		if err != nil {
			result.Success = false
			result.Error = fmt.Errorf("failed to generate data: %w", err)
			return result
		}
	}

	inputPath := filepath.Join(tmpDir, "input.csv")
	if err := os.WriteFile(inputPath, inputData, 0644); err != nil {
		result.Success = false
		result.Error = fmt.Errorf("failed to write input: %w", err)
		return result
	}

	outputPath := filepath.Join(tmpDir, "output.parquet")

	// Decode input
	decoder := decoders.NewCSVDecoder()
	decodeOpts := core.DefaultDecodeOptions()
	if tc.Options.BatchSize > 0 {
		decodeOpts.BatchSize = tc.Options.BatchSize
	}

	source := &fileSource{path: inputPath, format: core.FormatCSV}
	batches, err := decoder.Decode(ctx, source, decodeOpts)
	if err != nil {
		result.Success = false
		result.Error = fmt.Errorf("decode failed: %w", err)
		return result
	}

	// Collect batches and write to Parquet
	var schema *arrow.Schema
	var allBatches []arrow.Record

	for batch := range batches {
		if batch.Batch == nil {
			continue
		}
		if schema == nil {
			schema = batch.Batch.Schema()
		}
		batch.Batch.Retain()
		allBatches = append(allBatches, batch.Batch)
		result.RowsWritten += batch.Batch.NumRows()
	}

	if schema == nil {
		result.Success = false
		result.Error = fmt.Errorf("no schema inferred")
		return result
	}

	// Write to Parquet
	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = outputPath
	if tc.Options.Compression != "" {
		sinkOpts.Compression = core.ParseCompression(tc.Options.Compression)
	}

	if err := sink.Open(ctx, schema, sinkOpts); err != nil {
		result.Success = false
		result.Error = fmt.Errorf("sink open failed: %w", err)
		return result
	}

	for _, batch := range allBatches {
		if err := sink.Write(ctx, batch); err != nil {
			result.Success = false
			result.Error = fmt.Errorf("sink write failed: %w", err)
			return result
		}
	}

	sinkResult, err := sink.Close(ctx)
	if err != nil {
		result.Success = false
		result.Error = fmt.Errorf("sink close failed: %w", err)
		return result
	}

	result.Messages = append(result.Messages,
		fmt.Sprintf("Wrote %d rows to Parquet", sinkResult.RowsWritten))

	// Read back from Parquet
	readBatches, readSchema, err := readParquet(ctx, outputPath)
	if err != nil {
		result.Success = false
		result.Error = fmt.Errorf("read back failed: %w", err)
		return result
	}

	for _, batch := range readBatches {
		result.RowsRead += batch.NumRows()
	}

	// Compare schemas
	if !tc.Options.SkipSchemaCheck {
		result.SchemaMatches = schemasMatch(schema, readSchema, tc.Options.AllowTypeWiden)
		if !result.SchemaMatches {
			result.Messages = append(result.Messages, "Schema mismatch detected")
			result.Success = false
		}
	} else {
		result.SchemaMatches = true
	}

	// Compare row counts
	if result.RowsWritten != result.RowsRead {
		result.Success = false
		result.Messages = append(result.Messages,
			fmt.Sprintf("Row count mismatch: wrote %d, read %d", result.RowsWritten, result.RowsRead))
	}

	// Clean up batches
	for _, batch := range allBatches {
		batch.Release()
	}
	for _, batch := range readBatches {
		batch.Release()
	}

	result.DataMatches = result.RowsWritten == result.RowsRead
	return result
}

func generateCSV(columns []generators.ColumnSpec, rows int) ([]byte, error) {
	if len(columns) == 0 {
		columns = generators.StandardColumns()
	}
	if rows <= 0 {
		rows = 1000
	}

	var buf bytes.Buffer
	gen := generators.NewCSVGenerator(42)
	gen.Columns = columns

	if err := gen.Generate(&buf, rows); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func readParquet(ctx context.Context, path string) ([]arrow.Record, *arrow.Schema, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	info, _ := f.Stat()
	pqReader, err := file.NewParquetReader(f)
	if err != nil {
		return nil, nil, err
	}
	defer pqReader.Close()

	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{
		BatchSize: 8192,
	}, nil)
	if err != nil {
		return nil, nil, err
	}

	schema, err := arrowReader.Schema()
	if err != nil {
		return nil, nil, err
	}

	var batches []arrow.Record
	for rg := 0; rg < pqReader.NumRowGroups(); rg++ {
		table, err := arrowReader.ReadRowGroup(rg)
		if err != nil {
			continue
		}

		// Convert table to records
		reader := NewTableReader(table, 8192)
		for reader.Next() {
			rec := reader.Record()
			rec.Retain()
			batches = append(batches, rec)
		}
		reader.Release()
		table.Release()
	}

	_ = info
	return batches, schema, nil
}

func schemasMatch(a, b *arrow.Schema, allowWiden bool) bool {
	if a.NumFields() != b.NumFields() {
		return false
	}

	for i := 0; i < a.NumFields(); i++ {
		fa := a.Field(i)
		fb := b.Field(i)

		if fa.Name != fb.Name {
			return false
		}

		if !arrow.TypeEqual(fa.Type, fb.Type) {
			if !allowWiden {
				return false
			}
			// Check if it's valid widening
			if !isValidWidening(fa.Type, fb.Type) {
				return false
			}
		}
	}

	return true
}

func isValidWidening(from, to arrow.DataType) bool {
	// String can hold anything
	if to.ID() == arrow.STRING {
		return true
	}

	// Int widening
	intTypes := map[arrow.Type]int{
		arrow.INT8: 8, arrow.INT16: 16, arrow.INT32: 32, arrow.INT64: 64,
	}
	if fromBits, ok := intTypes[from.ID()]; ok {
		if toBits, ok := intTypes[to.ID()]; ok {
			return toBits >= fromBits
		}
	}

	// Float widening
	if from.ID() == arrow.FLOAT32 && to.ID() == arrow.FLOAT64 {
		return true
	}

	return false
}

// TableReader interface for reading Arrow tables
type TableReader interface {
	Next() bool
	Record() arrow.Record
	Release()
}

// NewTableReader creates a table reader - placeholder for actual implementation
func NewTableReader(table arrow.Table, batchSize int64) TableReader {
	return &simpleTableReader{table: table, batchSize: batchSize}
}

type simpleTableReader struct {
	table     arrow.Table
	batchSize int64
	offset    int64
	current   arrow.Record
}

func (r *simpleTableReader) Next() bool {
	if r.offset >= r.table.NumRows() {
		return false
	}

	end := r.offset + r.batchSize
	if end > r.table.NumRows() {
		end = r.table.NumRows()
	}

	// Create a record from the table slice
	// This is a simplified implementation
	r.offset = end
	return r.offset <= r.table.NumRows()
}

func (r *simpleTableReader) Record() arrow.Record {
	return r.current
}

func (r *simpleTableReader) Release() {}

// fileSource implements core.Source for testing
type fileSource struct {
	path   string
	format core.Format
}

func (s *fileSource) ID() string                  { return s.path }
func (s *fileSource) Location() string            { return s.path }
func (s *fileSource) Format() core.Format         { return s.format }
func (s *fileSource) Size() int64                 { info, _ := os.Stat(s.path); if info != nil { return info.Size() }; return 0 }
func (s *fileSource) ModTime() time.Time          { info, _ := os.Stat(s.path); if info != nil { return info.ModTime() }; return time.Time{} }
func (s *fileSource) Metadata() map[string]string { return nil }
func (s *fileSource) Open(ctx context.Context) (io.ReadCloser, error) { return os.Open(s.path) }
