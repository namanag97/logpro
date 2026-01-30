package ingest

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"

	ingerrors "github.com/logflow/logflow/pkg/ingest/errors"
)

// bytesToString converts a byte slice to a string without copying.
// The caller must ensure the byte slice is not modified while the string is in use.
// This is safe for read-only operations like strconv.Parse* and time.Parse.
func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// RobustPath handles messy data with error recovery and validation.
type RobustPath struct {
// Buffer pools for zero-allocation parsing
bufferPool  sync.Pool
recordPool  sync.Pool
}

// NewRobustPath creates a new robust processing path.
func NewRobustPath() *RobustPath {
return &RobustPath{
bufferPool: sync.Pool{
New: func() interface{} {
return make([]byte, 0, 64*1024) // 64KB buffers
},
},
recordPool: sync.Pool{
New: func() interface{} {
return &Record{
Fields: make([][]byte, 0, 32),
}
},
},
}
}

// Record represents a parsed row with error tracking.
type Record struct {
Fields      [][]byte
LineNumber  int64
ByteOffset  int64
ParseError  error
Recovered   bool
}

// Reset clears the record for reuse.
func (r *Record) Reset() {
r.Fields = r.Fields[:0]
r.LineNumber = 0
r.ByteOffset = 0
r.ParseError = nil
r.Recovered = false
}

// Process handles file conversion with robust error handling.
func (r *RobustPath) Process(ctx context.Context, inputPath, outputPath string, analysis *FileAnalysis, opts Options) (*Result, error) {
// Ensure output directory exists
if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
return nil, fmt.Errorf("failed to create output directory: %w", err)
}

switch analysis.Format {
case FormatCSV, FormatTSV:
return r.processCSV(ctx, inputPath, outputPath, analysis, opts)
case FormatJSON, FormatJSONL:
return r.processJSON(ctx, inputPath, outputPath, analysis, opts)
case FormatXES:
return r.processXES(ctx, inputPath, outputPath, analysis, opts)
case FormatGzip:
return r.processGzip(ctx, inputPath, outputPath, analysis, opts)
default:
return nil, fmt.Errorf("format %s not supported by robust path", analysis.Format)
}
}

// processCSV converts CSV with robust error handling.
func (r *RobustPath) processCSV(ctx context.Context, inputPath, outputPath string, analysis *FileAnalysis, opts Options) (*Result, error) {
// Open input file
f, err := os.Open(inputPath)
if err != nil {
return nil, fmt.Errorf("failed to open input: %w", err)
}
defer f.Close()

inputInfo, _ := f.Stat()

// Create buffered reader
bufSize := opts.BufferSize
if bufSize <= 0 {
bufSize = 256 * 1024
}
reader := bufio.NewReaderSize(f, bufSize)

// Parse header
headerLine, err := r.readLine(reader)
if err != nil {
return nil, fmt.Errorf("failed to read header: %w", err)
}

headers := r.parseCSVLine(headerLine, analysis.Delimiter, analysis.QuoteChar)
if len(headers) == 0 {
return nil, fmt.Errorf("no headers found")
}

// Infer types from sample
types := r.inferTypes(reader, analysis, headers, 1000)

// Reset reader (reopen file)
f.Seek(0, 0)
reader = bufio.NewReaderSize(f, bufSize)
reader.ReadBytes('\n') // Skip header

// Create Arrow schema
schema := r.buildArrowSchema(headers, types)

// Create Parquet writer
outFile, err := os.Create(outputPath)
if err != nil {
return nil, fmt.Errorf("failed to create output: %w", err)
}
defer outFile.Close()

writerProps := r.buildWriterProps(opts)
arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(memory.DefaultAllocator))

writer, err := pqarrow.NewFileWriter(schema, outFile, writerProps, arrowProps)
if err != nil {
return nil, fmt.Errorf("failed to create writer: %w", err)
}
defer writer.Close()

	// Process records in batches
	batchSize := 8192
	builders := r.createBuilders(schema, memory.DefaultAllocator)

	// Per-column timestamp format cache: -1 means "not yet locked"
	tsFormatIdx := make([]int, len(types))
	for i := range tsFormatIdx {
		tsFormatIdx[i] = -1
	}

	// Reusable CSV field buffer — avoids per-row allocation
	var csvBuf csvFieldsBuf
	csvBuf.fields = make([][]byte, 0, len(headers)+4)
	csvBuf.tmp = make([]byte, 0, 1024)

	// Reusable line buffer
	var lb lineBuffer
	lb.buf = make([]byte, 0, 4096)

	var rowCount int64
	var errorCount int64
	var recoveredCount int64
	lineNum := int64(1)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		line, err := r.readLineInto(&lb, reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read error at line %d: %w", lineNum, err)
		}

		lineNum++

		// Parse with error recovery using reusable buffer
		fields, parseErr, recovered := r.parseCSVLineRobustBuf(&csvBuf, line, analysis.Delimiter, analysis.QuoteChar, len(headers))

		if parseErr != nil && !recovered {
			errorCount++
			if opts.MaxErrors > 0 && errorCount >= int64(opts.MaxErrors) {
				return nil, fmt.Errorf("too many errors (%d), aborting", errorCount)
			}
			continue
		}

		if recovered {
			recoveredCount++
		}

		// Add to builders
		r.appendRecord(builders, types, fields, headers, tsFormatIdx)
		rowCount++

		// Flush batch if needed
		if rowCount > 0 && rowCount%int64(batchSize) == 0 {
			if err := r.flushBatch(writer, schema, builders); err != nil {
				return nil, fmt.Errorf("failed to write batch: %w", err)
			}
		}
	}

// Flush remaining
if rowCount%int64(batchSize) != 0 {
if err := r.flushBatch(writer, schema, builders); err != nil {
return nil, fmt.Errorf("failed to write final batch: %w", err)
}
}

// Get output size
outputInfo, _ := os.Stat(outputPath)
outputSize := int64(0)
if outputInfo != nil {
outputSize = outputInfo.Size()
}

result := &Result{
InputPath:       inputPath,
OutputPath:      outputPath,
Format:          analysis.Format,
Strategy:        StrategyRobustGo,
RowCount:        rowCount,
ColumnCount:     len(headers),
InputSize:       inputInfo.Size(),
OutputSize:      outputSize,
CompressionRate: float64(inputInfo.Size()) / float64(outputSize),
Quality: &QualityMetrics{
MalformedRows: errorCount,
RecoveredRows: recoveredCount,
},
}

return result, nil
}

// lineBuffer is a reusable buffer for readLine to avoid per-line allocations.
type lineBuffer struct {
	buf []byte
}

// readLineInto reads a line handling embedded newlines in quotes, reusing lb.
func (r *RobustPath) readLineInto(lb *lineBuffer, reader *bufio.Reader) ([]byte, error) {
	lb.buf = lb.buf[:0]
	inQuote := false

	for {
		part, err := reader.ReadBytes('\n')
		if len(part) > 0 {
			lb.buf = append(lb.buf, part...)

			for _, b := range part {
				if b == '"' {
					inQuote = !inQuote
				}
			}

			if !inQuote {
				line := lb.buf
				// Trim trailing \r\n in place
				for len(line) > 0 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
					line = line[:len(line)-1]
				}
				return line, nil
			}
		}

		if err != nil {
			if err == io.EOF && len(lb.buf) > 0 {
				line := lb.buf
				for len(line) > 0 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
					line = line[:len(line)-1]
				}
				return line, nil
			}
			return lb.buf, err
		}
	}
}

// readLine reads a line handling embedded newlines in quotes (allocating version).
func (r *RobustPath) readLine(reader *bufio.Reader) ([]byte, error) {
	var lb lineBuffer
	return r.readLineInto(&lb, reader)
}

// csvFieldsBuf is a reusable buffer for parseCSVLine results.
// It holds pre-allocated slices to avoid per-row allocations.
type csvFieldsBuf struct {
	fields [][]byte // reusable output slice
	tmp    []byte   // scratch buffer for fields that need unquoting
}

// parseCSVLineInto parses a CSV line into the reusable buf, returning the fields.
// For fields that don't contain escaped quotes, it returns sub-slices of line
// (zero allocation). Only fields with escaped quotes are copied into buf.tmp.
func (r *RobustPath) parseCSVLineInto(buf *csvFieldsBuf, line []byte, delim, quote byte) [][]byte {
	buf.fields = buf.fields[:0]
	buf.tmp = buf.tmp[:0]

	inQuote := false
	hasEscape := false
	fieldStart := 0
	isQuoted := false

	for i := 0; i <= len(line); i++ {
		if i == len(line) || (line[i] == delim && !inQuote) {
			// End of field
			start := fieldStart
			end := i

			if isQuoted && end > start+1 && line[start] == quote {
				start++ // skip opening quote
				if end > 0 && line[end-1] == quote {
					end-- // skip closing quote
				}
			}

			if hasEscape {
				// Must copy with unescaping
				tmpStart := len(buf.tmp)
				for j := start; j < end; j++ {
					if line[j] == quote && j+1 < end && line[j+1] == quote {
						buf.tmp = append(buf.tmp, quote)
						j++ // skip second quote
					} else {
						buf.tmp = append(buf.tmp, line[j])
					}
				}
				field := make([]byte, len(buf.tmp)-tmpStart)
				copy(field, buf.tmp[tmpStart:])
				buf.fields = append(buf.fields, field)
			} else {
				// Zero-copy: sub-slice of line
				buf.fields = append(buf.fields, line[start:end])
			}

			if i < len(line) {
				fieldStart = i + 1
				isQuoted = false
				hasEscape = false
			}
			continue
		}

		b := line[i]
		if b == quote {
			if !inQuote {
				if i == fieldStart {
					isQuoted = true
				}
				inQuote = true
			} else if i+1 < len(line) && line[i+1] == quote {
				hasEscape = true
				i++ // skip paired quote
			} else {
				inQuote = false
			}
		}
	}

	return buf.fields
}

// parseCSVLine parses a CSV line into fields (allocating version for sample phase).
func (r *RobustPath) parseCSVLine(line []byte, delim, quote byte) [][]byte {
	var buf csvFieldsBuf
	result := r.parseCSVLineInto(&buf, line, delim, quote)
	// Copy to owned slices since buf may be reused
	out := make([][]byte, len(result))
	for i, f := range result {
		cp := make([]byte, len(f))
		copy(cp, f)
		out[i] = cp
	}
	return out
}

// parseCSVLineRobust parses with error recovery (allocating version).
func (r *RobustPath) parseCSVLineRobust(line []byte, delim, quote byte, expectedCols int) ([][]byte, error, bool) {
	fields := r.parseCSVLine(line, delim, quote)
	return r.robustFixup(fields, expectedCols)
}

// parseCSVLineRobustBuf parses with error recovery using a reusable buffer.
func (r *RobustPath) parseCSVLineRobustBuf(buf *csvFieldsBuf, line []byte, delim, quote byte, expectedCols int) ([][]byte, error, bool) {
	fields := r.parseCSVLineInto(buf, line, delim, quote)
	return r.robustFixup(fields, expectedCols)
}

// robustFixup applies ragged-row padding/truncation and UTF-8 validation.
func (r *RobustPath) robustFixup(fields [][]byte, expectedCols int) ([][]byte, error, bool) {
	recovered := false

	// Handle ragged rows
	if len(fields) < expectedCols {
		for len(fields) < expectedCols {
			fields = append(fields, nil)
		}
		recovered = true
	} else if len(fields) > expectedCols {
		fields = fields[:expectedCols]
		recovered = true
	}

	// Only validate UTF-8 on fields that contain bytes >= 0x80.
	for i, f := range fields {
		if len(f) == 0 {
			continue
		}
		hasHigh := false
		for _, b := range f {
			if b >= 0x80 {
				hasHigh = true
				break
			}
		}
		if hasHigh && !utf8.Valid(f) {
			fields[i] = bytes.ToValidUTF8(f, []byte("\uFFFD"))
			recovered = true
		}
	}

	return fields, nil, recovered
}

// inferTypes samples rows to determine column types.
func (r *RobustPath) inferTypes(reader *bufio.Reader, analysis *FileAnalysis, headers [][]byte, sampleSize int) []ColumnType {
types := make([]ColumnType, len(headers))
for i := range types {
types[i] = TypeUnknown
}

counts := make([]map[ColumnType]int, len(headers))
for i := range counts {
counts[i] = make(map[ColumnType]int)
}

for i := 0; i < sampleSize; i++ {
line, err := r.readLine(reader)
if err != nil {
break
}

fields := r.parseCSVLine(line, analysis.Delimiter, analysis.QuoteChar)

for j, field := range fields {
if j >= len(counts) {
break
}
t := r.detectFieldType(field)
counts[j][t]++
}
}

// Select most common type per column
for i, countMap := range counts {
maxCount := 0
for t, count := range countMap {
if count > maxCount && t != TypeNull {
maxCount = count
types[i] = t
}
}
if types[i] == TypeUnknown {
types[i] = TypeString // Default to string
}
}

return types
}

// ColumnType represents inferred column type.
type ColumnType uint8

const (
TypeUnknown ColumnType = iota
TypeNull
TypeString
TypeInt64
TypeFloat64
TypeBool
TypeTimestamp
)

// detectFieldType determines the type of a field value.
// This is called only during the sample phase (typically 1000 rows), so
// string allocation here is acceptable.
func (r *RobustPath) detectFieldType(value []byte) ColumnType {
	if len(value) == 0 {
		return TypeNull
	}

	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 {
		return TypeNull
	}

	// Check for null values (byte comparison, no alloc)
	if r.isNullValue(trimmed) {
		return TypeNull
	}

	s := string(trimmed)

	// Check for boolean
	switch s {
	case "true", "false", "True", "False", "TRUE", "FALSE", "1", "0", "yes", "no":
		return TypeBool
	}

	// Check for integer
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return TypeInt64
	}

	// Check for float
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return TypeFloat64
	}

	// Check for timestamp using shared layouts
	for _, layout := range timestampLayouts {
		if _, err := time.Parse(layout, s); err == nil {
			return TypeTimestamp
		}
	}

	return TypeString
}

// buildArrowSchema creates Arrow schema from headers and types.
func (r *RobustPath) buildArrowSchema(headers [][]byte, types []ColumnType) *arrow.Schema {
fields := make([]arrow.Field, len(headers))

for i, header := range headers {
name := string(header)
var dataType arrow.DataType

switch types[i] {
case TypeInt64:
dataType = arrow.PrimitiveTypes.Int64
case TypeFloat64:
dataType = arrow.PrimitiveTypes.Float64
case TypeBool:
dataType = arrow.FixedWidthTypes.Boolean
case TypeTimestamp:
dataType = &arrow.TimestampType{Unit: arrow.Microsecond}
default:
dataType = arrow.BinaryTypes.String
}

fields[i] = arrow.Field{Name: name, Type: dataType, Nullable: true}
}

return arrow.NewSchema(fields, nil)
}

// createBuilders creates Arrow builders for each column.
func (r *RobustPath) createBuilders(schema *arrow.Schema, alloc memory.Allocator) []array.Builder {
builders := make([]array.Builder, schema.NumFields())

for i, field := range schema.Fields() {
switch field.Type.ID() {
case arrow.INT64:
builders[i] = array.NewInt64Builder(alloc)
case arrow.FLOAT64:
builders[i] = array.NewFloat64Builder(alloc)
case arrow.BOOL:
builders[i] = array.NewBooleanBuilder(alloc)
case arrow.TIMESTAMP:
builders[i] = array.NewTimestampBuilder(alloc, &arrow.TimestampType{Unit: arrow.Microsecond})
default:
builders[i] = array.NewStringBuilder(alloc)
}
}

return builders
}

// appendRecord adds a record to builders. It uses the pre-computed
// tsFormatIdx cache so that timestamp columns don't try all formats every row.
func (r *RobustPath) appendRecord(builders []array.Builder, types []ColumnType, fields [][]byte, headers [][]byte, tsFormatIdx []int) {
	for i, builder := range builders {
		var value []byte
		if i < len(fields) {
			value = fields[i]
		}

		// Trim once upfront, then check null on the trimmed result
		trimmed := bytes.TrimSpace(value)
		if len(trimmed) == 0 || r.isNullValueTrimmed(trimmed) {
			builder.AppendNull()
			continue
		}

		switch types[i] {
		case TypeInt64:
			// bytesToString avoids allocation for the strconv parse call
			if v, err := strconv.ParseInt(bytesToString(trimmed), 10, 64); err == nil {
				builder.(*array.Int64Builder).Append(v)
			} else {
				builder.AppendNull()
			}
		case TypeFloat64:
			if v, err := strconv.ParseFloat(bytesToString(trimmed), 64); err == nil {
				builder.(*array.Float64Builder).Append(v)
			} else {
				builder.AppendNull()
			}
		case TypeBool:
			if len(trimmed) == 0 {
				builder.AppendNull()
				continue
			}
			switch trimmed[0] {
			case 't', 'T', '1', 'y', 'Y':
				builder.(*array.BooleanBuilder).Append(true)
			case 'f', 'F', '0', 'n', 'N':
				builder.(*array.BooleanBuilder).Append(false)
			default:
				builder.AppendNull()
			}
		case TypeTimestamp:
			s := bytesToString(trimmed)
			if t, idx := r.parseTimestampCached(s, tsFormatIdx[i]); !t.IsZero() {
				builder.(*array.TimestampBuilder).Append(arrow.Timestamp(t.UnixMicro()))
				tsFormatIdx[i] = idx
			} else {
				builder.AppendNull()
			}
		default:
			// StringBuilder.Append copies internally, so bytesToString is safe here
			builder.(*array.StringBuilder).Append(bytesToString(trimmed))
		}
	}
}

// nullValues holds the canonical null representations as byte slices to avoid
// allocating strings on every call to isNullValue.
var nullValues = [][]byte{
	[]byte("NULL"), []byte("null"), []byte("NA"), []byte("N/A"),
	[]byte("n/a"), []byte("None"), []byte("none"), []byte("nil"),
	[]byte("-"), []byte("\\N"),
}

// isNullValue checks if value represents null without allocating a string.
func (r *RobustPath) isNullValue(value []byte) bool {
	return r.isNullValueTrimmed(bytes.TrimSpace(value))
}

// isNullValueTrimmed checks if an already-trimmed value represents null.
func (r *RobustPath) isNullValueTrimmed(v []byte) bool {
	if len(v) == 0 {
		return true
	}
	for _, nv := range nullValues {
		if bytes.Equal(v, nv) {
			return true
		}
	}
	return false
}

// timestampLayouts is the shared set of timestamp layouts used by both
// detection and parsing. Defined once to avoid repeated allocation.
var timestampLayouts = []string{
	time.RFC3339,
	time.RFC3339Nano,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.000",
	"2006-01-02 15:04:05.000",
	"2006-01-02",
	"01/02/2006",
	"02-01-2006",
	"01/02/2006 15:04:05",
	"02-01-2006 15:04:05",
}

// parseTimestamp attempts to parse a timestamp string (fallback, tries all formats).
func (r *RobustPath) parseTimestamp(s string) time.Time {
	for _, layout := range timestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// parseTimestampCached tries the cached format index first, falling back to
// a full scan. Returns the parsed time and the index of the successful format.
// hint == -1 means no cached format yet.
func (r *RobustPath) parseTimestampCached(s string, hint int) (time.Time, int) {
	// Try the cached format first
	if hint >= 0 && hint < len(timestampLayouts) {
		if t, err := time.Parse(timestampLayouts[hint], s); err == nil {
			return t, hint
		}
	}
	// Fall back to full scan
	for i, layout := range timestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, i
		}
	}
	return time.Time{}, -1
}

// flushBatch writes accumulated data to Parquet.
func (r *RobustPath) flushBatch(writer *pqarrow.FileWriter, schema *arrow.Schema, builders []array.Builder) error {
// Build arrays
arrays := make([]arrow.Array, len(builders))
for i, builder := range builders {
arrays[i] = builder.NewArray()
defer arrays[i].Release()
}

// Create record batch
record := array.NewRecord(schema, arrays, int64(arrays[0].Len()))
defer record.Release()

// Write
return writer.Write(record)
}

// buildWriterProps creates Parquet writer properties.
func (r *RobustPath) buildWriterProps(opts Options) *parquet.WriterProperties {
compression := compress.Codecs.Snappy
switch opts.Compression {
case "zstd":
compression = compress.Codecs.Zstd
case "gzip":
compression = compress.Codecs.Gzip
case "lz4":
compression = compress.Codecs.Lz4
case "none", "uncompressed":
compression = compress.Codecs.Uncompressed
}

rowGroupSize := int64(opts.RowGroupSize)
if rowGroupSize <= 0 {
rowGroupSize = 10000
}

return parquet.NewWriterProperties(
parquet.WithCompression(compression),
parquet.WithDictionaryDefault(true),
parquet.WithDataPageSize(1024*1024), // 1MB
)
}

// processJSON handles JSON/JSONL files.
func (r *RobustPath) processJSON(ctx context.Context, inputPath, outputPath string, analysis *FileAnalysis, opts Options) (*Result, error) {
f, err := os.Open(inputPath)
if err != nil {
return nil, fmt.Errorf("failed to open input: %w", err)
}
defer f.Close()

inputInfo, _ := f.Stat()

// Detect if JSONL or JSON array
isJSONL := analysis.Format == FormatJSONL

var records []map[string]interface{}
var rowCount int64

if isJSONL {
// Process line by line
scanner := bufio.NewScanner(f)
scanner.Buffer(make([]byte, 1024*1024), 32*1024*1024) // 32MB max line

for scanner.Scan() {
select {
case <-ctx.Done():
return nil, ctx.Err()
default:
}

line := scanner.Bytes()
if len(bytes.TrimSpace(line)) == 0 {
continue
}

var record map[string]interface{}
if err := json.Unmarshal(line, &record); err != nil {
continue // Skip malformed lines
}
records = append(records, record)
rowCount++
}
} else {
// Parse as JSON array
var arr []map[string]interface{}
decoder := json.NewDecoder(f)
if err := decoder.Decode(&arr); err != nil {
return nil, fmt.Errorf("failed to parse JSON: %w", err)
}
records = arr
rowCount = int64(len(arr))
}

if len(records) == 0 {
return nil, fmt.Errorf("no records found")
}

// Build schema from first few records
schema := r.buildJSONSchema(records[:min(100, len(records))])

// Write to Parquet
outFile, err := os.Create(outputPath)
if err != nil {
return nil, fmt.Errorf("failed to create output: %w", err)
}
defer outFile.Close()

writerProps := r.buildWriterProps(opts)
arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(memory.DefaultAllocator))

writer, err := pqarrow.NewFileWriter(schema, outFile, writerProps, arrowProps)
if err != nil {
return nil, fmt.Errorf("failed to create writer: %w", err)
}
defer writer.Close()

// Build and write batches
batchSize := 8192
builders := r.createStringBuilders(schema, memory.DefaultAllocator)

for i, record := range records {
r.appendJSONRecord(builders, schema, record)

if (i+1)%batchSize == 0 {
if err := r.flushStringBatch(writer, schema, builders); err != nil {
return nil, err
}
}
}

// Flush remaining
if len(records)%batchSize != 0 {
if err := r.flushStringBatch(writer, schema, builders); err != nil {
return nil, err
}
}

outputInfo, _ := os.Stat(outputPath)
outputSize := int64(0)
if outputInfo != nil {
outputSize = outputInfo.Size()
}

return &Result{
InputPath:       inputPath,
OutputPath:      outputPath,
Format:          analysis.Format,
Strategy:        StrategyRobustGo,
RowCount:        rowCount,
ColumnCount:     schema.NumFields(),
InputSize:       inputInfo.Size(),
OutputSize:      outputSize,
CompressionRate: float64(inputInfo.Size()) / float64(outputSize),
}, nil
}

// buildJSONSchema creates schema from JSON records.
func (r *RobustPath) buildJSONSchema(records []map[string]interface{}) *arrow.Schema {
// Collect all keys
keys := make(map[string]bool)
for _, record := range records {
for k := range record {
keys[k] = true
}
}

// Build schema (all strings for simplicity)
fields := make([]arrow.Field, 0, len(keys))
for k := range keys {
fields = append(fields, arrow.Field{Name: k, Type: arrow.BinaryTypes.String, Nullable: true})
}

return arrow.NewSchema(fields, nil)
}

// createStringBuilders creates string builders for all columns.
func (r *RobustPath) createStringBuilders(schema *arrow.Schema, alloc memory.Allocator) []*array.StringBuilder {
builders := make([]*array.StringBuilder, schema.NumFields())
for i := range builders {
builders[i] = array.NewStringBuilder(alloc)
}
return builders
}

// appendJSONRecord adds a JSON record to builders.
func (r *RobustPath) appendJSONRecord(builders []*array.StringBuilder, schema *arrow.Schema, record map[string]interface{}) {
for i, field := range schema.Fields() {
if val, ok := record[field.Name]; ok && val != nil {
builders[i].Append(fmt.Sprintf("%v", val))
} else {
builders[i].AppendNull()
}
}
}

// flushStringBatch writes string data to Parquet.
func (r *RobustPath) flushStringBatch(writer *pqarrow.FileWriter, schema *arrow.Schema, builders []*array.StringBuilder) error {
arrays := make([]arrow.Array, len(builders))
for i, builder := range builders {
arrays[i] = builder.NewArray()
defer arrays[i].Release()
}

record := array.NewRecord(schema, arrays, int64(arrays[0].Len()))
defer record.Release()

return writer.Write(record)
}

// processXES handles XES process mining files with streaming.
func (r *RobustPath) processXES(ctx context.Context, inputPath, outputPath string, analysis *FileAnalysis, opts Options) (*Result, error) {
// Use the existing XES parser but with robust error handling
// For now, delegate to a simplified implementation
return nil, fmt.Errorf("XES processing via robust path not yet implemented - use fast path")
}

// processGzip handles gzip-compressed files.
func (r *RobustPath) processGzip(ctx context.Context, inputPath, outputPath string, analysis *FileAnalysis, opts Options) (*Result, error) {
f, err := os.Open(inputPath)
if err != nil {
return nil, err
}
defer f.Close()

gz, err := gzip.NewReader(f)
if err != nil {
return nil, fmt.Errorf("failed to create gzip reader: %w", err)
}
defer gz.Close()

// Create temp file for decompressed data
tmpFile, err := os.CreateTemp("", "ingest-*.tmp")
if err != nil {
return nil, fmt.Errorf("failed to create temp file: %w", err)
}
tmpPath := tmpFile.Name()
defer os.Remove(tmpPath)

// Decompress
if _, err := io.Copy(tmpFile, gz); err != nil {
tmpFile.Close()
return nil, fmt.Errorf("decompression failed: %w", err)
}
tmpFile.Close()

// Analyze and process the decompressed file
innerAnalysis, err := NewDetector().Analyze(tmpPath)
if err != nil {
return nil, fmt.Errorf("failed to analyze decompressed file: %w", err)
}

return r.Process(ctx, tmpPath, outputPath, innerAnalysis, opts)
}

// StreamingStats tracks progress during streaming processing.
type StreamingStats struct {
BytesRead    atomic.Int64
RowsWritten  atomic.Int64
ErrorCount   atomic.Int64
RecoverCount atomic.Int64
}

func min(a, b int) int {
if a < b {
return a
}
return b
}
