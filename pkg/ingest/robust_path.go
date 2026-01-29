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

"github.com/apache/arrow/go/v14/arrow"
"github.com/apache/arrow/go/v14/arrow/array"
"github.com/apache/arrow/go/v14/arrow/memory"
"github.com/apache/arrow/go/v14/parquet"
"github.com/apache/arrow/go/v14/parquet/compress"
"github.com/apache/arrow/go/v14/parquet/pqarrow"
)

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

line, err := r.readLine(reader)
if err == io.EOF {
break
}
if err != nil {
return nil, fmt.Errorf("read error at line %d: %w", lineNum, err)
}

lineNum++

// Parse with error recovery
fields, parseErr, recovered := r.parseCSVLineRobust(line, analysis.Delimiter, analysis.QuoteChar, len(headers))

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
r.appendRecord(builders, types, fields, headers)
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

// readLine reads a line handling embedded newlines in quotes.
func (r *RobustPath) readLine(reader *bufio.Reader) ([]byte, error) {
var line []byte
inQuote := false

for {
part, err := reader.ReadBytes('\n')
if len(part) > 0 {
line = append(line, part...)

// Count quotes to track state
for _, b := range part {
if b == '"' {
inQuote = !inQuote
}
}

// If we're not in a quote, line is complete
if !inQuote {
return bytes.TrimRight(line, "\r\n"), nil
}
}

if err != nil {
if err == io.EOF && len(line) > 0 {
return bytes.TrimRight(line, "\r\n"), nil
}
return line, err
}
}
}

// parseCSVLine parses a CSV line into fields.
func (r *RobustPath) parseCSVLine(line []byte, delim, quote byte) [][]byte {
var fields [][]byte
var field []byte
inQuote := false

for i := 0; i < len(line); i++ {
b := line[i]

if b == quote {
if inQuote && i+1 < len(line) && line[i+1] == quote {
// Escaped quote
field = append(field, quote)
i++
} else {
inQuote = !inQuote
}
} else if b == delim && !inQuote {
fields = append(fields, field)
field = nil
} else {
field = append(field, b)
}
}

fields = append(fields, field)
return fields
}

// parseCSVLineRobust parses with error recovery.
func (r *RobustPath) parseCSVLineRobust(line []byte, delim, quote byte, expectedCols int) ([][]byte, error, bool) {
fields := r.parseCSVLine(line, delim, quote)
recovered := false

// Handle ragged rows
if len(fields) < expectedCols {
// Pad with empty fields
for len(fields) < expectedCols {
fields = append(fields, nil)
}
recovered = true
} else if len(fields) > expectedCols {
// Truncate (but keep note)
fields = fields[:expectedCols]
recovered = true
}

// Check for encoding issues
for i, f := range fields {
if !utf8.Valid(f) {
// Replace invalid UTF-8 with replacement character
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
func (r *RobustPath) detectFieldType(value []byte) ColumnType {
if len(value) == 0 {
return TypeNull
}

s := string(bytes.TrimSpace(value))

// Check for null values
switch s {
case "", "NULL", "null", "NA", "N/A", "n/a", "None", "none", "nil", "-":
return TypeNull
}

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

// Check for timestamp (common formats)
for _, layout := range []string{
time.RFC3339,
"2006-01-02T15:04:05",
"2006-01-02 15:04:05",
"2006-01-02",
"01/02/2006",
"02-01-2006",
} {
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

// appendRecord adds a record to builders.
func (r *RobustPath) appendRecord(builders []array.Builder, types []ColumnType, fields [][]byte, headers [][]byte) {
for i, builder := range builders {
var value []byte
if i < len(fields) {
value = fields[i]
}

if len(value) == 0 || r.isNullValue(value) {
builder.AppendNull()
continue
}

s := string(bytes.TrimSpace(value))

switch types[i] {
case TypeInt64:
if v, err := strconv.ParseInt(s, 10, 64); err == nil {
builder.(*array.Int64Builder).Append(v)
} else {
builder.AppendNull()
}
case TypeFloat64:
if v, err := strconv.ParseFloat(s, 64); err == nil {
builder.(*array.Float64Builder).Append(v)
} else {
builder.AppendNull()
}
case TypeBool:
switch s {
case "true", "True", "TRUE", "1", "yes":
builder.(*array.BooleanBuilder).Append(true)
case "false", "False", "FALSE", "0", "no":
builder.(*array.BooleanBuilder).Append(false)
default:
builder.AppendNull()
}
case TypeTimestamp:
if t := r.parseTimestamp(s); !t.IsZero() {
builder.(*array.TimestampBuilder).Append(arrow.Timestamp(t.UnixMicro()))
} else {
builder.AppendNull()
}
default:
builder.(*array.StringBuilder).Append(s)
}
}
}

// isNullValue checks if value represents null.
func (r *RobustPath) isNullValue(value []byte) bool {
s := string(bytes.TrimSpace(value))
switch s {
case "", "NULL", "null", "NA", "N/A", "n/a", "None", "none", "nil", "-", "\\N":
return true
}
return false
}

// parseTimestamp attempts to parse a timestamp string.
func (r *RobustPath) parseTimestamp(s string) time.Time {
layouts := []string{
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

for _, layout := range layouts {
if t, err := time.Parse(layout, s); err == nil {
return t
}
}
return time.Time{}
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
