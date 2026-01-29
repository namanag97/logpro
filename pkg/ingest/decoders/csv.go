// Package decoders provides format-specific decoders that produce Arrow RecordBatches.
package decoders

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// CSVDecoder decodes CSV files into Arrow RecordBatches.
type CSVDecoder struct {
	alloc memory.Allocator
}

// NewCSVDecoder creates a new CSV decoder.
func NewCSVDecoder() *CSVDecoder {
	return &CSVDecoder{
		alloc: memory.DefaultAllocator,
	}
}

// Formats returns supported formats.
func (d *CSVDecoder) Formats() []core.Format {
	return []core.Format{core.FormatCSV, core.FormatTSV}
}

// Decode decodes a CSV source into Arrow batches.
func (d *CSVDecoder) Decode(ctx context.Context, source core.Source, opts core.DecodeOptions) (<-chan core.DecodedBatch, error) {
	reader, err := source.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open source: %w", err)
	}

	// Detect delimiter
	delimiter := byte(',')
	if source.Format() == core.FormatTSV {
		delimiter = '\t'
	}

	// Get schema (infer if not provided)
	schema := opts.Schema
	if schema == nil {
		// Need to infer schema - read sample
		schema, err = d.InferSchema(ctx, source, opts.SampleSize)
		if err != nil {
			reader.Close()
			return nil, fmt.Errorf("schema inference failed: %w", err)
		}
		// Reopen source
		reader.Close()
		reader, err = source.Open(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to reopen source: %w", err)
		}
	}

	out := make(chan core.DecodedBatch, 4)

	go func() {
		defer close(out)
		defer reader.Close()

		bufReader := bufio.NewReaderSize(reader, 256*1024)

		// Skip header line (line 1)
		_, err := d.readLine(bufReader, '"')
		if err != nil {
			// BUG FIX: Don't silently fail on header read error
			out <- core.DecodedBatch{
				Errors: []core.RowError{{
					RowNumber: 1,
					Error:     fmt.Errorf("failed to read CSV header: %w", err),
				}},
				IsFinal: true,
			}
			return
		}

		batchSize := opts.BatchSize
		if batchSize <= 0 {
			batchSize = 8192
		}

		builders := d.createBuilders(schema)
		// BUG FIX: Track current builders for proper cleanup
		currentBuilders := builders

		// Ensure builders are always released on exit
		defer func() {
			if currentBuilders != nil {
				d.releaseBuilders(currentBuilders)
			}
		}()

		var rowCount int64
		var batchIndex int
		var batchStartLine int64 = 2 // First data row is line 2 (after header)
		var errors []core.RowError
		// BUG FIX: Line numbers - header is line 1, first data row is line 2
		lineNum := int64(1) // Will be incremented to 2 before first data row

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			line, err := d.readLine(bufReader, '"')
			if err == io.EOF {
				break
			}

			lineNum++ // Increment BEFORE processing so first data row = line 2

			if err != nil {
				errors = append(errors, core.RowError{
					RowNumber: lineNum,
					Error:     err,
				})
				if len(errors) >= opts.MaxErrors && opts.MaxErrors > 0 {
					break
				}
				continue
			}

			fields := d.parseFields(line, delimiter, '"')

			// Append to builders
			rowErr := d.appendRow(builders, schema, fields, lineNum)
			if rowErr != nil {
				errors = append(errors, *rowErr)
				if opts.ErrorPolicy == core.ErrorPolicyStrict {
					break
				}
				continue
			}

			rowCount++

			// Flush batch if needed
			if rowCount > 0 && rowCount%int64(batchSize) == 0 {
				batch := d.flushBuilders(schema, builders)
				select {
				case out <- core.DecodedBatch{
					Batch:     batch,
					Index:     batchIndex,
					RowOffset: batchStartLine, // BUG FIX: Use tracked start line
					Errors:    errors,
				}:
				case <-ctx.Done():
					batch.Release()
					return
				}
				batchIndex++
				batchStartLine = lineNum + 1 // Next batch starts after current line
				errors = nil
				// BUG FIX: Release old builders before creating new ones
				d.releaseBuilders(builders)
				builders = d.createBuilders(schema)
				currentBuilders = builders
			}
		}

		// Flush remaining
		if builders[0].Len() > 0 {
			batch := d.flushBuilders(schema, builders)
			select {
			case out <- core.DecodedBatch{
				Batch:     batch,
				Index:     batchIndex,
				RowOffset: batchStartLine,
				Errors:    errors,
				IsFinal:   true,
			}:
			case <-ctx.Done():
				batch.Release()
			}
		}
		// Mark as nil so defer doesn't double-release
		currentBuilders = nil
	}()

	return out, nil
}

// InferSchema infers schema from CSV data.
func (d *CSVDecoder) InferSchema(ctx context.Context, source core.Source, sampleSize int) (*arrow.Schema, error) {
	if sampleSize <= 0 {
		sampleSize = 10000
	}

	reader, err := source.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	delimiter := byte(',')
	if source.Format() == core.FormatTSV {
		delimiter = '\t'
	}

	bufReader := bufio.NewReaderSize(reader, 64*1024)

	// Read header
	headerLine, err := d.readLine(bufReader, '"')
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	headers := d.parseFields(headerLine, delimiter, '"')
	if len(headers) == 0 {
		return nil, fmt.Errorf("no headers found")
	}

	// Initialize type counters per column
	typeCounts := make([]map[inferredType]int, len(headers))
	for i := range typeCounts {
		typeCounts[i] = make(map[inferredType]int)
	}

	// Sample rows
	for i := 0; i < sampleSize; i++ {
		line, err := d.readLine(bufReader, '"')
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		fields := d.parseFields(line, delimiter, '"')
		for j, field := range fields {
			if j >= len(typeCounts) {
				break
			}
			t := d.inferFieldType(field)
			typeCounts[j][t]++
		}
	}

	// Build schema from most common types
	arrowFields := make([]arrow.Field, len(headers))
	for i, header := range headers {
		name := strings.TrimSpace(string(header))
		if name == "" {
			name = fmt.Sprintf("column_%d", i)
		}

		dataType := d.selectType(typeCounts[i])
		arrowFields[i] = arrow.Field{
			Name:     name,
			Type:     dataType,
			Nullable: true,
		}
	}

	return arrow.NewSchema(arrowFields, nil), nil
}

type inferredType int

const (
	typeNull inferredType = iota
	typeInt64
	typeFloat64
	typeBool
	typeTimestamp
	typeString
)

func (d *CSVDecoder) inferFieldType(value []byte) inferredType {
	s := strings.TrimSpace(string(value))

	if s == "" || d.isNullValue(s) {
		return typeNull
	}

	// Bool
	switch strings.ToLower(s) {
	case "true", "false", "yes", "no", "1", "0":
		return typeBool
	}

	// Int
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return typeInt64
	}

	// Float
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return typeFloat64
	}

	// Timestamp
	if d.parseTimestamp(s) != nil {
		return typeTimestamp
	}

	return typeString
}

func (d *CSVDecoder) selectType(counts map[inferredType]int) arrow.DataType {
	// Priority: if any non-null type exists, use most common
	var maxCount int
	var maxType inferredType = typeString

	for t, count := range counts {
		if t == typeNull {
			continue
		}
		if count > maxCount {
			maxCount = count
			maxType = t
		}
	}

	switch maxType {
	case typeInt64:
		return arrow.PrimitiveTypes.Int64
	case typeFloat64:
		return arrow.PrimitiveTypes.Float64
	case typeBool:
		return arrow.FixedWidthTypes.Boolean
	case typeTimestamp:
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	default:
		return arrow.BinaryTypes.String
	}
}

func (d *CSVDecoder) isNullValue(s string) bool {
	switch s {
	case "", "NULL", "null", "NA", "N/A", "n/a", "None", "none", "nil", "-", "\\N":
		return true
	}
	return false
}

func (d *CSVDecoder) parseTimestamp(s string) *time.Time {
	layouts := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
		"01/02/2006",
		"02-01-2006",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return &t
		}
	}
	return nil
}

func (d *CSVDecoder) readLine(reader *bufio.Reader, quote byte) ([]byte, error) {
	var line []byte
	inQuote := false

	for {
		part, err := reader.ReadBytes('\n')
		if len(part) > 0 {
			line = append(line, part...)

			for _, b := range part {
				if b == quote {
					inQuote = !inQuote
				}
			}

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

func (d *CSVDecoder) parseFields(line []byte, delim, quote byte) [][]byte {
	var fields [][]byte
	var field []byte
	inQuote := false

	for i := 0; i < len(line); i++ {
		b := line[i]

		if b == quote {
			if inQuote && i+1 < len(line) && line[i+1] == quote {
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

func (d *CSVDecoder) createBuilders(schema *arrow.Schema) []array.Builder {
	builders := make([]array.Builder, schema.NumFields())

	for i, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.INT64:
			builders[i] = array.NewInt64Builder(d.alloc)
		case arrow.FLOAT64:
			builders[i] = array.NewFloat64Builder(d.alloc)
		case arrow.BOOL:
			builders[i] = array.NewBooleanBuilder(d.alloc)
		case arrow.TIMESTAMP:
			builders[i] = array.NewTimestampBuilder(d.alloc, &arrow.TimestampType{Unit: arrow.Microsecond})
		case arrow.LIST:
			lt := field.Type.(*arrow.ListType)
			builders[i] = array.NewListBuilder(d.alloc, lt.Elem())
		case arrow.STRUCT:
			st := field.Type.(*arrow.StructType)
			builders[i] = array.NewStructBuilder(d.alloc, st)
		default:
			builders[i] = array.NewStringBuilder(d.alloc)
		}
	}

	return builders
}

func (d *CSVDecoder) releaseBuilders(builders []array.Builder) {
	for _, b := range builders {
		b.Release()
	}
}

func (d *CSVDecoder) appendRow(builders []array.Builder, schema *arrow.Schema, fields [][]byte, lineNum int64) *core.RowError {
	for i, builder := range builders {
		var value []byte
		if i < len(fields) {
			value = fields[i]
		}

		s := strings.TrimSpace(string(value))

		if s == "" || d.isNullValue(s) {
			builder.AppendNull()
			continue
		}

		switch schema.Field(i).Type.ID() {
		case arrow.INT64:
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				builder.AppendNull()
			} else {
				builder.(*array.Int64Builder).Append(v)
			}

		case arrow.FLOAT64:
			v, err := strconv.ParseFloat(s, 64)
			if err != nil {
				builder.AppendNull()
			} else {
				builder.(*array.Float64Builder).Append(v)
			}

		case arrow.BOOL:
			switch strings.ToLower(s) {
			case "true", "yes", "1":
				builder.(*array.BooleanBuilder).Append(true)
			case "false", "no", "0":
				builder.(*array.BooleanBuilder).Append(false)
			default:
				builder.AppendNull()
			}

		case arrow.TIMESTAMP:
			if t := d.parseTimestamp(s); t != nil {
				builder.(*array.TimestampBuilder).Append(arrow.Timestamp(t.UnixMicro()))
			} else {
				builder.AppendNull()
			}

		default:
			// Validate UTF-8
			if !utf8.ValidString(s) {
				s = string(bytes.ToValidUTF8([]byte(s), []byte("\uFFFD")))
			}
			builder.(*array.StringBuilder).Append(s)
		}
	}

	return nil
}

func (d *CSVDecoder) flushBuilders(schema *arrow.Schema, builders []array.Builder) arrow.Record {
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
	}

	return array.NewRecord(schema, arrays, int64(arrays[0].Len()))
}

// Ensure CSVDecoder can be opened from file path
func (d *CSVDecoder) DecodeFile(ctx context.Context, path string, opts core.DecodeOptions) (<-chan core.DecodedBatch, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	info, _ := f.Stat()

	source := &fileSource{
		path:   path,
		file:   f,
		size:   info.Size(),
		format: core.FormatCSV,
	}

	return d.Decode(ctx, source, opts)
}

// fileSource is a simple file-based source
type fileSource struct {
	path   string
	file   *os.File
	size   int64
	format core.Format
}

func (s *fileSource) ID() string                  { return s.path }
func (s *fileSource) Location() string            { return s.path }
func (s *fileSource) Format() core.Format         { return s.format }
func (s *fileSource) Size() int64                 { return s.size }
func (s *fileSource) ModTime() time.Time          { return time.Now() }
func (s *fileSource) Metadata() map[string]string { return nil }
func (s *fileSource) Open(ctx context.Context) (io.ReadCloser, error) {
	return os.Open(s.path)
}

func init() {
	Register(NewCSVDecoder())
}
