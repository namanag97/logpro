package decoders

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// JSONDecoder decodes JSON and JSONL files into Arrow RecordBatches.
type JSONDecoder struct {
	alloc memory.Allocator
}

// NewJSONDecoder creates a new JSON decoder.
func NewJSONDecoder() *JSONDecoder {
	return &JSONDecoder{
		alloc: memory.DefaultAllocator,
	}
}

// Formats returns supported formats.
func (d *JSONDecoder) Formats() []core.Format {
	return []core.Format{core.FormatJSON, core.FormatJSONL}
}

// Decode decodes a JSON source into Arrow batches.
func (d *JSONDecoder) Decode(ctx context.Context, source core.Source, opts core.DecodeOptions) (<-chan core.DecodedBatch, error) {
	reader, err := source.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open source: %w", err)
	}

	isJSONL := source.Format() == core.FormatJSONL

	// Get schema
	schema := opts.Schema
	if schema == nil {
		schema, err = d.InferSchema(ctx, source, opts.SampleSize)
		if err != nil {
			reader.Close()
			return nil, fmt.Errorf("schema inference failed: %w", err)
		}
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

		batchSize := opts.BatchSize
		if batchSize <= 0 {
			batchSize = 8192
		}

		if isJSONL {
			d.decodeJSONL(ctx, reader, schema, batchSize, opts, out)
		} else {
			d.decodeJSONArray(ctx, reader, schema, batchSize, opts, out)
		}
	}()

	return out, nil
}

func (d *JSONDecoder) decodeJSONL(ctx context.Context, reader io.Reader, schema *arrow.Schema, batchSize int, opts core.DecodeOptions, out chan<- core.DecodedBatch) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 32*1024*1024) // 32MB max line

	builders := d.createBuilders(schema)
	defer d.releaseBuilders(builders)

	var rowCount int
	var batchIndex int
	var errors []core.RowError
	lineNum := int64(0)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		lineNum++
		line := scanner.Bytes()

		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var record map[string]interface{}
		if err := json.Unmarshal(line, &record); err != nil {
			errors = append(errors, core.RowError{
				RowNumber: lineNum,
				Error:     err,
			})
			if opts.ErrorPolicy == core.ErrorPolicyStrict {
				break
			}
			continue
		}

		d.appendRecord(builders, schema, record)
		rowCount++

		if rowCount >= batchSize {
			batch := d.flushBuilders(schema, builders)
			select {
			case out <- core.DecodedBatch{
				Batch:     batch,
				Index:     batchIndex,
				RowOffset: lineNum - int64(rowCount),
				Errors:    errors,
			}:
			case <-ctx.Done():
				batch.Release()
				return
			}
			batchIndex++
			rowCount = 0
			errors = nil
			builders = d.createBuilders(schema)
		}
	}

	// Flush remaining
	if rowCount > 0 {
		batch := d.flushBuilders(schema, builders)
		select {
		case out <- core.DecodedBatch{
			Batch:   batch,
			Index:   batchIndex,
			Errors:  errors,
			IsFinal: true,
		}:
		case <-ctx.Done():
			batch.Release()
		}
	}
}

func (d *JSONDecoder) decodeJSONArray(ctx context.Context, reader io.Reader, schema *arrow.Schema, batchSize int, opts core.DecodeOptions, out chan<- core.DecodedBatch) {
	var records []map[string]interface{}
	decoder := json.NewDecoder(reader)

	if err := decoder.Decode(&records); err != nil {
		// Try as single object
		var single map[string]interface{}
		if err2 := json.Unmarshal([]byte(fmt.Sprint(err)), &single); err2 == nil {
			records = []map[string]interface{}{single}
		} else {
			return
		}
	}

	builders := d.createBuilders(schema)
	defer d.releaseBuilders(builders)

	var rowCount int
	var batchIndex int

	for i, record := range records {
		select {
		case <-ctx.Done():
			return
		default:
		}

		d.appendRecord(builders, schema, record)
		rowCount++

		if rowCount >= batchSize {
			batch := d.flushBuilders(schema, builders)
			select {
			case out <- core.DecodedBatch{
				Batch:     batch,
				Index:     batchIndex,
				RowOffset: int64(i - rowCount + 1),
			}:
			case <-ctx.Done():
				batch.Release()
				return
			}
			batchIndex++
			rowCount = 0
			builders = d.createBuilders(schema)
		}
	}

	// Flush remaining
	if rowCount > 0 {
		batch := d.flushBuilders(schema, builders)
		select {
		case out <- core.DecodedBatch{
			Batch:   batch,
			Index:   batchIndex,
			IsFinal: true,
		}:
		case <-ctx.Done():
			batch.Release()
		}
	}
}

// InferSchema infers schema from JSON data.
func (d *JSONDecoder) InferSchema(ctx context.Context, source core.Source, sampleSize int) (*arrow.Schema, error) {
	if sampleSize <= 0 {
		sampleSize = 1000
	}

	reader, err := source.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	isJSONL := source.Format() == core.FormatJSONL

	// Collect all keys and their types
	fieldTypes := make(map[string]map[inferredType]int)

	if isJSONL {
		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 1024*1024), 32*1024*1024)

		count := 0
		for scanner.Scan() && count < sampleSize {
			line := scanner.Bytes()
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}

			var record map[string]interface{}
			if err := json.Unmarshal(line, &record); err != nil {
				continue
			}

			d.collectTypes(record, fieldTypes)
			count++
		}
	} else {
		var records []map[string]interface{}
		decoder := json.NewDecoder(reader)
		if err := decoder.Decode(&records); err != nil {
			return nil, err
		}

		for i, record := range records {
			if i >= sampleSize {
				break
			}
			d.collectTypes(record, fieldTypes)
		}
	}

	if len(fieldTypes) == 0 {
		return nil, fmt.Errorf("no fields found")
	}

	// Sort field names for consistent ordering
	var fieldNames []string
	for name := range fieldTypes {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)

	// Build schema
	arrowFields := make([]arrow.Field, len(fieldNames))
	for i, name := range fieldNames {
		counts := fieldTypes[name]
		arrowFields[i] = arrow.Field{
			Name:     name,
			Type:     d.selectType(counts),
			Nullable: true,
		}
	}

	return arrow.NewSchema(arrowFields, nil), nil
}

func (d *JSONDecoder) collectTypes(record map[string]interface{}, fieldTypes map[string]map[inferredType]int) {
	for key, value := range record {
		if fieldTypes[key] == nil {
			fieldTypes[key] = make(map[inferredType]int)
		}

		t := d.inferValueType(value)
		fieldTypes[key][t]++
	}
}

func (d *JSONDecoder) inferValueType(value interface{}) inferredType {
	if value == nil {
		return typeNull
	}

	switch v := value.(type) {
	case bool:
		return typeBool
	case float64:
		// Check if it's actually an integer
		if v == float64(int64(v)) {
			return typeInt64
		}
		return typeFloat64
	case string:
		// Check if it's a timestamp
		if d.parseTimestamp(v) != nil {
			return typeTimestamp
		}
		return typeString
	default:
		return typeString
	}
}

func (d *JSONDecoder) selectType(counts map[inferredType]int) arrow.DataType {
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

func (d *JSONDecoder) parseTimestamp(s string) *time.Time {
	layouts := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return &t
		}
	}
	return nil
}

func (d *JSONDecoder) createBuilders(schema *arrow.Schema) []array.Builder {
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
		default:
			builders[i] = array.NewStringBuilder(d.alloc)
		}
	}

	return builders
}

func (d *JSONDecoder) releaseBuilders(builders []array.Builder) {
	for _, b := range builders {
		b.Release()
	}
}

func (d *JSONDecoder) appendRecord(builders []array.Builder, schema *arrow.Schema, record map[string]interface{}) {
	for i, field := range schema.Fields() {
		value, exists := record[field.Name]

		if !exists || value == nil {
			builders[i].AppendNull()
			continue
		}

		switch field.Type.ID() {
		case arrow.INT64:
			switch v := value.(type) {
			case float64:
				builders[i].(*array.Int64Builder).Append(int64(v))
			case string:
				if n, err := strconv.ParseInt(v, 10, 64); err == nil {
					builders[i].(*array.Int64Builder).Append(n)
				} else {
					builders[i].AppendNull()
				}
			default:
				builders[i].AppendNull()
			}

		case arrow.FLOAT64:
			switch v := value.(type) {
			case float64:
				builders[i].(*array.Float64Builder).Append(v)
			case string:
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					builders[i].(*array.Float64Builder).Append(f)
				} else {
					builders[i].AppendNull()
				}
			default:
				builders[i].AppendNull()
			}

		case arrow.BOOL:
			switch v := value.(type) {
			case bool:
				builders[i].(*array.BooleanBuilder).Append(v)
			default:
				builders[i].AppendNull()
			}

		case arrow.TIMESTAMP:
			switch v := value.(type) {
			case string:
				if t := d.parseTimestamp(v); t != nil {
					builders[i].(*array.TimestampBuilder).Append(arrow.Timestamp(t.UnixMicro()))
				} else {
					builders[i].AppendNull()
				}
			default:
				builders[i].AppendNull()
			}

		default:
			builders[i].(*array.StringBuilder).Append(fmt.Sprintf("%v", value))
		}
	}
}

func (d *JSONDecoder) flushBuilders(schema *arrow.Schema, builders []array.Builder) arrow.Record {
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
	}
	return array.NewRecord(schema, arrays, int64(arrays[0].Len()))
}

func init() {
	Register(NewJSONDecoder())
}
