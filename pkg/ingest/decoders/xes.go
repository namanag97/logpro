// Package decoders provides format-specific decoders that produce Arrow RecordBatches.
package decoders

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// XES attribute key constants
var (
	xesConceptName  = []byte("concept:name")
	xesTimeStamp    = []byte("time:timestamp")
	xesOrgResource  = []byte("org:resource")
	xesOrgGroup     = []byte("org:group")
	xesOrgRole      = []byte("org:role")
	xesLifecycleTr  = []byte("lifecycle:transition")
	xesCostTotal    = []byte("cost:total")
	xesCostCurrency = []byte("cost:currency")
)

// XML element names
var (
	xmlLog    = []byte("log")
	xmlTrace  = []byte("trace")
	xmlEvent  = []byte("event")
	xmlString = []byte("string")
	xmlDate   = []byte("date")
	xmlInt    = []byte("int")
	xmlFloat  = []byte("float")
	xmlBool   = []byte("boolean")
)

// XES parser states
type xesState uint8

const (
	xesStateInit xesState = iota
	xesStateLog
	xesStateTrace
	xesStateEvent
)

// XESDecoder decodes XES (eXtensible Event Stream) files into Arrow RecordBatches.
// XES is the IEEE standard for process mining event logs.
type XESDecoder struct {
	alloc memory.Allocator
}

// NewXESDecoder creates a new XES decoder.
func NewXESDecoder() *XESDecoder {
	return &XESDecoder{
		alloc: memory.DefaultAllocator,
	}
}

// Formats returns supported formats.
func (d *XESDecoder) Formats() []core.Format {
	return []core.Format{core.FormatXES}
}

// xesEvent holds parsed XES event data
type xesEvent struct {
	caseID     string
	activity   string
	timestamp  time.Time
	resource   string
	lifecycle  string
	attributes map[string]interface{}
}

// Decode decodes an XES source into Arrow batches.
func (d *XESDecoder) Decode(ctx context.Context, source core.Source, opts core.DecodeOptions) (<-chan core.DecodedBatch, error) {
	reader, err := source.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open source: %w", err)
	}

	// Infer schema if not provided
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

		d.decodeXES(ctx, reader, schema, batchSize, opts, out)
	}()

	return out, nil
}

// decodeXES performs streaming XES parsing using a state machine
func (d *XESDecoder) decodeXES(ctx context.Context, reader io.Reader, schema *arrow.Schema, batchSize int, opts core.DecodeOptions, out chan<- core.DecodedBatch) {
	bufReader := bufio.NewReaderSize(reader, 256*1024)

	builders := d.createBuilders(schema)
	defer d.releaseBuilders(builders)

	state := xesStateInit
	var currentCaseID string
	var currentEvent *xesEvent
	var rowCount int
	var batchIndex int
	var errors []core.RowError
	var lineNum int64

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line, err := bufReader.ReadBytes('>')
		if err != nil && err != io.EOF {
			errors = append(errors, core.RowError{
				RowNumber: lineNum,
				Error:     err,
			})
			break
		}
		if len(line) == 0 && err == io.EOF {
			break
		}

		lineNum++
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		// Process based on current state
		switch {
		case d.isOpenTag(line, xmlLog):
			state = xesStateLog

		case d.isOpenTag(line, xmlTrace):
			state = xesStateTrace
			currentCaseID = ""

		case d.isCloseTag(line, xmlTrace):
			state = xesStateLog
			currentCaseID = ""

		case d.isOpenTag(line, xmlEvent):
			state = xesStateEvent
			currentEvent = &xesEvent{
				caseID:     currentCaseID,
				attributes: make(map[string]interface{}),
			}

		case d.isCloseTag(line, xmlEvent):
			if currentEvent != nil {
				// Append event to builders
				d.appendEvent(builders, schema, currentEvent)
				rowCount++

				// Flush batch if needed
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

				currentEvent = nil
			}
			state = xesStateTrace

		case state == xesStateTrace && d.isAttributeTag(line):
			// Trace-level attribute (for case ID)
			key, value := d.extractAttribute(line)
			if bytes.Equal(key, xesConceptName) {
				currentCaseID = string(value)
			}

		case state == xesStateEvent && d.isAttributeTag(line):
			// Event-level attribute
			if currentEvent != nil {
				d.processEventAttribute(line, currentEvent)
			}
		}

		if err == io.EOF {
			break
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

// InferSchema infers schema from XES data by sampling events.
func (d *XESDecoder) InferSchema(ctx context.Context, source core.Source, sampleSize int) (*arrow.Schema, error) {
	if sampleSize <= 0 {
		sampleSize = 1000
	}

	reader, err := source.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	bufReader := bufio.NewReaderSize(reader, 64*1024)

	// Track attribute types
	attrTypes := make(map[string]map[inferredType]int)

	// Standard XES fields
	standardFields := []string{"case_id", "activity", "timestamp", "resource", "lifecycle"}
	for _, f := range standardFields {
		attrTypes[f] = make(map[inferredType]int)
	}

	state := xesStateInit
	eventCount := 0

	for eventCount < sampleSize {
		line, err := bufReader.ReadBytes('>')
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		switch {
		case d.isOpenTag(line, xmlLog):
			state = xesStateLog
		case d.isOpenTag(line, xmlTrace):
			state = xesStateTrace
		case d.isOpenTag(line, xmlEvent):
			state = xesStateEvent
		case d.isCloseTag(line, xmlEvent):
			eventCount++
			state = xesStateTrace
		case state == xesStateEvent && d.isAttributeTag(line):
			key, value := d.extractAttribute(line)
			if key == nil {
				continue
			}

			keyStr := string(key)
			attrType := d.inferAttributeType(line, value)

			// Map standard XES attributes to our fields
			switch {
			case bytes.Equal(key, xesConceptName):
				attrTypes["activity"][typeString]++
			case bytes.Equal(key, xesTimeStamp):
				attrTypes["timestamp"][typeTimestamp]++
			case bytes.Equal(key, xesOrgResource):
				attrTypes["resource"][typeString]++
			case bytes.Equal(key, xesLifecycleTr):
				attrTypes["lifecycle"][typeString]++
			default:
				// Custom attribute
				if attrTypes[keyStr] == nil {
					attrTypes[keyStr] = make(map[inferredType]int)
				}
				attrTypes[keyStr][attrType]++
			}
		}
	}

	// Always have case_id as string
	attrTypes["case_id"][typeString]++

	// Sort field names for consistent ordering
	var fieldNames []string
	for name := range attrTypes {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)

	// Build schema
	arrowFields := make([]arrow.Field, len(fieldNames))
	for i, name := range fieldNames {
		counts := attrTypes[name]
		arrowFields[i] = arrow.Field{
			Name:     name,
			Type:     d.selectType(counts),
			Nullable: true,
		}
	}

	return arrow.NewSchema(arrowFields, nil), nil
}

// isOpenTag checks if line is an opening tag for the given element.
func (d *XESDecoder) isOpenTag(line, element []byte) bool {
	if len(line) < len(element)+2 {
		return false
	}
	if line[0] != '<' {
		return false
	}
	offset := 1
	if bytes.HasPrefix(line[offset:], element) {
		next := offset + len(element)
		if next >= len(line) {
			return true
		}
		c := line[next]
		return c == '>' || c == ' ' || c == '\t' || c == '/'
	}
	return false
}

// isCloseTag checks if line is a closing tag for the given element.
func (d *XESDecoder) isCloseTag(line, element []byte) bool {
	if len(line) < len(element)+3 {
		return false
	}
	// Check for </element>
	if line[0] == '<' && line[1] == '/' {
		return bytes.HasPrefix(line[2:], element)
	}
	// Check for self-closing <element ... />
	if line[0] == '<' && bytes.HasPrefix(line[1:], element) {
		return line[len(line)-2] == '/' && line[len(line)-1] == '>'
	}
	return false
}

// isAttributeTag checks if line is an XES attribute element.
func (d *XESDecoder) isAttributeTag(line []byte) bool {
	if len(line) < 3 || line[0] != '<' {
		return false
	}
	return bytes.HasPrefix(line[1:], xmlString) ||
		bytes.HasPrefix(line[1:], xmlDate) ||
		bytes.HasPrefix(line[1:], xmlInt) ||
		bytes.HasPrefix(line[1:], xmlFloat) ||
		bytes.HasPrefix(line[1:], xmlBool)
}

// extractAttribute extracts key and value from an XES attribute element.
func (d *XESDecoder) extractAttribute(line []byte) (key, value []byte) {
	key = d.extractAttrValue(line, []byte("key=\""))
	value = d.extractAttrValue(line, []byte("value=\""))
	return key, value
}

// extractAttrValue extracts an XML attribute value.
func (d *XESDecoder) extractAttrValue(line, prefix []byte) []byte {
	idx := bytes.Index(line, prefix)
	if idx < 0 {
		return nil
	}
	start := idx + len(prefix)
	end := bytes.IndexByte(line[start:], '"')
	if end < 0 {
		return nil
	}
	return line[start : start+end]
}

// processEventAttribute processes an attribute element and updates the event.
func (d *XESDecoder) processEventAttribute(line []byte, event *xesEvent) {
	key, value := d.extractAttribute(line)
	if key == nil || value == nil {
		return
	}

	switch {
	case bytes.Equal(key, xesConceptName):
		event.activity = string(value)

	case bytes.Equal(key, xesTimeStamp):
		ts, err := d.parseXESTimestamp(value)
		if err == nil {
			event.timestamp = ts
		}

	case bytes.Equal(key, xesOrgResource):
		event.resource = string(value)

	case bytes.Equal(key, xesLifecycleTr):
		event.lifecycle = string(value)

	default:
		// Store as generic attribute with proper type
		keyStr := string(key)
		valueStr := string(value)

		if bytes.HasPrefix(line[1:], xmlInt) {
			if v, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
				event.attributes[keyStr] = v
			} else {
				event.attributes[keyStr] = valueStr
			}
		} else if bytes.HasPrefix(line[1:], xmlFloat) {
			if v, err := strconv.ParseFloat(valueStr, 64); err == nil {
				event.attributes[keyStr] = v
			} else {
				event.attributes[keyStr] = valueStr
			}
		} else if bytes.HasPrefix(line[1:], xmlBool) {
			event.attributes[keyStr] = valueStr == "true"
		} else if bytes.HasPrefix(line[1:], xmlDate) {
			if ts, err := d.parseXESTimestamp(value); err == nil {
				event.attributes[keyStr] = ts
			} else {
				event.attributes[keyStr] = valueStr
			}
		} else {
			event.attributes[keyStr] = valueStr
		}
	}
}

// parseXESTimestamp parses XES timestamp format.
func (d *XESDecoder) parseXESTimestamp(ts []byte) (time.Time, error) {
	formats := []string{
		"2006-01-02T15:04:05.000Z07:00",
		"2006-01-02T15:04:05.000000Z07:00",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		time.RFC3339,
		time.RFC3339Nano,
	}

	tsStr := string(ts)
	for _, format := range formats {
		t, err := time.Parse(format, tsStr)
		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("could not parse timestamp: %s", tsStr)
}

// inferAttributeType determines the type from the XML element name.
func (d *XESDecoder) inferAttributeType(line, value []byte) inferredType {
	if bytes.HasPrefix(line[1:], xmlDate) {
		return typeTimestamp
	}
	if bytes.HasPrefix(line[1:], xmlInt) {
		return typeInt64
	}
	if bytes.HasPrefix(line[1:], xmlFloat) {
		return typeFloat64
	}
	if bytes.HasPrefix(line[1:], xmlBool) {
		return typeBool
	}
	return typeString
}

func (d *XESDecoder) selectType(counts map[inferredType]int) arrow.DataType {
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

func (d *XESDecoder) createBuilders(schema *arrow.Schema) []array.Builder {
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

func (d *XESDecoder) releaseBuilders(builders []array.Builder) {
	for _, b := range builders {
		if b != nil {
			b.Release()
		}
	}
}

func (d *XESDecoder) appendEvent(builders []array.Builder, schema *arrow.Schema, event *xesEvent) {
	for i, field := range schema.Fields() {
		var value interface{}

		switch field.Name {
		case "case_id":
			value = event.caseID
		case "activity":
			value = event.activity
		case "timestamp":
			value = event.timestamp
		case "resource":
			value = event.resource
		case "lifecycle":
			value = event.lifecycle
		default:
			value = event.attributes[field.Name]
		}

		d.appendValue(builders[i], field.Type, value)
	}
}

func (d *XESDecoder) appendValue(builder array.Builder, dtype arrow.DataType, value interface{}) {
	if value == nil {
		builder.AppendNull()
		return
	}

	switch dtype.ID() {
	case arrow.INT64:
		b := builder.(*array.Int64Builder)
		switch v := value.(type) {
		case int64:
			b.Append(v)
		case int:
			b.Append(int64(v))
		case float64:
			b.Append(int64(v))
		case string:
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				b.Append(n)
			} else {
				b.AppendNull()
			}
		default:
			b.AppendNull()
		}

	case arrow.FLOAT64:
		b := builder.(*array.Float64Builder)
		switch v := value.(type) {
		case float64:
			b.Append(v)
		case int64:
			b.Append(float64(v))
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				b.Append(f)
			} else {
				b.AppendNull()
			}
		default:
			b.AppendNull()
		}

	case arrow.BOOL:
		b := builder.(*array.BooleanBuilder)
		switch v := value.(type) {
		case bool:
			b.Append(v)
		case string:
			b.Append(v == "true" || v == "1" || v == "yes")
		default:
			b.AppendNull()
		}

	case arrow.TIMESTAMP:
		b := builder.(*array.TimestampBuilder)
		switch v := value.(type) {
		case time.Time:
			if v.IsZero() {
				b.AppendNull()
			} else {
				b.Append(arrow.Timestamp(v.UnixMicro()))
			}
		case string:
			if ts, err := d.parseXESTimestamp([]byte(v)); err == nil {
				b.Append(arrow.Timestamp(ts.UnixMicro()))
			} else {
				b.AppendNull()
			}
		default:
			b.AppendNull()
		}

	default:
		b := builder.(*array.StringBuilder)
		switch v := value.(type) {
		case string:
			if v == "" {
				b.AppendNull()
			} else {
				b.Append(v)
			}
		case time.Time:
			b.Append(v.Format(time.RFC3339))
		default:
			b.Append(fmt.Sprintf("%v", v))
		}
	}
}

func (d *XESDecoder) flushBuilders(schema *arrow.Schema, builders []array.Builder) arrow.Record {
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
	}
	return array.NewRecord(schema, arrays, int64(arrays[0].Len()))
}

// DecodeFile decodes an XES file directly from a path.
func (d *XESDecoder) DecodeFile(ctx context.Context, path string, opts core.DecodeOptions) (<-chan core.DecodedBatch, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	info, _ := f.Stat()

	source := &xesFileSource{
		path:   path,
		file:   f,
		size:   info.Size(),
		format: core.FormatXES,
	}

	return d.Decode(ctx, source, opts)
}

// xesFileSource is a simple file-based source for XES files
type xesFileSource struct {
	path   string
	file   *os.File
	size   int64
	format core.Format
}

func (s *xesFileSource) ID() string                  { return s.path }
func (s *xesFileSource) Location() string            { return s.path }
func (s *xesFileSource) Format() core.Format         { return s.format }
func (s *xesFileSource) Size() int64                 { return s.size }
func (s *xesFileSource) ModTime() time.Time          { return time.Now() }
func (s *xesFileSource) Metadata() map[string]string { return nil }
func (s *xesFileSource) Open(ctx context.Context) (io.ReadCloser, error) {
	return os.Open(s.path)
}

func init() {
	Register(NewXESDecoder())
}
