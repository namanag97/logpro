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

// xesEvent holds parsed XES event data including trace-level attributes
type xesEvent struct {
	// Standard XES fields
	caseID    string
	activity  string
	timestamp time.Time
	resource  string
	lifecycle string

	// Trace-level attributes (inherited by all events in trace)
	traceAttrs map[string]interface{}

	// Event-level attributes
	eventAttrs map[string]interface{}

	// Lineage/audit fields
	sourceFile      string
	ingestTimestamp time.Time
	eventIndex      int64
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

		d.decodeXES(ctx, reader, schema, batchSize, opts, out, source.Location())
	}()

	return out, nil
}

// decodeXES performs streaming XES parsing using a state machine
func (d *XESDecoder) decodeXES(ctx context.Context, reader io.Reader, schema *arrow.Schema, batchSize int, opts core.DecodeOptions, out chan<- core.DecodedBatch, sourceFile string) {
	bufReader := bufio.NewReaderSize(reader, 256*1024)

	builders := d.createBuilders(schema)
	defer d.releaseBuilders(builders)

	state := xesStateInit
	var currentCaseID string
	var currentTraceAttrs map[string]interface{}
	var currentEvent *xesEvent
	var rowCount int
	var batchIndex int
	var errors []core.RowError
	var lineNum int64
	var eventIndex int64
	ingestTime := time.Now()

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
			currentTraceAttrs = make(map[string]interface{})

		case d.isCloseTag(line, xmlTrace):
			state = xesStateLog
			currentCaseID = ""
			currentTraceAttrs = nil

		case d.isOpenTag(line, xmlEvent):
			state = xesStateEvent
			currentEvent = &xesEvent{
				caseID:          currentCaseID,
				traceAttrs:      currentTraceAttrs,
				eventAttrs:      make(map[string]interface{}),
				sourceFile:      sourceFile,
				ingestTimestamp: ingestTime,
				eventIndex:      eventIndex,
			}
			eventIndex++

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
			// Trace-level attribute (for case ID and trace attributes)
			key, value := d.extractAttribute(line)
			if key == nil {
				continue
			}
			keyStr := string(key)

			if bytes.Equal(key, xesConceptName) {
				currentCaseID = string(value)
			} else {
				// Store trace attribute with proper type
				currentTraceAttrs[keyStr] = d.parseAttributeValue(line, value)
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
// Captures both trace-level and event-level attributes.
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

	// Track attribute types - separate trace and event attrs
	traceAttrTypes := make(map[string]map[inferredType]int)
	eventAttrTypes := make(map[string]map[inferredType]int)

	// Standard XES fields (always present)
	standardFields := map[string]arrow.DataType{
		"case_id":   arrow.BinaryTypes.String,
		"activity":  arrow.BinaryTypes.String,
		"timestamp": &arrow.TimestampType{Unit: arrow.Microsecond},
		"resource":  arrow.BinaryTypes.String,
		"lifecycle": arrow.BinaryTypes.String,
	}

	// Lineage fields
	lineageFields := map[string]arrow.DataType{
		"_source_file":      arrow.BinaryTypes.String,
		"_ingest_timestamp": &arrow.TimestampType{Unit: arrow.Microsecond},
		"_event_index":      arrow.PrimitiveTypes.Int64,
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

		case state == xesStateTrace && d.isAttributeTag(line):
			// Trace-level attribute
			key, value := d.extractAttribute(line)
			if key == nil || bytes.Equal(key, xesConceptName) {
				continue
			}
			keyStr := "trace_" + string(key) // Prefix trace attrs
			if traceAttrTypes[keyStr] == nil {
				traceAttrTypes[keyStr] = make(map[inferredType]int)
			}
			traceAttrTypes[keyStr][d.inferAttributeType(line, value)]++

		case state == xesStateEvent && d.isAttributeTag(line):
			// Event-level attribute
			key, value := d.extractAttribute(line)
			if key == nil {
				continue
			}
			keyStr := string(key)

			// Skip standard fields (already handled)
			if bytes.Equal(key, xesConceptName) || bytes.Equal(key, xesTimeStamp) ||
				bytes.Equal(key, xesOrgResource) || bytes.Equal(key, xesLifecycleTr) {
				continue
			}

			if eventAttrTypes[keyStr] == nil {
				eventAttrTypes[keyStr] = make(map[inferredType]int)
			}
			eventAttrTypes[keyStr][d.inferAttributeType(line, value)]++
		}
	}

	// Build schema: standard fields + trace attrs + event attrs + lineage
	var arrowFields []arrow.Field

	// 1. Standard fields (in order)
	for _, name := range []string{"case_id", "activity", "timestamp", "resource", "lifecycle"} {
		arrowFields = append(arrowFields, arrow.Field{
			Name:     name,
			Type:     standardFields[name],
			Nullable: true,
		})
	}

	// 2. Trace-level attributes (sorted for consistency)
	var traceNames []string
	for name := range traceAttrTypes {
		traceNames = append(traceNames, name)
	}
	sort.Strings(traceNames)
	for _, name := range traceNames {
		arrowFields = append(arrowFields, arrow.Field{
			Name:     name,
			Type:     d.selectType(traceAttrTypes[name]),
			Nullable: true,
		})
	}

	// 3. Event-level attributes (sorted)
	var eventNames []string
	for name := range eventAttrTypes {
		eventNames = append(eventNames, name)
	}
	sort.Strings(eventNames)
	for _, name := range eventNames {
		arrowFields = append(arrowFields, arrow.Field{
			Name:     name,
			Type:     d.selectType(eventAttrTypes[name]),
			Nullable: true,
		})
	}

	// 4. Lineage fields
	for _, name := range []string{"_source_file", "_ingest_timestamp", "_event_index"} {
		arrowFields = append(arrowFields, arrow.Field{
			Name:     name,
			Type:     lineageFields[name],
			Nullable: false,
		})
	}

	return arrow.NewSchema(arrowFields, nil), nil
}

// parseAttributeValue parses an attribute value with proper type
func (d *XESDecoder) parseAttributeValue(line, value []byte) interface{} {
	valueStr := string(value)

	if bytes.HasPrefix(line[1:], xmlInt) {
		if v, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
			return v
		}
	} else if bytes.HasPrefix(line[1:], xmlFloat) {
		if v, err := strconv.ParseFloat(valueStr, 64); err == nil {
			return v
		}
	} else if bytes.HasPrefix(line[1:], xmlBool) {
		return valueStr == "true"
	} else if bytes.HasPrefix(line[1:], xmlDate) {
		if ts, err := d.parseXESTimestamp(value); err == nil {
			return ts
		}
	}
	return valueStr
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
		// Store as event attribute with proper type
		keyStr := string(key)
		event.eventAttrs[keyStr] = d.parseAttributeValue(line, value)
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
		"2006-01-02T15:04:05.000+01:00",
		"2006-01-02T15:04:05.000+02:00",
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
		// Standard fields
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

		// Lineage fields
		case "_source_file":
			value = event.sourceFile
		case "_ingest_timestamp":
			value = event.ingestTimestamp
		case "_event_index":
			value = event.eventIndex

		default:
			// Check trace attributes first (prefixed with "trace_")
			if len(field.Name) > 6 && field.Name[:6] == "trace_" {
				if event.traceAttrs != nil {
					value = event.traceAttrs[field.Name[6:]]
				}
			} else {
				// Event attribute
				if event.eventAttrs != nil {
					value = event.eventAttrs[field.Name]
				}
			}
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
