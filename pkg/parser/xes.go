package parser

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"time"

	"github.com/logflow/logflow/internal/model"
	"github.com/logflow/logflow/internal/pool"
)

// XES attribute key constants (as byte slices for zero-alloc comparison)
var (
	xesConceptName  = []byte("concept:name")
	xesTimeStamp    = []byte("time:timestamp")
	xesOrgResource  = []byte("org:resource")
	xesLifecycleTr  = []byte("lifecycle:transition")
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
	stateInit xesState = iota
	stateLog
	stateTrace
	stateEvent
	stateAttribute
)

// XESParser implements streaming XES parsing using a state machine.
type XESParser struct {
	cfg        Config
	bufferPool *pool.BufferPool
	eventPool  *pool.EventPool
}

// NewXESParser creates a new XES parser.
func NewXESParser(cfg Config) *XESParser {
	return &XESParser{
		cfg:        cfg,
		bufferPool: pool.NewBufferPool(cfg.BufferSize),
		eventPool:  pool.NewEventPool(),
	}
}

// Parse implements the Parser interface using a streaming state machine.
func (p *XESParser) Parse(ctx context.Context, r io.Reader, out chan<- *model.Event) error {
	reader := bufio.NewReaderSize(r, p.cfg.BufferSize)

	state := stateInit
	var currentCaseID []byte
	var currentEvent *model.Event
	buf := p.bufferPool.Get()
	defer p.bufferPool.Put(buf)

	for {
		select {
		case <-ctx.Done():
			return ErrContextCanceled
		default:
		}

		line, err := reader.ReadBytes('>')
		if err != nil && err != io.EOF {
			return err
		}
		if len(line) == 0 && err == io.EOF {
			break
		}

		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		// Process based on current state
		switch {
		case isOpenTag(line, xmlLog):
			state = stateLog

		case isOpenTag(line, xmlTrace):
			state = stateTrace
			currentCaseID = nil

		case isCloseTag(line, xmlTrace):
			state = stateLog
			currentCaseID = nil

		case isOpenTag(line, xmlEvent):
			state = stateEvent
			currentEvent = p.eventPool.Get()
			if currentCaseID != nil {
				currentEvent.CaseID = append(currentEvent.CaseID[:0], currentCaseID...)
			}

		case isCloseTag(line, xmlEvent):
			if currentEvent != nil {
				select {
				case out <- currentEvent:
				case <-ctx.Done():
					p.eventPool.Put(currentEvent)
					return ErrContextCanceled
				}
				currentEvent = nil
			}
			state = stateTrace

		case state == stateTrace && isAttributeTag(line):
			// Trace-level attribute (for case ID)
			key, value := p.extractAttribute(line)
			if bytes.Equal(key, xesConceptName) {
				currentCaseID = make([]byte, len(value))
				copy(currentCaseID, value)
			}

		case state == stateEvent && isAttributeTag(line):
			// Event-level attribute
			if currentEvent != nil {
				p.processEventAttribute(line, currentEvent)
			}
		}

		if err == io.EOF {
			break
		}
	}

	return nil
}

// isOpenTag checks if line is an opening tag for the given element.
func isOpenTag(line, element []byte) bool {
	if len(line) < len(element)+2 {
		return false
	}
	if line[0] != '<' {
		return false
	}
	// Check for <element or <element>
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
func isCloseTag(line, element []byte) bool {
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
func isAttributeTag(line []byte) bool {
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
func (p *XESParser) extractAttribute(line []byte) (key, value []byte) {
	key = p.extractAttrValue(line, []byte("key=\""))
	value = p.extractAttrValue(line, []byte("value=\""))
	return key, value
}

// extractAttrValue extracts an XML attribute value.
func (p *XESParser) extractAttrValue(line, prefix []byte) []byte {
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
func (p *XESParser) processEventAttribute(line []byte, event *model.Event) {
	key, value := p.extractAttribute(line)
	if key == nil || value == nil {
		return
	}

	switch {
	case bytes.Equal(key, xesConceptName):
		event.Activity = append(event.Activity[:0], value...)

	case bytes.Equal(key, xesTimeStamp):
		ts, err := p.parseXESTimestamp(value)
		if err == nil {
			event.Timestamp = ts
		}

	case bytes.Equal(key, xesOrgResource):
		event.Resource = append(event.Resource[:0], value...)

	default:
		// Store as generic attribute
		attr := model.Attribute{
			Key:   make([]byte, len(key)),
			Value: make([]byte, len(value)),
			Type:  p.detectAttributeType(line),
		}
		copy(attr.Key, key)
		copy(attr.Value, value)
		event.Attributes = append(event.Attributes, attr)
	}
}

// detectAttributeType determines the attribute type from the XML element name.
func (p *XESParser) detectAttributeType(line []byte) model.AttrType {
	if bytes.HasPrefix(line[1:], xmlDate) {
		return model.AttrTypeTimestamp
	}
	if bytes.HasPrefix(line[1:], xmlInt) {
		return model.AttrTypeInt
	}
	if bytes.HasPrefix(line[1:], xmlFloat) {
		return model.AttrTypeFloat
	}
	if bytes.HasPrefix(line[1:], xmlBool) {
		return model.AttrTypeBool
	}
	return model.AttrTypeString
}

// parseXESTimestamp parses XES timestamp format to nanoseconds.
func (p *XESParser) parseXESTimestamp(ts []byte) (int64, error) {
	formats := []string{
		"2006-01-02T15:04:05.000Z07:00",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		time.RFC3339,
		time.RFC3339Nano,
	}

	tsStr := string(ts) // Unavoidable for time.Parse
	for _, format := range formats {
		t, err := time.Parse(format, tsStr)
		if err == nil {
			return t.UnixNano(), nil
		}
	}

	return 0, ErrInvalidTimestamp
}
