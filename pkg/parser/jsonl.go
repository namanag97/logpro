package parser

import (
	"bufio"
	"bytes"
	"context"
	"io"

	"github.com/logflow/logflow/internal/model"
	"github.com/logflow/logflow/internal/pool"
)

// JSONLParser implements streaming JSONL (Newline-delimited JSON) parsing.
// Each line is a complete JSON object representing an event.
type JSONLParser struct {
	cfg        Config
	bufferPool *pool.BufferPool
	eventPool  *pool.EventPool
}

// NewJSONLParser creates a new JSONL parser.
func NewJSONLParser(cfg Config) *JSONLParser {
	return &JSONLParser{
		cfg:        cfg,
		bufferPool: pool.NewBufferPool(cfg.BufferSize),
		eventPool:  pool.NewEventPool(),
	}
}

// Parse implements the Parser interface for JSONL format.
func (p *JSONLParser) Parse(ctx context.Context, r io.Reader, out chan<- *model.Event) error {
	reader := bufio.NewReaderSize(r, p.cfg.BufferSize)

	for {
		select {
		case <-ctx.Done():
			return ErrContextCanceled
		default:
		}

		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return err
		}
		if len(line) == 0 && err == io.EOF {
			break
		}

		// Trim whitespace
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		// Skip non-JSON lines
		if line[0] != '{' {
			continue
		}

		event := p.eventPool.Get()

		// Parse JSON object using zero-allocation scanner
		if err := p.parseJSONObject(line, event); err != nil {
			p.eventPool.Put(event)
			continue
		}

		select {
		case out <- event:
		case <-ctx.Done():
			p.eventPool.Put(event)
			return ErrContextCanceled
		}

		if err == io.EOF {
			break
		}
	}

	return nil
}

// parseJSONObject parses a JSON object line into an Event using a byte-level scanner.
// This avoids using encoding/json for maximum performance.
func (p *JSONLParser) parseJSONObject(line []byte, event *model.Event) error {
	// Simple JSON scanner state
	const (
		stateKey = iota
		stateColon
		stateValue
		stateComma
	)

	state := stateKey
	keyStart, keyEnd := -1, -1
	valueStart, valueEnd := -1, -1
	inString := false
	escaped := false
	depth := 0

	for i := 0; i < len(line); i++ {
		c := line[i]

		// Handle escape sequences
		if escaped {
			escaped = false
			continue
		}
		if c == '\\' {
			escaped = true
			continue
		}

		// Track string boundaries
		if c == '"' {
			if !inString {
				inString = true
				if state == stateKey && keyStart < 0 {
					keyStart = i + 1
				} else if state == stateValue && valueStart < 0 {
					valueStart = i + 1
				}
			} else {
				inString = false
				if state == stateKey && keyEnd < 0 {
					keyEnd = i
				} else if state == stateValue && valueEnd < 0 {
					valueEnd = i
				}
			}
			continue
		}

		if inString {
			continue
		}

		// Track object/array depth
		if c == '{' || c == '[' {
			depth++
			if state == stateValue && valueStart < 0 {
				valueStart = i
			}
			continue
		}
		if c == '}' || c == ']' {
			depth--
			if depth == 0 && state == stateValue && valueEnd < 0 {
				valueEnd = i + 1
			}
			continue
		}

		if depth > 1 {
			continue
		}

		// State transitions
		switch c {
		case ':':
			if state == stateKey || state == stateColon {
				state = stateValue
			}

		case ',':
			if state == stateValue {
				// End of value, process the key-value pair
				if valueEnd < 0 {
					// Find end of non-string value
					valueEnd = i
				}
				p.processKeyValue(line, keyStart, keyEnd, valueStart, valueEnd, event)

				// Reset for next pair
				keyStart, keyEnd = -1, -1
				valueStart, valueEnd = -1, -1
				state = stateKey
			}
		}
	}

	// Process final key-value pair
	if keyStart >= 0 && keyEnd > keyStart && valueStart >= 0 {
		if valueEnd < 0 {
			// Find end of final value
			valueEnd = len(line) - 1 // Exclude closing brace
		}
		p.processKeyValue(line, keyStart, keyEnd, valueStart, valueEnd, event)
	}

	return nil
}

// processKeyValue extracts and assigns a key-value pair to the event.
func (p *JSONLParser) processKeyValue(line []byte, keyStart, keyEnd, valueStart, valueEnd int, event *model.Event) {
	if keyEnd <= keyStart || valueEnd <= valueStart {
		return
	}

	key := line[keyStart:keyEnd]
	value := extractJSONValue(line[valueStart:valueEnd])

	// Map to standard event fields
	switch {
	case p.matchesColumn(key, p.cfg.CaseIDColumn):
		event.CaseID = append(event.CaseID[:0], value...)

	case p.matchesColumn(key, p.cfg.ActivityColumn):
		event.Activity = append(event.Activity[:0], value...)

	case p.matchesColumn(key, p.cfg.TimestampColumn):
		ts, err := pool.ParseTimestampNanosFast(value)
		if err == nil {
			event.Timestamp = ts
		}

	case p.matchesColumn(key, p.cfg.ResourceColumn):
		event.Resource = append(event.Resource[:0], value...)

	default:
		// Store as attribute
		attr := model.Attribute{
			Key:   make([]byte, len(key)),
			Value: make([]byte, len(value)),
			Type:  model.AttrTypeString,
		}
		copy(attr.Key, key)
		copy(attr.Value, value)
		event.Attributes = append(event.Attributes, attr)
	}
}

// matchesColumn checks if a key matches the configured column name.
func (p *JSONLParser) matchesColumn(key []byte, columnName string) bool {
	return bytes.Equal(key, []byte(columnName))
}

// extractJSONValue extracts the actual value from a JSON value token.
// Handles strings (removes quotes), numbers, booleans, null.
func extractJSONValue(v []byte) []byte {
	v = bytes.TrimSpace(v)
	if len(v) == 0 {
		return v
	}

	// String value - remove quotes
	if v[0] == '"' && len(v) > 1 && v[len(v)-1] == '"' {
		return v[1 : len(v)-1]
	}

	// Handle null
	if bytes.Equal(v, []byte("null")) {
		return nil
	}

	// Return as-is (number, boolean)
	return v
}
