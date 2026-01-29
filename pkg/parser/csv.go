package parser

import (
	"bufio"
	"context"
	"io"
	"time"

	"github.com/logflow/logflow/internal/model"
	"github.com/logflow/logflow/internal/pool"
)

// CSVParser implements byte-level CSV parsing without strings.Split.
type CSVParser struct {
	cfg        Config
	bufferPool *pool.BufferPool
	eventPool  *pool.EventPool
	scanner    *CSVScanner
}

// NewCSVParser creates a new CSV parser.
func NewCSVParser(cfg Config) *CSVParser {
	return &CSVParser{
		cfg:        cfg,
		bufferPool: pool.NewBufferPool(cfg.BufferSize),
		eventPool:  pool.NewEventPool(),
		scanner:    NewCSVScanner(cfg.Delimiter),
	}
}

// Parse implements the Parser interface.
func (p *CSVParser) Parse(ctx context.Context, r io.Reader, out chan<- *model.Event) error {
	reader := bufio.NewReaderSize(r, p.cfg.BufferSize)

	// Parse header line to get column indices
	headerLine, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return err
	}
	if len(headerLine) == 0 {
		return ErrInvalidCSV
	}

	columns := p.parseHeaderLine(headerLine)
	colMap := p.buildColumnMap(columns)

	caseIdx, ok := colMap[p.cfg.CaseIDColumn]
	if !ok {
		return ErrMissingColumn
	}
	actIdx, ok := colMap[p.cfg.ActivityColumn]
	if !ok {
		return ErrMissingColumn
	}
	tsIdx, ok := colMap[p.cfg.TimestampColumn]
	if !ok {
		return ErrMissingColumn
	}
	resIdx := colMap[p.cfg.ResourceColumn] // Optional

	// Parse data lines
	lineNum := 1
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

		lineNum++

		// Trim trailing newline/carriage return
		line = trimLineEnding(line)
		if len(line) == 0 {
			continue
		}

		event := p.eventPool.Get()

		// Use FSM scanner for proper edge case handling
		fields := p.scanner.ScanLine(line)
		if len(fields) <= caseIdx || len(fields) <= actIdx || len(fields) <= tsIdx {
			p.eventPool.Put(event)
			continue
		}

		// Copy field values to event (avoids retaining references to line buffer)
		event.CaseID = append(event.CaseID[:0], fields[caseIdx]...)
		event.Activity = append(event.Activity[:0], fields[actIdx]...)

		// Parse timestamp
		ts, err := p.parseTimestamp(fields[tsIdx])
		if err != nil {
			p.eventPool.Put(event)
			continue
		}
		event.Timestamp = ts

		// Optional resource field
		if resIdx >= 0 && resIdx < len(fields) {
			event.Resource = append(event.Resource[:0], fields[resIdx]...)
		}

		// Add other columns as attributes
		for i, col := range columns {
			if i == caseIdx || i == actIdx || i == tsIdx || i == resIdx {
				continue
			}
			if i < len(fields) && len(fields[i]) > 0 {
				attr := model.Attribute{
					Key:   make([]byte, len(col)),
					Value: make([]byte, len(fields[i])),
					Type:  model.AttrTypeString,
				}
				copy(attr.Key, col)
				copy(attr.Value, fields[i])
				event.Attributes = append(event.Attributes, attr)
			}
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

// parseHeaderLine extracts column names from the header line.
func (p *CSVParser) parseHeaderLine(line []byte) [][]byte {
	line = trimLineEnding(line)
	return p.parseCSVLine(line)
}

// buildColumnMap creates a map of column name to index.
func (p *CSVParser) buildColumnMap(columns [][]byte) map[string]int {
	m := make(map[string]int, len(columns))
	for i, col := range columns {
		// Convert to string for map key (unavoidable for lookup)
		m[string(col)] = i
	}
	return m
}

// parseCSVLine parses a CSV line using byte-level scanning.
// Handles quoted fields with embedded delimiters and quotes.
func (p *CSVParser) parseCSVLine(line []byte) [][]byte {
	if len(line) == 0 {
		return nil
	}

	fields := make([][]byte, 0, 16)
	delim := p.cfg.Delimiter
	start := 0
	inQuotes := false

	for i := 0; i < len(line); i++ {
		c := line[i]

		if c == '"' {
			if !inQuotes {
				inQuotes = true
			} else {
				// Check for escaped quote
				if i+1 < len(line) && line[i+1] == '"' {
					i++ // Skip escaped quote
				} else {
					inQuotes = false
				}
			}
		} else if c == delim && !inQuotes {
			field := line[start:i]
			// Remove quotes if present
			field = unquoteField(field)
			fields = append(fields, field)
			start = i + 1
		}
	}

	// Last field
	field := line[start:]
	field = unquoteField(field)
	fields = append(fields, field)

	return fields
}

// unquoteField removes surrounding quotes and unescapes embedded quotes.
func unquoteField(field []byte) []byte {
	if len(field) < 2 {
		return field
	}
	if field[0] == '"' && field[len(field)-1] == '"' {
		field = field[1 : len(field)-1]
		// Unescape embedded quotes
		result := make([]byte, 0, len(field))
		for i := 0; i < len(field); i++ {
			if field[i] == '"' && i+1 < len(field) && field[i+1] == '"' {
				result = append(result, '"')
				i++ // Skip second quote
			} else {
				result = append(result, field[i])
			}
		}
		return result
	}
	return field
}

// parseTimestamp parses a timestamp string into nanoseconds since epoch.
func (p *CSVParser) parseTimestamp(ts []byte) (int64, error) {
	// Use fast timestamp parser for common formats
	nanos, err := pool.ParseTimestampNanosFast(ts)
	if err == nil {
		return nanos, nil
	}

	// Fallback to configured format
	tsStr := string(ts)
	t, err := time.Parse(p.cfg.TimestampFormat, tsStr)
	if err == nil {
		return t.UnixNano(), nil
	}

	return 0, ErrInvalidTimestamp
}

// trimLineEnding removes trailing \n and \r characters.
func trimLineEnding(line []byte) []byte {
	for len(line) > 0 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
		line = line[:len(line)-1]
	}
	return line
}
