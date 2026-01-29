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

// AccessLogParser implements parsing for Apache/Nginx combined log format.
// Format: %h %l %u %t "%r" %>s %b "%{Referer}i" "%{User-Agent}i"
// Example: 127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "-" "Mozilla/5.0"
type AccessLogParser struct {
	cfg        Config
	bufferPool *pool.BufferPool
	eventPool  *pool.EventPool

	// Configuration for log parsing
	includeUserAgent bool
	includeReferer   bool
}

// LogConfig holds access log parser configuration.
type LogConfig struct {
	IncludeUserAgent bool
	IncludeReferer   bool
}

// NewAccessLogParser creates a new access log parser.
func NewAccessLogParser(cfg Config) *AccessLogParser {
	return &AccessLogParser{
		cfg:              cfg,
		bufferPool:       pool.NewBufferPool(cfg.BufferSize),
		eventPool:        pool.NewEventPool(),
		includeUserAgent: false, // Default: skip to reduce bloat
		includeReferer:   false, // Default: skip to reduce bloat
	}
}

// Parse implements the Parser interface for access log format.
func (p *AccessLogParser) Parse(ctx context.Context, r io.Reader, out chan<- *model.Event) error {
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

		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		event := p.eventPool.Get()

		if err := p.parseLine(line, event); err != nil {
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

// parseLine parses a single access log line into an Event.
func (p *AccessLogParser) parseLine(line []byte, event *model.Event) error {
	// Combined Log Format fields:
	// 1. Remote host (IP) -> Resource
	// 2. Ident (usually -)
	// 3. Remote user
	// 4. Time [timestamp]
	// 5. Request "GET /path HTTP/1.1" -> Activity (path)
	// 6. Status code
	// 7. Bytes
	// 8. Referer
	// 9. User-Agent

	pos := 0

	// 1. Parse IP address (Resource)
	ip, newPos := p.readToken(line, pos)
	if len(ip) == 0 {
		return ErrInvalidCSV
	}
	event.Resource = append(event.Resource[:0], ip...)
	pos = newPos

	// 2-3. Skip ident and remote user
	_, pos = p.readToken(line, pos) // ident
	_, pos = p.readToken(line, pos) // user

	// 4. Parse timestamp [10/Oct/2000:13:55:36 -0700]
	ts, newPos := p.readBracketedField(line, pos)
	if len(ts) > 0 {
		timestamp, err := p.parseLogTimestamp(ts)
		if err == nil {
			event.Timestamp = timestamp
		}
	}
	pos = newPos

	// 5. Parse request "GET /path HTTP/1.1"
	request, newPos := p.readQuotedField(line, pos)
	if len(request) > 0 {
		activity := p.extractPath(request)
		event.Activity = append(event.Activity[:0], activity...)
	}
	pos = newPos

	// 6-7. Parse status code and bytes
	status, pos := p.readToken(line, pos)
	bytesServed, pos := p.readToken(line, pos)

	// Store status and bytes as attributes
	if len(status) > 0 {
		event.Attributes = append(event.Attributes, model.Attribute{
			Key:   []byte("status"),
			Value: copyBytes(status),
			Type:  model.AttrTypeInt,
		})
	}
	if len(bytesServed) > 0 && !bytes.Equal(bytesServed, []byte("-")) {
		event.Attributes = append(event.Attributes, model.Attribute{
			Key:   []byte("bytes"),
			Value: copyBytes(bytesServed),
			Type:  model.AttrTypeInt,
		})
	}

	// 8. Referer (optional)
	if p.includeReferer {
		referer, newPos := p.readQuotedField(line, pos)
		pos = newPos
		if len(referer) > 0 && !bytes.Equal(referer, []byte("-")) {
			event.Attributes = append(event.Attributes, model.Attribute{
				Key:   []byte("referer"),
				Value: copyBytes(referer),
				Type:  model.AttrTypeString,
			})
		}
	} else {
		_, pos = p.readQuotedField(line, pos) // Skip
	}

	// 9. User-Agent (optional - usually skip due to size)
	if p.includeUserAgent {
		userAgent, _ := p.readQuotedField(line, pos)
		if len(userAgent) > 0 && !bytes.Equal(userAgent, []byte("-")) {
			event.Attributes = append(event.Attributes, model.Attribute{
				Key:   []byte("user_agent"),
				Value: copyBytes(userAgent),
				Type:  model.AttrTypeString,
			})
		}
	}

	// Generate Case ID from IP (session tracking)
	// In real usage, this would combine IP + date or use session cookies
	event.CaseID = append(event.CaseID[:0], ip...)

	return nil
}

// readToken reads a space-delimited token.
func (p *AccessLogParser) readToken(line []byte, pos int) ([]byte, int) {
	// Skip leading spaces
	for pos < len(line) && line[pos] == ' ' {
		pos++
	}

	start := pos
	for pos < len(line) && line[pos] != ' ' {
		pos++
	}

	return line[start:pos], pos
}

// readBracketedField reads a field enclosed in brackets [].
func (p *AccessLogParser) readBracketedField(line []byte, pos int) ([]byte, int) {
	// Skip to opening bracket
	for pos < len(line) && line[pos] != '[' {
		pos++
	}
	if pos >= len(line) {
		return nil, pos
	}
	pos++ // Skip '['

	start := pos
	for pos < len(line) && line[pos] != ']' {
		pos++
	}

	end := pos
	if pos < len(line) {
		pos++ // Skip ']'
	}

	return line[start:end], pos
}

// readQuotedField reads a field enclosed in double quotes.
func (p *AccessLogParser) readQuotedField(line []byte, pos int) ([]byte, int) {
	// Skip to opening quote
	for pos < len(line) && line[pos] != '"' {
		pos++
	}
	if pos >= len(line) {
		return nil, pos
	}
	pos++ // Skip opening '"'

	start := pos
	escaped := false
	for pos < len(line) {
		if escaped {
			escaped = false
			pos++
			continue
		}
		if line[pos] == '\\' {
			escaped = true
			pos++
			continue
		}
		if line[pos] == '"' {
			break
		}
		pos++
	}

	end := pos
	if pos < len(line) {
		pos++ // Skip closing '"'
	}

	return line[start:end], pos
}

// extractPath extracts the URL path from a request line "GET /path HTTP/1.1".
func (p *AccessLogParser) extractPath(request []byte) []byte {
	// Find first space (after method)
	start := 0
	for start < len(request) && request[start] != ' ' {
		start++
	}
	start++ // Skip space

	// Find second space (before HTTP version)
	end := start
	for end < len(request) && request[end] != ' ' {
		end++
	}

	if start < end {
		return request[start:end]
	}
	return request
}

// parseLogTimestamp parses access log timestamp format.
// Format: 10/Oct/2000:13:55:36 -0700
func (p *AccessLogParser) parseLogTimestamp(ts []byte) (int64, error) {
	// Apache/Nginx log format: DD/Mon/YYYY:HH:MM:SS +ZZZZ
	layout := "02/Jan/2006:15:04:05 -0700"
	t, err := time.Parse(layout, string(ts))
	if err != nil {
		return 0, err
	}
	return t.UnixNano(), nil
}

// copyBytes creates a copy of a byte slice.
func copyBytes(b []byte) []byte {
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}
