package errors

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// ErrorStream collects and persists error records.
type ErrorStream struct {
	mu sync.Mutex

	// Output
	path    string
	format  ErrorFormat
	writer  *csv.Writer
	file    *os.File
	encoder *json.Encoder

	// Buffering
	buffer     []ErrorRecord
	bufferSize int

	// Stats
	count int64
}

// ErrorFormat defines the output format for errors.
type ErrorFormat int

const (
	ErrorFormatCSV ErrorFormat = iota
	ErrorFormatJSON
	ErrorFormatJSONL
)

// ErrorRecord represents a quarantined record.
type ErrorRecord struct {
	Timestamp  time.Time              `json:"timestamp"`
	SourceID   string                 `json:"source_id"`
	RowNumber  int64                  `json:"row_number"`
	Column     string                 `json:"column,omitempty"`
	RawData    string                 `json:"raw_data,omitempty"`
	ErrorType  string                 `json:"error_type"`
	ErrorMsg   string                 `json:"error_message"`
	Recovered  bool                   `json:"recovered"`
	Extra      map[string]interface{} `json:"extra,omitempty"`
}

// NewErrorStream creates a new error stream.
func NewErrorStream(path string, format ErrorFormat) *ErrorStream {
	return &ErrorStream{
		path:       path,
		format:     format,
		bufferSize: 100,
	}
}

// Open initializes the error stream.
func (s *ErrorStream) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.Create(s.path)
	if err != nil {
		return fmt.Errorf("failed to create error file: %w", err)
	}
	s.file = f

	switch s.format {
	case ErrorFormatCSV:
		s.writer = csv.NewWriter(f)
		// Write header
		s.writer.Write([]string{
			"timestamp", "source_id", "row_number", "column",
			"raw_data", "error_type", "error_message", "recovered",
		})
	case ErrorFormatJSON:
		s.encoder = json.NewEncoder(f)
		s.encoder.SetIndent("", "  ")
	case ErrorFormatJSONL:
		s.encoder = json.NewEncoder(f)
	}

	return nil
}

// Write writes an error record.
func (s *ErrorStream) Write(record ErrorRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now()
	}

	s.count++

	switch s.format {
	case ErrorFormatCSV:
		recovered := "false"
		if record.Recovered {
			recovered = "true"
		}
		return s.writer.Write([]string{
			record.Timestamp.Format(time.RFC3339),
			record.SourceID,
			fmt.Sprintf("%d", record.RowNumber),
			record.Column,
			record.RawData,
			record.ErrorType,
			record.ErrorMsg,
			recovered,
		})

	case ErrorFormatJSON, ErrorFormatJSONL:
		return s.encoder.Encode(record)
	}

	return nil
}

// WriteFromRowError converts a core.RowError to ErrorRecord and writes it.
func (s *ErrorStream) WriteFromRowError(sourceID string, rowNum int64, column string, err error) error {
	return s.Write(ErrorRecord{
		SourceID:  sourceID,
		RowNumber: rowNum,
		Column:    column,
		ErrorMsg:  err.Error(),
	})
}

// Flush flushes any buffered data.
func (s *ErrorStream) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writer != nil {
		s.writer.Flush()
		return s.writer.Error()
	}
	return nil
}

// Close closes the error stream.
func (s *ErrorStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writer != nil {
		s.writer.Flush()
	}

	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// Count returns the number of errors written.
func (s *ErrorStream) Count() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// Path returns the output path.
func (s *ErrorStream) Path() string {
	return s.path
}


// ErrorCollector collects errors for later processing.
type ErrorCollector struct {
	mu     sync.Mutex
	errors []ErrorRecord
	limit  int
}

// NewErrorCollector creates an error collector.
func NewErrorCollector(limit int) *ErrorCollector {
	return &ErrorCollector{
		limit: limit,
	}
}

// Add adds an error record.
func (c *ErrorCollector) Add(record ErrorRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.limit > 0 && len(c.errors) >= c.limit {
		return // Drop if over limit
	}

	c.errors = append(c.errors, record)
}

// Errors returns all collected errors.
func (c *ErrorCollector) Errors() []ErrorRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.errors
}

// Count returns the error count.
func (c *ErrorCollector) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.errors)
}

// Clear clears collected errors.
func (c *ErrorCollector) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors = nil
}

// WriteTo writes all errors to a stream.
func (c *ErrorCollector) WriteTo(ctx context.Context, stream *ErrorStream) error {
	c.mu.Lock()
	errors := c.errors
	c.mu.Unlock()

	for _, err := range errors {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := stream.Write(err); err != nil {
			return err
		}
	}

	return stream.Flush()
}
