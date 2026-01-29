// Package adapters provides Source and Sink implementations for various formats.
package adapters

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/logflow/logflow/internal/model"
	"github.com/logflow/logflow/internal/pool"
	"github.com/logflow/logflow/pkg/pipeline"
)

// CSVSource reads events from CSV files.
type CSVSource struct {
	cfg          pipeline.Config
	bufferPool   *pool.BufferPool
	eventPool    *pool.EventPool
	delimiter    byte
	errorHandler *pipeline.ErrorHandler

	// Column indices (resolved from header)
	caseIdx      int
	activityIdx  int
	timestampIdx int
	resourceIdx  int

	// Tracking for error context
	currentRow  int64
	byteOffset  int64
	sourceFile  string
}

// NewCSVSource creates a new CSV source.
func NewCSVSource(cfg pipeline.Config) (*CSVSource, error) {
	delimiter := byte(',')
	if d, ok := cfg.ProcessorOptions["delimiter"].(byte); ok {
		delimiter = d
	}

	// Initialize error handler with configured policy
	errorHandler := pipeline.NewErrorHandler(cfg.ErrorPolicy).
		WithMaxErrors(cfg.MaxErrors)

	if cfg.OnError != nil {
		errorHandler.WithOnError(cfg.OnError)
	}
	if cfg.OnSkip != nil {
		errorHandler.WithOnSkip(cfg.OnSkip)
	}

	return &CSVSource{
		cfg:          cfg,
		bufferPool:   pool.NewBufferPool(cfg.BufferSize),
		eventPool:    pool.NewEventPool(),
		delimiter:    delimiter,
		errorHandler: errorHandler,
		caseIdx:      -1,
		activityIdx:  -1,
		timestampIdx: -1,
		resourceIdx:  -1,
		sourceFile:   cfg.SourcePath,
	}, nil
}

// Name returns the adapter name.
func (s *CSVSource) Name() string {
	return "csv"
}

// SupportsFormat returns true for CSV-compatible formats.
func (s *CSVSource) SupportsFormat(format string) bool {
	return format == "csv" || format == "txt" || format == "tsv"
}

// Read implements Source.Read.
func (s *CSVSource) Read(ctx context.Context, r io.Reader, out chan<- *pipeline.Event) error {
	reader := bufio.NewReaderSize(r, s.cfg.BufferSize)
	s.currentRow = 0
	s.byteOffset = 0

	// Parse header
	headerLine, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return err
	}
	if len(headerLine) == 0 {
		return nil
	}
	s.byteOffset += int64(len(headerLine))

	columns := s.parseLine(trimLineEnding(headerLine))
	s.resolveColumnIndices(columns)

	// Validate required columns
	if s.caseIdx < 0 || s.activityIdx < 0 || s.timestampIdx < 0 {
		return &ColumnError{
			Message: "missing required columns",
			Missing: s.getMissingColumns(),
		}
	}

	expectedCols := len(columns)

	// Parse data rows
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		lineStart := s.byteOffset
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return err
		}
		if len(line) == 0 && err == io.EOF {
			break
		}

		s.currentRow++
		s.byteOffset += int64(len(line))

		line = trimLineEnding(line)
		if len(line) == 0 {
			continue
		}

		fields := s.parseLine(line)

		// Check for ragged rows
		if len(fields) != expectedCols {
			errRec := pipeline.ErrorRecord{
				RowNumber:  s.currentRow,
				ByteOffset: lineStart,
				RawData:    append([]byte{}, line...),
				ErrorType:  pipeline.ErrorTypeMalformedRow,
				Message:    fmt.Sprintf("column count mismatch: expected %d, got %d", expectedCols, len(fields)),
				SourceFile: s.sourceFile,
				Timestamp:  time.Now(),
			}

			cont, handleErr := s.errorHandler.HandleError(errRec)
			if !cont {
				return handleErr
			}
			continue
		}

		event := s.eventPool.Get()

		// Extract required fields with validation
		var parseErr error

		if s.caseIdx < len(fields) {
			event.CaseID = append(event.CaseID[:0], fields[s.caseIdx]...)
		}
		if s.activityIdx < len(fields) {
			event.Activity = append(event.Activity[:0], fields[s.activityIdx]...)
		}
		if s.timestampIdx < len(fields) {
			ts, tsErr := pool.ParseTimestampNanosFast(fields[s.timestampIdx])
			if tsErr != nil {
				parseErr = tsErr
			}
			event.Timestamp = ts
		}
		if s.resourceIdx >= 0 && s.resourceIdx < len(fields) {
			event.Resource = append(event.Resource[:0], fields[s.resourceIdx]...)
		}

		// Handle timestamp parse errors
		if parseErr != nil {
			s.eventPool.Put(event)

			errRec := pipeline.ErrorRecord{
				RowNumber:  s.currentRow,
				ByteOffset: lineStart,
				RawData:    append([]byte{}, line...),
				ErrorType:  pipeline.ErrorTypeInvalidTimestamp,
				Message:    "failed to parse timestamp: " + parseErr.Error(),
				Column:     s.cfg.TimestampColumn,
				SourceFile: s.sourceFile,
				Timestamp:  time.Now(),
			}

			cont, handleErr := s.errorHandler.HandleError(errRec)
			if !cont {
				return handleErr
			}
			continue
		}

		// Add remaining columns as attributes
		for i, col := range columns {
			if i == s.caseIdx || i == s.activityIdx || i == s.timestampIdx || i == s.resourceIdx {
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
			s.eventPool.Put(event)
			return ctx.Err()
		}

		// Report progress
		if s.cfg.OnProgress != nil && s.currentRow%1000 == 0 {
			s.cfg.OnProgress(s.currentRow, s.byteOffset)
		}

		if err == io.EOF {
			break
		}
	}

	return nil
}

// ErrorStats returns error handling statistics.
func (s *CSVSource) ErrorStats() pipeline.ErrorStats {
	return s.errorHandler.Stats()
}

// Errors returns collected error records.
func (s *CSVSource) Errors() []pipeline.ErrorRecord {
	return s.errorHandler.Errors()
}

// resolveColumnIndices finds the indices of required columns.
func (s *CSVSource) resolveColumnIndices(columns [][]byte) {
	for i, col := range columns {
		colStr := string(col)
		switch colStr {
		case s.cfg.CaseIDColumn:
			s.caseIdx = i
		case s.cfg.ActivityColumn:
			s.activityIdx = i
		case s.cfg.TimestampColumn:
			s.timestampIdx = i
		case s.cfg.ResourceColumn:
			s.resourceIdx = i
		}
	}
}

// getMissingColumns returns names of missing required columns.
func (s *CSVSource) getMissingColumns() []string {
	var missing []string
	if s.caseIdx < 0 {
		missing = append(missing, s.cfg.CaseIDColumn)
	}
	if s.activityIdx < 0 {
		missing = append(missing, s.cfg.ActivityColumn)
	}
	if s.timestampIdx < 0 {
		missing = append(missing, s.cfg.TimestampColumn)
	}
	return missing
}

// parseLine parses a CSV line into fields using state machine.
func (s *CSVSource) parseLine(line []byte) [][]byte {
	if len(line) == 0 {
		return nil
	}

	fields := make([][]byte, 0, 16)
	delim := s.delimiter
	start := 0
	inQuotes := false

	for i := 0; i < len(line); i++ {
		c := line[i]

		if c == '"' {
			if !inQuotes {
				inQuotes = true
			} else if i+1 < len(line) && line[i+1] == '"' {
				i++ // Skip escaped quote
			} else {
				inQuotes = false
			}
		} else if c == delim && !inQuotes {
			fields = append(fields, unquote(line[start:i]))
			start = i + 1
		}
	}

	// Last field
	fields = append(fields, unquote(line[start:]))
	return fields
}

// unquote removes surrounding quotes from a field.
func unquote(field []byte) []byte {
	if len(field) < 2 {
		return field
	}
	if field[0] == '"' && field[len(field)-1] == '"' {
		return field[1 : len(field)-1]
	}
	return field
}

// trimLineEnding removes trailing CR/LF.
func trimLineEnding(line []byte) []byte {
	for len(line) > 0 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
		line = line[:len(line)-1]
	}
	return line
}

// ColumnError indicates missing or invalid columns.
type ColumnError struct {
	Message string
	Missing []string
}

func (e *ColumnError) Error() string {
	return e.Message
}

// --- Factory function for registry ---

// CSVSourceFactory creates a CSVSource from config.
func CSVSourceFactory(cfg pipeline.Config) (pipeline.Source, error) {
	return NewCSVSource(cfg)
}

// parseTimestamp parses common timestamp formats.
func parseTimestamp(ts []byte) int64 {
	// Try fast parsing first
	if nanos, err := pool.ParseTimestampNanosFast(ts); err == nil {
		return nanos
	}

	// Fallback to standard library
	formats := []string{
		"2006-01-02 15:04:05.000000",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
		time.RFC3339,
		time.RFC3339Nano,
	}

	str := string(ts)
	for _, fmt := range formats {
		if t, err := time.Parse(fmt, str); err == nil {
			return t.UnixNano()
		}
	}

	return 0
}
