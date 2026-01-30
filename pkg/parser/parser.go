// Package parser provides interfaces and implementations for parsing
// process mining data formats (XES, CSV, JSON).
package parser

import (
	"context"
	"io"

	"github.com/logflow/logflow/internal/model"
)

// Parser defines the interface for parsing process mining data.
// Implementations must be safe for concurrent use and must not
// retain references to the output channel after returning.
type Parser interface {
	// Parse reads from r and sends parsed events to out.
	// It should respect context cancellation.
	// The caller is responsible for closing the out channel.
	Parse(ctx context.Context, r io.Reader, out chan<- *model.Event) error
}

// Format represents a supported input format.
type Format uint8

const (
	FormatUnknown Format = iota
	FormatCSV
	FormatXES
	FormatJSON
	FormatJSONL
	FormatAccessLog
	FormatXLSX
	FormatParquet
)

// String returns the format name.
func (f Format) String() string {
	switch f {
	case FormatCSV:
		return "csv"
	case FormatXES:
		return "xes"
	case FormatJSON:
		return "json"
	case FormatJSONL:
		return "jsonl"
	case FormatAccessLog:
		return "accesslog"
	case FormatXLSX:
		return "xlsx"
	case FormatParquet:
		return "parquet"
	default:
		return "unknown"
	}
}

// ParseFormat parses a format string.
func ParseFormat(s string) Format {
	switch s {
	case "csv", "CSV":
		return FormatCSV
	case "xes", "XES":
		return FormatXES
	case "json", "JSON":
		return FormatJSON
	case "jsonl", "JSONL", "ndjson", "NDJSON":
		return FormatJSONL
	case "accesslog", "access_log", "log":
		return FormatAccessLog
	case "xlsx", "XLSX", "excel", "Excel":
		return FormatXLSX
	case "parquet", "Parquet", "pq":
		return FormatParquet
	default:
		return FormatUnknown
	}
}

// Config holds common parser configuration.
type Config struct {
	// BatchSize is the number of events to buffer before sending.
	BatchSize int

	// BufferSize is the size of the read buffer in bytes.
	BufferSize int

	// ColumnMapping maps logical role names to physical column names.
	// Process-mining roles: "case_id", "activity", "timestamp", "resource".
	// When empty, all columns are preserved as generic attributes.
	ColumnMapping map[string]string

	// TimestampFormat is the expected timestamp format (Go time layout).
	TimestampFormat string

	// Delimiter is the field delimiter for CSV (default: comma).
	Delimiter byte
}

// Column returns the physical column name for a logical role, or "" if unmapped.
func (c Config) Column(role string) string {
	if c.ColumnMapping != nil {
		return c.ColumnMapping[role]
	}
	return ""
}

// HasPMColumns returns true if process-mining columns are configured.
func (c Config) HasPMColumns() bool {
	return c.Column("case_id") != "" && c.Column("activity") != "" && c.Column("timestamp") != ""
}

// DefaultConfig returns a Config with sensible defaults.
// No column mapping is set — all columns are preserved as-is.
func DefaultConfig() Config {
	return Config{
		BatchSize:       1024,
		BufferSize:      64 * 1024,
		ColumnMapping:   make(map[string]string),
		TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
		Delimiter:       ',',
	}
}

// NewParser creates a parser for the given format.
func NewParser(format Format, cfg Config) (Parser, error) {
	switch format {
	case FormatCSV:
		return NewCSVParser(cfg), nil
	case FormatXES:
		return NewXESParser(cfg), nil
	case FormatJSON, FormatJSONL:
		return NewJSONLParser(cfg), nil
	case FormatAccessLog:
		return NewAccessLogParser(cfg), nil
	case FormatXLSX:
		return NewXLSXParser(cfg), nil
	default:
		return nil, ErrUnsupportedFormat
	}
}
