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

	// CaseIDColumn is the name/index of the case ID column (CSV).
	// Deprecated: Use ColumnMapping["case_id"] via Column().
	CaseIDColumn string

	// ActivityColumn is the name/index of the activity column (CSV).
	// Deprecated: Use ColumnMapping["activity"] via Column().
	ActivityColumn string

	// TimestampColumn is the name/index of the timestamp column (CSV).
	// Deprecated: Use ColumnMapping["timestamp"] via Column().
	TimestampColumn string

	// ResourceColumn is the name/index of the resource column (CSV).
	// Deprecated: Use ColumnMapping["resource"] via Column().
	ResourceColumn string

	// ColumnMapping maps logical role names to physical column names.
	// Standard roles: "case_id", "activity", "timestamp", "resource".
	ColumnMapping map[string]string

	// TimestampFormat is the expected timestamp format (Go time layout).
	TimestampFormat string

	// Delimiter is the field delimiter for CSV (default: comma).
	Delimiter byte
}

// Column returns the physical column name for a logical role.
// It checks ColumnMapping first, then falls back to the legacy fields.
func (c Config) Column(role string) string {
	if c.ColumnMapping != nil {
		if v, ok := c.ColumnMapping[role]; ok && v != "" {
			return v
		}
	}
	switch role {
	case "case_id":
		return c.CaseIDColumn
	case "activity":
		return c.ActivityColumn
	case "timestamp":
		return c.TimestampColumn
	case "resource":
		return c.ResourceColumn
	}
	return ""
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		BatchSize:       1024,
		BufferSize:      64 * 1024,
		CaseIDColumn:    "case:concept:name",
		ActivityColumn:  "concept:name",
		TimestampColumn: "time:timestamp",
		ResourceColumn:  "org:resource",
		ColumnMapping: map[string]string{
			"case_id":   "case:concept:name",
			"activity":  "concept:name",
			"timestamp": "time:timestamp",
			"resource":  "org:resource",
		},
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
