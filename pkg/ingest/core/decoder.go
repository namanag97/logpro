package core

import (
	"context"

	"github.com/apache/arrow/go/v14/arrow"
)

// Decoder transforms bytes into Arrow RecordBatches.
type Decoder interface {
	// Decode reads from a source and produces Arrow batches.
	Decode(ctx context.Context, source Source, opts DecodeOptions) (<-chan DecodedBatch, error)

	// InferSchema infers the schema from the source.
	InferSchema(ctx context.Context, source Source, sampleSize int) (*arrow.Schema, error)

	// Formats returns the formats this decoder supports.
	Formats() []Format
}

// DecodeOptions configures decoding behavior.
type DecodeOptions struct {
	// Schema to use (nil = infer)
	Schema *arrow.Schema

	// BatchSize is the target number of rows per batch.
	BatchSize int

	// SampleSize for schema inference.
	SampleSize int

	// ErrorPolicy defines how to handle errors.
	ErrorPolicy ErrorPolicy

	// ColumnFilter selects specific columns (nil = all).
	ColumnFilter []string

	// MaxErrors before aborting (0 = unlimited).
	MaxErrors int

	// NullStrings to recognize as NULL.
	NullStrings []string

	// Parallel enables parallel decoding where possible.
	Parallel bool
}

// DefaultDecodeOptions returns sensible defaults.
func DefaultDecodeOptions() DecodeOptions {
	return DecodeOptions{
		BatchSize:   8192,
		SampleSize:  10000,
		ErrorPolicy: ErrorPolicySkip,
		MaxErrors:   1000,
		NullStrings: []string{"", "NULL", "null", "NA", "N/A", "None", "nil"},
		Parallel:    true,
	}
}

// DecodedBatch wraps an Arrow RecordBatch with metadata.
type DecodedBatch struct {
	// Batch is the Arrow RecordBatch.
	Batch arrow.Record

	// Index is the batch number (0-based).
	Index int

	// RowOffset is the starting row number in the source.
	RowOffset int64

	// Errors encountered during decoding.
	Errors []RowError

	// BytesRead from source for this batch.
	BytesRead int64
}

// RowError represents an error in a specific row.
type RowError struct {
	RowNumber int64
	Column    string
	Value     string
	Error     error
	Recovered bool
}

// ErrorPolicy defines error handling behavior.
type ErrorPolicy uint8

const (
	ErrorPolicyStrict    ErrorPolicy = iota // Fail on first error
	ErrorPolicySkip                         // Skip bad rows
	ErrorPolicyQuarantine                   // Send bad rows to quarantine
	ErrorPolicyRecover                      // Attempt to recover
)

func (p ErrorPolicy) String() string {
	names := []string{"strict", "skip", "quarantine", "recover"}
	if int(p) < len(names) {
		return names[p]
	}
	return "unknown"
}
