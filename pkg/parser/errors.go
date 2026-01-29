package parser

import "errors"

var (
	// ErrUnsupportedFormat is returned when the input format is not supported.
	ErrUnsupportedFormat = errors.New("parser: unsupported format")

	// ErrInvalidCSV is returned when CSV parsing fails.
	ErrInvalidCSV = errors.New("parser: invalid CSV format")

	// ErrInvalidXES is returned when XES parsing fails.
	ErrInvalidXES = errors.New("parser: invalid XES format")

	// ErrMissingColumn is returned when a required column is missing.
	ErrMissingColumn = errors.New("parser: required column missing")

	// ErrInvalidTimestamp is returned when timestamp parsing fails.
	ErrInvalidTimestamp = errors.New("parser: invalid timestamp format")

	// ErrContextCanceled is returned when the context is canceled.
	ErrContextCanceled = errors.New("parser: context canceled")
)
