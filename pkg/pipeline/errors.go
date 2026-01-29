// Package pipeline provides error handling policies for data ingestion.
package pipeline

import (
	"fmt"
	"sync"
	"time"
)

// ErrorPolicy determines how parsing errors are handled.
type ErrorPolicy int

const (
	// ErrorPolicyStrict aborts on first error.
	ErrorPolicyStrict ErrorPolicy = iota
	// ErrorPolicySkip skips bad rows and continues processing.
	ErrorPolicySkip
	// ErrorPolicyQuarantine writes bad rows to a separate error file.
	ErrorPolicyQuarantine
)

func (p ErrorPolicy) String() string {
	switch p {
	case ErrorPolicyStrict:
		return "strict"
	case ErrorPolicySkip:
		return "skip"
	case ErrorPolicyQuarantine:
		return "quarantine"
	default:
		return "unknown"
	}
}

// ParseErrorPolicy parses a string into an ErrorPolicy.
func ParseErrorPolicy(s string) ErrorPolicy {
	switch s {
	case "strict":
		return ErrorPolicyStrict
	case "skip":
		return ErrorPolicySkip
	case "quarantine":
		return ErrorPolicyQuarantine
	default:
		return ErrorPolicyStrict
	}
}

// ErrorRecord represents a single parsing error with context.
type ErrorRecord struct {
	// RowNumber is the 1-based row number where the error occurred.
	RowNumber int64
	// ByteOffset is the byte position in the file where the error occurred.
	ByteOffset int64
	// RawData contains the original data that caused the error.
	RawData []byte
	// ErrorType categorizes the error.
	ErrorType ErrorType
	// Message describes the error.
	Message string
	// Column is the column name/index if the error is column-specific.
	Column string
	// Timestamp when the error occurred.
	Timestamp time.Time
	// SourceFile is the name of the file being processed.
	SourceFile string
}

// ErrorType categorizes parsing errors.
type ErrorType int

const (
	ErrorTypeUnknown ErrorType = iota
	ErrorTypeMalformedRow
	ErrorTypeMissingColumn
	ErrorTypeInvalidType
	ErrorTypeInvalidTimestamp
	ErrorTypeEncodingError
	ErrorTypeQuotingError
	ErrorTypeTruncated
)

func (t ErrorType) String() string {
	switch t {
	case ErrorTypeMalformedRow:
		return "malformed_row"
	case ErrorTypeMissingColumn:
		return "missing_column"
	case ErrorTypeInvalidType:
		return "invalid_type"
	case ErrorTypeInvalidTimestamp:
		return "invalid_timestamp"
	case ErrorTypeEncodingError:
		return "encoding_error"
	case ErrorTypeQuotingError:
		return "quoting_error"
	case ErrorTypeTruncated:
		return "truncated"
	default:
		return "unknown"
	}
}

// ErrorHandler manages error collection and callbacks.
type ErrorHandler struct {
	mu sync.Mutex

	policy       ErrorPolicy
	maxErrors    int64 // Maximum errors before aborting (0 = unlimited)
	errorCount   int64
	skippedCount int64

	// Collected errors (limited to avoid memory issues)
	errors    []ErrorRecord
	maxStored int

	// Callbacks
	onError func(ErrorRecord)
	onSkip  func(rowNum int64, reason string)

	// Quarantine writer (if policy is Quarantine)
	quarantineWriter func(ErrorRecord) error
}

// NewErrorHandler creates a new error handler with the given policy.
func NewErrorHandler(policy ErrorPolicy) *ErrorHandler {
	return &ErrorHandler{
		policy:    policy,
		maxStored: 1000, // Keep last 1000 errors
		errors:    make([]ErrorRecord, 0, 100),
	}
}

// WithMaxErrors sets the maximum number of errors before aborting.
func (h *ErrorHandler) WithMaxErrors(max int64) *ErrorHandler {
	h.maxErrors = max
	return h
}

// WithOnError sets a callback for each error.
func (h *ErrorHandler) WithOnError(fn func(ErrorRecord)) *ErrorHandler {
	h.onError = fn
	return h
}

// WithOnSkip sets a callback for skipped rows.
func (h *ErrorHandler) WithOnSkip(fn func(rowNum int64, reason string)) *ErrorHandler {
	h.onSkip = fn
	return h
}

// WithQuarantineWriter sets a writer for quarantined records.
func (h *ErrorHandler) WithQuarantineWriter(fn func(ErrorRecord) error) *ErrorHandler {
	h.quarantineWriter = fn
	return h
}

// HandleError processes an error according to the policy.
// Returns true if processing should continue, false if it should abort.
func (h *ErrorHandler) HandleError(err ErrorRecord) (continueProcessing bool, returnErr error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.errorCount++

	// Store error (with limit)
	if len(h.errors) < h.maxStored {
		h.errors = append(h.errors, err)
	}

	// Invoke callback
	if h.onError != nil {
		h.onError(err)
	}

	// Check max errors limit
	if h.maxErrors > 0 && h.errorCount >= h.maxErrors {
		return false, fmt.Errorf("maximum error count (%d) exceeded at row %d: %s",
			h.maxErrors, err.RowNumber, err.Message)
	}

	switch h.policy {
	case ErrorPolicyStrict:
		return false, fmt.Errorf("error at row %d, byte %d: %s",
			err.RowNumber, err.ByteOffset, err.Message)

	case ErrorPolicySkip:
		h.skippedCount++
		if h.onSkip != nil {
			h.onSkip(err.RowNumber, err.Message)
		}
		return true, nil

	case ErrorPolicyQuarantine:
		h.skippedCount++
		if h.quarantineWriter != nil {
			if writeErr := h.quarantineWriter(err); writeErr != nil {
				// Log quarantine write failure but continue
				return true, nil
			}
		}
		if h.onSkip != nil {
			h.onSkip(err.RowNumber, err.Message)
		}
		return true, nil

	default:
		return false, fmt.Errorf("unknown error policy")
	}
}

// Stats returns error statistics.
func (h *ErrorHandler) Stats() ErrorStats {
	h.mu.Lock()
	defer h.mu.Unlock()

	return ErrorStats{
		ErrorCount:   h.errorCount,
		SkippedCount: h.skippedCount,
		Policy:       h.policy,
	}
}

// Errors returns collected errors.
func (h *ErrorHandler) Errors() []ErrorRecord {
	h.mu.Lock()
	defer h.mu.Unlock()

	result := make([]ErrorRecord, len(h.errors))
	copy(result, h.errors)
	return result
}

// Reset clears all collected errors.
func (h *ErrorHandler) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.errorCount = 0
	h.skippedCount = 0
	h.errors = h.errors[:0]
}

// ErrorStats contains error processing statistics.
type ErrorStats struct {
	ErrorCount   int64
	SkippedCount int64
	Policy       ErrorPolicy
}

// ProcessingResult contains the outcome of a processing operation.
type ProcessingResult struct {
	RowsProcessed int64
	RowsSucceeded int64
	RowsFailed    int64
	RowsSkipped   int64
	Errors        []ErrorRecord
	Duration      time.Duration
}

// Throughput returns rows per second.
func (r ProcessingResult) Throughput() float64 {
	if r.Duration == 0 {
		return 0
	}
	return float64(r.RowsProcessed) / r.Duration.Seconds()
}

// SuccessRate returns the percentage of successful rows.
func (r ProcessingResult) SuccessRate() float64 {
	if r.RowsProcessed == 0 {
		return 100.0
	}
	return float64(r.RowsSucceeded) / float64(r.RowsProcessed) * 100
}
