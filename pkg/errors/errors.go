// Package errors provides production-grade error handling for LogFlow.
// It implements structured errors with codes, context, and stack traces.
package errors

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
)

// Error codes for programmatic handling
type Code string

const (
	// Input errors (1xx)
	CodeFileNotFound     Code = "E101"
	CodeFilePermission   Code = "E102"
	CodeInvalidFormat    Code = "E103"
	CodeMissingColumn    Code = "E104"
	CodeInvalidTimestamp Code = "E105"
	CodeEncodingError    Code = "E106"

	// Processing errors (2xx)
	CodeParseFailed      Code = "E201"
	CodeTransformFailed  Code = "E202"
	CodeValidationFailed Code = "E203"
	CodeMemoryLimit      Code = "E204"

	// Output errors (3xx)
	CodeWriteFailed    Code = "E301"
	CodeDiskFull       Code = "E302"
	CodeCompressionErr Code = "E303"

	// System errors (4xx)
	CodeContextCanceled Code = "E401"
	CodeTimeout         Code = "E402"
	CodePanic           Code = "E403"
	CodeGoroutineLeak   Code = "E404"

	// DuckDB errors (5xx)
	CodeDuckDBInit  Code = "E501"
	CodeDuckDBQuery Code = "E502"
	CodeDuckDBWrite Code = "E503"

	// Unknown
	CodeUnknown Code = "E999"
)

// LogFlowError is the base error type for all LogFlow errors.
type LogFlowError struct {
	Code       Code
	Message    string
	Cause      error
	Context    map[string]interface{}
	StackTrace []Frame
}

// Frame represents a stack frame.
type Frame struct {
	Function string
	File     string
	Line     int
}

// Error implements the error interface.
func (e *LogFlowError) Error() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("[%s] %s", e.Code, e.Message))

	if len(e.Context) > 0 {
		sb.WriteString(" (")
		first := true
		for k, v := range e.Context {
			if !first {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%s=%v", k, v))
			first = false
		}
		sb.WriteString(")")
	}

	if e.Cause != nil {
		sb.WriteString(": ")
		sb.WriteString(e.Cause.Error())
	}

	return sb.String()
}

// Unwrap returns the underlying cause.
func (e *LogFlowError) Unwrap() error {
	return e.Cause
}

// Is checks if this error matches a target error.
func (e *LogFlowError) Is(target error) bool {
	if t, ok := target.(*LogFlowError); ok {
		return e.Code == t.Code
	}
	return false
}

// WithContext adds context to the error.
func (e *LogFlowError) WithContext(key string, value interface{}) *LogFlowError {
	if e.Context == nil {
		e.Context = make(map[string]interface{})
	}
	e.Context[key] = value
	return e
}

// New creates a new LogFlowError.
func New(code Code, message string) *LogFlowError {
	return &LogFlowError{
		Code:       code,
		Message:    message,
		StackTrace: captureStack(2),
	}
}

// Wrap wraps an existing error with additional context.
func Wrap(err error, code Code, message string) *LogFlowError {
	if err == nil {
		return nil
	}

	return &LogFlowError{
		Code:       code,
		Message:    message,
		Cause:      err,
		StackTrace: captureStack(2),
	}
}

// Wrapf wraps an error with a formatted message.
func Wrapf(err error, code Code, format string, args ...interface{}) *LogFlowError {
	return Wrap(err, code, fmt.Sprintf(format, args...))
}

// captureStack captures the current stack trace.
func captureStack(skip int) []Frame {
	var frames []Frame
	pcs := make([]uintptr, 32)
	n := runtime.Callers(skip+1, pcs)
	pcs = pcs[:n]

	cf := runtime.CallersFrames(pcs)
	for {
		frame, more := cf.Next()
		frames = append(frames, Frame{
			Function: frame.Function,
			File:     frame.File,
			Line:     frame.Line,
		})
		if !more || len(frames) >= 10 {
			break
		}
	}
	return frames
}

// FormatStack returns a formatted stack trace.
func (e *LogFlowError) FormatStack() string {
	var sb strings.Builder
	for _, f := range e.StackTrace {
		sb.WriteString(fmt.Sprintf("  at %s\n    %s:%d\n", f.Function, f.File, f.Line))
	}
	return sb.String()
}

// --- Convenience constructors ---

// FileNotFound creates a file not found error.
func FileNotFound(path string) *LogFlowError {
	return New(CodeFileNotFound, "file not found").WithContext("path", path)
}

// MissingColumn creates a missing column error.
func MissingColumn(column string, available []string) *LogFlowError {
	return New(CodeMissingColumn, "required column not found").
		WithContext("column", column).
		WithContext("available", available)
}

// InvalidTimestamp creates a timestamp parsing error.
func InvalidTimestamp(value string, row int) *LogFlowError {
	return New(CodeInvalidTimestamp, "failed to parse timestamp").
		WithContext("value", value).
		WithContext("row", row)
}

// ParseError creates a parsing error with location.
func ParseError(format string, row int, err error) *LogFlowError {
	return Wrap(err, CodeParseFailed, "parse error").
		WithContext("format", format).
		WithContext("row", row)
}

// ContextCanceled creates a cancellation error.
func ContextCanceled(operation string) *LogFlowError {
	return New(CodeContextCanceled, "operation canceled").
		WithContext("operation", operation)
}

// --- Error checking utilities ---

// IsCode checks if an error has a specific code.
func IsCode(err error, code Code) bool {
	var lfErr *LogFlowError
	if errors.As(err, &lfErr) {
		return lfErr.Code == code
	}
	return false
}

// GetCode extracts the error code from an error.
func GetCode(err error) Code {
	var lfErr *LogFlowError
	if errors.As(err, &lfErr) {
		return lfErr.Code
	}
	return CodeUnknown
}

// IsRetryable returns true if the error is retryable.
func IsRetryable(err error) bool {
	code := GetCode(err)
	switch code {
	case CodeTimeout, CodeDiskFull:
		return true
	default:
		return false
	}
}

// IsFatal returns true if the error is unrecoverable.
func IsFatal(err error) bool {
	code := GetCode(err)
	switch code {
	case CodePanic, CodeGoroutineLeak, CodeMemoryLimit:
		return true
	default:
		return false
	}
}

// MultiError collects multiple errors.
type MultiError struct {
	Errors []error
}

// Error implements the error interface.
func (m *MultiError) Error() string {
	if len(m.Errors) == 0 {
		return "no errors"
	}
	if len(m.Errors) == 1 {
		return m.Errors[0].Error()
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d errors occurred:\n", len(m.Errors)))
	for i, err := range m.Errors {
		sb.WriteString(fmt.Sprintf("  %d. %s\n", i+1, err.Error()))
	}
	return sb.String()
}

// Add adds an error to the collection.
func (m *MultiError) Add(err error) {
	if err != nil {
		m.Errors = append(m.Errors, err)
	}
}

// HasErrors returns true if any errors were collected.
func (m *MultiError) HasErrors() bool {
	return len(m.Errors) > 0
}

// Combined returns nil if no errors, the single error if one, or the MultiError.
func (m *MultiError) Combined() error {
	switch len(m.Errors) {
	case 0:
		return nil
	case 1:
		return m.Errors[0]
	default:
		return m
	}
}
