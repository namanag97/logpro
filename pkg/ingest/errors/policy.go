// Package errors provides error handling for the ingestion pipeline.
package errors

import (
	"fmt"
	"sync"
)

// Policy defines error handling behavior.
type Policy uint8

const (
	PolicyStrict    Policy = iota // Fail on first error
	PolicySkip                    // Skip bad rows
	PolicyQuarantine              // Send bad rows to quarantine
	PolicyRecover                 // Attempt to recover bad rows
)

func (p Policy) String() string {
	names := []string{"strict", "skip", "quarantine", "recover"}
	if int(p) < len(names) {
		return names[p]
	}
	return "unknown"
}

// RowError represents an error in a specific row.
type RowError struct {
	RowNumber  int64
	ByteOffset int64
	Column     string
	Value      string
	Error      error
	Recovered  bool
}

func (e *RowError) String() string {
	return fmt.Sprintf("row %d, column %s: %v", e.RowNumber, e.Column, e.Error)
}

// Handler manages errors during ingestion.
type Handler struct {
	mu sync.Mutex

	policy    Policy
	maxErrors int
	errors    []RowError
	quarantine Quarantine
	onError   func(RowError)
}

// NewHandler creates a new error handler.
func NewHandler(policy Policy, maxErrors int) *Handler {
	return &Handler{
		policy:    policy,
		maxErrors: maxErrors,
		errors:    make([]RowError, 0, 100),
	}
}

// Handle processes an error according to policy.
func (h *Handler) Handle(err RowError) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.errors = append(h.errors, err)

	if h.onError != nil {
		h.onError(err)
	}

	switch h.policy {
	case PolicyStrict:
		return fmt.Errorf("strict mode: %s", err.String())

	case PolicySkip:
		// Just record, don't fail
		if h.maxErrors > 0 && len(h.errors) >= h.maxErrors {
			return fmt.Errorf("max errors (%d) exceeded", h.maxErrors)
		}
		return nil

	case PolicyQuarantine:
		if h.quarantine != nil {
			h.quarantine.Add(err)
		}
		return nil

	case PolicyRecover:
		// Mark as potentially recovered
		err.Recovered = true
		return nil

	default:
		return nil
	}
}

// Errors returns all collected errors.
func (h *Handler) Errors() []RowError {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]RowError{}, h.errors...)
}

// ErrorCount returns the number of errors.
func (h *Handler) ErrorCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.errors)
}

// SetQuarantine sets the quarantine destination.
func (h *Handler) SetQuarantine(q Quarantine) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.quarantine = q
}

// OnError sets a callback for errors.
func (h *Handler) OnError(fn func(RowError)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onError = fn
}

// Reset clears all errors.
func (h *Handler) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errors = h.errors[:0]
}
