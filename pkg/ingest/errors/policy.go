// Package errors provides error handling for the ingestion pipeline.
package errors

import (
	"fmt"
	"sync"
)

// Policy defines error handling behavior.
type Policy uint8

const (
	PolicyStrict     Policy = iota // Fail on first error
	PolicySkip                     // Skip bad rows
	PolicyQuarantine               // Send bad rows to quarantine
	PolicyRecover                  // Attempt to recover bad rows
)

func (p Policy) String() string {
	names := []string{"strict", "skip", "quarantine", "recover"}
	if int(p) < len(names) {
		return names[p]
	}
	return "unknown"
}

// Action defines what action the pipeline should take after handling an error.
type Action uint8

const (
	// ActionContinue means processing should continue normally.
	ActionContinue Action = iota
	// ActionSkip means the current row should be skipped.
	ActionSkip
	// ActionAbort means processing should stop immediately.
	ActionAbort
)

// PolicyHandler defines a pluggable error handling strategy.
type PolicyHandler interface {
	// Name returns the unique name for this policy handler.
	Name() string

	// Handle processes a RowError and returns what action the pipeline should take.
	Handle(err RowError, quarantine Quarantine) (Action, error)
}

// --- Built-in policy handlers ---

// StrictPolicyHandler aborts on first error.
type StrictPolicyHandler struct{}

func (h *StrictPolicyHandler) Name() string { return "strict" }

func (h *StrictPolicyHandler) Handle(err RowError, quarantine Quarantine) (Action, error) {
	return ActionAbort, fmt.Errorf("strict mode: %s", err.String())
}

// SkipPolicyHandler skips bad rows without failing.
type SkipPolicyHandler struct{}

func (h *SkipPolicyHandler) Name() string { return "skip" }

func (h *SkipPolicyHandler) Handle(err RowError, quarantine Quarantine) (Action, error) {
	return ActionSkip, nil
}

// QuarantinePolicyHandler sends bad rows to quarantine.
type QuarantinePolicyHandler struct{}

func (h *QuarantinePolicyHandler) Name() string { return "quarantine" }

func (h *QuarantinePolicyHandler) Handle(err RowError, quarantine Quarantine) (Action, error) {
	if quarantine != nil {
		quarantine.Add(err)
	}
	return ActionContinue, nil
}

// RecoverPolicyHandler attempts to recover bad rows.
type RecoverPolicyHandler struct{}

func (h *RecoverPolicyHandler) Name() string { return "recover" }

func (h *RecoverPolicyHandler) Handle(err RowError, quarantine Quarantine) (Action, error) {
	err.Recovered = true
	return ActionContinue, nil
}

// --- Policy Registry ---

// PolicyRegistry stores named policy handlers.
type PolicyRegistry struct {
	mu       sync.RWMutex
	handlers map[string]PolicyHandler
}

// NewPolicyRegistry creates a new empty PolicyRegistry.
func NewPolicyRegistry() *PolicyRegistry {
	return &PolicyRegistry{
		handlers: make(map[string]PolicyHandler),
	}
}

// Register adds a PolicyHandler to the registry.
func (r *PolicyRegistry) Register(handler PolicyHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[handler.Name()] = handler
}

// Get retrieves a PolicyHandler by name. Returns nil if not found.
func (r *PolicyRegistry) Get(name string) PolicyHandler {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.handlers[name]
}

// List returns the names of all registered policy handlers.
func (r *PolicyRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	return names
}

// defaultPolicyRegistry is the package-level registry pre-loaded with built-in handlers.
var defaultPolicyRegistry = func() *PolicyRegistry {
	r := NewPolicyRegistry()
	r.Register(&StrictPolicyHandler{})
	r.Register(&SkipPolicyHandler{})
	r.Register(&QuarantinePolicyHandler{})
	r.Register(&RecoverPolicyHandler{})
	return r
}()

// RegisterPolicy registers a PolicyHandler in the default registry.
func RegisterPolicy(handler PolicyHandler) {
	defaultPolicyRegistry.Register(handler)
}

// GetPolicy retrieves a PolicyHandler by name from the default registry.
func GetPolicy(name string) PolicyHandler {
	return defaultPolicyRegistry.Get(name)
}

// ListPolicies returns the names of all registered policies in the default registry.
func ListPolicies() []string {
	return defaultPolicyRegistry.List()
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

	policy        Policy
	policyHandler PolicyHandler
	maxErrors     int
	errors        []RowError
	quarantine    Quarantine
	onError       func(RowError)
}

// NewHandler creates a new error handler.
// It attempts to resolve a pluggable PolicyHandler from the default registry
// using the policy's string name. If no handler is registered, the built-in
// switch-based logic is used as a fallback.
func NewHandler(policy Policy, maxErrors int) *Handler {
	h := &Handler{
		policy:    policy,
		maxErrors: maxErrors,
		errors:    make([]RowError, 0, 100),
	}
	// Try to resolve a pluggable handler from the registry.
	if ph := defaultPolicyRegistry.Get(policy.String()); ph != nil {
		h.policyHandler = ph
	}
	return h
}

// NewHandlerWithPolicy creates a new error handler using a named policy from the registry.
// Returns an error if the policy name is not found.
func NewHandlerWithPolicy(policyName string, maxErrors int) (*Handler, error) {
	ph := defaultPolicyRegistry.Get(policyName)
	if ph == nil {
		return nil, fmt.Errorf("unknown error policy: %q", policyName)
	}
	// Map known names back to the Policy enum for backward compat.
	var policy Policy
	switch policyName {
	case "strict":
		policy = PolicyStrict
	case "skip":
		policy = PolicySkip
	case "quarantine":
		policy = PolicyQuarantine
	case "recover":
		policy = PolicyRecover
	default:
		// Custom policy -- default enum value is PolicyStrict (0) but the
		// policyHandler will be used so the enum is only a fallback label.
		policy = PolicyStrict
	}
	return &Handler{
		policy:        policy,
		policyHandler: ph,
		maxErrors:     maxErrors,
		errors:        make([]RowError, 0, 100),
	}, nil
}

// Handle processes an error according to policy.
// If a pluggable PolicyHandler is set it is used; otherwise the built-in
// switch logic provides backward-compatible behavior.
func (h *Handler) Handle(err RowError) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.errors = append(h.errors, err)

	if h.onError != nil {
		h.onError(err)
	}

	// Use pluggable handler if available.
	if h.policyHandler != nil {
		action, handleErr := h.policyHandler.Handle(err, h.quarantine)
		if handleErr != nil {
			return handleErr
		}
		switch action {
		case ActionAbort:
			// The handler already returned a nil error but requested abort.
			return fmt.Errorf("policy %q aborted", h.policyHandler.Name())
		case ActionSkip:
			if h.maxErrors > 0 && len(h.errors) >= h.maxErrors {
				return fmt.Errorf("max errors (%d) exceeded", h.maxErrors)
			}
			return nil
		default: // ActionContinue
			return nil
		}
	}

	// Fallback: built-in switch for backward compatibility.
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
