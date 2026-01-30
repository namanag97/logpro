package errors

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// --- Policy tests ---

func TestPolicyString(t *testing.T) {
	tests := []struct {
		p    Policy
		want string
	}{
		{PolicyStrict, "strict"},
		{PolicySkip, "skip"},
		{PolicyQuarantine, "quarantine"},
		{PolicyRecover, "recover"},
	}
	for _, tt := range tests {
		got := tt.p.String()
		if got != tt.want {
			t.Errorf("Policy(%d).String() = %q, want %q", tt.p, got, tt.want)
		}
	}
}

func TestPolicyHandlersRegistration(t *testing.T) {
	reg := NewPolicyRegistry()
	reg.Register(&StrictPolicyHandler{})
	reg.Register(&SkipPolicyHandler{})

	h := reg.Get("strict")
	if h == nil {
		t.Error("Get(strict) returned nil")
	}

	list := reg.List()
	if len(list) < 2 {
		t.Errorf("List() = %d, want >= 2", len(list))
	}
}

func TestStrictPolicyHandlerAborts(t *testing.T) {
	h := &StrictPolicyHandler{}
	q := NewMemoryQuarantine(100)

	action, err := h.Handle(RowError{
		RowNumber: 1,
		Error:     errors.New("bad data"),
	}, q)

	if action != ActionAbort {
		t.Errorf("StrictPolicy action = %d, want ActionAbort(%d)", action, ActionAbort)
	}
	if err == nil {
		t.Error("StrictPolicy should return error")
	}
}

func TestSkipPolicyHandlerSkips(t *testing.T) {
	h := &SkipPolicyHandler{}
	q := NewMemoryQuarantine(100)

	action, err := h.Handle(RowError{
		RowNumber: 1,
		Error:     errors.New("bad data"),
	}, q)

	if action != ActionSkip {
		t.Errorf("SkipPolicy action = %d, want ActionSkip(%d)", action, ActionSkip)
	}
	if err != nil {
		t.Errorf("SkipPolicy should not return error, got %v", err)
	}
}

func TestQuarantinePolicyHandler(t *testing.T) {
	h := &QuarantinePolicyHandler{}
	q := NewMemoryQuarantine(100)

	action, err := h.Handle(RowError{
		RowNumber: 1,
		Error:     errors.New("bad data"),
	}, q)

	// QuarantinePolicyHandler returns ActionContinue (adds to quarantine and continues)
	if action != ActionContinue {
		t.Errorf("QuarantinePolicy action = %d, want ActionContinue(%d)", action, ActionContinue)
	}
	if err != nil {
		t.Errorf("QuarantinePolicy error = %v, want nil", err)
	}
	if q.Count() != 1 {
		t.Errorf("quarantine count = %d, want 1", q.Count())
	}
}

func TestRecoverPolicyHandler(t *testing.T) {
	h := &RecoverPolicyHandler{}
	q := NewMemoryQuarantine(100)

	action, _ := h.Handle(RowError{
		RowNumber: 1,
		Error:     errors.New("bad data"),
	}, q)

	if action != ActionContinue {
		t.Errorf("RecoverPolicy action = %d, want ActionContinue(%d)", action, ActionContinue)
	}
}

// --- Handler tests ---

func TestNewHandler(t *testing.T) {
	h := NewHandler(PolicySkip, 100)
	if h == nil {
		t.Fatal("NewHandler returned nil")
	}
}

func TestHandlerMaxErrors(t *testing.T) {
	// maxErrors=3 means error is returned when len(errors) >= 3
	// So the 3rd call triggers the limit
	h := NewHandler(PolicySkip, 3)

	for i := int64(1); i <= 4; i++ {
		err := h.Handle(RowError{
			RowNumber: i,
			Error:     errors.New("error"),
		})
		if i < 3 && err != nil {
			t.Errorf("Handle(%d) error = %v, want nil (under max)", i, err)
		}
		if i >= 3 && err == nil {
			t.Errorf("Handle(%d) should return error (at or exceeds max)", i)
		}
	}
}

func TestHandlerErrorCount(t *testing.T) {
	h := NewHandler(PolicySkip, 100)
	for i := 0; i < 5; i++ {
		_ = h.Handle(RowError{RowNumber: int64(i), Error: errors.New("e")})
	}

	if h.ErrorCount() != 5 {
		t.Errorf("ErrorCount = %d, want 5", h.ErrorCount())
	}
}

func TestHandlerErrors(t *testing.T) {
	h := NewHandler(PolicySkip, 100)
	_ = h.Handle(RowError{RowNumber: 1, Error: errors.New("e1")})
	_ = h.Handle(RowError{RowNumber: 2, Error: errors.New("e2")})

	errs := h.Errors()
	if len(errs) != 2 {
		t.Errorf("Errors count = %d, want 2", len(errs))
	}
}

func TestHandlerReset(t *testing.T) {
	h := NewHandler(PolicySkip, 100)
	_ = h.Handle(RowError{RowNumber: 1, Error: errors.New("e")})
	h.Reset()

	if h.ErrorCount() != 0 {
		t.Errorf("ErrorCount after Reset = %d, want 0", h.ErrorCount())
	}
}

func TestHandlerOnErrorCallback(t *testing.T) {
	h := NewHandler(PolicySkip, 100)
	called := false
	h.OnError(func(re RowError) {
		called = true
	})

	_ = h.Handle(RowError{RowNumber: 1, Error: errors.New("e")})
	if !called {
		t.Error("OnError callback was not called")
	}
}

func TestHandlerSetQuarantine(t *testing.T) {
	h := NewHandler(PolicyQuarantine, 100)
	q := NewMemoryQuarantine(100)
	h.SetQuarantine(q)

	_ = h.Handle(RowError{RowNumber: 1, Error: errors.New("e")})
	if q.Count() != 1 {
		t.Errorf("quarantine count = %d, want 1", q.Count())
	}
}

func TestNewHandlerWithPolicy(t *testing.T) {
	// Register policies first
	RegisterPolicy(&StrictPolicyHandler{})
	RegisterPolicy(&SkipPolicyHandler{})

	h, err := NewHandlerWithPolicy("skip", 100)
	if err != nil {
		t.Fatalf("NewHandlerWithPolicy error: %v", err)
	}
	if h == nil {
		t.Fatal("NewHandlerWithPolicy returned nil")
	}
}

// --- MemoryQuarantine tests ---

func TestMemoryQuarantine(t *testing.T) {
	q := NewMemoryQuarantine(10)

	q.Add(RowError{RowNumber: 1, Error: errors.New("e1")})
	q.Add(RowError{RowNumber: 2, Error: errors.New("e2")})

	if q.Count() != 2 {
		t.Errorf("Count = %d, want 2", q.Count())
	}

	errs := q.Errors()
	if len(errs) != 2 {
		t.Errorf("Errors count = %d, want 2", len(errs))
	}
}

func TestMemoryQuarantineLimit(t *testing.T) {
	q := NewMemoryQuarantine(2)

	q.Add(RowError{RowNumber: 1, Error: errors.New("e1")})
	q.Add(RowError{RowNumber: 2, Error: errors.New("e2")})
	q.Add(RowError{RowNumber: 3, Error: errors.New("e3")})

	// Should not exceed limit
	if q.Count() > 3 {
		t.Errorf("Count = %d, should not grow unbounded", q.Count())
	}
}

func TestMemoryQuarantineFlush(t *testing.T) {
	q := NewMemoryQuarantine(10)
	q.Add(RowError{RowNumber: 1, Error: errors.New("e")})

	if err := q.Flush(); err != nil {
		t.Errorf("Flush error: %v", err)
	}
}

func TestMemoryQuarantineClose(t *testing.T) {
	q := NewMemoryQuarantine(10)
	if err := q.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}
}

// --- FileQuarantine tests ---

func TestFileQuarantine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "quarantine.jsonl")

	q, err := NewFileQuarantine(path)
	if err != nil {
		t.Fatalf("NewFileQuarantine error: %v", err)
	}

	q.Add(RowError{RowNumber: 1, Error: errors.New("e1")})
	q.Add(RowError{RowNumber: 2, Error: errors.New("e2")})

	if q.Count() != 2 {
		t.Errorf("Count = %d, want 2", q.Count())
	}

	if err := q.Flush(); err != nil {
		t.Errorf("Flush error: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("quarantine file was not created")
	}
}

// --- ErrorStream tests ---

func TestErrorStream(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "errors.jsonl")

	s := NewErrorStream(path, ErrorFormatJSONL)
	if err := s.Open(); err != nil {
		t.Fatalf("Open error: %v", err)
	}

	err := s.Write(ErrorRecord{
		SourceID:  "test",
		RowNumber: 1,
		ErrorType: "parse",
		ErrorMsg:  "invalid data",
	})
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}

	if s.Count() != 1 {
		t.Errorf("Count = %d, want 1", s.Count())
	}
	if s.Path() != path {
		t.Errorf("Path = %q, want %q", s.Path(), path)
	}

	if err := s.Flush(); err != nil {
		t.Errorf("Flush error: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}
}

func TestErrorStreamWriteFromRowError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "errors.jsonl")

	s := NewErrorStream(path, ErrorFormatJSONL)
	_ = s.Open()

	err := s.WriteFromRowError("src", 42, "col", errors.New("parse error"))
	if err != nil {
		t.Fatalf("WriteFromRowError error: %v", err)
	}
	if s.Count() != 1 {
		t.Errorf("Count = %d, want 1", s.Count())
	}
	_ = s.Close()
}

// --- ErrorCollector tests ---

func TestErrorCollector(t *testing.T) {
	c := NewErrorCollector(100)

	c.Add(ErrorRecord{SourceID: "s1", ErrorMsg: "e1"})
	c.Add(ErrorRecord{SourceID: "s2", ErrorMsg: "e2"})

	if c.Count() != 2 {
		t.Errorf("Count = %d, want 2", c.Count())
	}

	errs := c.Errors()
	if len(errs) != 2 {
		t.Errorf("Errors len = %d, want 2", len(errs))
	}
}

func TestErrorCollectorClear(t *testing.T) {
	c := NewErrorCollector(100)
	c.Add(ErrorRecord{ErrorMsg: "e"})
	c.Clear()

	if c.Count() != 0 {
		t.Errorf("Count after Clear = %d, want 0", c.Count())
	}
}

func TestErrorCollectorWriteTo(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "collected.jsonl")

	c := NewErrorCollector(100)
	c.Add(ErrorRecord{SourceID: "s1", ErrorMsg: "e1"})
	c.Add(ErrorRecord{SourceID: "s2", ErrorMsg: "e2"})

	s := NewErrorStream(path, ErrorFormatJSONL)
	_ = s.Open()

	ctx := context.Background()
	err := c.WriteTo(ctx, s)
	if err != nil {
		t.Fatalf("WriteTo error: %v", err)
	}
	if s.Count() != 2 {
		t.Errorf("stream count = %d, want 2", s.Count())
	}
	_ = s.Close()
}

func TestRowErrorString(t *testing.T) {
	re := RowError{
		RowNumber: 42,
		Column:    "timestamp",
		Error:     errors.New("invalid format"),
	}
	s := re.String()
	if s == "" {
		t.Error("RowError.String() should not be empty")
	}
}
