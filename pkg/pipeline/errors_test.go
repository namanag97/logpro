package pipeline

import (
	"testing"
	"time"
)

func TestErrorPolicy_String(t *testing.T) {
	tests := []struct {
		policy   ErrorPolicy
		expected string
	}{
		{ErrorPolicyStrict, "strict"},
		{ErrorPolicySkip, "skip"},
		{ErrorPolicyQuarantine, "quarantine"},
		{ErrorPolicy(99), "unknown"},
	}

	for _, tt := range tests {
		got := tt.policy.String()
		if got != tt.expected {
			t.Errorf("ErrorPolicy(%d).String() = %q, want %q", tt.policy, got, tt.expected)
		}
	}
}

func TestParseErrorPolicy(t *testing.T) {
	tests := []struct {
		input    string
		expected ErrorPolicy
	}{
		{"strict", ErrorPolicyStrict},
		{"skip", ErrorPolicySkip},
		{"quarantine", ErrorPolicyQuarantine},
		{"unknown", ErrorPolicyStrict}, // default to strict
		{"", ErrorPolicyStrict},
	}

	for _, tt := range tests {
		got := ParseErrorPolicy(tt.input)
		if got != tt.expected {
			t.Errorf("ParseErrorPolicy(%q) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}

func TestErrorHandler_Strict(t *testing.T) {
	handler := NewErrorHandler(ErrorPolicyStrict)

	errRec := ErrorRecord{
		RowNumber:  1,
		ByteOffset: 100,
		Message:    "test error",
		ErrorType:  ErrorTypeMalformedRow,
		Timestamp:  time.Now(),
	}

	cont, err := handler.HandleError(errRec)

	if cont {
		t.Error("Expected continueProcessing=false for strict policy")
	}
	if err == nil {
		t.Error("Expected error for strict policy")
	}

	stats := handler.Stats()
	if stats.ErrorCount != 1 {
		t.Errorf("Expected ErrorCount=1, got %d", stats.ErrorCount)
	}
}

func TestErrorHandler_Skip(t *testing.T) {
	handler := NewErrorHandler(ErrorPolicySkip)

	var skippedRow int64
	var skippedReason string
	handler.WithOnSkip(func(row int64, reason string) {
		skippedRow = row
		skippedReason = reason
	})

	errRec := ErrorRecord{
		RowNumber:  42,
		ByteOffset: 1000,
		Message:    "column mismatch",
		ErrorType:  ErrorTypeMalformedRow,
		Timestamp:  time.Now(),
	}

	cont, err := handler.HandleError(errRec)

	if !cont {
		t.Error("Expected continueProcessing=true for skip policy")
	}
	if err != nil {
		t.Errorf("Expected no error for skip policy, got: %v", err)
	}
	if skippedRow != 42 {
		t.Errorf("Expected skippedRow=42, got %d", skippedRow)
	}
	if skippedReason != "column mismatch" {
		t.Errorf("Expected skippedReason=%q, got %q", "column mismatch", skippedReason)
	}

	stats := handler.Stats()
	if stats.SkippedCount != 1 {
		t.Errorf("Expected SkippedCount=1, got %d", stats.SkippedCount)
	}
}

func TestErrorHandler_Quarantine(t *testing.T) {
	handler := NewErrorHandler(ErrorPolicyQuarantine)

	var quarantinedRecord ErrorRecord
	handler.WithQuarantineWriter(func(rec ErrorRecord) error {
		quarantinedRecord = rec
		return nil
	})

	errRec := ErrorRecord{
		RowNumber:  99,
		ByteOffset: 5000,
		RawData:    []byte("bad,data,here"),
		Message:    "parse error",
		ErrorType:  ErrorTypeInvalidType,
		Timestamp:  time.Now(),
	}

	cont, err := handler.HandleError(errRec)

	if !cont {
		t.Error("Expected continueProcessing=true for quarantine policy")
	}
	if err != nil {
		t.Errorf("Expected no error for quarantine policy, got: %v", err)
	}
	if quarantinedRecord.RowNumber != 99 {
		t.Errorf("Expected quarantined record row=99, got %d", quarantinedRecord.RowNumber)
	}
	if string(quarantinedRecord.RawData) != "bad,data,here" {
		t.Errorf("Expected quarantined raw data preserved")
	}
}

func TestErrorHandler_MaxErrors(t *testing.T) {
	handler := NewErrorHandler(ErrorPolicySkip).WithMaxErrors(3)

	for i := 0; i < 3; i++ {
		cont, err := handler.HandleError(ErrorRecord{
			RowNumber: int64(i + 1),
			Message:   "error",
			Timestamp: time.Now(),
		})

		if i < 2 {
			if !cont {
				t.Errorf("Error %d: expected continue=true", i)
			}
			if err != nil {
				t.Errorf("Error %d: expected no error", i)
			}
		} else {
			// Third error should trigger max limit
			if cont {
				t.Error("Error 3: expected continue=false after max errors")
			}
			if err == nil {
				t.Error("Error 3: expected error after max errors")
			}
		}
	}
}

func TestErrorHandler_Callback(t *testing.T) {
	handler := NewErrorHandler(ErrorPolicySkip)

	var callbackCalled bool
	var callbackRecord ErrorRecord
	handler.WithOnError(func(rec ErrorRecord) {
		callbackCalled = true
		callbackRecord = rec
	})

	handler.HandleError(ErrorRecord{
		RowNumber: 7,
		Message:   "test",
		Timestamp: time.Now(),
	})

	if !callbackCalled {
		t.Error("Expected OnError callback to be called")
	}
	if callbackRecord.RowNumber != 7 {
		t.Errorf("Expected callback record row=7, got %d", callbackRecord.RowNumber)
	}
}

func TestErrorHandler_ErrorCollection(t *testing.T) {
	handler := NewErrorHandler(ErrorPolicySkip)

	for i := 0; i < 5; i++ {
		handler.HandleError(ErrorRecord{
			RowNumber: int64(i + 1),
			Message:   "error",
			Timestamp: time.Now(),
		})
	}

	errors := handler.Errors()
	if len(errors) != 5 {
		t.Errorf("Expected 5 collected errors, got %d", len(errors))
	}

	// Reset and verify
	handler.Reset()
	errors = handler.Errors()
	if len(errors) != 0 {
		t.Errorf("Expected 0 errors after reset, got %d", len(errors))
	}
}

func TestProcessingResult_Throughput(t *testing.T) {
	result := ProcessingResult{
		RowsProcessed: 1000,
		Duration:      time.Second,
	}

	throughput := result.Throughput()
	if throughput != 1000.0 {
		t.Errorf("Expected throughput=1000, got %f", throughput)
	}
}

func TestProcessingResult_SuccessRate(t *testing.T) {
	result := ProcessingResult{
		RowsProcessed: 100,
		RowsSucceeded: 95,
		RowsFailed:    5,
	}

	rate := result.SuccessRate()
	if rate != 95.0 {
		t.Errorf("Expected success rate=95, got %f", rate)
	}
}
