package hooks

import (
	"context"
	"errors"
	"testing"
)

func TestHookManager_PreDecodeHooks(t *testing.T) {
	mgr := NewHookManager()

	var called1, called2 bool

	mgr.RegisterPreDecode(func(ctx context.Context, info *SourceInfo) (context.Context, error) {
		called1 = true
		return ctx, nil
	})

	mgr.RegisterPreDecode(func(ctx context.Context, info *SourceInfo) (context.Context, error) {
		called2 = true
		return ctx, nil
	})

	info := &SourceInfo{Path: "/test/file.csv"}
	_, err := mgr.RunPreDecode(context.Background(), info)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !called1 || !called2 {
		t.Error("Not all hooks were called")
	}
}

func TestHookManager_PreDecodeHookError(t *testing.T) {
	mgr := NewHookManager()

	expectedErr := errors.New("auth failed")
	mgr.RegisterPreDecode(func(ctx context.Context, info *SourceInfo) (context.Context, error) {
		return ctx, expectedErr
	})

	info := &SourceInfo{Path: "/test/file.csv"}
	_, err := mgr.RunPreDecode(context.Background(), info)

	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
}

func TestHookManager_PreWriteMetadata(t *testing.T) {
	mgr := NewHookManager()

	mgr.RegisterPreWrite(MetadataHook(map[string]string{
		"pipeline": "test",
		"version":  "1.0",
	}))

	info := &WriteInfo{Path: "/output.parquet"}
	err := mgr.RunPreWrite(context.Background(), info)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if info.Metadata["pipeline"] != "test" {
		t.Error("Metadata not added")
	}
	if info.Metadata["version"] != "1.0" {
		t.Error("Metadata not added")
	}
}

func TestHookManager_PostWriteLogging(t *testing.T) {
	mgr := NewHookManager()

	var logMessage string
	mgr.RegisterPostWrite(LoggingHook(func(format string, args ...interface{}) {
		logMessage = format
	}))

	result := &WriteResult{
		Path:      "/output.parquet",
		RowCount:  1000,
		SizeBytes: 50000,
	}
	err := mgr.RunPostWrite(context.Background(), result)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if logMessage == "" {
		t.Error("Logger was not called")
	}
}

func TestHookManager_ErrorHook(t *testing.T) {
	mgr := NewHookManager()

	var capturedErr error
	var capturedPhase string

	mgr.RegisterError(func(ctx context.Context, err error, phase string) error {
		capturedErr = err
		capturedPhase = phase
		return err
	})

	originalErr := errors.New("parse error")
	err := mgr.RunError(context.Background(), originalErr, "decode")

	if err != originalErr {
		t.Errorf("Expected original error, got %v", err)
	}
	if capturedErr != originalErr {
		t.Error("Error hook did not receive error")
	}
	if capturedPhase != "decode" {
		t.Errorf("Expected phase 'decode', got %q", capturedPhase)
	}
}

func TestHookManager_Clear(t *testing.T) {
	mgr := NewHookManager()

	called := false
	mgr.RegisterPreDecode(func(ctx context.Context, info *SourceInfo) (context.Context, error) {
		called = true
		return ctx, nil
	})

	mgr.Clear()

	info := &SourceInfo{}
	mgr.RunPreDecode(context.Background(), info)

	if called {
		t.Error("Hook should not be called after Clear()")
	}
}

func TestProgressTracker(t *testing.T) {
	var reported Progress
	tracker := NewProgressTracker(100, func(p Progress) {
		reported = p
	})

	// Add 50 rows - should not trigger report
	tracker.AddRows(50)
	if reported.RowsRead != 0 {
		t.Error("Should not report before interval")
	}

	// Add 60 more rows - should trigger report
	tracker.AddRows(60)
	if reported.RowsRead != 110 {
		t.Errorf("Expected 110 rows reported, got %d", reported.RowsRead)
	}

	// Check current progress
	progress := tracker.GetProgress()
	if progress.RowsRead != 110 {
		t.Errorf("Expected 110 rows in progress, got %d", progress.RowsRead)
	}
}

func TestProgressTracker_SetCurrentFile(t *testing.T) {
	tracker := NewProgressTracker(100, nil)

	tracker.SetCurrentFile("/data/file1.csv")

	progress := tracker.GetProgress()
	if progress.CurrentFile != "/data/file1.csv" {
		t.Errorf("Expected CurrentFile to be set, got %q", progress.CurrentFile)
	}
}
