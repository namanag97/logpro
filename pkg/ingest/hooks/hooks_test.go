package hooks

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// --- Manager basic tests ---

func TestNewManager(t *testing.T) {
	m := NewManager()
	if m == nil {
		t.Fatal("NewManager returned nil")
	}
}

// --- PreDecode hook tests ---

func TestPreDecodeHook(t *testing.T) {
	m := NewManager()
	called := false

	m.RegisterPreDecode(func(ctx context.Context, source core.Source) (core.Source, error) {
		called = true
		return source, nil
	})

	src := &core.SourceInfo{ID_: "test", Format_: core.FormatCSV}
	result, err := m.RunPreDecode(context.Background(), src)
	if err != nil {
		t.Fatalf("RunPreDecode error: %v", err)
	}
	if !called {
		t.Error("PreDecode hook was not called")
	}
	if result == nil {
		t.Error("RunPreDecode returned nil source")
	}
}

func TestPreDecodeHookChaining(t *testing.T) {
	m := NewManager()

	// First hook modifies the source
	m.RegisterPreDecode(func(ctx context.Context, source core.Source) (core.Source, error) {
		return &core.SourceInfo{
			ID_:     source.ID() + "-modified",
			Format_: source.Format(),
		}, nil
	})

	src := &core.SourceInfo{ID_: "original", Format_: core.FormatCSV}
	result, err := m.RunPreDecode(context.Background(), src)
	if err != nil {
		t.Fatalf("RunPreDecode error: %v", err)
	}
	if result.ID() != "original-modified" {
		t.Errorf("ID = %q, want %q", result.ID(), "original-modified")
	}
}

// --- Progress hook tests ---

func TestProgressHook(t *testing.T) {
	m := NewManager()
	var receivedProgress Progress

	m.RegisterProgress(func(progress Progress) {
		receivedProgress = progress
	})

	m.ReportProgress(Progress{
		SourceID:  "test.csv",
		BytesRead: 1024,
		RowsRead:  100,
		Percent:   0.5,
	})

	if receivedProgress.SourceID != "test.csv" {
		t.Errorf("SourceID = %q, want %q", receivedProgress.SourceID, "test.csv")
	}
	if receivedProgress.BytesRead != 1024 {
		t.Errorf("BytesRead = %d, want 1024", receivedProgress.BytesRead)
	}
}

// --- Error hook tests ---

func TestErrorHook(t *testing.T) {
	m := NewManager()
	var receivedCtx ErrorContext

	m.RegisterError(func(err error, ctx ErrorContext) {
		receivedCtx = ctx
	})

	m.ReportError(nil, ErrorContext{
		SourceID:  "test.csv",
		RowNumber: 42,
		Phase:     "decode",
	})

	if receivedCtx.RowNumber != 42 {
		t.Errorf("RowNumber = %d, want 42", receivedCtx.RowNumber)
	}
	if receivedCtx.Phase != "decode" {
		t.Errorf("Phase = %q, want %q", receivedCtx.Phase, "decode")
	}
}

// --- Priority ordering tests ---

func TestHookPriorityOrdering(t *testing.T) {
	m := NewManager()
	var order []int

	// Register with priorities in reverse order
	m.RegisterProgressWithPriority(func(progress Progress) {
		order = append(order, 3)
	}, 30)

	m.RegisterProgressWithPriority(func(progress Progress) {
		order = append(order, 1)
	}, 10)

	m.RegisterProgressWithPriority(func(progress Progress) {
		order = append(order, 2)
	}, 20)

	m.ReportProgress(Progress{})

	if len(order) != 3 {
		t.Fatalf("order length = %d, want 3", len(order))
	}
	for i := 0; i < len(order); i++ {
		if order[i] != i+1 {
			t.Errorf("order[%d] = %d, want %d (priority ordering broken)", i, order[i], i+1)
		}
	}
}

// --- Conditional hook tests ---

func TestConditionalPreDecodeHook(t *testing.T) {
	m := NewManager()
	var callCount int32

	// This hook should only run when condition is true
	m.RegisterPreDecodeWithPriority(
		func(ctx context.Context, source core.Source) (core.Source, error) {
			atomic.AddInt32(&callCount, 1)
			return source, nil
		},
		100,
		func(ctx context.Context) bool {
			return false // condition is false
		},
	)

	src := &core.SourceInfo{ID_: "test", Format_: core.FormatCSV}
	_, _ = m.RunPreDecode(context.Background(), src)

	if atomic.LoadInt32(&callCount) != 0 {
		t.Error("conditional hook should not have been called when condition is false")
	}
}

func TestConditionalHookTrue(t *testing.T) {
	m := NewManager()
	called := false

	m.RegisterPreDecodeWithPriority(
		func(ctx context.Context, source core.Source) (core.Source, error) {
			called = true
			return source, nil
		},
		100,
		func(ctx context.Context) bool {
			return true
		},
	)

	src := &core.SourceInfo{ID_: "test", Format_: core.FormatCSV}
	_, _ = m.RunPreDecode(context.Background(), src)

	if !called {
		t.Error("conditional hook should have been called when condition is true")
	}
}

// --- Helper function tests ---

func TestLoggingHook(t *testing.T) {
	var logged string
	hook := LoggingHook(func(format string, args ...interface{}) {
		logged = format
	})

	hook(Progress{SourceID: "test", Percent: 0.5})

	if logged == "" {
		t.Error("LoggingHook should have logged something")
	}
}

func TestMetadataHook(t *testing.T) {
	extra := map[string]string{"version": "1.0"}
	hook := MetadataHook(extra)

	metadata := map[string]string{"existing": "value"}
	err := hook(context.Background(), nil, metadata)
	if err != nil {
		t.Fatalf("MetadataHook error: %v", err)
	}
	if metadata["version"] != "1.0" {
		t.Errorf("metadata[version] = %q, want %q", metadata["version"], "1.0")
	}
	if metadata["existing"] != "value" {
		t.Error("existing metadata should be preserved")
	}
}

// --- Multiple hooks of same type ---

func TestMultipleProgressHooks(t *testing.T) {
	m := NewManager()
	var count int32

	for i := 0; i < 5; i++ {
		m.RegisterProgress(func(progress Progress) {
			atomic.AddInt32(&count, 1)
		})
	}

	m.ReportProgress(Progress{})

	if atomic.LoadInt32(&count) != 5 {
		t.Errorf("count = %d, want 5", count)
	}
}

func TestMultipleErrorHooks(t *testing.T) {
	m := NewManager()
	var count int32

	for i := 0; i < 3; i++ {
		m.RegisterError(func(err error, ctx ErrorContext) {
			atomic.AddInt32(&count, 1)
		})
	}

	m.ReportError(nil, ErrorContext{})

	if atomic.LoadInt32(&count) != 3 {
		t.Errorf("count = %d, want 3", count)
	}
}
