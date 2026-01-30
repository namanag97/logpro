package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/logflow/logflow/pkg/checkpoint"
	"github.com/logflow/logflow/pkg/resilience"
	"github.com/logflow/logflow/pkg/telemetry"
)

// --- Mock Source/Sink/Processor for testing ---

type entMockSource struct {
	events []*Event
}

func (m *entMockSource) Name() string { return "mock-source" }
func (m *entMockSource) SupportsFormat(format string) bool { return true }
func (m *entMockSource) Read(ctx context.Context, r interface{}, out chan<- *Event) error {
	defer close(out)
	for _, e := range m.events {
		select {
		case out <- e:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type entMockSink struct {
	received []*Event
}

func (m *entMockSink) Name() string { return "mock-sink" }
func (m *entMockSink) Write(ctx context.Context, in <-chan *Event) error {
	for e := range in {
		m.received = append(m.received, e)
	}
	return nil
}
func (m *entMockSink) Close() error { return nil }

type entMockProcessor struct {
	name      string
	processed int
}

func (m *entMockProcessor) Name() string { return m.name }
func (m *entMockProcessor) Process(ctx context.Context, in <-chan *Event, out chan<- *Event) error {
	defer close(out)
	for e := range in {
		m.processed++
		select {
		case out <- e:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func makeTestEvents(n int) []*Event {
	events := make([]*Event, n)
	for i := range events {
		events[i] = &Event{
			CaseID:    []byte("case1"),
			Activity:  []byte("A"),
			Timestamp: time.Now().UnixNano(),
		}
	}
	return events
}

// --- DefaultEnterpriseConfig tests ---

func TestDefaultEnterpriseConfig(t *testing.T) {
	cfg := DefaultEnterpriseConfig()

	if cfg.Pipeline.BufferSize <= 0 {
		t.Error("default buffer size should be > 0")
	}
}

// --- TracedProcessor tests ---

func TestTracedProcessorName(t *testing.T) {
	inner := &entMockProcessor{name: "inner"}
	tracer := telemetry.NewTracer("test")
	tp := NewTracedProcessor(inner, tracer)

	if tp.Name() != "inner" {
		t.Errorf("Name = %q, want %q", tp.Name(), "inner")
	}
}

func TestTracedProcessorProcess(t *testing.T) {
	inner := &entMockProcessor{name: "inner"}
	tracer := telemetry.NewTracer("test")
	tp := NewTracedProcessor(inner, tracer)

	in := make(chan *Event, 5)
	out := make(chan *Event, 5)
	events := makeTestEvents(3)
	for _, e := range events {
		in <- e
	}
	close(in)

	err := tp.Process(context.Background(), in, out)
	if err != nil {
		t.Fatalf("Process error: %v", err)
	}

	count := 0
	for range out {
		count++
	}
	if count != 3 {
		t.Errorf("output count = %d, want 3", count)
	}
	if inner.processed != 3 {
		t.Errorf("inner processed = %d, want 3", inner.processed)
	}
}

// --- ResilientProcessor tests ---

func TestResilientProcessorName(t *testing.T) {
	inner := &entMockProcessor{name: "resilient-inner"}
	cb := resilience.NewCircuitBreaker()
	pp := resilience.NewPoisonPillHandler()
	rp := NewResilientProcessor(inner, cb, pp)

	if rp.Name() != "resilient-inner" {
		t.Errorf("Name = %q, want %q", rp.Name(), "resilient-inner")
	}
}

func TestResilientProcessorProcess(t *testing.T) {
	inner := &entMockProcessor{name: "inner"}
	cb := resilience.NewCircuitBreaker().WithMaxConcurrent(1000)
	pp := resilience.NewPoisonPillHandler()
	rp := NewResilientProcessor(inner, cb, pp)

	in := make(chan *Event, 5)
	out := make(chan *Event, 5)
	events := makeTestEvents(3)
	for _, e := range events {
		in <- e
	}
	close(in)

	err := rp.Process(context.Background(), in, out)
	if err != nil {
		t.Fatalf("Process error: %v", err)
	}

	count := 0
	for range out {
		count++
	}
	if count != 3 {
		t.Errorf("output count = %d, want 3", count)
	}
}

func TestResilientProcessorCircuitBreakerState(t *testing.T) {
	cb := resilience.NewCircuitBreaker()
	state := cb.State()
	if state != resilience.CircuitClosed {
		t.Errorf("initial state = %d, want CircuitClosed", state)
	}
}

// --- CheckpointedProcessor tests ---

func TestCheckpointedProcessorName(t *testing.T) {
	inner := &entMockProcessor{name: "cp-inner"}
	dir := t.TempDir()
	mgr, _ := checkpoint.NewManager(dir)
	cp := mgr.Create("job-cp", "in.csv", "out.parquet")
	cpp := NewCheckpointedProcessor(inner, cp, 2)

	if cpp.Name() != "cp-inner" {
		t.Errorf("Name = %q, want %q", cpp.Name(), "cp-inner")
	}
}

func TestCheckpointedProcessorProcess(t *testing.T) {
	inner := &entMockProcessor{name: "inner"}
	dir := t.TempDir()
	mgr, _ := checkpoint.NewManager(dir)
	cp := mgr.Create("job-cp2", "in.csv", "out.parquet")
	cpp := NewCheckpointedProcessor(inner, cp, 2)

	in := make(chan *Event, 10)
	out := make(chan *Event, 10)
	events := makeTestEvents(5)
	for _, e := range events {
		in <- e
	}
	close(in)

	err := cpp.Process(context.Background(), in, out)
	if err != nil {
		t.Fatalf("Process error: %v", err)
	}

	count := 0
	for range out {
		count++
	}
	if count != 5 {
		t.Errorf("output count = %d, want 5", count)
	}
}

// --- DLQ tests ---

func TestDLQWriterAndReader(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultDLQConfig(dir)
	writer, err := NewDLQWriter(cfg)
	if err != nil {
		t.Fatalf("NewDLQWriter error: %v", err)
	}

	record := DLQRecord{
		RawData:      []byte("bad data"),
		RowNumber:    42,
		ErrorType:    "parse",
		ErrorMessage: "invalid format",
		SourceFile:   "test.csv",
		JobID:        "job1",
		Recoverable:  true,
		Timestamp:    time.Now(),
	}

	writer.Write(record)
	stats := writer.Stats()
	if stats.RecordCount != 1 {
		t.Errorf("DLQ RecordCount = %d, want 1", stats.RecordCount)
	}

	writer.Flush()
	writer.Close()
}

func TestDefaultDLQConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultDLQConfig(dir)

	if cfg.MaxRecords < 0 {
		t.Error("MaxRecords should be >= 0")
	}
	if cfg.MaxBytes < 0 {
		t.Error("MaxBytes should be >= 0")
	}
}

func TestDLQProcessorName(t *testing.T) {
	dir := t.TempDir()
	inner := &entMockProcessor{name: "dlq-inner"}
	dlq, err := NewDLQWriter(DefaultDLQConfig(dir))
	if err != nil {
		t.Fatalf("NewDLQWriter error: %v", err)
	}
	dp := NewDLQProcessor(inner, dlq, "job1", "test.csv")

	if dp.Name() != "dlq-inner" {
		t.Errorf("Name = %q, want %q", dp.Name(), "dlq-inner")
	}
	dlq.Close()
}

// --- Pipeline checkpoint manager tests ---

func TestCheckpointManager(t *testing.T) {
	dir := t.TempDir()
	cm, err := NewCheckpointManager(dir)
	if err != nil {
		t.Fatalf("NewCheckpointManager error: %v", err)
	}

	cp := cm.Create("job1", "input.csv")
	if cp == nil {
		t.Fatal("Create returned nil")
	}

	got := cm.Get("job1")
	if got == nil {
		t.Fatal("Get returned nil")
	}

	cm.Update("job1", func(c *Checkpoint) {
		c.BytesRead = 2048
		c.RowsRead = 100
	})

	updated := cm.Get("job1")
	if updated.BytesRead != 2048 {
		t.Errorf("BytesRead = %d, want 2048", updated.BytesRead)
	}
}

func TestCheckpointManagerComplete(t *testing.T) {
	dir := t.TempDir()
	cm, _ := NewCheckpointManager(dir)
	_ = cm.Create("job1", "input.csv")

	cm.Complete("job1")

	cp := cm.Get("job1")
	if cp.Status != CheckpointStatusCompleted {
		t.Errorf("status = %q, want %q", cp.Status, CheckpointStatusCompleted)
	}
}

func TestCheckpointManagerFail(t *testing.T) {
	dir := t.TempDir()
	cm, _ := NewCheckpointManager(dir)
	_ = cm.Create("job1", "input.csv")

	cm.Fail("job1", context.DeadlineExceeded)

	cp := cm.Get("job1")
	if cp.Status != CheckpointStatusFailed {
		t.Errorf("status = %q, want %q", cp.Status, CheckpointStatusFailed)
	}
}

func TestCheckpointManagerListIncomplete(t *testing.T) {
	dir := t.TempDir()
	cm, _ := NewCheckpointManager(dir)
	_ = cm.Create("job1", "a.csv")
	_ = cm.Create("job2", "b.csv")
	cm.Complete("job2")

	incomplete := cm.ListIncomplete()
	if len(incomplete) != 1 {
		t.Errorf("incomplete count = %d, want 1", len(incomplete))
	}
}

func TestCheckpointManagerDelete(t *testing.T) {
	dir := t.TempDir()
	cm, _ := NewCheckpointManager(dir)
	_ = cm.Create("job1", "input.csv")
	cm.Delete("job1")

	if cm.Get("job1") != nil {
		t.Error("Get after Delete should return nil")
	}
}

func TestCheckpointManagerCanResume(t *testing.T) {
	dir := t.TempDir()
	cm, _ := NewCheckpointManager(dir)
	cp := cm.Create("job1", "input.csv")
	cm.Update("job1", func(c *Checkpoint) {
		c.BytesRead = 1000
	})

	_ = cp
	_, canResume := cm.CanResume("input.csv")
	if !canResume {
		t.Error("CanResume should return true for incomplete checkpoint")
	}
}

// --- ErrorHandler tests ---

func TestErrorHandlerStrict(t *testing.T) {
	h := NewErrorHandler(ErrorPolicyStrict)
	_, err := h.HandleError(ErrorRecord{
		ErrorType: ErrorTypeMalformedRow,
		Message:   "bad row",
	})
	if err == nil {
		t.Error("strict policy should return error")
	}
}

func TestErrorHandlerSkip(t *testing.T) {
	h := NewErrorHandler(ErrorPolicySkip)
	_, err := h.HandleError(ErrorRecord{
		ErrorType: ErrorTypeMalformedRow,
		Message:   "bad row",
	})
	if err != nil {
		t.Errorf("skip policy error = %v, want nil", err)
	}

	stats := h.Stats()
	if stats.SkippedCount != 1 {
		t.Errorf("skipped = %d, want 1", stats.SkippedCount)
	}
}

func TestErrorHandlerMaxErrors(t *testing.T) {
	h := NewErrorHandler(ErrorPolicySkip).WithMaxErrors(2)

	_, _ = h.HandleError(ErrorRecord{ErrorType: ErrorTypeMalformedRow})
	_, _ = h.HandleError(ErrorRecord{ErrorType: ErrorTypeMalformedRow})
	_, err := h.HandleError(ErrorRecord{ErrorType: ErrorTypeMalformedRow})

	if err == nil {
		t.Error("should return error after exceeding max errors")
	}
}

func TestErrorHandlerReset(t *testing.T) {
	h := NewErrorHandler(ErrorPolicySkip)
	_, _ = h.HandleError(ErrorRecord{ErrorType: ErrorTypeMalformedRow})
	h.Reset()

	stats := h.Stats()
	if stats.ErrorCount != 0 {
		t.Errorf("error count after reset = %d, want 0", stats.ErrorCount)
	}
}

func TestErrorHandlerCallbacks(t *testing.T) {
	onErrorCalled := false
	onSkipCalled := false

	h := NewErrorHandler(ErrorPolicySkip).
		WithOnError(func(er ErrorRecord) { onErrorCalled = true }).
		WithOnSkip(func(rowNum int64, reason string) { onSkipCalled = true })

	_, _ = h.HandleError(ErrorRecord{ErrorType: ErrorTypeMalformedRow, RowNumber: 1})

	if !onErrorCalled {
		t.Error("OnError callback was not called")
	}
	if !onSkipCalled {
		t.Error("OnSkip callback was not called")
	}
}

// --- EnterpriseOrchestrator status ---

func TestEnterpriseOrchestratorCreation(t *testing.T) {
	cfg := DefaultEnterpriseConfig()
	cfg.CheckpointDir = t.TempDir()

	eo, _ := NewEnterpriseOrchestrator(cfg)
	if eo == nil {
		t.Fatal("NewEnterpriseOrchestrator returned nil")
	}
}

func TestEnterpriseOrchestratorHealthy(t *testing.T) {
	cfg := DefaultEnterpriseConfig()
	cfg.CheckpointDir = t.TempDir()

	eo, _ := NewEnterpriseOrchestrator(cfg)
	if !eo.IsHealthy() {
		t.Error("new enterprise orchestrator should be healthy")
	}
}

func TestEnterpriseOrchestratorCircuitBreakerState(t *testing.T) {
	cfg := DefaultEnterpriseConfig()
	cfg.CheckpointDir = t.TempDir()

	eo, _ := NewEnterpriseOrchestrator(cfg)
	state := eo.CircuitBreakerState()
	if state != resilience.CircuitClosed {
		t.Errorf("circuit breaker state = %d, want CircuitClosed", state)
	}
}

func TestEnterpriseOrchestratorTelemetryMetrics(t *testing.T) {
	cfg := DefaultEnterpriseConfig()
	cfg.CheckpointDir = t.TempDir()

	eo, _ := NewEnterpriseOrchestrator(cfg)
	metrics := eo.TelemetryMetrics()
	if metrics == nil {
		t.Fatal("TelemetryMetrics returned nil")
	}
}

func TestEnterpriseOrchestratorDLQStats(t *testing.T) {
	cfg := DefaultEnterpriseConfig()
	cfg.CheckpointDir = t.TempDir()
	cfg.DLQPath = filepath.Join(t.TempDir(), "dlq")
	os.MkdirAll(cfg.DLQPath, 0755)

	eo, _ := NewEnterpriseOrchestrator(cfg)
	stats := eo.DLQStats()
	if stats.RecordCount != 0 {
		t.Errorf("initial DLQ RecordCount = %d, want 0", stats.RecordCount)
	}
}

func TestEnterpriseOrchestratorStatus(t *testing.T) {
	cfg := DefaultEnterpriseConfig()
	cfg.CheckpointDir = t.TempDir()

	eo, _ := NewEnterpriseOrchestrator(cfg)
	status := eo.Status()
	if !status.IsHealthy {
		t.Error("status.IsHealthy should be true")
	}
}

// --- Deduplicator tests ---

func TestNewDeduplicator(t *testing.T) {
	d := NewDeduplicator(DeduplicationConfig{
		KeyColumns: []string{"case_id", "activity"},
		Strategy:   DeduplicationSkip,
	})
	if d == nil {
		t.Fatal("NewDeduplicator returned nil")
	}
}

func TestDeduplicatorCheckAndAdd(t *testing.T) {
	d := NewDeduplicator(DeduplicationConfig{
		KeyColumns: []string{"id"},
		Strategy:   DeduplicationKeepFirst,
	})

	record := map[string]interface{}{"id": "1", "value": "a"}
	dup, _ := d.CheckAndAdd(record)
	if dup {
		t.Error("first record should not be duplicate")
	}

	dup, _ = d.CheckAndAdd(record)
	if !dup {
		t.Error("second identical record should be duplicate")
	}
}

func TestDeduplicatorStats(t *testing.T) {
	d := NewDeduplicator(DeduplicationConfig{
		KeyColumns: []string{"id"},
		Strategy:   DeduplicationKeepFirst,
	})

	d.CheckAndAdd(map[string]interface{}{"id": "1"})
	d.CheckAndAdd(map[string]interface{}{"id": "2"})
	d.CheckAndAdd(map[string]interface{}{"id": "1"}) // duplicate

	stats := d.Stats()
	if stats.TotalSeen != 3 {
		t.Errorf("TotalSeen = %d, want 3", stats.TotalSeen)
	}
	if stats.DuplicateCount != 1 {
		t.Errorf("DuplicateCount = %d, want 1", stats.DuplicateCount)
	}
}

func TestDeduplicatorReset(t *testing.T) {
	d := NewDeduplicator(DeduplicationConfig{
		KeyColumns: []string{"id"},
		Strategy:   DeduplicationSkip,
	})

	d.CheckAndAdd(map[string]interface{}{"id": "1"})
	d.Reset()

	dup, _ := d.CheckAndAdd(map[string]interface{}{"id": "1"})
	if dup {
		t.Error("after Reset, record should not be duplicate")
	}
}

func TestDeduplicateBatch(t *testing.T) {
	records := []map[string]interface{}{
		{"id": "1", "v": "a"},
		{"id": "2", "v": "b"},
		{"id": "1", "v": "c"},
		{"id": "3", "v": "d"},
	}

	result, _ := DeduplicateBatch(records, []string{"id"})
	if len(result) != 3 {
		t.Errorf("deduplicated batch len = %d, want 3", len(result))
	}
}

// --- Guard against using wrong checkpoint package ---
// The enterprise file uses its own Checkpoint struct (pipeline.Checkpoint)
// and also references resilience.Checkpoint / checkpoint.Checkpoint.
// We test both to ensure no confusion.

func TestResilienceCheckpointUsedInPipeline(t *testing.T) {
	cp := resilience.NewCheckpoint("in.csv", "out.parquet", "hash")
	cp.Update(100, 10)
	if !cp.ShouldResume() {
		t.Error("resilience checkpoint should resume")
	}
}

func TestCheckpointPackageManager(t *testing.T) {
	dir := t.TempDir()
	m, err := checkpoint.NewManager(dir)
	if err != nil {
		t.Fatalf("checkpoint.NewManager error: %v", err)
	}
	cp := m.Create("j1", "in.csv", "out.parquet")
	if cp == nil {
		t.Fatal("checkpoint.Create returned nil")
	}
}
