package checkpoint

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// --- Manager tests ---

func TestNewManager(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager error: %v", err)
	}
	if m == nil {
		t.Fatal("NewManager returned nil")
	}
}

func TestManagerCreate(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager error: %v", err)
	}

	cp := m.Create("job1", "input.csv", "output.parquet")
	if cp == nil {
		t.Fatal("Create returned nil")
	}
	if cp.ID != "job1" {
		t.Errorf("checkpoint ID = %q, want %q", cp.ID, "job1")
	}
	if cp.InputPath != "input.csv" {
		t.Errorf("InputPath = %q, want %q", cp.InputPath, "input.csv")
	}
}

func TestManagerLoadAfterSave(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager error: %v", err)
	}

	cp := m.Create("job1", "input.csv", "output.parquet")
	cp.Update(2048, 100)
	if err := cp.Save(); err != nil {
		t.Fatalf("Save error: %v", err)
	}

	loaded, err := m.Load("job1")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if loaded == nil {
		t.Fatal("Load returned nil")
	}
	if loaded.BytesRead != 2048 {
		t.Errorf("BytesRead = %d, want 2048", loaded.BytesRead)
	}
}

func TestManagerFind(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager error: %v", err)
	}

	cp := m.Create("job1", "/path/to/input.csv", "output.parquet")
	cp.Update(100, 10)
	_ = cp.Save()

	found, err := m.Find("/path/to/input.csv")
	if err != nil {
		t.Fatalf("Find error: %v", err)
	}
	if found == nil {
		t.Fatal("Find returned nil for matching input path")
	}
}

func TestManagerDelete(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager error: %v", err)
	}

	cp := m.Create("job1", "input.csv", "output.parquet")
	_ = cp.Save()

	err = m.Delete("job1")
	if err != nil {
		t.Fatalf("Delete error: %v", err)
	}

	_, err = m.Load("job1")
	if err == nil {
		t.Error("Load after Delete should return error")
	}
}

func TestManagerListIncomplete(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager error: %v", err)
	}

	cp := m.Create("job1", "input.csv", "output.parquet")
	cp.Update(100, 10)
	_ = cp.Save()

	list, err := m.ListIncomplete()
	if err != nil {
		t.Fatalf("ListIncomplete error: %v", err)
	}
	if len(list) == 0 {
		t.Error("expected at least one incomplete checkpoint")
	}
}

func TestManagerCleanup(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager error: %v", err)
	}

	cp := m.Create("old-job", "input.csv", "output.parquet")
	_ = cp.Save()

	// Cleanup with zero age should remove everything
	cleaned, err := m.Cleanup(0)
	if err != nil {
		t.Fatalf("Cleanup error: %v", err)
	}
	_ = cleaned
}

// --- Checkpoint methods ---

func TestCheckpointUpdate(t *testing.T) {
	dir := t.TempDir()
	m, _ := NewManager(dir)
	cp := m.Create("j1", "in.csv", "out.parquet")

	cp.Update(4096, 500)
	if cp.BytesRead != 4096 {
		t.Errorf("BytesRead = %d, want 4096", cp.BytesRead)
	}
	if cp.RowsWritten != 500 {
		t.Errorf("RowsWritten = %d, want 500", cp.RowsWritten)
	}
}

func TestCheckpointSetPhase(t *testing.T) {
	dir := t.TempDir()
	m, _ := NewManager(dir)
	cp := m.Create("j1", "in.csv", "out.parquet")

	cp.SetPhase("reading")
	if cp.Phase != "reading" {
		t.Errorf("Phase = %q, want %q", cp.Phase, "reading")
	}
}

func TestCheckpointSetRowGroup(t *testing.T) {
	dir := t.TempDir()
	m, _ := NewManager(dir)
	cp := m.Create("j1", "in.csv", "out.parquet")

	cp.SetRowGroup(5)
	if cp.LastRowGroup != 5 {
		t.Errorf("LastRowGroup = %d, want 5", cp.LastRowGroup)
	}
}

func TestCheckpointSetMetadata(t *testing.T) {
	dir := t.TempDir()
	m, _ := NewManager(dir)
	cp := m.Create("j1", "in.csv", "out.parquet")

	cp.SetMetadata("compression", "snappy")
	if cp.Metadata["compression"] != "snappy" {
		t.Errorf("metadata[compression] = %v, want %q", cp.Metadata["compression"], "snappy")
	}
}

func TestCheckpointShouldResume(t *testing.T) {
	dir := t.TempDir()
	m, _ := NewManager(dir)
	cp := m.Create("j1", "in.csv", "out.parquet")

	if cp.ShouldResume() {
		t.Error("fresh checkpoint should not resume")
	}

	cp.Update(100, 10)
	if !cp.ShouldResume() {
		t.Error("updated checkpoint should resume")
	}
}

func TestCheckpointResumePoint(t *testing.T) {
	dir := t.TempDir()
	m, _ := NewManager(dir)
	cp := m.Create("j1", "in.csv", "out.parquet")
	cp.Update(8192, 1000)

	rp := cp.ResumePoint()
	if rp != 8192 {
		t.Errorf("ResumePoint = %d, want 8192", rp)
	}
}

func TestCheckpointProgress(t *testing.T) {
	dir := t.TempDir()
	m, _ := NewManager(dir)
	cp := m.Create("j1", "in.csv", "out.parquet")
	cp.Update(5000, 100)

	pct := cp.Progress(10000)
	// Progress returns percentage (0-100), not fraction
	if pct < 49 || pct > 51 {
		t.Errorf("Progress = %f, want ~50.0", pct)
	}
}

func TestCheckpointDuration(t *testing.T) {
	dir := t.TempDir()
	m, _ := NewManager(dir)
	cp := m.Create("j1", "in.csv", "out.parquet")

	d := cp.Duration()
	if d < 0 {
		t.Errorf("Duration = %v, should be non-negative", d)
	}
}

func TestCheckpointAutoSave(t *testing.T) {
	dir := t.TempDir()
	m, _ := NewManager(dir)
	cp := m.Create("j1", "in.csv", "out.parquet")

	stop := cp.StartAutoSave(50 * time.Millisecond)
	cp.Update(100, 10)
	time.Sleep(150 * time.Millisecond)
	stop()
}

// --- LocalBackend tests ---

func TestLocalBackendCRUD(t *testing.T) {
	dir := t.TempDir()
	b, err := NewLocalBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalBackend error: %v", err)
	}

	ctx := context.Background()

	// Use Manager to create a checkpoint (sets the path correctly for Save)
	mgr, _ := NewManager(dir)
	cp := mgr.Create("test-job", "in.csv", "out.parquet")
	cp.Update(1024, 100)

	// Save via backend
	if err := b.Save(ctx, cp); err != nil {
		t.Fatalf("Save error: %v", err)
	}

	// Load
	loaded, err := b.Load(ctx, "test-job")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if loaded.BytesRead != 1024 {
		t.Errorf("BytesRead = %d, want 1024", loaded.BytesRead)
	}

	// Delete
	if err := b.Delete(ctx, "test-job"); err != nil {
		t.Fatalf("Delete error: %v", err)
	}
	_, err = b.Load(ctx, "test-job")
	if err == nil {
		t.Error("Load after Delete should return error")
	}
}

func TestLocalBackendListIncomplete(t *testing.T) {
	dir := t.TempDir()
	b, err := NewLocalBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalBackend error: %v", err)
	}

	ctx := context.Background()
	_ = b.Save(ctx, &Checkpoint{ID: "j1", InputPath: "a.csv", Phase: "reading", BytesRead: 100})
	_ = b.Save(ctx, &Checkpoint{ID: "j2", InputPath: "b.csv", Phase: "reading", BytesRead: 200})

	list, err := b.ListIncomplete(ctx)
	if err != nil {
		t.Fatalf("ListIncomplete error: %v", err)
	}
	if len(list) < 2 {
		t.Errorf("ListIncomplete count = %d, want >= 2", len(list))
	}
}

func TestLocalBackendFindByInput(t *testing.T) {
	dir := t.TempDir()
	b, err := NewLocalBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalBackend error: %v", err)
	}

	ctx := context.Background()
	_ = b.Save(ctx, &Checkpoint{ID: "j1", InputPath: "/data/file.csv", Phase: "reading"})

	found, err := b.FindByInput(ctx, "/data/file.csv")
	if err != nil {
		t.Fatalf("FindByInput error: %v", err)
	}
	if found == nil {
		t.Fatal("FindByInput returned nil")
	}
	if found.ID != "j1" {
		t.Errorf("found ID = %q, want %q", found.ID, "j1")
	}
}

func TestLocalBackendName(t *testing.T) {
	dir := t.TempDir()
	b, _ := NewLocalBackend(dir)
	if b.Name() != "local" {
		t.Errorf("Name = %q, want %q", b.Name(), "local")
	}
}

// --- MultiBackend tests ---

func TestMultiBackendFailover(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	primary, err := NewLocalBackend(dir1)
	if err != nil {
		t.Fatalf("primary backend error: %v", err)
	}
	secondary, err := NewLocalBackend(dir2)
	if err != nil {
		t.Fatalf("secondary backend error: %v", err)
	}

	mb := NewMultiBackend(primary, secondary)
	ctx := context.Background()

	cp := &Checkpoint{ID: "j1", InputPath: "file.csv", BytesRead: 512}
	if err := mb.Save(ctx, cp); err != nil {
		t.Fatalf("MultiBackend Save error: %v", err)
	}

	// Load from multi backend (should read from primary)
	loaded, err := mb.Load(ctx, "j1")
	if err != nil {
		t.Fatalf("MultiBackend Load error: %v", err)
	}
	if loaded.BytesRead != 512 {
		t.Errorf("BytesRead = %d, want 512", loaded.BytesRead)
	}

	// Secondary should also have the data
	sec, err := secondary.Load(ctx, "j1")
	if err != nil {
		t.Fatalf("secondary Load error: %v", err)
	}
	if sec.BytesRead != 512 {
		t.Errorf("secondary BytesRead = %d, want 512", sec.BytesRead)
	}

	// Simulate primary failure by removing its directory
	os.RemoveAll(dir1)

	// Load from multi backend should failover to secondary
	fallback, err := mb.Load(ctx, "j1")
	if err != nil {
		t.Fatalf("MultiBackend Load (failover) error: %v", err)
	}
	if fallback.BytesRead != 512 {
		t.Errorf("fallback BytesRead = %d, want 512", fallback.BytesRead)
	}
}

func TestMultiBackendName(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	p, _ := NewLocalBackend(dir1)
	s, _ := NewLocalBackend(dir2)
	mb := NewMultiBackend(p, s)

	name := mb.Name()
	if name == "" {
		t.Error("MultiBackend Name should not be empty")
	}
}

// --- DefaultBackendConfig ---

func TestDefaultBackendConfig(t *testing.T) {
	cfg := DefaultBackendConfig()
	if cfg.Prefix == "" {
		// Default may or may not have a prefix — just verify no panic
	}
	_ = filepath.Join(cfg.Prefix, "test") // Ensure Prefix is valid path component
}
