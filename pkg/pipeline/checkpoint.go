// Package pipeline provides checkpointing for resumable ingestion.
// Checkpoints track progress so failed jobs can resume from where they left off.
package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Checkpoint represents the state of an ingestion job at a point in time.
type Checkpoint struct {
	// Job identification
	JobID     string `json:"job_id"`
	SourceFile string `json:"source_file"`

	// Progress tracking
	BytesRead    int64 `json:"bytes_read"`
	RowsRead     int64 `json:"rows_read"`
	RowsWritten  int64 `json:"rows_written"`
	RowsSkipped  int64 `json:"rows_skipped"`
	BatchesWritten int `json:"batches_written"`

	// Position for resume
	LastOffset    int64  `json:"last_offset"`     // Byte offset in source file
	LastRowNumber int64  `json:"last_row_number"` // Row number in source
	LastChecksum  string `json:"last_checksum,omitempty"` // Checksum of last processed batch

	// Output tracking
	OutputFiles []string `json:"output_files"`
	OutputBytes int64    `json:"output_bytes"`

	// Timing
	StartTime      time.Time `json:"start_time"`
	LastUpdateTime time.Time `json:"last_update_time"`

	// State
	Status    CheckpointStatus `json:"status"`
	Error     string           `json:"error,omitempty"`

	// Metadata
	Metadata map[string]string `json:"metadata,omitempty"`
}

// CheckpointStatus represents the state of a checkpoint.
type CheckpointStatus string

const (
	CheckpointStatusInProgress CheckpointStatus = "in_progress"
	CheckpointStatusCompleted  CheckpointStatus = "completed"
	CheckpointStatusFailed     CheckpointStatus = "failed"
	CheckpointStatusAborted    CheckpointStatus = "aborted"
)

// CheckpointManager manages checkpoints for ingestion jobs.
type CheckpointManager struct {
	mu sync.RWMutex

	baseDir       string
	checkpoints   map[string]*Checkpoint
	saveInterval  time.Duration
	lastSaveTime  map[string]time.Time

	// Auto-save control
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewCheckpointManager creates a new checkpoint manager.
func NewCheckpointManager(baseDir string) (*CheckpointManager, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	mgr := &CheckpointManager{
		baseDir:      baseDir,
		checkpoints:  make(map[string]*Checkpoint),
		saveInterval: 10 * time.Second,
		lastSaveTime: make(map[string]time.Time),
		stopChan:     make(chan struct{}),
	}

	// Load existing checkpoints
	if err := mgr.loadAll(); err != nil {
		return nil, err
	}

	// Start auto-save goroutine
	mgr.wg.Add(1)
	go mgr.autoSaveLoop()

	return mgr, nil
}

// Create creates a new checkpoint for a job.
func (m *CheckpointManager) Create(jobID, sourceFile string) *Checkpoint {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := &Checkpoint{
		JobID:          jobID,
		SourceFile:     sourceFile,
		StartTime:      time.Now(),
		LastUpdateTime: time.Now(),
		Status:         CheckpointStatusInProgress,
		Metadata:       make(map[string]string),
	}

	m.checkpoints[jobID] = cp
	return cp
}

// Get retrieves a checkpoint by job ID.
func (m *CheckpointManager) Get(jobID string) *Checkpoint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.checkpoints[jobID]
}

// Update updates a checkpoint with new progress.
func (m *CheckpointManager) Update(jobID string, update func(*Checkpoint)) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp, ok := m.checkpoints[jobID]
	if !ok {
		return fmt.Errorf("checkpoint not found: %s", jobID)
	}

	update(cp)
	cp.LastUpdateTime = time.Now()

	// Check if we should save
	if time.Since(m.lastSaveTime[jobID]) >= m.saveInterval {
		if err := m.saveCheckpoint(cp); err != nil {
			return err
		}
		m.lastSaveTime[jobID] = time.Now()
	}

	return nil
}

// Complete marks a checkpoint as completed.
func (m *CheckpointManager) Complete(jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp, ok := m.checkpoints[jobID]
	if !ok {
		return fmt.Errorf("checkpoint not found: %s", jobID)
	}

	cp.Status = CheckpointStatusCompleted
	cp.LastUpdateTime = time.Now()

	return m.saveCheckpoint(cp)
}

// Fail marks a checkpoint as failed.
func (m *CheckpointManager) Fail(jobID string, err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp, ok := m.checkpoints[jobID]
	if !ok {
		return fmt.Errorf("checkpoint not found: %s", jobID)
	}

	cp.Status = CheckpointStatusFailed
	cp.Error = err.Error()
	cp.LastUpdateTime = time.Now()

	return m.saveCheckpoint(cp)
}

// CanResume checks if a job can be resumed from checkpoint.
func (m *CheckpointManager) CanResume(sourceFile string) (*Checkpoint, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, cp := range m.checkpoints {
		if cp.SourceFile == sourceFile &&
			(cp.Status == CheckpointStatusInProgress || cp.Status == CheckpointStatusFailed) {
			return cp, true
		}
	}
	return nil, false
}

// GetResumeOffset returns the byte offset to resume from.
func (m *CheckpointManager) GetResumeOffset(jobID string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cp, ok := m.checkpoints[jobID]
	if !ok {
		return 0, fmt.Errorf("checkpoint not found: %s", jobID)
	}

	return cp.LastOffset, nil
}

// ListIncomplete returns all incomplete checkpoints.
func (m *CheckpointManager) ListIncomplete() []*Checkpoint {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*Checkpoint
	for _, cp := range m.checkpoints {
		if cp.Status == CheckpointStatusInProgress || cp.Status == CheckpointStatusFailed {
			result = append(result, cp)
		}
	}
	return result
}

// Delete removes a checkpoint.
func (m *CheckpointManager) Delete(jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.checkpoints, jobID)
	delete(m.lastSaveTime, jobID)

	// Remove file
	path := filepath.Join(m.baseDir, jobID+".json")
	return os.Remove(path)
}

// Cleanup removes old completed checkpoints.
func (m *CheckpointManager) Cleanup(maxAge time.Duration) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	removed := 0

	for jobID, cp := range m.checkpoints {
		if cp.Status == CheckpointStatusCompleted && cp.LastUpdateTime.Before(cutoff) {
			delete(m.checkpoints, jobID)
			delete(m.lastSaveTime, jobID)
			os.Remove(filepath.Join(m.baseDir, jobID+".json"))
			removed++
		}
	}

	return removed
}

func (m *CheckpointManager) saveCheckpoint(cp *Checkpoint) error {
	path := filepath.Join(m.baseDir, cp.JobID+".json")

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	// Atomic write
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename checkpoint: %w", err)
	}

	return nil
}

func (m *CheckpointManager) loadAll() error {
	entries, err := os.ReadDir(m.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		path := filepath.Join(m.baseDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		var cp Checkpoint
		if err := json.Unmarshal(data, &cp); err != nil {
			continue
		}

		m.checkpoints[cp.JobID] = &cp
	}

	return nil
}

func (m *CheckpointManager) autoSaveLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.saveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			// Final save on shutdown
			m.saveAll()
			return
		case <-ticker.C:
			m.saveAll()
		}
	}
}

func (m *CheckpointManager) saveAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, cp := range m.checkpoints {
		if cp.Status == CheckpointStatusInProgress {
			m.saveCheckpoint(cp)
		}
	}
}

// Close stops the checkpoint manager.
func (m *CheckpointManager) Close() error {
	close(m.stopChan)
	m.wg.Wait()
	return nil
}

// Progress returns a progress summary for a checkpoint.
type Progress struct {
	JobID        string        `json:"job_id"`
	SourceFile   string        `json:"source_file"`
	BytesRead    int64         `json:"bytes_read"`
	RowsRead     int64         `json:"rows_read"`
	RowsWritten  int64         `json:"rows_written"`
	Percent      float64       `json:"percent"`
	Status       string        `json:"status"`
	Elapsed      time.Duration `json:"elapsed"`
	ETA          time.Duration `json:"eta,omitempty"`
	RowsPerSec   float64       `json:"rows_per_sec"`
	BytesPerSec  float64       `json:"bytes_per_sec"`
}

// GetProgress returns progress info for a checkpoint.
func (cp *Checkpoint) GetProgress(totalBytes int64) Progress {
	elapsed := time.Since(cp.StartTime)

	var percent float64
	if totalBytes > 0 {
		percent = float64(cp.BytesRead) / float64(totalBytes) * 100
	}

	var eta time.Duration
	if percent > 0 && percent < 100 {
		remaining := 100 - percent
		eta = time.Duration(float64(elapsed) * remaining / percent)
	}

	var rowsPerSec, bytesPerSec float64
	if elapsed.Seconds() > 0 {
		rowsPerSec = float64(cp.RowsRead) / elapsed.Seconds()
		bytesPerSec = float64(cp.BytesRead) / elapsed.Seconds()
	}

	return Progress{
		JobID:       cp.JobID,
		SourceFile:  cp.SourceFile,
		BytesRead:   cp.BytesRead,
		RowsRead:    cp.RowsRead,
		RowsWritten: cp.RowsWritten,
		Percent:     percent,
		Status:      string(cp.Status),
		Elapsed:     elapsed,
		ETA:         eta,
		RowsPerSec:  rowsPerSec,
		BytesPerSec: bytesPerSec,
	}
}
