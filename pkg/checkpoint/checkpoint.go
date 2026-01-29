// Package checkpoint provides resume capability for interrupted conversions.
// Critical for Lambda where jobs may be interrupted by timeout.
package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Checkpoint tracks conversion progress for resume capability.
type Checkpoint struct {
	// Identification
	ID        string `json:"id"`
	InputPath string `json:"input_path"`
	OutputPath string `json:"output_path"`

	// Progress
	BytesRead    int64 `json:"bytes_read"`
	RowsWritten  int64 `json:"rows_written"`
	LastRowGroup int   `json:"last_row_group"`

	// State
	Phase       string    `json:"phase"` // reading, writing, complete
	StartedAt   time.Time `json:"started_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`

	// For resume
	TempOutputPath string                 `json:"temp_output_path,omitempty"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`

	// Internal
	path string
	mu   sync.Mutex
}

// Manager handles checkpoint persistence.
type Manager struct {
	dir      string
	interval time.Duration
	mu       sync.RWMutex
	active   map[string]*Checkpoint
}

// NewManager creates a new checkpoint manager.
func NewManager(checkpointDir string) (*Manager, error) {
	if err := os.MkdirAll(checkpointDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	return &Manager{
		dir:      checkpointDir,
		interval: 5 * time.Second,
		active:   make(map[string]*Checkpoint),
	}, nil
}

// Create creates a new checkpoint for a job.
func (m *Manager) Create(id, inputPath, outputPath string) *Checkpoint {
	cp := &Checkpoint{
		ID:         id,
		InputPath:  inputPath,
		OutputPath: outputPath,
		Phase:      "starting",
		StartedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		Metadata:   make(map[string]interface{}),
		path:       filepath.Join(m.dir, id+".checkpoint"),
	}

	m.mu.Lock()
	m.active[id] = cp
	m.mu.Unlock()

	cp.Save()
	return cp
}

// Load loads a checkpoint from disk.
func (m *Manager) Load(id string) (*Checkpoint, error) {
	path := filepath.Join(m.dir, id+".checkpoint")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}
	cp.path = path

	m.mu.Lock()
	m.active[id] = &cp
	m.mu.Unlock()

	return &cp, nil
}

// Find finds an existing checkpoint for an input file.
func (m *Manager) Find(inputPath string) (*Checkpoint, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".checkpoint" {
			continue
		}

		path := filepath.Join(m.dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		var cp Checkpoint
		if err := json.Unmarshal(data, &cp); err != nil {
			continue
		}

		if cp.InputPath == inputPath && cp.Phase != "complete" {
			cp.path = path
			return &cp, nil
		}
	}

	return nil, os.ErrNotExist
}

// Delete removes a checkpoint.
func (m *Manager) Delete(id string) error {
	m.mu.Lock()
	delete(m.active, id)
	m.mu.Unlock()

	path := filepath.Join(m.dir, id+".checkpoint")
	return os.Remove(path)
}

// ListIncomplete returns all incomplete checkpoints.
func (m *Manager) ListIncomplete() ([]*Checkpoint, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil, err
	}

	var checkpoints []*Checkpoint
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".checkpoint" {
			continue
		}

		data, err := os.ReadFile(filepath.Join(m.dir, entry.Name()))
		if err != nil {
			continue
		}

		var cp Checkpoint
		if err := json.Unmarshal(data, &cp); err != nil {
			continue
		}

		if cp.Phase != "complete" {
			cp.path = filepath.Join(m.dir, entry.Name())
			checkpoints = append(checkpoints, &cp)
		}
	}

	return checkpoints, nil
}

// Cleanup removes old completed checkpoints.
func (m *Manager) Cleanup(maxAge time.Duration) (int, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return 0, err
	}

	cutoff := time.Now().Add(-maxAge)
	removed := 0

	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".checkpoint" {
			continue
		}

		path := filepath.Join(m.dir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoff) {
			if err := os.Remove(path); err == nil {
				removed++
			}
		}
	}

	return removed, nil
}

// --- Checkpoint Methods ---

// Update updates the checkpoint progress.
func (c *Checkpoint) Update(bytesRead, rowsWritten int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.BytesRead = bytesRead
	c.RowsWritten = rowsWritten
	c.UpdatedAt = time.Now()
}

// SetPhase updates the phase.
func (c *Checkpoint) SetPhase(phase string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Phase = phase
	c.UpdatedAt = time.Now()

	if phase == "complete" {
		now := time.Now()
		c.CompletedAt = &now
	}
}

// SetRowGroup updates the last written row group.
func (c *Checkpoint) SetRowGroup(rowGroup int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.LastRowGroup = rowGroup
	c.UpdatedAt = time.Now()
}

// SetMetadata sets a metadata value.
func (c *Checkpoint) SetMetadata(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Metadata[key] = value
}

// Save persists the checkpoint to disk.
func (c *Checkpoint) Save() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}

	// Write to temp file first, then rename (atomic)
	tempPath := c.path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tempPath, c.path)
}

// ShouldResume returns true if this checkpoint can be resumed.
func (c *Checkpoint) ShouldResume() bool {
	return c.Phase != "complete" && c.BytesRead > 0
}

// ResumePoint returns the byte offset to resume from.
func (c *Checkpoint) ResumePoint() int64 {
	return c.BytesRead
}

// Progress returns progress as a percentage (0-100).
func (c *Checkpoint) Progress(totalBytes int64) float64 {
	if totalBytes == 0 {
		return 0
	}
	return float64(c.BytesRead) * 100 / float64(totalBytes)
}

// Duration returns how long the job has been running.
func (c *Checkpoint) Duration() time.Duration {
	if c.CompletedAt != nil {
		return c.CompletedAt.Sub(c.StartedAt)
	}
	return time.Since(c.StartedAt)
}

// --- Auto-Save Goroutine ---

// StartAutoSave starts automatic checkpoint saving.
func (c *Checkpoint) StartAutoSave(interval time.Duration) func() {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				c.Save()
				return
			case <-ticker.C:
				c.Save()
			}
		}
	}()
	return func() { close(done) }
}
