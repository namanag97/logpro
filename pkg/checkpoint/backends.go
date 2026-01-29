// Package checkpoint provides backend interfaces for checkpoint persistence.
package checkpoint

import (
	"context"
)

// Backend defines the interface for checkpoint storage backends.
// Implementations can store checkpoints in various locations (local, S3, Redis, etc.)
type Backend interface {
	// Save persists a checkpoint to the backend.
	Save(ctx context.Context, cp *Checkpoint) error

	// Load retrieves a checkpoint by ID.
	Load(ctx context.Context, id string) (*Checkpoint, error)

	// Delete removes a checkpoint.
	Delete(ctx context.Context, id string) error

	// List returns all checkpoints matching the prefix.
	List(ctx context.Context, prefix string) ([]*Checkpoint, error)

	// ListIncomplete returns all checkpoints that haven't completed.
	ListIncomplete(ctx context.Context) ([]*Checkpoint, error)

	// FindByInput finds an incomplete checkpoint for the given input path.
	FindByInput(ctx context.Context, inputPath string) (*Checkpoint, error)

	// Name returns the backend name for logging/debugging.
	Name() string
}

// BackendConfig holds common backend configuration.
type BackendConfig struct {
	// Prefix is prepended to all checkpoint IDs/keys
	Prefix string

	// Encryption enables encryption at rest (if supported by backend)
	Encryption bool

	// Compression enables compression (if supported by backend)
	Compression bool
}

// DefaultBackendConfig returns sensible defaults.
func DefaultBackendConfig() BackendConfig {
	return BackendConfig{
		Prefix: "checkpoints/",
	}
}

// MultiBackend wraps multiple backends for redundancy.
type MultiBackend struct {
	primary   Backend
	secondary Backend
}

// NewMultiBackend creates a backend that writes to both primary and secondary.
func NewMultiBackend(primary, secondary Backend) *MultiBackend {
	return &MultiBackend{
		primary:   primary,
		secondary: secondary,
	}
}

// Save writes to both backends (primary first).
func (m *MultiBackend) Save(ctx context.Context, cp *Checkpoint) error {
	if err := m.primary.Save(ctx, cp); err != nil {
		return err
	}
	// Secondary is best-effort
	_ = m.secondary.Save(ctx, cp)
	return nil
}

// Load reads from primary, falls back to secondary.
func (m *MultiBackend) Load(ctx context.Context, id string) (*Checkpoint, error) {
	cp, err := m.primary.Load(ctx, id)
	if err == nil {
		return cp, nil
	}
	return m.secondary.Load(ctx, id)
}

// Delete removes from both backends.
func (m *MultiBackend) Delete(ctx context.Context, id string) error {
	err1 := m.primary.Delete(ctx, id)
	err2 := m.secondary.Delete(ctx, id)
	if err1 != nil {
		return err1
	}
	return err2
}

// List returns combined results from both backends.
func (m *MultiBackend) List(ctx context.Context, prefix string) ([]*Checkpoint, error) {
	return m.primary.List(ctx, prefix)
}

// ListIncomplete returns incomplete checkpoints from primary.
func (m *MultiBackend) ListIncomplete(ctx context.Context) ([]*Checkpoint, error) {
	return m.primary.ListIncomplete(ctx)
}

// FindByInput finds checkpoint from primary, falls back to secondary.
func (m *MultiBackend) FindByInput(ctx context.Context, inputPath string) (*Checkpoint, error) {
	cp, err := m.primary.FindByInput(ctx, inputPath)
	if err == nil {
		return cp, nil
	}
	return m.secondary.FindByInput(ctx, inputPath)
}

// Name returns the combined backend names.
func (m *MultiBackend) Name() string {
	return m.primary.Name() + "+" + m.secondary.Name()
}

// LocalBackend wraps the existing file-based Manager as a Backend.
type LocalBackend struct {
	mgr *Manager
}

// NewLocalBackend creates a backend using local filesystem.
func NewLocalBackend(dir string) (*LocalBackend, error) {
	mgr, err := NewManager(dir)
	if err != nil {
		return nil, err
	}
	return &LocalBackend{mgr: mgr}, nil
}

// Save persists a checkpoint to local filesystem.
func (b *LocalBackend) Save(ctx context.Context, cp *Checkpoint) error {
	return cp.Save()
}

// Load retrieves a checkpoint from local filesystem.
func (b *LocalBackend) Load(ctx context.Context, id string) (*Checkpoint, error) {
	return b.mgr.Load(id)
}

// Delete removes a checkpoint from local filesystem.
func (b *LocalBackend) Delete(ctx context.Context, id string) error {
	return b.mgr.Delete(id)
}

// List returns all checkpoints with the given prefix.
func (b *LocalBackend) List(ctx context.Context, prefix string) ([]*Checkpoint, error) {
	return b.mgr.ListIncomplete()
}

// ListIncomplete returns all incomplete checkpoints.
func (b *LocalBackend) ListIncomplete(ctx context.Context) ([]*Checkpoint, error) {
	return b.mgr.ListIncomplete()
}

// FindByInput finds an incomplete checkpoint for the input path.
func (b *LocalBackend) FindByInput(ctx context.Context, inputPath string) (*Checkpoint, error) {
	return b.mgr.Find(inputPath)
}

// Name returns "local".
func (b *LocalBackend) Name() string {
	return "local"
}

// ManagerWithBackend extends Manager to support pluggable backends.
type ManagerWithBackend struct {
	*Manager
	backend Backend
}

// NewManagerWithBackend creates a manager with a custom backend.
func NewManagerWithBackend(localDir string, backend Backend) (*ManagerWithBackend, error) {
	mgr, err := NewManager(localDir)
	if err != nil {
		return nil, err
	}
	return &ManagerWithBackend{
		Manager: mgr,
		backend: backend,
	}, nil
}

// CreateWithBackend creates a checkpoint and saves to backend.
func (m *ManagerWithBackend) CreateWithBackend(ctx context.Context, id, inputPath, outputPath string) (*Checkpoint, error) {
	cp := m.Create(id, inputPath, outputPath)
	if err := m.backend.Save(ctx, cp); err != nil {
		return nil, err
	}
	return cp, nil
}

// LoadFromBackend loads a checkpoint from the backend.
func (m *ManagerWithBackend) LoadFromBackend(ctx context.Context, id string) (*Checkpoint, error) {
	return m.backend.Load(ctx, id)
}

// SaveToBackend saves a checkpoint to the backend.
func (m *ManagerWithBackend) SaveToBackend(ctx context.Context, cp *Checkpoint) error {
	return m.backend.Save(ctx, cp)
}

// Backend returns the configured backend.
func (m *ManagerWithBackend) Backend() Backend {
	return m.backend
}
