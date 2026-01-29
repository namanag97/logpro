package errors

import (
	"encoding/json"
	"os"
	"sync"
)

// Quarantine stores rejected rows for later inspection.
type Quarantine interface {
	// Add adds an error to quarantine.
	Add(err RowError)

	// Errors returns all quarantined errors.
	Errors() []RowError

	// Count returns number of quarantined items.
	Count() int

	// Flush writes quarantine to storage.
	Flush() error

	// Close closes the quarantine.
	Close() error
}

// MemoryQuarantine stores errors in memory.
type MemoryQuarantine struct {
	mu     sync.Mutex
	errors []RowError
	limit  int
}

// NewMemoryQuarantine creates a memory-based quarantine.
func NewMemoryQuarantine(limit int) *MemoryQuarantine {
	return &MemoryQuarantine{
		errors: make([]RowError, 0, 1000),
		limit:  limit,
	}
}

func (q *MemoryQuarantine) Add(err RowError) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.limit > 0 && len(q.errors) >= q.limit {
		return // Drop if at limit
	}
	q.errors = append(q.errors, err)
}

func (q *MemoryQuarantine) Errors() []RowError {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]RowError{}, q.errors...)
}

func (q *MemoryQuarantine) Count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.errors)
}

func (q *MemoryQuarantine) Flush() error {
	return nil // Memory-only
}

func (q *MemoryQuarantine) Close() error {
	return nil
}

// FileQuarantine writes errors to a file.
type FileQuarantine struct {
	mu     sync.Mutex
	path   string
	file   *os.File
	errors []RowError
	count  int
}

// NewFileQuarantine creates a file-based quarantine.
func NewFileQuarantine(path string) (*FileQuarantine, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return &FileQuarantine{
		path:   path,
		file:   file,
		errors: make([]RowError, 0),
	}, nil
}

func (q *FileQuarantine) Add(err RowError) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.errors = append(q.errors, err)
	q.count++

	// Write immediately
	data, _ := json.Marshal(err)
	q.file.Write(data)
	q.file.WriteString("\n")
}

func (q *FileQuarantine) Errors() []RowError {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]RowError{}, q.errors...)
}

func (q *FileQuarantine) Count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}

func (q *FileQuarantine) Flush() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.file.Sync()
}

func (q *FileQuarantine) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.file.Close()
}
