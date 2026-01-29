package sources

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// MemorySource provides data from memory (for testing).
type MemorySource struct {
	id     string
	data   []byte
	format core.Format
	meta   map[string]string
}

// NewMemorySource creates a source from bytes.
func NewMemorySource(id string, data []byte, format core.Format) *MemorySource {
	return &MemorySource{
		id:     id,
		data:   data,
		format: format,
		meta:   make(map[string]string),
	}
}

func (m *MemorySource) ID() string                  { return m.id }
func (m *MemorySource) Location() string            { return "memory://" + m.id }
func (m *MemorySource) Format() core.Format         { return m.format }
func (m *MemorySource) Size() int64                 { return int64(len(m.data)) }
func (m *MemorySource) ModTime() time.Time          { return time.Now() }
func (m *MemorySource) Metadata() map[string]string { return m.meta }

// Open returns a reader for the data.
func (m *MemorySource) Open(ctx context.Context) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(m.data)), nil
}
