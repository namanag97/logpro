package sources

import (
	"context"
	"io"
	"time"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// StreamSource wraps an io.Reader as a Source.
type StreamSource struct {
	id     string
	reader io.Reader
	format core.Format
	size   int64
	meta   map[string]string
}

// NewStreamSource creates a source from an io.Reader.
func NewStreamSource(id string, reader io.Reader, format core.Format) *StreamSource {
	return &StreamSource{
		id:     id,
		reader: reader,
		format: format,
		size:   -1, // Unknown
		meta:   make(map[string]string),
	}
}

func (s *StreamSource) ID() string                  { return s.id }
func (s *StreamSource) Location() string            { return "stream://" + s.id }
func (s *StreamSource) Format() core.Format         { return s.format }
func (s *StreamSource) Size() int64                 { return s.size }
func (s *StreamSource) ModTime() time.Time          { return time.Now() }
func (s *StreamSource) Metadata() map[string]string { return s.meta }

// Open returns the reader (can only be called once).
func (s *StreamSource) Open(ctx context.Context) (io.ReadCloser, error) {
	if closer, ok := s.reader.(io.ReadCloser); ok {
		return closer, nil
	}
	return io.NopCloser(s.reader), nil
}

// SetSize sets the known size.
func (s *StreamSource) SetSize(size int64) {
	s.size = size
}

// SetMetadata sets a metadata value.
func (s *StreamSource) SetMetadata(key, value string) {
	s.meta[key] = value
}
