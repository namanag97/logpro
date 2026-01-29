// Package decoders provides format-specific decoders.
package decoders

import (
	"fmt"
	"sync"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// Registry manages decoder registration.
type Registry struct {
	mu       sync.RWMutex
	decoders map[core.Format]core.Decoder
}

// DefaultRegistry is the global decoder registry.
var DefaultRegistry = NewRegistry()

// NewRegistry creates a decoder registry.
func NewRegistry() *Registry {
	return &Registry{
		decoders: make(map[core.Format]core.Decoder),
	}
}

// Register registers a decoder for formats.
func (r *Registry) Register(decoder core.Decoder) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, format := range decoder.Formats() {
		r.decoders[format] = decoder
	}
}

// Get returns the decoder for a format.
func (r *Registry) Get(format core.Format) (core.Decoder, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if decoder, ok := r.decoders[format]; ok {
		return decoder, nil
	}
	return nil, fmt.Errorf("no decoder for format: %s", format)
}

// Formats returns all registered formats.
func (r *Registry) Formats() []core.Format {
	r.mu.RLock()
	defer r.mu.RUnlock()

	formats := make([]core.Format, 0, len(r.decoders))
	for f := range r.decoders {
		formats = append(formats, f)
	}
	return formats
}

// Register registers a decoder with the default registry.
func Register(decoder core.Decoder) {
	DefaultRegistry.Register(decoder)
}

// Get retrieves a decoder from the default registry.
func Get(format core.Format) (core.Decoder, error) {
	return DefaultRegistry.Get(format)
}
