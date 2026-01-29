// Package registry provides a central registry for Sources, Sinks, and Processors.
// This enables runtime selection without if/else chains in main code.
package registry

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/logflow/logflow/pkg/pipeline"
)

// SourceFactory creates a new Source instance.
type SourceFactory func(cfg pipeline.Config) (pipeline.Source, error)

// SinkFactory creates a new Sink instance.
type SinkFactory func(cfg pipeline.Config) (pipeline.Sink, error)

// ProcessorFactory creates a new Processor instance.
type ProcessorFactory func(cfg pipeline.Config) (pipeline.Processor, error)

// Registry holds all registered adapters and processors.
type Registry struct {
	mu sync.RWMutex

	sources    map[string]SourceFactory
	sinks      map[string]SinkFactory
	processors map[string]ProcessorFactory

	// Format to extension mapping
	formatExtensions map[string][]string
}

// Global default registry
var defaultRegistry = NewRegistry()

// NewRegistry creates a new empty registry.
func NewRegistry() *Registry {
	return &Registry{
		sources:          make(map[string]SourceFactory),
		sinks:            make(map[string]SinkFactory),
		processors:       make(map[string]ProcessorFactory),
		formatExtensions: make(map[string][]string),
	}
}

// RegisterSource adds a source factory to the registry.
func (r *Registry) RegisterSource(name string, factory SourceFactory, extensions ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sources[name] = factory
	if len(extensions) > 0 {
		r.formatExtensions[name] = extensions
	}
}

// RegisterSink adds a sink factory to the registry.
func (r *Registry) RegisterSink(name string, factory SinkFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sinks[name] = factory
}

// RegisterProcessor adds a processor factory to the registry.
func (r *Registry) RegisterProcessor(name string, factory ProcessorFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.processors[name] = factory
}

// GetSource returns a source for the given format.
func (r *Registry) GetSource(format string, cfg pipeline.Config) (pipeline.Source, error) {
	r.mu.RLock()
	factory, ok := r.sources[format]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown source format: %s", format)
	}

	return factory(cfg)
}

// GetSink returns a sink for the given format.
func (r *Registry) GetSink(format string, cfg pipeline.Config) (pipeline.Sink, error) {
	r.mu.RLock()
	factory, ok := r.sinks[format]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown sink format: %s", format)
	}

	return factory(cfg)
}

// GetProcessor returns a processor by name.
func (r *Registry) GetProcessor(name string, cfg pipeline.Config) (pipeline.Processor, error) {
	r.mu.RLock()
	factory, ok := r.processors[name]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown processor: %s", name)
	}

	return factory(cfg)
}

// DetectFormat determines the format from a file path.
func (r *Registry) DetectFormat(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	ext = strings.TrimPrefix(ext, ".")

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check each registered format's extensions
	for format, extensions := range r.formatExtensions {
		for _, e := range extensions {
			if e == ext {
				return format
			}
		}
	}

	// Fallback: use extension as format
	return ext
}

// ListSources returns all registered source formats.
func (r *Registry) ListSources() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	formats := make([]string, 0, len(r.sources))
	for name := range r.sources {
		formats = append(formats, name)
	}
	return formats
}

// ListSinks returns all registered sink formats.
func (r *Registry) ListSinks() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	formats := make([]string, 0, len(r.sinks))
	for name := range r.sinks {
		formats = append(formats, name)
	}
	return formats
}

// ListProcessors returns all registered processor names.
func (r *Registry) ListProcessors() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.processors))
	for name := range r.processors {
		names = append(names, name)
	}
	return names
}

// --- Global registry functions ---

// RegisterSource adds a source to the default registry.
func RegisterSource(name string, factory SourceFactory, extensions ...string) {
	defaultRegistry.RegisterSource(name, factory, extensions...)
}

// RegisterSink adds a sink to the default registry.
func RegisterSink(name string, factory SinkFactory) {
	defaultRegistry.RegisterSink(name, factory)
}

// RegisterProcessor adds a processor to the default registry.
func RegisterProcessor(name string, factory ProcessorFactory) {
	defaultRegistry.RegisterProcessor(name, factory)
}

// GetSource returns a source from the default registry.
func GetSource(format string, cfg pipeline.Config) (pipeline.Source, error) {
	return defaultRegistry.GetSource(format, cfg)
}

// GetSink returns a sink from the default registry.
func GetSink(format string, cfg pipeline.Config) (pipeline.Sink, error) {
	return defaultRegistry.GetSink(format, cfg)
}

// GetProcessor returns a processor from the default registry.
func GetProcessor(name string, cfg pipeline.Config) (pipeline.Processor, error) {
	return defaultRegistry.GetProcessor(name, cfg)
}

// DetectFormat determines format from the default registry.
func DetectFormat(path string) string {
	return defaultRegistry.DetectFormat(path)
}

// ListSources lists sources from the default registry.
func ListSources() []string {
	return defaultRegistry.ListSources()
}

// ListSinks lists sinks from the default registry.
func ListSinks() []string {
	return defaultRegistry.ListSinks()
}

// ListProcessors lists processors from the default registry.
func ListProcessors() []string {
	return defaultRegistry.ListProcessors()
}

// Default returns the default registry for direct access.
func Default() *Registry {
	return defaultRegistry
}
