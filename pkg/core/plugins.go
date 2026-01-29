// Package core - Plugin system for optional features.
// Plugins add functionality WITHOUT modifying the core conversion flow.
package core

import (
	"context"
)

// Plugin defines the interface for optional features.
type Plugin interface {
	// Name returns the plugin identifier.
	Name() string

	// Description returns human-readable description.
	Description() string

	// CanProcess returns true if this plugin can handle the result.
	CanProcess(result *ConversionResult) bool
}

// PreProcessor runs before conversion.
type PreProcessor interface {
	Plugin
	PreProcess(ctx context.Context, inputPath string, opts *ConversionOptions) error
}

// PostProcessor runs after conversion.
type PostProcessor interface {
	Plugin
	PostProcess(ctx context.Context, result *ConversionResult) (*ConversionResult, error)
}

// Analyzer extracts insights without modifying data.
type Analyzer interface {
	Plugin
	Analyze(ctx context.Context, result *ConversionResult) (interface{}, error)
}

// PluginRegistry manages available plugins.
type PluginRegistry struct {
	preProcessors  []PreProcessor
	postProcessors []PostProcessor
	analyzers      []Analyzer
}

// NewPluginRegistry creates an empty registry.
func NewPluginRegistry() *PluginRegistry {
	return &PluginRegistry{}
}

// RegisterPreProcessor adds a pre-processor plugin.
func (r *PluginRegistry) RegisterPreProcessor(p PreProcessor) {
	r.preProcessors = append(r.preProcessors, p)
}

// RegisterPostProcessor adds a post-processor plugin.
func (r *PluginRegistry) RegisterPostProcessor(p PostProcessor) {
	r.postProcessors = append(r.postProcessors, p)
}

// RegisterAnalyzer adds an analyzer plugin.
func (r *PluginRegistry) RegisterAnalyzer(a Analyzer) {
	r.analyzers = append(r.analyzers, a)
}

// GetPreProcessors returns all pre-processors.
func (r *PluginRegistry) GetPreProcessors() []PreProcessor {
	return r.preProcessors
}

// GetPostProcessors returns all post-processors.
func (r *PluginRegistry) GetPostProcessors() []PostProcessor {
	return r.postProcessors
}

// GetAnalyzers returns all analyzers.
func (r *PluginRegistry) GetAnalyzers() []Analyzer {
	return r.analyzers
}

// ListPlugins returns info about all registered plugins.
func (r *PluginRegistry) ListPlugins() []PluginInfo {
	var plugins []PluginInfo

	for _, p := range r.preProcessors {
		plugins = append(plugins, PluginInfo{
			Name:        p.Name(),
			Description: p.Description(),
			Type:        "pre-processor",
		})
	}
	for _, p := range r.postProcessors {
		plugins = append(plugins, PluginInfo{
			Name:        p.Name(),
			Description: p.Description(),
			Type:        "post-processor",
		})
	}
	for _, p := range r.analyzers {
		plugins = append(plugins, PluginInfo{
			Name:        p.Name(),
			Description: p.Description(),
			Type:        "analyzer",
		})
	}

	return plugins
}

// PluginInfo describes a plugin.
type PluginInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Type        string `json:"type"`
}
