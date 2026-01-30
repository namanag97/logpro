package ingest

import (
	"context"
	"sync"
)

// StrategyPlugin defines a pluggable processing strategy.
// This extends the built-in strategy routing in pipeline.go with
// user-registered strategies that are checked first.
type StrategyPlugin interface {
	Name() string
	CanHandle(analysis *FileAnalysis, heuristics *Heuristics) (bool, float64) // applies, confidence
	Process(ctx context.Context, inputPath, outputPath string, analysis *FileAnalysis, opts Options) (*Result, error)
}

// StrategyRegistry holds registered strategies.
type StrategyRegistry struct {
	mu         sync.RWMutex
	strategies []StrategyPlugin
}

var defaultStrategyRegistry = &StrategyRegistry{}

// NewStrategyRegistry creates a new empty strategy registry.
func NewStrategyRegistry() *StrategyRegistry { return &StrategyRegistry{} }

// Register adds a strategy to the registry.
func (r *StrategyRegistry) Register(s StrategyPlugin) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.strategies = append(r.strategies, s)
}

// Select returns the strategy with highest confidence that can handle the analysis.
func (r *StrategyRegistry) Select(analysis *FileAnalysis, heuristics *Heuristics) StrategyPlugin {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var best StrategyPlugin
	var bestConf float64
	for _, s := range r.strategies {
		if ok, conf := s.CanHandle(analysis, heuristics); ok && conf > bestConf {
			best = s
			bestConf = conf
		}
	}
	return best
}

// List returns the names of all registered strategies.
func (r *StrategyRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, len(r.strategies))
	for i, s := range r.strategies {
		names[i] = s.Name()
	}
	return names
}

// Package-level convenience functions operating on the default registry.

// RegisterStrategy registers a strategy plugin with the default registry.
func RegisterStrategy(s StrategyPlugin) { defaultStrategyRegistry.Register(s) }

// SelectStrategy selects the best matching strategy plugin from the default registry.
func SelectStrategy(a *FileAnalysis, h *Heuristics) StrategyPlugin {
	return defaultStrategyRegistry.Select(a, h)
}

// ListStrategies returns names of all strategies in the default registry.
func ListStrategies() []string { return defaultStrategyRegistry.List() }
