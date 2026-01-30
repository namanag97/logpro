// Package detect - Pluggable detection rules for strategy selection.
package detect

import (
	"sort"
	"sync"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// DetectionRule evaluates an analysis and recommends a strategy.
type DetectionRule interface {
	Name() string
	Priority() int // Lower = evaluated first
	Evaluate(analysis *Analysis) (strategy Strategy, confidence float64, applies bool)
}

// RuleEngine manages detection rules and selects the best strategy.
type RuleEngine struct {
	mu     sync.RWMutex
	rules  []DetectionRule
	sorted bool
}

var defaultRuleEngine = NewRuleEngine()

// NewRuleEngine creates a rule engine with built-in rules.
func NewRuleEngine() *RuleEngine {
	r := &RuleEngine{}
	r.Register(&LargeFileRule{})
	r.Register(&CleanNativeRule{})
	r.Register(&XESRule{})
	r.Register(&DirtyFileRule{})
	r.Register(&DefaultRule{})
	return r
}

// Register adds a detection rule.
func (r *RuleEngine) Register(rule DetectionRule) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rules = append(r.rules, rule)
	r.sorted = false
}

// Evaluate runs all rules and returns the highest-confidence match.
func (r *RuleEngine) Evaluate(analysis *Analysis) (Strategy, float64) {
	r.mu.RLock()
	if !r.sorted {
		r.mu.RUnlock()
		r.mu.Lock()
		sort.SliceStable(r.rules, func(i, j int) bool {
			return r.rules[i].Priority() < r.rules[j].Priority()
		})
		r.sorted = true
		r.mu.Unlock()
		r.mu.RLock()
	}
	rules := make([]DetectionRule, len(r.rules))
	copy(rules, r.rules)
	r.mu.RUnlock()

	var bestStrategy Strategy
	var bestConf float64

	for _, rule := range rules {
		strategy, conf, applies := rule.Evaluate(analysis)
		if applies && conf > bestConf {
			bestStrategy = strategy
			bestConf = conf
		}
	}

	if bestConf == 0 {
		return StrategyRobustGo, 0.5
	}
	return bestStrategy, bestConf
}

// List returns all registered rule names.
func (r *RuleEngine) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, len(r.rules))
	for i, rule := range r.rules {
		names[i] = rule.Name()
	}
	return names
}

// Package-level convenience functions.

// RegisterRule adds a rule to the default engine.
func RegisterRule(rule DetectionRule) { defaultRuleEngine.Register(rule) }

// EvaluateRules runs the default engine.
func EvaluateRules(analysis *Analysis) (Strategy, float64) {
	return defaultRuleEngine.Evaluate(analysis)
}

// ListRules returns all rule names from the default engine.
func ListRules() []string { return defaultRuleEngine.List() }

// --- Built-in rules ---

// LargeFileRule recommends streaming for files > 2GB.
type LargeFileRule struct{}

func (LargeFileRule) Name() string   { return "large-file" }
func (LargeFileRule) Priority() int  { return 10 }
func (LargeFileRule) Evaluate(a *Analysis) (Strategy, float64, bool) {
	if a.Size > 2*1024*1024*1024 {
		return StrategyStreaming, 0.95, true
	}
	return 0, 0, false
}

// CleanNativeRule recommends DuckDB for clean files in native formats.
type CleanNativeRule struct{}

func (CleanNativeRule) Name() string   { return "clean-native" }
func (CleanNativeRule) Priority() int  { return 20 }
func (CleanNativeRule) Evaluate(a *Analysis) (Strategy, float64, bool) {
	if !a.IsClean {
		return 0, 0, false
	}
	switch a.Format {
	case core.FormatCSV, core.FormatTSV, core.FormatJSON, core.FormatJSONL, core.FormatParquet:
		return StrategyFastDuckDB, 0.9, true
	}
	return 0, 0, false
}

// XESRule recommends robust Go processing for XES files.
type XESRule struct{}

func (XESRule) Name() string   { return "xes" }
func (XESRule) Priority() int  { return 30 }
func (XESRule) Evaluate(a *Analysis) (Strategy, float64, bool) {
	if a.Format == core.FormatXES {
		return StrategyRobustGo, 0.8, true
	}
	return 0, 0, false
}

// DirtyFileRule recommends robust processing for dirty files.
type DirtyFileRule struct{}

func (DirtyFileRule) Name() string   { return "dirty-file" }
func (DirtyFileRule) Priority() int  { return 40 }
func (DirtyFileRule) Evaluate(a *Analysis) (Strategy, float64, bool) {
	if !a.IsClean {
		return StrategyRobustGo, 0.85, true
	}
	return 0, 0, false
}

// DefaultRule is the fallback for all other cases.
type DefaultRule struct{}

func (DefaultRule) Name() string   { return "default" }
func (DefaultRule) Priority() int  { return 100 }
func (DefaultRule) Evaluate(a *Analysis) (Strategy, float64, bool) {
	if a.IsClean {
		return StrategyRobustGo, 0.7, true
	}
	return StrategyRobustGo, 0.5, true
}
