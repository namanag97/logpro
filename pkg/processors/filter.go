// Package processors provides Processor implementations for data transformation.
package processors

import (
	"bytes"
	"context"
	"regexp"
	"sync"

	"github.com/logflow/logflow/pkg/pipeline"
)

func init() {
	// Register built-in filter operators.
	registerBuiltinFilterOps()
}

// --- Filter Operator Registry ---

// FilterOp defines a function that compares a field value against a rule value.
type FilterOp func(fieldValue, ruleValue []byte) bool

var (
	operatorMu       sync.RWMutex
	operatorRegistry = make(map[string]FilterOp)
)

// registerBuiltinFilterOps populates the operator registry with default operators.
func registerBuiltinFilterOps() {
	builtins := map[string]FilterOp{
		"eq": func(fieldValue, ruleValue []byte) bool {
			return bytes.Equal(fieldValue, ruleValue)
		},
		"ne": func(fieldValue, ruleValue []byte) bool {
			return !bytes.Equal(fieldValue, ruleValue)
		},
		"contains": func(fieldValue, ruleValue []byte) bool {
			return bytes.Contains(fieldValue, ruleValue)
		},
		"prefix": func(fieldValue, ruleValue []byte) bool {
			return bytes.HasPrefix(fieldValue, ruleValue)
		},
		"suffix": func(fieldValue, ruleValue []byte) bool {
			return bytes.HasSuffix(fieldValue, ruleValue)
		},
		"regex": func(fieldValue, ruleValue []byte) bool {
			re, err := regexp.Compile(string(ruleValue))
			if err != nil {
				return false
			}
			return re.Match(fieldValue)
		},
		"empty": func(fieldValue, _ []byte) bool {
			return len(fieldValue) == 0
		},
		"notempty": func(fieldValue, _ []byte) bool {
			return len(fieldValue) > 0
		},
		"gt": func(fieldValue, ruleValue []byte) bool {
			return bytes.Compare(fieldValue, ruleValue) > 0
		},
		"lt": func(fieldValue, ruleValue []byte) bool {
			return bytes.Compare(fieldValue, ruleValue) < 0
		},
		"starts_with": func(fieldValue, ruleValue []byte) bool {
			return bytes.HasPrefix(fieldValue, ruleValue)
		},
		"ends_with": func(fieldValue, ruleValue []byte) bool {
			return bytes.HasSuffix(fieldValue, ruleValue)
		},
	}

	operatorMu.Lock()
	defer operatorMu.Unlock()
	for name, op := range builtins {
		operatorRegistry[name] = op
	}
}

// RegisterFilterOp registers a custom filter operator by name.
// If an operator with the same name already exists it is replaced.
func RegisterFilterOp(name string, op FilterOp) {
	operatorMu.Lock()
	defer operatorMu.Unlock()
	operatorRegistry[name] = op
}

// GetFilterOp retrieves a registered filter operator by name.
func GetFilterOp(name string) (FilterOp, bool) {
	operatorMu.RLock()
	defer operatorMu.RUnlock()
	op, ok := operatorRegistry[name]
	return op, ok
}

// --- FilterProcessor ---

// FilterProcessor drops events based on rules.
type FilterProcessor struct {
	rules []FilterRule
}

// FilterRule defines a single filter condition.
type FilterRule struct {
	Field    string // "case_id", "activity", "resource", or attribute name
	Operator string // "eq", "ne", "contains", "regex", "gt", "lt"
	Value    []byte
	Regex    *regexp.Regexp // Compiled if operator is "regex"
	Exclude  bool           // If true, matching events are dropped
}

// NewFilterProcessor creates a filter with the given rules.
func NewFilterProcessor(rules []FilterRule) *FilterProcessor {
	// Compile regex patterns
	for i := range rules {
		if rules[i].Operator == "regex" {
			rules[i].Regex, _ = regexp.Compile(string(rules[i].Value))
		}
	}

	return &FilterProcessor{rules: rules}
}

// Name returns the processor name.
func (p *FilterProcessor) Name() string {
	return "filter"
}

// Process implements Processor.Process.
func (p *FilterProcessor) Process(ctx context.Context, in <-chan *pipeline.Event, out chan<- *pipeline.Event) error {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-in:
			if !ok {
				return nil
			}

			if p.shouldKeep(event) {
				select {
				case out <- event:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
}

// shouldKeep returns true if the event passes all filters.
func (p *FilterProcessor) shouldKeep(event *pipeline.Event) bool {
	for _, rule := range p.rules {
		matches := p.matchesRule(event, rule)

		if rule.Exclude && matches {
			return false // Drop if exclusion rule matches
		}
		if !rule.Exclude && !matches {
			return false // Drop if inclusion rule doesn't match
		}
	}
	return true
}

// matchesRule checks if an event matches a single rule.
func (p *FilterProcessor) matchesRule(event *pipeline.Event, rule FilterRule) bool {
	var value []byte

	switch rule.Field {
	case "case_id":
		value = event.CaseID
	case "activity":
		value = event.Activity
	case "resource":
		value = event.Resource
	default:
		// Check attributes
		for _, attr := range event.Attributes {
			if string(attr.Key) == rule.Field {
				value = attr.Value
				break
			}
		}
	}

	return p.compareValue(value, rule)
}

// compareValue applies the operator to compare values.
// It first looks up the operator in the registry; if not found it falls back
// to the built-in switch for backward compatibility (particularly for "regex"
// which benefits from the pre-compiled Regex in FilterRule).
func (p *FilterProcessor) compareValue(value []byte, rule FilterRule) bool {
	// Special case: "regex" uses the pre-compiled regexp from FilterRule.
	if rule.Operator == "regex" {
		if rule.Regex != nil {
			return rule.Regex.Match(value)
		}
		return false
	}

	// Look up from the operator registry first.
	operatorMu.RLock()
	op, ok := operatorRegistry[rule.Operator]
	operatorMu.RUnlock()
	if ok {
		return op(value, rule.Value)
	}

	// Fallback: built-in switch for any unregistered operators.
	switch rule.Operator {
	case "eq":
		return bytes.Equal(value, rule.Value)
	case "ne":
		return !bytes.Equal(value, rule.Value)
	case "contains":
		return bytes.Contains(value, rule.Value)
	case "prefix":
		return bytes.HasPrefix(value, rule.Value)
	case "suffix":
		return bytes.HasSuffix(value, rule.Value)
	case "empty":
		return len(value) == 0
	case "notempty":
		return len(value) > 0
	default:
		return false
	}
}

// FilterProcessorFactory creates a FilterProcessor from config.
func FilterProcessorFactory(cfg pipeline.Config) (pipeline.Processor, error) {
	// Extract rules from config
	rules := []FilterRule{}

	if rulesOpt, ok := cfg.ProcessorOptions["filter_rules"].([]FilterRule); ok {
		rules = rulesOpt
	}

	return NewFilterProcessor(rules), nil
}

// --- Builder for constructing filter rules ---

// FilterBuilder provides a fluent interface for building filters.
type FilterBuilder struct {
	rules []FilterRule
}

// NewFilterBuilder creates a new filter builder.
func NewFilterBuilder() *FilterBuilder {
	return &FilterBuilder{rules: make([]FilterRule, 0)}
}

// Include adds an inclusion rule (keep matching events).
func (b *FilterBuilder) Include(field, operator string, value []byte) *FilterBuilder {
	b.rules = append(b.rules, FilterRule{
		Field:    field,
		Operator: operator,
		Value:    value,
		Exclude:  false,
	})
	return b
}

// Exclude adds an exclusion rule (drop matching events).
func (b *FilterBuilder) Exclude(field, operator string, value []byte) *FilterBuilder {
	b.rules = append(b.rules, FilterRule{
		Field:    field,
		Operator: operator,
		Value:    value,
		Exclude:  true,
	})
	return b
}

// Build creates the FilterProcessor.
func (b *FilterBuilder) Build() *FilterProcessor {
	return NewFilterProcessor(b.rules)
}
