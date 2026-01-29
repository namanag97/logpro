// Package processors provides Processor implementations for data transformation.
package processors

import (
	"bytes"
	"context"
	"regexp"

	"github.com/logflow/logflow/pkg/pipeline"
)

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
func (p *FilterProcessor) compareValue(value []byte, rule FilterRule) bool {
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
	case "regex":
		if rule.Regex != nil {
			return rule.Regex.Match(value)
		}
		return false
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
