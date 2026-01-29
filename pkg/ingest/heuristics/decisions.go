package heuristics

import "github.com/logflow/logflow/pkg/ingest/core"

// DecisionTable provides rule-based overrides.
type DecisionTable struct {
	rules []DecisionRule
}

// DecisionRule defines conditions and actions.
type DecisionRule struct {
	// Conditions
	Format      core.Format
	MinSize     int64
	MaxSize     int64
	IsClean     bool
	CheckClean  bool

	// Actions
	Strategy     Strategy
	BatchSize    int
	RowGroupSize int
	Compression  string
}

// NewDecisionTable creates a table with default rules.
func NewDecisionTable() *DecisionTable {
	return &DecisionTable{
		rules: []DecisionRule{
			// Large clean CSV
			{
				Format:       core.FormatCSV,
				MinSize:      100 * 1024 * 1024,
				CheckClean:   true,
				IsClean:      true,
				Strategy:     StrategyFastDuckDB,
				BatchSize:    16384,
				RowGroupSize: 100000,
				Compression:  "snappy",
			},
			// XES always robust
			{
				Format:   core.FormatXES,
				Strategy: StrategyRobustGo,
			},
			// Parquet re-compression
			{
				Format:      core.FormatParquet,
				Strategy:    StrategyFastDuckDB,
				Compression: "zstd",
			},
		},
	}
}

// Lookup finds matching rule.
func (t *DecisionTable) Lookup(format core.Format, size int64, isClean bool) *DecisionRule {
	for i := range t.rules {
		rule := &t.rules[i]

		if rule.Format != core.FormatUnknown && rule.Format != format {
			continue
		}

		if rule.MinSize > 0 && size < rule.MinSize {
			continue
		}

		if rule.MaxSize > 0 && size > rule.MaxSize {
			continue
		}

		if rule.CheckClean && rule.IsClean != isClean {
			continue
		}

		return rule
	}
	return nil
}

// ApplyRule applies a rule to a plan.
func (p *Plan) ApplyRule(rule *DecisionRule) {
	if rule.Strategy != 0 {
		p.Strategy = rule.Strategy
	}
	if rule.BatchSize > 0 {
		p.BatchSize = rule.BatchSize
	}
	if rule.RowGroupSize > 0 {
		p.RowGroupSize = rule.RowGroupSize
	}
	if rule.Compression != "" {
		p.Compression = rule.Compression
	}
}
