// Package healing provides a self-healing parser that automatically fixes common data issues.
package healing

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Parser defines the interface for parsers that can be wrapped with healing.
type Parser interface {
	// Parse attempts to parse a row and returns the result or an error.
	Parse(row []byte) (interface{}, error)
}

// HealingParser wraps a parser with automatic error detection and fixing.
type HealingParser struct {
	inner    Parser
	rules    []FixRule
	detector *Detector
	stats    *FixStats
	cfg      HealingConfig
}

// HealingConfig configures the healing parser.
type HealingConfig struct {
	// MaxFixAttempts limits how many fix rules to try per row
	MaxFixAttempts int

	// FailOnUnfixable returns an error if the row can't be fixed
	FailOnUnfixable bool

	// EnableDetection runs pattern detection before fixing
	EnableDetection bool

	// RecordAllAttempts tracks all fix attempts (more memory)
	RecordAllAttempts bool
}

// DefaultHealingConfig returns sensible defaults.
func DefaultHealingConfig() HealingConfig {
	return HealingConfig{
		MaxFixAttempts:    5,
		FailOnUnfixable:   true,
		EnableDetection:   true,
		RecordAllAttempts: false,
	}
}

// FixStats tracks fix statistics for observability.
type FixStats struct {
	mu sync.RWMutex

	// Counters
	TotalRows     int64
	SuccessRows   int64
	FixedRows     int64
	UnfixableRows int64

	// Per-rule statistics
	RuleAttempts map[string]int64
	RuleSuccess  map[string]int64
	RuleFailed   map[string]int64

	// Pattern statistics (if detection enabled)
	PatternCounts map[PatternType]int64

	// Sample unfixable rows (for debugging)
	SampleUnfixable []UnfixableRow
	maxSamples      int
}

// UnfixableRow captures info about a row that couldn't be fixed.
type UnfixableRow struct {
	RowNumber int64
	RawData   []byte
	Error     error
	Patterns  []ErrorPattern
	Attempts  []FixAttempt
}

// FixAttempt records a single fix attempt.
type FixAttempt struct {
	Rule    string
	Success bool
	Error   error
}

// NewFixStats creates new fix statistics tracker.
func NewFixStats() *FixStats {
	return &FixStats{
		RuleAttempts:    make(map[string]int64),
		RuleSuccess:     make(map[string]int64),
		RuleFailed:      make(map[string]int64),
		PatternCounts:   make(map[PatternType]int64),
		SampleUnfixable: make([]UnfixableRow, 0, 100),
		maxSamples:      100,
	}
}

// RecordFix records a successful fix.
func (s *FixStats) RecordFix(ruleName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.FixedRows++
	s.RuleSuccess[ruleName]++
}

// RecordAttempt records a fix attempt.
func (s *FixStats) RecordAttempt(ruleName string, success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.RuleAttempts[ruleName]++
	if success {
		s.RuleSuccess[ruleName]++
	} else {
		s.RuleFailed[ruleName]++
	}
}

// RecordUnfixable records an unfixable row.
func (s *FixStats) RecordUnfixable(row UnfixableRow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	atomic.AddInt64(&s.UnfixableRows, 1)
	if len(s.SampleUnfixable) < s.maxSamples {
		s.SampleUnfixable = append(s.SampleUnfixable, row)
	}
}

// RecordPattern records a detected error pattern.
func (s *FixStats) RecordPattern(pattern PatternType) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.PatternCounts[pattern]++
}

// Summary returns a summary of fix statistics.
func (s *FixStats) Summary() FixSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return FixSummary{
		TotalRows:     atomic.LoadInt64(&s.TotalRows),
		SuccessRows:   atomic.LoadInt64(&s.SuccessRows),
		FixedRows:     atomic.LoadInt64(&s.FixedRows),
		UnfixableRows: atomic.LoadInt64(&s.UnfixableRows),
		FixRate:       s.calculateFixRate(),
		TopRules:      s.topRules(5),
		TopPatterns:   s.topPatterns(5),
	}
}

func (s *FixStats) calculateFixRate() float64 {
	total := atomic.LoadInt64(&s.TotalRows)
	if total == 0 {
		return 0
	}
	fixed := atomic.LoadInt64(&s.FixedRows)
	return float64(fixed) / float64(total) * 100
}

func (s *FixStats) topRules(n int) []RuleStats {
	var stats []RuleStats
	for name, attempts := range s.RuleAttempts {
		success := s.RuleSuccess[name]
		stats = append(stats, RuleStats{
			Name:        name,
			Attempts:    attempts,
			Successes:   success,
			SuccessRate: float64(success) / float64(attempts) * 100,
		})
	}

	// Sort by success count
	for i := 0; i < len(stats) && i < n; i++ {
		maxIdx := i
		for j := i + 1; j < len(stats); j++ {
			if stats[j].Successes > stats[maxIdx].Successes {
				maxIdx = j
			}
		}
		stats[i], stats[maxIdx] = stats[maxIdx], stats[i]
	}

	if len(stats) > n {
		stats = stats[:n]
	}
	return stats
}

func (s *FixStats) topPatterns(n int) []PatternStats {
	var stats []PatternStats
	for pattern, count := range s.PatternCounts {
		stats = append(stats, PatternStats{
			Type:  pattern,
			Count: count,
		})
	}

	// Sort by count
	for i := 0; i < len(stats) && i < n; i++ {
		maxIdx := i
		for j := i + 1; j < len(stats); j++ {
			if stats[j].Count > stats[maxIdx].Count {
				maxIdx = j
			}
		}
		stats[i], stats[maxIdx] = stats[maxIdx], stats[i]
	}

	if len(stats) > n {
		stats = stats[:n]
	}
	return stats
}

// FixSummary is a summary of fix statistics.
type FixSummary struct {
	TotalRows     int64
	SuccessRows   int64
	FixedRows     int64
	UnfixableRows int64
	FixRate       float64
	TopRules      []RuleStats
	TopPatterns   []PatternStats
}

// RuleStats shows per-rule statistics.
type RuleStats struct {
	Name        string
	Attempts    int64
	Successes   int64
	SuccessRate float64
}

// PatternStats shows per-pattern statistics.
type PatternStats struct {
	Type  PatternType
	Count int64
}

// NewHealingParser creates a new self-healing parser.
func NewHealingParser(inner Parser, rules []FixRule, cfg HealingConfig) *HealingParser {
	sortedRules := SortRulesByPriority(rules)

	return &HealingParser{
		inner:    inner,
		rules:    sortedRules,
		detector: NewDetector(DefaultDetectorConfig()),
		stats:    NewFixStats(),
		cfg:      cfg,
	}
}

// NewDefaultHealingParser creates a healing parser with default rules.
func NewDefaultHealingParser(inner Parser) *HealingParser {
	return NewHealingParser(inner, DefaultRules(), DefaultHealingConfig())
}

// Parse attempts to parse a row, fixing errors automatically.
func (h *HealingParser) Parse(row []byte) (interface{}, error) {
	atomic.AddInt64(&h.stats.TotalRows, 1)
	rowNum := atomic.LoadInt64(&h.stats.TotalRows)

	// Try parsing the original row first
	result, err := h.inner.Parse(row)
	if err == nil {
		atomic.AddInt64(&h.stats.SuccessRows, 1)
		return result, nil
	}

	// Detect error patterns if enabled
	var patterns []ErrorPattern
	if h.cfg.EnableDetection {
		patterns = h.detector.Detect(row)
		for _, p := range patterns {
			h.stats.RecordPattern(p.Type)
		}
	}

	// Try each fix rule
	var attempts []FixAttempt
	currentRow := row

	for i, rule := range h.rules {
		if i >= h.cfg.MaxFixAttempts {
			break
		}

		if rule.CanFix(currentRow, err) {
			h.stats.RecordAttempt(rule.Name(), false) // Will update on success

			fixed, fixErr := rule.Fix(currentRow)
			attempt := FixAttempt{
				Rule:    rule.Name(),
				Success: false,
				Error:   fixErr,
			}

			if fixErr == nil {
				// Try parsing the fixed row
				result, parseErr := h.inner.Parse(fixed)
				if parseErr == nil {
					// Success!
					h.stats.RecordFix(rule.Name())
					atomic.AddInt64(&h.stats.SuccessRows, 1)
					attempt.Success = true
					attempts = append(attempts, attempt)
					return result, nil
				}

				// Fix applied but still can't parse - try next rule
				currentRow = fixed
				err = parseErr
			}

			if h.cfg.RecordAllAttempts {
				attempts = append(attempts, attempt)
			}
		}
	}

	// Unable to fix
	if h.cfg.FailOnUnfixable {
		h.stats.RecordUnfixable(UnfixableRow{
			RowNumber: rowNum,
			RawData:   row,
			Error:     err,
			Patterns:  patterns,
			Attempts:  attempts,
		})
		return nil, fmt.Errorf("unable to parse row %d after %d fix attempts: %w",
			rowNum, len(attempts), err)
	}

	// Return original error if not failing on unfixable
	return nil, err
}

// ParseWithDetails parses a row and returns detailed fix information.
func (h *HealingParser) ParseWithDetails(row []byte) (*ParseResult, error) {
	atomic.AddInt64(&h.stats.TotalRows, 1)
	rowNum := atomic.LoadInt64(&h.stats.TotalRows)

	result := &ParseResult{
		RowNumber:     rowNum,
		OriginalRow:   row,
		FixedRow:      row,
		Patterns:      nil,
		FixesApplied:  nil,
		ParseAttempts: 0,
	}

	// Detect patterns
	if h.cfg.EnableDetection {
		result.Patterns = h.detector.Detect(row)
	}

	// Try parsing original
	parsed, err := h.inner.Parse(row)
	result.ParseAttempts++
	if err == nil {
		result.Value = parsed
		result.Success = true
		atomic.AddInt64(&h.stats.SuccessRows, 1)
		return result, nil
	}
	result.OriginalError = err

	// Try fixes
	currentRow := row
	for i, rule := range h.rules {
		if i >= h.cfg.MaxFixAttempts {
			break
		}

		if rule.CanFix(currentRow, err) {
			fixed, fixErr := rule.Fix(currentRow)
			if fixErr != nil {
				continue
			}

			parsed, parseErr := h.inner.Parse(fixed)
			result.ParseAttempts++

			if parseErr == nil {
				result.Value = parsed
				result.Success = true
				result.FixedRow = fixed
				result.FixesApplied = append(result.FixesApplied, rule.Name())
				h.stats.RecordFix(rule.Name())
				atomic.AddInt64(&h.stats.SuccessRows, 1)
				return result, nil
			}

			// Fix applied, continue with fixed row
			currentRow = fixed
			err = parseErr
			result.FixesApplied = append(result.FixesApplied, rule.Name()+" (partial)")
		}
	}

	result.FinalError = err
	return result, err
}

// ParseResult contains detailed parsing results.
type ParseResult struct {
	RowNumber     int64
	OriginalRow   []byte
	FixedRow      []byte
	Value         interface{}
	Success       bool
	Patterns      []ErrorPattern
	FixesApplied  []string
	ParseAttempts int
	OriginalError error
	FinalError    error
}

// Stats returns the fix statistics.
func (h *HealingParser) Stats() *FixStats {
	return h.stats
}

// Summary returns a summary of fix statistics.
func (h *HealingParser) Summary() FixSummary {
	return h.stats.Summary()
}

// AddRule adds a fix rule to the parser.
func (h *HealingParser) AddRule(rule FixRule) {
	h.rules = append(h.rules, rule)
	h.rules = SortRulesByPriority(h.rules)
}

// SetRules replaces all fix rules.
func (h *HealingParser) SetRules(rules []FixRule) {
	h.rules = SortRulesByPriority(rules)
}

// Rules returns the current fix rules.
func (h *HealingParser) Rules() []FixRule {
	return h.rules
}

// --- Convenience Functions ---

// FixRow attempts to fix a row using default rules.
func FixRow(row []byte) ([]byte, []string, error) {
	rules := DefaultRules()
	var appliedRules []string
	current := row

	for _, rule := range rules {
		if rule.CanFix(current, nil) {
			fixed, err := rule.Fix(current)
			if err == nil {
				current = fixed
				appliedRules = append(appliedRules, rule.Name())
			}
		}
	}

	return current, appliedRules, nil
}

// AutoDetectAndFix detects patterns and applies appropriate fixes.
func AutoDetectAndFix(row []byte) ([]byte, *AnalysisReport, error) {
	detector := NewDetector(DefaultDetectorConfig())
	patterns := detector.Detect(row)

	if len(patterns) == 0 {
		return row, nil, nil
	}

	// Build report
	report := &AnalysisReport{
		TotalRows:     1,
		RowsWithErrors: 1,
		PatternCounts: make(map[PatternType]int),
	}
	for _, p := range patterns {
		report.PatternCounts[p.Type]++
	}

	// Apply fixes based on detected patterns
	rules := DefaultRules()
	current := row

	for _, rule := range rules {
		if rule.CanFix(current, nil) {
			fixed, err := rule.Fix(current)
			if err == nil {
				current = fixed
				report.RecommendedRules = append(report.RecommendedRules, rule.Name())
			}
		}
	}

	return current, report, nil
}
