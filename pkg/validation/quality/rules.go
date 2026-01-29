// Package quality provides data quality validation rules.
// Similar to Great Expectations - configurable rules for data quality checks.
package quality

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Rule represents a data quality validation rule.
type Rule interface {
	// Name returns the rule name.
	Name() string
	// Validate checks a value against the rule.
	Validate(value interface{}) *ValidationResult
	// Column returns the column this rule applies to (empty for row-level rules).
	Column() string
}

// ValidationResult contains the outcome of a validation.
type ValidationResult struct {
	Valid       bool
	RuleName    string
	Column      string
	Value       interface{}
	Message     string
	Severity    Severity
	Suggestions []string
}

// Severity indicates the importance of a validation failure.
type Severity int

const (
	SeverityWarning Severity = iota
	SeverityError
	SeverityCritical
)

func (s Severity) String() string {
	switch s {
	case SeverityWarning:
		return "warning"
	case SeverityError:
		return "error"
	case SeverityCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// --- Built-in Rules ---

// NotNullRule ensures a value is not null/empty.
type NotNullRule struct {
	column   string
	severity Severity
}

func NewNotNullRule(column string) *NotNullRule {
	return &NotNullRule{column: column, severity: SeverityError}
}

func (r *NotNullRule) Name() string   { return "not_null" }
func (r *NotNullRule) Column() string { return r.column }

func (r *NotNullRule) WithSeverity(s Severity) *NotNullRule {
	r.severity = s
	return r
}

func (r *NotNullRule) Validate(value interface{}) *ValidationResult {
	valid := value != nil
	if s, ok := value.(string); ok {
		valid = strings.TrimSpace(s) != ""
	}
	if b, ok := value.([]byte); ok {
		valid = len(b) > 0
	}

	return &ValidationResult{
		Valid:    valid,
		RuleName: r.Name(),
		Column:   r.column,
		Value:    value,
		Message:  fmt.Sprintf("Column '%s' must not be null or empty", r.column),
		Severity: r.severity,
	}
}

// RangeRule ensures a numeric value is within a range.
type RangeRule struct {
	column   string
	min      *float64
	max      *float64
	severity Severity
}

func NewRangeRule(column string) *RangeRule {
	return &RangeRule{column: column, severity: SeverityError}
}

func (r *RangeRule) Name() string   { return "range" }
func (r *RangeRule) Column() string { return r.column }

func (r *RangeRule) Min(v float64) *RangeRule {
	r.min = &v
	return r
}

func (r *RangeRule) Max(v float64) *RangeRule {
	r.max = &v
	return r
}

func (r *RangeRule) WithSeverity(s Severity) *RangeRule {
	r.severity = s
	return r
}

func (r *RangeRule) Validate(value interface{}) *ValidationResult {
	num, err := toFloat64(value)
	if err != nil {
		return &ValidationResult{
			Valid:    false,
			RuleName: r.Name(),
			Column:   r.column,
			Value:    value,
			Message:  fmt.Sprintf("Column '%s' is not a valid number", r.column),
			Severity: r.severity,
		}
	}

	valid := true
	var msg string

	if r.min != nil && num < *r.min {
		valid = false
		msg = fmt.Sprintf("Column '%s' value %v is less than minimum %v", r.column, num, *r.min)
	}
	if r.max != nil && num > *r.max {
		valid = false
		msg = fmt.Sprintf("Column '%s' value %v is greater than maximum %v", r.column, num, *r.max)
	}

	if valid {
		msg = "Value is within range"
	}

	return &ValidationResult{
		Valid:    valid,
		RuleName: r.Name(),
		Column:   r.column,
		Value:    value,
		Message:  msg,
		Severity: r.severity,
	}
}

// RegexRule ensures a value matches a pattern.
type RegexRule struct {
	column   string
	pattern  *regexp.Regexp
	severity Severity
	desc     string
}

func NewRegexRule(column, pattern string) (*RegexRule, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern: %w", err)
	}
	return &RegexRule{
		column:   column,
		pattern:  re,
		severity: SeverityError,
		desc:     pattern,
	}, nil
}

func (r *RegexRule) Name() string   { return "regex" }
func (r *RegexRule) Column() string { return r.column }

func (r *RegexRule) WithSeverity(s Severity) *RegexRule {
	r.severity = s
	return r
}

func (r *RegexRule) WithDescription(desc string) *RegexRule {
	r.desc = desc
	return r
}

func (r *RegexRule) Validate(value interface{}) *ValidationResult {
	str := toString(value)
	valid := r.pattern.MatchString(str)

	return &ValidationResult{
		Valid:    valid,
		RuleName: r.Name(),
		Column:   r.column,
		Value:    value,
		Message:  fmt.Sprintf("Column '%s' does not match pattern: %s", r.column, r.desc),
		Severity: r.severity,
	}
}

// InSetRule ensures a value is in an allowed set.
type InSetRule struct {
	column     string
	allowedSet map[string]bool
	severity   Severity
}

func NewInSetRule(column string, allowed []string) *InSetRule {
	set := make(map[string]bool)
	for _, v := range allowed {
		set[strings.ToLower(v)] = true
	}
	return &InSetRule{
		column:     column,
		allowedSet: set,
		severity:   SeverityError,
	}
}

func (r *InSetRule) Name() string   { return "in_set" }
func (r *InSetRule) Column() string { return r.column }

func (r *InSetRule) WithSeverity(s Severity) *InSetRule {
	r.severity = s
	return r
}

func (r *InSetRule) Validate(value interface{}) *ValidationResult {
	str := strings.ToLower(toString(value))
	valid := r.allowedSet[str]

	var suggestions []string
	if !valid {
		for k := range r.allowedSet {
			suggestions = append(suggestions, k)
		}
	}

	return &ValidationResult{
		Valid:       valid,
		RuleName:    r.Name(),
		Column:      r.column,
		Value:       value,
		Message:     fmt.Sprintf("Column '%s' value '%v' not in allowed set", r.column, value),
		Severity:    r.severity,
		Suggestions: suggestions,
	}
}

// DateFormatRule ensures a date value matches expected format.
type DateFormatRule struct {
	column   string
	formats  []string
	severity Severity
}

func NewDateFormatRule(column string, formats ...string) *DateFormatRule {
	if len(formats) == 0 {
		formats = []string{
			"2006-01-02",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z",
			"2006-01-02T15:04:05-07:00",
			time.RFC3339,
		}
	}
	return &DateFormatRule{
		column:   column,
		formats:  formats,
		severity: SeverityError,
	}
}

func (r *DateFormatRule) Name() string   { return "date_format" }
func (r *DateFormatRule) Column() string { return r.column }

func (r *DateFormatRule) WithSeverity(s Severity) *DateFormatRule {
	r.severity = s
	return r
}

func (r *DateFormatRule) Validate(value interface{}) *ValidationResult {
	str := toString(value)

	for _, format := range r.formats {
		if _, err := time.Parse(format, str); err == nil {
			return &ValidationResult{
				Valid:    true,
				RuleName: r.Name(),
				Column:   r.column,
				Value:    value,
			}
		}
	}

	return &ValidationResult{
		Valid:       false,
		RuleName:    r.Name(),
		Column:      r.column,
		Value:       value,
		Message:     fmt.Sprintf("Column '%s' value '%v' is not a valid date", r.column, value),
		Severity:    r.severity,
		Suggestions: r.formats,
	}
}

// LengthRule ensures a string value has length within bounds.
type LengthRule struct {
	column   string
	min      *int
	max      *int
	severity Severity
}

func NewLengthRule(column string) *LengthRule {
	return &LengthRule{column: column, severity: SeverityError}
}

func (r *LengthRule) Name() string   { return "length" }
func (r *LengthRule) Column() string { return r.column }

func (r *LengthRule) Min(v int) *LengthRule {
	r.min = &v
	return r
}

func (r *LengthRule) Max(v int) *LengthRule {
	r.max = &v
	return r
}

func (r *LengthRule) WithSeverity(s Severity) *LengthRule {
	r.severity = s
	return r
}

func (r *LengthRule) Validate(value interface{}) *ValidationResult {
	str := toString(value)
	length := len(str)

	valid := true
	var msg string

	if r.min != nil && length < *r.min {
		valid = false
		msg = fmt.Sprintf("Column '%s' length %d is less than minimum %d", r.column, length, *r.min)
	}
	if r.max != nil && length > *r.max {
		valid = false
		msg = fmt.Sprintf("Column '%s' length %d exceeds maximum %d", r.column, length, *r.max)
	}

	return &ValidationResult{
		Valid:    valid,
		RuleName: r.Name(),
		Column:   r.column,
		Value:    value,
		Message:  msg,
		Severity: r.severity,
	}
}

// UniqueRule tracks unique values (stateful).
type UniqueRule struct {
	column   string
	seen     map[string]bool
	severity Severity
}

func NewUniqueRule(column string) *UniqueRule {
	return &UniqueRule{
		column:   column,
		seen:     make(map[string]bool),
		severity: SeverityError,
	}
}

func (r *UniqueRule) Name() string   { return "unique" }
func (r *UniqueRule) Column() string { return r.column }

func (r *UniqueRule) WithSeverity(s Severity) *UniqueRule {
	r.severity = s
	return r
}

func (r *UniqueRule) Validate(value interface{}) *ValidationResult {
	str := toString(value)

	if r.seen[str] {
		return &ValidationResult{
			Valid:    false,
			RuleName: r.Name(),
			Column:   r.column,
			Value:    value,
			Message:  fmt.Sprintf("Column '%s' has duplicate value: %v", r.column, value),
			Severity: r.severity,
		}
	}

	r.seen[str] = true
	return &ValidationResult{Valid: true, RuleName: r.Name(), Column: r.column, Value: value}
}

func (r *UniqueRule) Reset() {
	r.seen = make(map[string]bool)
}

// --- Validator ---

// Validator runs multiple rules against data.
type Validator struct {
	rules      []Rule
	stopOnFail bool
}

// NewValidator creates a new validator.
func NewValidator() *Validator {
	return &Validator{}
}

// AddRule adds a rule to the validator.
func (v *Validator) AddRule(rule Rule) *Validator {
	v.rules = append(v.rules, rule)
	return v
}

// StopOnFirstFailure configures validator to stop on first failure.
func (v *Validator) StopOnFirstFailure(stop bool) *Validator {
	v.stopOnFail = stop
	return v
}

// ValidateRow validates a row against all rules.
func (v *Validator) ValidateRow(row map[string]interface{}) []*ValidationResult {
	var results []*ValidationResult

	for _, rule := range v.rules {
		col := rule.Column()
		value := row[col]

		result := rule.Validate(value)
		if !result.Valid {
			results = append(results, result)
			if v.stopOnFail {
				return results
			}
		}
	}

	return results
}

// IsValid returns true if row passes all rules.
func (v *Validator) IsValid(row map[string]interface{}) bool {
	return len(v.ValidateRow(row)) == 0
}

// --- Helpers ---

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func toFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case string:
		return strconv.ParseFloat(val, 64)
	case []byte:
		return strconv.ParseFloat(string(val), 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

// --- Predefined Rule Sets ---

// EmailRule returns a regex rule for email validation.
func EmailRule(column string) *RegexRule {
	rule, _ := NewRegexRule(column, `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	return rule.WithDescription("valid email format")
}

// URLRule returns a regex rule for URL validation.
func URLRule(column string) *RegexRule {
	rule, _ := NewRegexRule(column, `^https?://[^\s/$.?#].[^\s]*$`)
	return rule.WithDescription("valid URL format")
}

// UUIDRule returns a regex rule for UUID validation.
func UUIDRule(column string) *RegexRule {
	rule, _ := NewRegexRule(column, `^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	return rule.WithDescription("valid UUID format")
}

// PhoneRule returns a regex rule for phone number validation.
func PhoneRule(column string) *RegexRule {
	rule, _ := NewRegexRule(column, `^\+?[1-9]\d{1,14}$`)
	return rule.WithDescription("valid phone number (E.164 format)")
}

// PositiveNumberRule returns a range rule for positive numbers.
func PositiveNumberRule(column string) *RangeRule {
	return NewRangeRule(column).Min(0)
}
