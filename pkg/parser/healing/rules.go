// Package healing provides rule-based auto-detection and fixing of malformed data rows.
package healing

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// FixRule defines the interface for data repair rules.
type FixRule interface {
	// Name returns the rule name for logging/stats.
	Name() string

	// Description returns a human-readable description of what this rule fixes.
	Description() string

	// CanFix returns true if this rule can fix the given error.
	CanFix(row []byte, err error) bool

	// Fix attempts to repair the row. Returns the fixed row or an error.
	Fix(row []byte) ([]byte, error)

	// Priority returns the rule priority (lower = higher priority).
	// Rules are applied in priority order.
	Priority() int
}

// --- Built-in Fix Rules ---

// FixEncodingRule detects and converts UTF-16/Latin-1 to UTF-8.
type FixEncodingRule struct{}

func NewFixEncodingRule() *FixEncodingRule {
	return &FixEncodingRule{}
}

func (r *FixEncodingRule) Name() string        { return "encoding" }
func (r *FixEncodingRule) Description() string { return "Convert UTF-16/Latin-1 to UTF-8" }
func (r *FixEncodingRule) Priority() int       { return 10 }

func (r *FixEncodingRule) CanFix(row []byte, err error) bool {
	// Check for invalid UTF-8
	if !utf8.Valid(row) {
		return true
	}
	// Check for BOM markers
	if hasBOM(row) {
		return true
	}
	return false
}

func (r *FixEncodingRule) Fix(row []byte) ([]byte, error) {
	// Remove BOM if present
	row = stripBOM(row)

	// If invalid UTF-8, try to fix
	if !utf8.Valid(row) {
		// Try Latin-1 to UTF-8 conversion
		fixed := make([]byte, 0, len(row)*2)
		for _, b := range row {
			if b < 0x80 {
				fixed = append(fixed, b)
			} else {
				// Convert Latin-1 extended chars to UTF-8
				fixed = append(fixed, 0xC0|(b>>6), 0x80|(b&0x3F))
			}
		}
		return fixed, nil
	}

	return row, nil
}

func hasBOM(data []byte) bool {
	if len(data) >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		return true // UTF-8 BOM
	}
	if len(data) >= 2 {
		if data[0] == 0xFE && data[1] == 0xFF {
			return true // UTF-16 BE BOM
		}
		if data[0] == 0xFF && data[1] == 0xFE {
			return true // UTF-16 LE BOM
		}
	}
	return false
}

func stripBOM(data []byte) []byte {
	if len(data) >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		return data[3:]
	}
	if len(data) >= 2 {
		if (data[0] == 0xFE && data[1] == 0xFF) || (data[0] == 0xFF && data[1] == 0xFE) {
			return data[2:]
		}
	}
	return data
}

// FixQuotingRule balances unmatched quotes in CSV/text data.
type FixQuotingRule struct {
	quoteChar byte
}

func NewFixQuotingRule() *FixQuotingRule {
	return &FixQuotingRule{quoteChar: '"'}
}

func (r *FixQuotingRule) WithQuoteChar(c byte) *FixQuotingRule {
	r.quoteChar = c
	return r
}

func (r *FixQuotingRule) Name() string        { return "quoting" }
func (r *FixQuotingRule) Description() string { return "Balance unmatched quotes" }
func (r *FixQuotingRule) Priority() int       { return 20 }

func (r *FixQuotingRule) CanFix(row []byte, err error) bool {
	// Check for odd number of quotes (unbalanced)
	count := bytes.Count(row, []byte{r.quoteChar})
	return count%2 != 0
}

func (r *FixQuotingRule) Fix(row []byte) ([]byte, error) {
	count := bytes.Count(row, []byte{r.quoteChar})
	if count%2 == 0 {
		return row, nil // Already balanced
	}

	// Strategy: find the last quote and determine if we should add or remove
	lastQuoteIdx := bytes.LastIndexByte(row, r.quoteChar)

	// Check if last quote is at end of field (near delimiter or EOL)
	if lastQuoteIdx == len(row)-1 || (lastQuoteIdx < len(row)-1 && (row[lastQuoteIdx+1] == ',' || row[lastQuoteIdx+1] == '\n')) {
		// Missing opening quote - add at start of field
		// Find the delimiter before the last quote
		fieldStart := lastQuoteIdx
		for fieldStart > 0 && row[fieldStart-1] != ',' {
			fieldStart--
		}

		// Insert quote at field start
		fixed := make([]byte, 0, len(row)+1)
		fixed = append(fixed, row[:fieldStart]...)
		fixed = append(fixed, r.quoteChar)
		fixed = append(fixed, row[fieldStart:]...)
		return fixed, nil
	}

	// Likely missing closing quote - add at end
	return append(row, r.quoteChar), nil
}

// FixDelimiterRule normalizes mixed delimiters (e.g., tabs vs commas).
type FixDelimiterRule struct {
	targetDelimiter byte
	candidates      []byte
}

func NewFixDelimiterRule() *FixDelimiterRule {
	return &FixDelimiterRule{
		targetDelimiter: ',',
		candidates:      []byte{'\t', ';', '|'},
	}
}

func (r *FixDelimiterRule) WithTargetDelimiter(d byte) *FixDelimiterRule {
	r.targetDelimiter = d
	return r
}

func (r *FixDelimiterRule) Name() string        { return "delimiter" }
func (r *FixDelimiterRule) Description() string { return "Normalize mixed delimiters" }
func (r *FixDelimiterRule) Priority() int       { return 30 }

func (r *FixDelimiterRule) CanFix(row []byte, err error) bool {
	// Check if row has alternative delimiters
	targetCount := bytes.Count(row, []byte{r.targetDelimiter})

	for _, candidate := range r.candidates {
		candidateCount := bytes.Count(row, []byte{candidate})
		// If candidate appears more than target, likely wrong delimiter
		if candidateCount > targetCount && candidateCount > 2 {
			return true
		}
	}
	return false
}

func (r *FixDelimiterRule) Fix(row []byte) ([]byte, error) {
	// Find the most likely actual delimiter
	targetCount := bytes.Count(row, []byte{r.targetDelimiter})
	bestCandidate := r.targetDelimiter
	bestCount := targetCount

	for _, candidate := range r.candidates {
		count := bytes.Count(row, []byte{candidate})
		if count > bestCount {
			bestCandidate = candidate
			bestCount = count
		}
	}

	if bestCandidate == r.targetDelimiter {
		return row, nil // No change needed
	}

	// Replace the wrong delimiter with the target
	fixed := bytes.ReplaceAll(row, []byte{bestCandidate}, []byte{r.targetDelimiter})
	return fixed, nil
}

// FixNewlineRule handles \r\n vs \n inconsistencies.
type FixNewlineRule struct{}

func NewFixNewlineRule() *FixNewlineRule {
	return &FixNewlineRule{}
}

func (r *FixNewlineRule) Name() string        { return "newline" }
func (r *FixNewlineRule) Description() string { return "Normalize \\r\\n to \\n" }
func (r *FixNewlineRule) Priority() int       { return 40 }

func (r *FixNewlineRule) CanFix(row []byte, err error) bool {
	return bytes.Contains(row, []byte{'\r'})
}

func (r *FixNewlineRule) Fix(row []byte) ([]byte, error) {
	// Replace \r\n with \n
	fixed := bytes.ReplaceAll(row, []byte{'\r', '\n'}, []byte{'\n'})
	// Remove remaining \r
	fixed = bytes.ReplaceAll(fixed, []byte{'\r'}, []byte{})
	return fixed, nil
}

// FixNullByteRule strips embedded null bytes.
type FixNullByteRule struct{}

func NewFixNullByteRule() *FixNullByteRule {
	return &FixNullByteRule{}
}

func (r *FixNullByteRule) Name() string        { return "null_byte" }
func (r *FixNullByteRule) Description() string { return "Strip embedded null bytes" }
func (r *FixNullByteRule) Priority() int       { return 50 }

func (r *FixNullByteRule) CanFix(row []byte, err error) bool {
	return bytes.Contains(row, []byte{0})
}

func (r *FixNullByteRule) Fix(row []byte) ([]byte, error) {
	return bytes.ReplaceAll(row, []byte{0}, []byte{}), nil
}

// FixWhitespaceRule trims and normalizes whitespace.
type FixWhitespaceRule struct {
	trimLeading  bool
	trimTrailing bool
	normalizeSpace bool
}

func NewFixWhitespaceRule() *FixWhitespaceRule {
	return &FixWhitespaceRule{
		trimLeading:    true,
		trimTrailing:   true,
		normalizeSpace: false, // Don't collapse internal spaces by default
	}
}

func (r *FixWhitespaceRule) Name() string        { return "whitespace" }
func (r *FixWhitespaceRule) Description() string { return "Trim and normalize whitespace" }
func (r *FixWhitespaceRule) Priority() int       { return 60 }

func (r *FixWhitespaceRule) CanFix(row []byte, err error) bool {
	// Check for leading/trailing whitespace
	if len(row) > 0 {
		if r.trimLeading && (row[0] == ' ' || row[0] == '\t') {
			return true
		}
		if r.trimTrailing && (row[len(row)-1] == ' ' || row[len(row)-1] == '\t') {
			return true
		}
	}
	return false
}

func (r *FixWhitespaceRule) Fix(row []byte) ([]byte, error) {
	result := row

	if r.trimLeading {
		result = bytes.TrimLeft(result, " \t")
	}
	if r.trimTrailing {
		result = bytes.TrimRight(result, " \t")
	}

	return result, nil
}

// FixEscapeRule handles malformed escape sequences.
type FixEscapeRule struct {
	escapeChar byte
}

func NewFixEscapeRule() *FixEscapeRule {
	return &FixEscapeRule{escapeChar: '\\'}
}

func (r *FixEscapeRule) Name() string        { return "escape" }
func (r *FixEscapeRule) Description() string { return "Fix malformed escape sequences" }
func (r *FixEscapeRule) Priority() int       { return 70 }

func (r *FixEscapeRule) CanFix(row []byte, err error) bool {
	// Check for trailing escape char (incomplete escape)
	if len(row) > 0 && row[len(row)-1] == r.escapeChar {
		return true
	}
	// Check for invalid escape sequences
	for i := 0; i < len(row)-1; i++ {
		if row[i] == r.escapeChar {
			next := row[i+1]
			// Common valid escapes: \n, \t, \r, \\, \", \'
			if next != 'n' && next != 't' && next != 'r' && next != r.escapeChar &&
				next != '"' && next != '\'' && next != '0' {
				return true
			}
		}
	}
	return false
}

func (r *FixEscapeRule) Fix(row []byte) ([]byte, error) {
	var fixed []byte
	i := 0
	for i < len(row) {
		if row[i] == r.escapeChar {
			if i+1 >= len(row) {
				// Trailing escape - remove it
				break
			}
			next := row[i+1]
			switch next {
			case 'n':
				fixed = append(fixed, '\n')
				i += 2
			case 't':
				fixed = append(fixed, '\t')
				i += 2
			case 'r':
				fixed = append(fixed, '\r')
				i += 2
			case r.escapeChar:
				fixed = append(fixed, r.escapeChar)
				i += 2
			case '"', '\'':
				fixed = append(fixed, next)
				i += 2
			case '0':
				// Null byte - skip
				i += 2
			default:
				// Invalid escape - keep as literal
				fixed = append(fixed, row[i])
				i++
			}
		} else {
			fixed = append(fixed, row[i])
			i++
		}
	}
	return fixed, nil
}

// FixHTMLEntityRule converts HTML entities to their character equivalents.
type FixHTMLEntityRule struct{}

var htmlEntityPattern = regexp.MustCompile(`&(#[0-9]+|#x[0-9a-fA-F]+|[a-zA-Z]+);`)

var htmlEntityMap = map[string]string{
	"amp":   "&",
	"lt":    "<",
	"gt":    ">",
	"quot":  "\"",
	"apos":  "'",
	"nbsp":  " ",
	"ndash": "-",
	"mdash": "--",
	"copy":  "(c)",
	"reg":   "(r)",
	"trade": "(tm)",
}

func NewFixHTMLEntityRule() *FixHTMLEntityRule {
	return &FixHTMLEntityRule{}
}

func (r *FixHTMLEntityRule) Name() string        { return "html_entity" }
func (r *FixHTMLEntityRule) Description() string { return "Convert HTML entities to characters" }
func (r *FixHTMLEntityRule) Priority() int       { return 80 }

func (r *FixHTMLEntityRule) CanFix(row []byte, err error) bool {
	return htmlEntityPattern.Match(row)
}

func (r *FixHTMLEntityRule) Fix(row []byte) ([]byte, error) {
	fixed := htmlEntityPattern.ReplaceAllFunc(row, func(match []byte) []byte {
		entity := string(match[1 : len(match)-1]) // Strip & and ;

		// Check named entities
		if replacement, ok := htmlEntityMap[strings.ToLower(entity)]; ok {
			return []byte(replacement)
		}

		// Handle numeric entities
		if len(entity) > 1 && entity[0] == '#' {
			var codePoint int
			if len(entity) > 2 && (entity[1] == 'x' || entity[1] == 'X') {
				// Hex: &#xFF;
				_, _ = fmt.Sscanf(entity[2:], "%x", &codePoint)
			} else {
				// Decimal: &#255;
				_, _ = fmt.Sscanf(entity[1:], "%d", &codePoint)
			}
			if codePoint > 0 && codePoint <= 0x10FFFF {
				return []byte(string(rune(codePoint)))
			}
		}

		return match // Return unchanged if not recognized
	})

	return fixed, nil
}

// FixColumnCountRule handles rows with incorrect column counts.
type FixColumnCountRule struct {
	expectedColumns int
	delimiter       byte
	fillValue       string
}

func NewFixColumnCountRule(expectedColumns int) *FixColumnCountRule {
	return &FixColumnCountRule{
		expectedColumns: expectedColumns,
		delimiter:       ',',
		fillValue:       "",
	}
}

func (r *FixColumnCountRule) WithDelimiter(d byte) *FixColumnCountRule {
	r.delimiter = d
	return r
}

func (r *FixColumnCountRule) WithFillValue(v string) *FixColumnCountRule {
	r.fillValue = v
	return r
}

func (r *FixColumnCountRule) Name() string        { return "column_count" }
func (r *FixColumnCountRule) Description() string { return "Fix incorrect column counts" }
func (r *FixColumnCountRule) Priority() int       { return 90 }

func (r *FixColumnCountRule) CanFix(row []byte, err error) bool {
	if r.expectedColumns <= 0 {
		return false
	}
	actualColumns := bytes.Count(row, []byte{r.delimiter}) + 1
	return actualColumns != r.expectedColumns
}

func (r *FixColumnCountRule) Fix(row []byte) ([]byte, error) {
	actualColumns := bytes.Count(row, []byte{r.delimiter}) + 1

	if actualColumns < r.expectedColumns {
		// Add missing columns
		missing := r.expectedColumns - actualColumns
		for i := 0; i < missing; i++ {
			row = append(row, r.delimiter)
			row = append(row, []byte(r.fillValue)...)
		}
	} else if actualColumns > r.expectedColumns {
		// Truncate extra columns
		fields := bytes.Split(row, []byte{r.delimiter})
		row = bytes.Join(fields[:r.expectedColumns], []byte{r.delimiter})
	}

	return row, nil
}

// --- Default Rule Set ---

// DefaultRules returns the default set of fix rules.
func DefaultRules() []FixRule {
	return []FixRule{
		NewFixEncodingRule(),
		NewFixQuotingRule(),
		NewFixDelimiterRule(),
		NewFixNewlineRule(),
		NewFixNullByteRule(),
		NewFixWhitespaceRule(),
		NewFixEscapeRule(),
		NewFixHTMLEntityRule(),
	}
}

// SortRulesByPriority sorts rules by their priority (lower first).
func SortRulesByPriority(rules []FixRule) []FixRule {
	sorted := make([]FixRule, len(rules))
	copy(sorted, rules)

	for i := 0; i < len(sorted); i++ {
		minIdx := i
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j].Priority() < sorted[minIdx].Priority() {
				minIdx = j
			}
		}
		sorted[i], sorted[minIdx] = sorted[minIdx], sorted[i]
	}

	return sorted
}
