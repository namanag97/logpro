// Package healing provides error pattern detection for malformed data.
package healing

import (
	"bytes"
	"regexp"
	"strings"
	"unicode/utf8"
)

// ErrorPattern represents a detected error pattern.
type ErrorPattern struct {
	Type        PatternType
	Description string
	Location    int    // Byte offset where error was detected
	Context     string // Surrounding context for debugging
	Confidence  float64 // 0.0 to 1.0
}

// PatternType categorizes detected error patterns.
type PatternType int

const (
	PatternUnknown PatternType = iota
	PatternEncodingError
	PatternUnbalancedQuotes
	PatternWrongDelimiter
	PatternInconsistentNewlines
	PatternNullBytes
	PatternExcessiveWhitespace
	PatternMalformedEscape
	PatternHTMLEntities
	PatternColumnMismatch
	PatternTruncatedData
	PatternBinaryData
)

// String returns the pattern type name.
func (p PatternType) String() string {
	switch p {
	case PatternEncodingError:
		return "encoding_error"
	case PatternUnbalancedQuotes:
		return "unbalanced_quotes"
	case PatternWrongDelimiter:
		return "wrong_delimiter"
	case PatternInconsistentNewlines:
		return "inconsistent_newlines"
	case PatternNullBytes:
		return "null_bytes"
	case PatternExcessiveWhitespace:
		return "excessive_whitespace"
	case PatternMalformedEscape:
		return "malformed_escape"
	case PatternHTMLEntities:
		return "html_entities"
	case PatternColumnMismatch:
		return "column_mismatch"
	case PatternTruncatedData:
		return "truncated_data"
	case PatternBinaryData:
		return "binary_data"
	default:
		return "unknown"
	}
}

// Detector analyzes data for error patterns.
type Detector struct {
	// Configuration
	cfg DetectorConfig

	// Compiled patterns
	htmlEntityRegex *regexp.Regexp
	escapeRegex     *regexp.Regexp
}

// DetectorConfig configures the error detector.
type DetectorConfig struct {
	// ExpectedColumns is the expected column count (0 = auto-detect)
	ExpectedColumns int

	// Delimiter is the expected field delimiter
	Delimiter byte

	// QuoteChar is the expected quote character
	QuoteChar byte

	// MaxSampleSize limits the bytes analyzed for pattern detection
	MaxSampleSize int

	// BinaryThreshold is the ratio of non-printable chars to trigger binary detection
	BinaryThreshold float64
}

// DefaultDetectorConfig returns sensible defaults.
func DefaultDetectorConfig() DetectorConfig {
	return DetectorConfig{
		Delimiter:       ',',
		QuoteChar:       '"',
		MaxSampleSize:   4096,
		BinaryThreshold: 0.1,
	}
}

// NewDetector creates a new error pattern detector.
func NewDetector(cfg DetectorConfig) *Detector {
	return &Detector{
		cfg:             cfg,
		htmlEntityRegex: regexp.MustCompile(`&(#[0-9]+|#x[0-9a-fA-F]+|[a-zA-Z]+);`),
		escapeRegex:     regexp.MustCompile(`\\[^nrtv0'"\\]`),
	}
}

// Detect analyzes a row and returns all detected error patterns.
func (d *Detector) Detect(row []byte) []ErrorPattern {
	var patterns []ErrorPattern

	// Limit sample size
	sample := row
	if len(sample) > d.cfg.MaxSampleSize {
		sample = sample[:d.cfg.MaxSampleSize]
	}

	// Check for binary data first
	if binaryPattern := d.detectBinary(sample); binaryPattern != nil {
		patterns = append(patterns, *binaryPattern)
		return patterns // Don't analyze further if binary
	}

	// Run all pattern detectors
	if p := d.detectEncoding(sample); p != nil {
		patterns = append(patterns, *p)
	}
	if p := d.detectQuoting(sample); p != nil {
		patterns = append(patterns, *p)
	}
	if p := d.detectDelimiter(sample); p != nil {
		patterns = append(patterns, *p)
	}
	if p := d.detectNewlines(sample); p != nil {
		patterns = append(patterns, *p)
	}
	if p := d.detectNullBytes(sample); p != nil {
		patterns = append(patterns, *p)
	}
	if p := d.detectWhitespace(sample); p != nil {
		patterns = append(patterns, *p)
	}
	if p := d.detectEscapes(sample); p != nil {
		patterns = append(patterns, *p)
	}
	if p := d.detectHTMLEntities(sample); p != nil {
		patterns = append(patterns, *p)
	}
	if p := d.detectColumnMismatch(row); p != nil {
		patterns = append(patterns, *p)
	}
	if p := d.detectTruncation(sample); p != nil {
		patterns = append(patterns, *p)
	}

	return patterns
}

// DetectPrimary returns the most likely primary error pattern.
func (d *Detector) DetectPrimary(row []byte) *ErrorPattern {
	patterns := d.Detect(row)
	if len(patterns) == 0 {
		return nil
	}

	// Return highest confidence pattern
	var best *ErrorPattern
	for i := range patterns {
		if best == nil || patterns[i].Confidence > best.Confidence {
			best = &patterns[i]
		}
	}
	return best
}

// detectBinary checks for binary/non-text data.
func (d *Detector) detectBinary(data []byte) *ErrorPattern {
	nonPrintable := 0
	for _, b := range data {
		if b < 32 && b != '\t' && b != '\n' && b != '\r' {
			nonPrintable++
		}
	}

	ratio := float64(nonPrintable) / float64(len(data))
	if ratio > d.cfg.BinaryThreshold {
		return &ErrorPattern{
			Type:        PatternBinaryData,
			Description: "Data appears to be binary, not text",
			Confidence:  ratio,
		}
	}
	return nil
}

// detectEncoding checks for encoding issues.
func (d *Detector) detectEncoding(data []byte) *ErrorPattern {
	if !utf8.Valid(data) {
		// Find first invalid byte
		for i, b := range data {
			if b > 127 {
				return &ErrorPattern{
					Type:        PatternEncodingError,
					Description: "Invalid UTF-8 sequence",
					Location:    i,
					Context:     extractContext(data, i, 10),
					Confidence:  0.9,
				}
			}
		}
	}

	// Check for BOM
	if hasBOM(data) {
		return &ErrorPattern{
			Type:        PatternEncodingError,
			Description: "Byte Order Mark (BOM) detected",
			Location:    0,
			Confidence:  1.0,
		}
	}

	return nil
}

// detectQuoting checks for quote issues.
func (d *Detector) detectQuoting(data []byte) *ErrorPattern {
	count := bytes.Count(data, []byte{d.cfg.QuoteChar})
	if count%2 != 0 {
		// Find unbalanced quote
		inQuote := false
		var lastQuotePos int
		for i, b := range data {
			if b == d.cfg.QuoteChar {
				inQuote = !inQuote
				lastQuotePos = i
			}
		}
		if inQuote {
			return &ErrorPattern{
				Type:        PatternUnbalancedQuotes,
				Description: "Unbalanced quote character",
				Location:    lastQuotePos,
				Context:     extractContext(data, lastQuotePos, 20),
				Confidence:  0.95,
			}
		}
	}
	return nil
}

// detectDelimiter checks for delimiter issues.
func (d *Detector) detectDelimiter(data []byte) *ErrorPattern {
	expectedCount := bytes.Count(data, []byte{d.cfg.Delimiter})

	// Check common alternatives
	alternatives := []struct {
		char  byte
		name  string
	}{
		{'\t', "tab"},
		{';', "semicolon"},
		{'|', "pipe"},
	}

	for _, alt := range alternatives {
		if alt.char == d.cfg.Delimiter {
			continue
		}
		altCount := bytes.Count(data, []byte{alt.char})
		if altCount > expectedCount && altCount > 2 {
			return &ErrorPattern{
				Type:        PatternWrongDelimiter,
				Description: "Data appears to use " + alt.name + " delimiter",
				Confidence:  float64(altCount) / float64(altCount+expectedCount+1),
			}
		}
	}
	return nil
}

// detectNewlines checks for newline inconsistencies.
func (d *Detector) detectNewlines(data []byte) *ErrorPattern {
	hasCRLF := bytes.Contains(data, []byte{'\r', '\n'})
	hasLF := bytes.Contains(data, []byte{'\n'})
	hasCR := bytes.Contains(data, []byte{'\r'})

	// Inconsistent if mixed or uses \r
	if hasCRLF || hasCR {
		return &ErrorPattern{
			Type:        PatternInconsistentNewlines,
			Description: "Windows-style line endings (CRLF) detected",
			Confidence:  0.85,
		}
	}
	if !hasLF && len(data) > 100 {
		// No newlines in large data might indicate truncation
		return nil
	}
	return nil
}

// detectNullBytes checks for embedded null bytes.
func (d *Detector) detectNullBytes(data []byte) *ErrorPattern {
	idx := bytes.IndexByte(data, 0)
	if idx >= 0 {
		return &ErrorPattern{
			Type:        PatternNullBytes,
			Description: "Embedded null byte detected",
			Location:    idx,
			Context:     extractContext(data, idx, 10),
			Confidence:  1.0,
		}
	}
	return nil
}

// detectWhitespace checks for whitespace issues.
func (d *Detector) detectWhitespace(data []byte) *ErrorPattern {
	if len(data) == 0 {
		return nil
	}

	// Leading whitespace
	if data[0] == ' ' || data[0] == '\t' {
		return &ErrorPattern{
			Type:        PatternExcessiveWhitespace,
			Description: "Leading whitespace",
			Location:    0,
			Confidence:  0.7,
		}
	}

	// Trailing whitespace (before newline)
	endIdx := len(data) - 1
	for endIdx > 0 && (data[endIdx] == '\n' || data[endIdx] == '\r') {
		endIdx--
	}
	if endIdx > 0 && (data[endIdx] == ' ' || data[endIdx] == '\t') {
		return &ErrorPattern{
			Type:        PatternExcessiveWhitespace,
			Description: "Trailing whitespace",
			Location:    endIdx,
			Confidence:  0.7,
		}
	}

	return nil
}

// detectEscapes checks for malformed escape sequences.
func (d *Detector) detectEscapes(data []byte) *ErrorPattern {
	// Trailing backslash
	if len(data) > 0 && data[len(data)-1] == '\\' {
		return &ErrorPattern{
			Type:        PatternMalformedEscape,
			Description: "Trailing backslash (incomplete escape)",
			Location:    len(data) - 1,
			Confidence:  0.9,
		}
	}

	// Invalid escape sequences
	matches := d.escapeRegex.FindIndex(data)
	if matches != nil {
		return &ErrorPattern{
			Type:        PatternMalformedEscape,
			Description: "Unrecognized escape sequence",
			Location:    matches[0],
			Context:     extractContext(data, matches[0], 10),
			Confidence:  0.8,
		}
	}

	return nil
}

// detectHTMLEntities checks for HTML entities.
func (d *Detector) detectHTMLEntities(data []byte) *ErrorPattern {
	matches := d.htmlEntityRegex.FindIndex(data)
	if matches != nil {
		entity := string(data[matches[0]:matches[1]])
		return &ErrorPattern{
			Type:        PatternHTMLEntities,
			Description: "HTML entity detected: " + entity,
			Location:    matches[0],
			Context:     extractContext(data, matches[0], 20),
			Confidence:  0.95,
		}
	}
	return nil
}

// detectColumnMismatch checks for column count issues.
func (d *Detector) detectColumnMismatch(data []byte) *ErrorPattern {
	if d.cfg.ExpectedColumns <= 0 {
		return nil
	}

	actualColumns := bytes.Count(data, []byte{d.cfg.Delimiter}) + 1
	if actualColumns != d.cfg.ExpectedColumns {
		desc := "Too few columns"
		if actualColumns > d.cfg.ExpectedColumns {
			desc = "Too many columns"
		}
		return &ErrorPattern{
			Type:        PatternColumnMismatch,
			Description: desc,
			Confidence:  0.9,
		}
	}
	return nil
}

// detectTruncation checks for signs of data truncation.
func (d *Detector) detectTruncation(data []byte) *ErrorPattern {
	if len(data) == 0 {
		return nil
	}

	// Check if last field is incomplete (ends with quote inside)
	lastQuote := bytes.LastIndexByte(data, d.cfg.QuoteChar)
	if lastQuote > 0 && lastQuote < len(data)-1 {
		// Quote not at end, check if it's the last character before delimiter
		remaining := data[lastQuote+1:]
		if !bytes.Contains(remaining, []byte{d.cfg.Delimiter}) &&
			!bytes.Contains(remaining, []byte{'\n'}) {
			// Looks truncated
			return &ErrorPattern{
				Type:        PatternTruncatedData,
				Description: "Data appears truncated",
				Location:    len(data) - 1,
				Confidence:  0.7,
			}
		}
	}

	return nil
}

// extractContext extracts surrounding context for debugging.
func extractContext(data []byte, pos, radius int) string {
	start := pos - radius
	if start < 0 {
		start = 0
	}
	end := pos + radius
	if end > len(data) {
		end = len(data)
	}

	context := string(data[start:end])
	// Make non-printable chars visible
	context = strings.ReplaceAll(context, "\n", "\\n")
	context = strings.ReplaceAll(context, "\r", "\\r")
	context = strings.ReplaceAll(context, "\t", "\\t")

	return context
}

// --- Analysis Report ---

// AnalysisReport summarizes detected patterns across multiple rows.
type AnalysisReport struct {
	TotalRows        int
	RowsWithErrors   int
	PatternCounts    map[PatternType]int
	SampleErrors     []RowError
	MostCommonError  PatternType
	RecommendedRules []string
}

// RowError captures error info for a specific row.
type RowError struct {
	RowNumber int
	Patterns  []ErrorPattern
}

// Analyzer performs batch analysis of data.
type Analyzer struct {
	detector *Detector
	maxSamples int
}

// NewAnalyzer creates a new batch analyzer.
func NewAnalyzer(cfg DetectorConfig) *Analyzer {
	return &Analyzer{
		detector:   NewDetector(cfg),
		maxSamples: 100, // Max sample errors to keep
	}
}

// Analyze processes multiple rows and returns a report.
func (a *Analyzer) Analyze(rows [][]byte) *AnalysisReport {
	report := &AnalysisReport{
		TotalRows:     len(rows),
		PatternCounts: make(map[PatternType]int),
		SampleErrors:  make([]RowError, 0, a.maxSamples),
	}

	for i, row := range rows {
		patterns := a.detector.Detect(row)
		if len(patterns) > 0 {
			report.RowsWithErrors++

			for _, p := range patterns {
				report.PatternCounts[p.Type]++
			}

			if len(report.SampleErrors) < a.maxSamples {
				report.SampleErrors = append(report.SampleErrors, RowError{
					RowNumber: i,
					Patterns:  patterns,
				})
			}
		}
	}

	// Find most common error
	var maxCount int
	for pType, count := range report.PatternCounts {
		if count > maxCount {
			maxCount = count
			report.MostCommonError = pType
		}
	}

	// Recommend rules based on detected patterns
	report.RecommendedRules = a.recommendRules(report.PatternCounts)

	return report
}

// recommendRules suggests fix rules based on detected patterns.
func (a *Analyzer) recommendRules(counts map[PatternType]int) []string {
	var rules []string

	ruleMap := map[PatternType]string{
		PatternEncodingError:        "FixEncodingRule",
		PatternUnbalancedQuotes:     "FixQuotingRule",
		PatternWrongDelimiter:       "FixDelimiterRule",
		PatternInconsistentNewlines: "FixNewlineRule",
		PatternNullBytes:            "FixNullByteRule",
		PatternExcessiveWhitespace:  "FixWhitespaceRule",
		PatternMalformedEscape:      "FixEscapeRule",
		PatternHTMLEntities:         "FixHTMLEntityRule",
		PatternColumnMismatch:       "FixColumnCountRule",
	}

	for pType, count := range counts {
		if count > 0 {
			if rule, ok := ruleMap[pType]; ok {
				rules = append(rules, rule)
			}
		}
	}

	return rules
}
