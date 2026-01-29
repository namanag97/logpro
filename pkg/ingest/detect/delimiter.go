package detect

import (
	"bytes"
	"math"
	"unicode/utf8"
)

// analyzeCSV performs CSV-specific analysis.
func analyzeCSV(sample []byte, a *Analysis) {
	a.Delimiter = detectDelimiter(sample)
	if a.Delimiter == '\t' {
		a.Format = 4 // FormatTSV - avoid import cycle
	}

	a.QuoteChar = detectQuoteChar(sample)
	a.LineEnding = detectLineEnding(sample)
	a.EstimatedCols = countColumns(sample, a.Delimiter, a.QuoteChar)
	a.HasHeader = detectHeader(sample, a.Delimiter, a.QuoteChar)
	a.HasQuotedFields = bytes.IndexByte(sample, a.QuoteChar) >= 0
	a.HasEmbeddedNewlines = hasEmbeddedNewlines(sample, a.QuoteChar)
	a.HasMalformedQuotes = hasMalformedQuotes(sample, a.QuoteChar)
	a.HasRaggedRows = hasRaggedRows(sample, a.Delimiter, a.QuoteChar, a.EstimatedCols)
	a.HasEncodingErrors = !utf8.Valid(sample)
	a.NullValueStrings = detectNullValues(sample, a.Delimiter)
}

// detectDelimiter finds the delimiter.
func detectDelimiter(sample []byte) byte {
	candidates := []byte{',', '\t', ';', '|', ':'}
	bestDelim := byte(',')
	bestScore := math.MaxFloat64

	for _, delim := range candidates {
		counts := countDelimiterPerLine(sample, delim)
		if len(counts) < 2 {
			continue
		}

		avg := mean(counts)
		if avg < 1 {
			continue
		}

		v := variance(counts)
		score := v / avg
		if score < bestScore {
			bestScore = score
			bestDelim = delim
		}
	}

	return bestDelim
}

func countDelimiterPerLine(sample []byte, delim byte) []int {
	var counts []int
	inQuote := false
	count := 0

	for _, b := range sample {
		if b == '"' {
			inQuote = !inQuote
			continue
		}
		if !inQuote {
			if b == delim {
				count++
			} else if b == '\n' {
				counts = append(counts, count)
				count = 0
			}
		}
	}
	return counts
}

func detectQuoteChar(sample []byte) byte {
	dq := bytes.Count(sample, []byte(`"`))
	sq := bytes.Count(sample, []byte(`'`))
	if dq >= sq {
		return '"'
	}
	return '\''
}

func detectLineEnding(sample []byte) LineEnding {
	crlf := bytes.Count(sample, []byte("\r\n"))
	lf := bytes.Count(sample, []byte("\n")) - crlf
	cr := bytes.Count(sample, []byte("\r")) - crlf

	if crlf > 0 && lf == 0 && cr == 0 {
		return LineEndingCRLF
	}
	if lf > 0 && crlf == 0 && cr == 0 {
		return LineEndingLF
	}
	if cr > 0 && crlf == 0 && lf == 0 {
		return LineEndingCR
	}
	if crlf > 0 || lf > 0 || cr > 0 {
		return LineEndingMixed
	}
	return LineEndingUnknown
}

func countColumns(sample []byte, delim, quote byte) int {
	lineEnd := bytes.IndexByte(sample, '\n')
	if lineEnd < 0 {
		lineEnd = len(sample)
	}

	line := sample[:lineEnd]
	count := 1
	inQuote := false

	for _, b := range line {
		if b == quote {
			inQuote = !inQuote
		} else if b == delim && !inQuote {
			count++
		}
	}
	return count
}

func detectHeader(sample []byte, delim, quote byte) bool {
	lines := bytes.SplitN(sample, []byte("\n"), 3)
	if len(lines) < 2 {
		return false
	}

	firstFields := parseFields(lines[0], delim, quote)
	secondFields := parseFields(lines[1], delim, quote)

	if len(firstFields) == 0 || len(secondFields) == 0 {
		return false
	}

	firstNumeric := 0
	secondNumeric := 0

	for _, f := range firstFields {
		if looksNumeric(f) {
			firstNumeric++
		}
	}
	for _, f := range secondFields {
		if looksNumeric(f) {
			secondNumeric++
		}
	}

	return secondNumeric > firstNumeric
}

func parseFields(line []byte, delim, quote byte) [][]byte {
	var fields [][]byte
	var field []byte
	inQuote := false

	for _, b := range line {
		if b == quote {
			inQuote = !inQuote
		} else if b == delim && !inQuote {
			fields = append(fields, field)
			field = nil
		} else if b != '\r' {
			field = append(field, b)
		}
	}
	return append(fields, field)
}

func looksNumeric(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	hasDigit := false
	for _, b := range value {
		if b >= '0' && b <= '9' {
			hasDigit = true
		} else if b != '.' && b != '-' && b != '+' && b != 'e' && b != 'E' && b != ',' {
			return false
		}
	}
	return hasDigit
}

func hasEmbeddedNewlines(sample []byte, quote byte) bool {
	inQuote := false
	for _, b := range sample {
		if b == quote {
			inQuote = !inQuote
		} else if b == '\n' && inQuote {
			return true
		}
	}
	return false
}

func hasMalformedQuotes(sample []byte, quote byte) bool {
	count := 0
	for _, b := range sample {
		if b == quote {
			count++
		}
	}
	return count%2 != 0
}

func hasRaggedRows(sample []byte, delim, quote byte, expectedCols int) bool {
	lines := bytes.Split(sample, []byte("\n"))
	if len(lines) > 1 {
		lines = lines[:len(lines)-1]
	}

	raggedCount := 0
	checkedRows := 0

	for i, line := range lines {
		if i == 0 || len(line) == 0 {
			continue
		}

		cols := 1
		inQuote := false
		for _, b := range line {
			if b == quote {
				inQuote = !inQuote
			} else if b == delim && !inQuote {
				cols++
			}
		}

		checkedRows++
		if cols != expectedCols {
			raggedCount++
		}
	}

	if checkedRows == 0 {
		return false
	}
	return float64(raggedCount)/float64(checkedRows) > 0.05
}

func detectNullValues(sample []byte, delim byte) []string {
	nullPatterns := []string{"", "NULL", "null", "NA", "N/A", "n/a", "None", "none", "nil", "\\N", "-"}
	found := make(map[string]bool)

	lines := bytes.Split(sample, []byte("\n"))
	for _, line := range lines {
		fields := bytes.Split(line, []byte{delim})
		for _, field := range fields {
			s := string(bytes.TrimSpace(field))
			for _, pattern := range nullPatterns {
				if s == pattern {
					found[pattern] = true
				}
			}
		}
	}

	var result []string
	for p := range found {
		result = append(result, p)
	}
	return result
}

func mean(values []int) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0
	for _, v := range values {
		sum += v
	}
	return float64(sum) / float64(len(values))
}

func variance(values []int) float64 {
	if len(values) == 0 {
		return 0
	}
	m := mean(values)
	sum := 0.0
	for _, v := range values {
		diff := float64(v) - m
		sum += diff * diff
	}
	return sum / float64(len(values))
}
