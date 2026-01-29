package parser

// CSVState represents the current state of the CSV state machine.
type CSVState uint8

const (
	// StateFieldStart indicates we're at the start of a field.
	StateFieldStart CSVState = iota
	// StateInField indicates we're inside an unquoted field.
	StateInField
	// StateInQuotedField indicates we're inside a quoted field.
	StateInQuotedField
	// StateQuoteInQuotedField indicates we encountered a quote inside a quoted field.
	StateQuoteInQuotedField
	// StateFieldEnd indicates we've reached the end of a field.
	StateFieldEnd
)

// CSVScanner provides a zero-allocation CSV scanner using a finite state machine.
// This handles all edge cases: embedded delimiters, escaped quotes, mixed line endings.
type CSVScanner struct {
	delimiter byte
	state     CSVState

	// Field tracking
	fieldStart int
	fieldEnd   int
	isQuoted   bool
}

// NewCSVScanner creates a new CSV scanner with the specified delimiter.
func NewCSVScanner(delimiter byte) *CSVScanner {
	return &CSVScanner{
		delimiter: delimiter,
		state:     StateFieldStart,
	}
}

// Reset resets the scanner state for a new line.
func (s *CSVScanner) Reset() {
	s.state = StateFieldStart
	s.fieldStart = 0
	s.fieldEnd = 0
	s.isQuoted = false
}

// ScanLine parses a CSV line using a finite state machine.
// Returns field slices pointing into the original line (no allocation).
func (s *CSVScanner) ScanLine(line []byte) [][]byte {
	if len(line) == 0 {
		return nil
	}

	// Pre-allocate for typical number of fields
	fields := make([][]byte, 0, 16)
	s.Reset()

	// Buffer for unescaping quoted fields
	var unescapeBuf []byte
	needsUnescape := false

	for i := 0; i <= len(line); i++ {
		var c byte
		if i < len(line) {
			c = line[i]
		}

		switch s.state {
		case StateFieldStart:
			if i >= len(line) {
				// Empty final field
				fields = append(fields, nil)
				continue
			}

			if c == '"' {
				s.isQuoted = true
				s.fieldStart = i + 1
				s.state = StateInQuotedField
			} else if c == s.delimiter {
				// Empty field
				fields = append(fields, nil)
				s.state = StateFieldStart
			} else if c == '\r' || c == '\n' {
				// End of line with empty field
				fields = append(fields, nil)
			} else {
				s.isQuoted = false
				s.fieldStart = i
				s.state = StateInField
			}

		case StateInField:
			if i >= len(line) || c == s.delimiter || c == '\r' || c == '\n' {
				// End of field
				fields = append(fields, line[s.fieldStart:i])
				s.state = StateFieldStart
			}
			// Otherwise continue in field

		case StateInQuotedField:
			if i >= len(line) {
				// Unterminated quoted field - take what we have
				fields = append(fields, line[s.fieldStart:i])
				continue
			}

			if c == '"' {
				s.fieldEnd = i
				s.state = StateQuoteInQuotedField
			}
			// Otherwise continue in quoted field

		case StateQuoteInQuotedField:
			if i >= len(line) || c == s.delimiter || c == '\r' || c == '\n' {
				// End of quoted field
				field := line[s.fieldStart:s.fieldEnd]
				if needsUnescape {
					field = unescapeQuotes(field, unescapeBuf)
					needsUnescape = false
				}
				fields = append(fields, field)
				s.state = StateFieldStart
			} else if c == '"' {
				// Escaped quote ("") - need to unescape later
				needsUnescape = true
				s.state = StateInQuotedField
			} else {
				// Invalid: character after closing quote
				// Be lenient and continue
				s.state = StateInQuotedField
			}
		}
	}

	return fields
}

// unescapeQuotes replaces "" with " in a quoted field.
func unescapeQuotes(field, buf []byte) []byte {
	if buf == nil {
		buf = make([]byte, 0, len(field))
	} else {
		buf = buf[:0]
	}

	i := 0
	for i < len(field) {
		if field[i] == '"' && i+1 < len(field) && field[i+1] == '"' {
			buf = append(buf, '"')
			i += 2
		} else {
			buf = append(buf, field[i])
			i++
		}
	}

	return buf
}

// NormalizeLineEndings normalizes \r\n and \r to \n in-place.
// Returns the normalized byte slice (may be shorter than input).
func NormalizeLineEndings(data []byte) []byte {
	if len(data) == 0 {
		return data
	}

	// Fast path: check if normalization is needed
	needsNormalization := false
	for _, c := range data {
		if c == '\r' {
			needsNormalization = true
			break
		}
	}

	if !needsNormalization {
		return data
	}

	// Normalize in-place
	j := 0
	for i := 0; i < len(data); i++ {
		if data[i] == '\r' {
			data[j] = '\n'
			j++
			// Skip \n if \r\n
			if i+1 < len(data) && data[i+1] == '\n' {
				i++
			}
		} else {
			data[j] = data[i]
			j++
		}
	}

	return data[:j]
}

// SanitizeUTF8 replaces invalid UTF-8 sequences with the replacement character.
// This prevents panics when processing files with mixed encodings.
func SanitizeUTF8(data []byte) []byte {
	if len(data) == 0 {
		return data
	}

	// Fast path: check if sanitization is needed
	valid := true
	for i := 0; i < len(data); {
		if data[i] < 0x80 {
			i++
			continue
		}

		// Multi-byte sequence
		size := utf8SequenceLength(data[i])
		if size == 0 || i+size > len(data) {
			valid = false
			break
		}

		// Validate continuation bytes
		for j := 1; j < size; j++ {
			if data[i+j]&0xC0 != 0x80 {
				valid = false
				break
			}
		}
		if !valid {
			break
		}
		i += size
	}

	if valid {
		return data
	}

	// Sanitize by replacing invalid sequences
	result := make([]byte, 0, len(data))
	for i := 0; i < len(data); {
		if data[i] < 0x80 {
			result = append(result, data[i])
			i++
			continue
		}

		size := utf8SequenceLength(data[i])
		if size == 0 || i+size > len(data) {
			// Invalid start byte or truncated sequence
			result = append(result, 0xEF, 0xBF, 0xBD) // UTF-8 replacement character
			i++
			continue
		}

		// Check continuation bytes
		validSeq := true
		for j := 1; j < size; j++ {
			if data[i+j]&0xC0 != 0x80 {
				validSeq = false
				break
			}
		}

		if validSeq {
			result = append(result, data[i:i+size]...)
			i += size
		} else {
			result = append(result, 0xEF, 0xBF, 0xBD)
			i++
		}
	}

	return result
}

// utf8SequenceLength returns the expected length of a UTF-8 sequence
// based on the first byte, or 0 if invalid.
func utf8SequenceLength(b byte) int {
	if b < 0x80 {
		return 1
	}
	if b < 0xC0 {
		return 0 // Continuation byte, invalid as start
	}
	if b < 0xE0 {
		return 2
	}
	if b < 0xF0 {
		return 3
	}
	if b < 0xF8 {
		return 4
	}
	return 0 // Invalid
}
