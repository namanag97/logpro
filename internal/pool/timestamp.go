package pool

import (
	"time"
)

// FastTimestampParser provides optimized timestamp parsing using byte inspection
// to avoid string allocations in hot paths.
type FastTimestampParser struct {
	// Common format detectors
	iso8601    bool
	dateOnly   bool
	excelEpoch bool
}

// Common timestamp layouts ordered by likelihood
var commonLayouts = []string{
	"2006-01-02T15:04:05.000Z07:00", // ISO 8601 with millis
	"2006-01-02T15:04:05Z07:00",     // ISO 8601
	"2006-01-02T15:04:05.000Z",      // ISO 8601 UTC with millis
	"2006-01-02T15:04:05Z",          // ISO 8601 UTC
	"2006-01-02T15:04:05",           // ISO 8601 local
	"2006-01-02 15:04:05.000",       // Space separator with millis
	"2006-01-02 15:04:05",           // Space separator
	"2006-01-02",                    // Date only
	"02/01/2006 15:04:05",           // DD/MM/YYYY
	"01/02/2006 15:04:05",           // MM/DD/YYYY
	"2006/01/02 15:04:05",           // YYYY/MM/DD
	time.RFC3339,
	time.RFC3339Nano,
}

// ParseTimestampNanosFast parses a timestamp byte slice to nanoseconds since epoch.
// Uses byte inspection to fast-path common formats.
func ParseTimestampNanosFast(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, ErrInvalidTimestamp
	}

	// Fast path: Check for ISO 8601 format (most common)
	if len(b) >= 10 && b[4] == '-' && b[7] == '-' {
		return parseISO8601Fast(b)
	}

	// Check for Excel epoch (numeric)
	if isNumeric(b) {
		return parseExcelEpoch(b)
	}

	// Fallback to standard parsing
	s := BytesToString(b)
	for _, layout := range commonLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixNano(), nil
		}
	}

	return 0, ErrInvalidTimestamp
}

// parseISO8601Fast parses ISO 8601 format using direct byte arithmetic.
func parseISO8601Fast(b []byte) (int64, error) {
	if len(b) < 10 {
		return 0, ErrInvalidTimestamp
	}

	// Parse YYYY-MM-DD
	year := parseInt4(b[0:4])
	month := parseInt2(b[5:7])
	day := parseInt2(b[8:10])

	if year < 0 || month < 1 || month > 12 || day < 1 || day > 31 {
		return 0, ErrInvalidTimestamp
	}

	var hour, minute, second, nsec int
	var loc *time.Location = time.UTC

	// Check for time component
	if len(b) > 10 && (b[10] == 'T' || b[10] == ' ') {
		if len(b) >= 19 {
			hour = parseInt2(b[11:13])
			minute = parseInt2(b[14:16])
			second = parseInt2(b[17:19])

			// Parse milliseconds/microseconds/nanoseconds
			if len(b) > 19 && b[19] == '.' {
				fracEnd := 20
				for fracEnd < len(b) && b[fracEnd] >= '0' && b[fracEnd] <= '9' {
					fracEnd++
				}
				fracStr := b[20:fracEnd]
				nsec = parseFraction(fracStr)
			}

			// Check for timezone
			for i := 19; i < len(b); i++ {
				if b[i] == 'Z' {
					loc = time.UTC
					break
				}
				if b[i] == '+' || b[i] == '-' {
					// Parse timezone offset
					if i+5 <= len(b) {
						offsetHours := parseInt2(b[i+1 : i+3])
						offsetMins := 0
						if i+6 <= len(b) && b[i+3] == ':' {
							offsetMins = parseInt2(b[i+4 : i+6])
						} else if i+5 <= len(b) {
							offsetMins = parseInt2(b[i+3 : i+5])
						}
						offset := offsetHours*3600 + offsetMins*60
						if b[i] == '-' {
							offset = -offset
						}
						loc = time.FixedZone("", offset)
					}
					break
				}
			}
		}
	}

	t := time.Date(year, time.Month(month), day, hour, minute, second, nsec, loc)
	return t.UnixNano(), nil
}

// parseExcelEpoch parses Excel-style epoch (days since 1899-12-30).
func parseExcelEpoch(b []byte) (int64, error) {
	// Excel epoch starts at 1899-12-30
	// Day 1 = 1900-01-01, but Excel incorrectly considers 1900 a leap year
	excelEpoch := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)

	// Parse the numeric value
	val, err := ParseFloat64(b)
	if err != nil {
		return 0, ErrInvalidTimestamp
	}

	// Convert days to nanoseconds
	days := int64(val)
	fraction := val - float64(days)

	// Add days to epoch
	t := excelEpoch.AddDate(0, 0, int(days))

	// Add fractional day (time component)
	if fraction > 0 {
		nsec := int64(fraction * 24 * 60 * 60 * 1e9)
		t = t.Add(time.Duration(nsec))
	}

	return t.UnixNano(), nil
}

// parseInt4 parses a 4-byte integer without allocation.
func parseInt4(b []byte) int {
	if len(b) != 4 {
		return -1
	}
	return int(b[0]-'0')*1000 + int(b[1]-'0')*100 + int(b[2]-'0')*10 + int(b[3]-'0')
}

// parseInt2 parses a 2-byte integer without allocation.
func parseInt2(b []byte) int {
	if len(b) != 2 {
		return -1
	}
	return int(b[0]-'0')*10 + int(b[1]-'0')
}

// parseFraction parses fractional seconds to nanoseconds.
func parseFraction(b []byte) int {
	if len(b) == 0 {
		return 0
	}

	// Pad or truncate to 9 digits (nanoseconds)
	var result int64
	multiplier := int64(100000000) // Start with 10^8

	for i := 0; i < len(b) && i < 9; i++ {
		digit := int64(b[i] - '0')
		result += digit * multiplier
		multiplier /= 10
	}

	return int(result)
}

// isNumeric checks if a byte slice contains only numeric characters.
func isNumeric(b []byte) bool {
	if len(b) == 0 {
		return false
	}

	dotCount := 0
	for i, c := range b {
		if c >= '0' && c <= '9' {
			continue
		}
		if c == '.' && dotCount == 0 {
			dotCount++
			continue
		}
		if c == '-' && i == 0 {
			continue
		}
		return false
	}
	return true
}

// ErrInvalidTimestamp indicates a timestamp parsing error.
var ErrInvalidTimestamp = &TimestampError{"invalid timestamp format"}

// TimestampError represents a timestamp parsing error.
type TimestampError struct {
	msg string
}

func (e *TimestampError) Error() string {
	return e.msg
}

// DateAmbiguityDetector helps resolve DD/MM vs MM/DD ambiguity.
type DateAmbiguityDetector struct {
	samples    [][]byte
	maxSamples int
}

// NewDateAmbiguityDetector creates a detector with the specified sample size.
func NewDateAmbiguityDetector(maxSamples int) *DateAmbiguityDetector {
	return &DateAmbiguityDetector{
		samples:    make([][]byte, 0, maxSamples),
		maxSamples: maxSamples,
	}
}

// AddSample adds a timestamp sample for analysis.
func (d *DateAmbiguityDetector) AddSample(ts []byte) {
	if len(d.samples) < d.maxSamples {
		cp := make([]byte, len(ts))
		copy(cp, ts)
		d.samples = append(d.samples, cp)
	}
}

// DetectFormat analyzes samples to determine the most likely date format.
// Returns "DMY" for day-first, "MDY" for month-first, or "YMD" for year-first.
func (d *DateAmbiguityDetector) DetectFormat() string {
	if len(d.samples) == 0 {
		return "YMD" // Default to ISO format
	}

	// Check if any sample has a day > 12 in the first position
	// If so, it must be DD/MM format
	dayFirst := 0
	monthFirst := 0

	for _, sample := range d.samples {
		parts := splitDateParts(sample)
		if len(parts) < 3 {
			continue
		}

		first := parseInt2(parts[0])
		second := parseInt2(parts[1])

		// If first > 12, it must be a day
		if first > 12 {
			dayFirst++
		}
		// If second > 12, it must be a day, so first is month
		if second > 12 {
			monthFirst++
		}
	}

	if dayFirst > monthFirst {
		return "DMY"
	}
	if monthFirst > dayFirst {
		return "MDY"
	}
	return "YMD" // Default if ambiguous
}

// splitDateParts splits a date by common separators.
func splitDateParts(b []byte) [][]byte {
	var parts [][]byte
	start := 0

	for i := 0; i < len(b); i++ {
		if b[i] == '/' || b[i] == '-' || b[i] == '.' {
			if start < i {
				parts = append(parts, b[start:i])
			}
			start = i + 1
		}
	}
	if start < len(b) {
		// Take only digits for the last part
		end := start
		for end < len(b) && b[end] >= '0' && b[end] <= '9' {
			end++
		}
		if start < end {
			parts = append(parts, b[start:end])
		}
	}

	return parts
}
