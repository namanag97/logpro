package pool

import (
	"strconv"
	"time"
	"unsafe"
)

// Zero-allocation helper functions for common operations.

// BytesToString converts a byte slice to a string without allocation.
// WARNING: The returned string shares memory with the byte slice.
// Do not modify the byte slice after calling this function if you
// need the string to remain valid.
func BytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// StringToBytes converts a string to a byte slice without allocation.
// WARNING: The returned byte slice shares memory with the string.
// Never modify the returned byte slice.
func StringToBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// ParseInt64 parses an int64 from a byte slice without allocation.
func ParseInt64(b []byte) (int64, error) {
	// Use unsafe conversion for parsing
	return strconv.ParseInt(BytesToString(b), 10, 64)
}

// ParseFloat64 parses a float64 from a byte slice without allocation.
func ParseFloat64(b []byte) (float64, error) {
	return strconv.ParseFloat(BytesToString(b), 64)
}

// ParseBool parses a boolean from a byte slice without allocation.
func ParseBool(b []byte) (bool, error) {
	return strconv.ParseBool(BytesToString(b))
}

// AppendInt64 appends an int64 to a byte slice.
func AppendInt64(dst []byte, v int64) []byte {
	return strconv.AppendInt(dst, v, 10)
}

// AppendFloat64 appends a float64 to a byte slice.
func AppendFloat64(dst []byte, v float64) []byte {
	return strconv.AppendFloat(dst, v, 'g', -1, 64)
}

// AppendBool appends a boolean to a byte slice.
func AppendBool(dst []byte, v bool) []byte {
	return strconv.AppendBool(dst, v)
}

// ParseTimestampNanos parses a timestamp string into nanoseconds since epoch.
// Tries multiple common formats.
func ParseTimestampNanos(b []byte) (int64, error) {
	s := BytesToString(b)

	// Common timestamp formats
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.000Z07:00",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t.UnixNano(), nil
		}
	}

	// Try Unix timestamp (seconds)
	if unix, err := strconv.ParseInt(s, 10, 64); err == nil {
		return unix * 1e9, nil
	}

	// Try Unix timestamp with milliseconds
	if unix, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(unix * 1e9), nil
	}

	return 0, strconv.ErrSyntax
}

// ByteSlice provides a poolable byte slice with common operations.
type ByteSlice struct {
	data []byte
}

// NewByteSlice creates a new ByteSlice with the given capacity.
func NewByteSlice(cap int) *ByteSlice {
	return &ByteSlice{
		data: make([]byte, 0, cap),
	}
}

// Reset clears the slice for reuse.
func (s *ByteSlice) Reset() {
	s.data = s.data[:0]
}

// Append appends bytes to the slice.
func (s *ByteSlice) Append(b []byte) {
	s.data = append(s.data, b...)
}

// AppendByte appends a single byte.
func (s *ByteSlice) AppendByte(b byte) {
	s.data = append(s.data, b)
}

// Bytes returns the underlying byte slice.
func (s *ByteSlice) Bytes() []byte {
	return s.data
}

// Len returns the current length.
func (s *ByteSlice) Len() int {
	return len(s.data)
}

// Cap returns the current capacity.
func (s *ByteSlice) Cap() int {
	return cap(s.data)
}

// String returns the data as a string (allocates).
func (s *ByteSlice) String() string {
	return string(s.data)
}

// UnsafeString returns the data as a string without allocation.
func (s *ByteSlice) UnsafeString() string {
	return BytesToString(s.data)
}

// Copy creates a copy of the byte slice.
func (s *ByteSlice) Copy() []byte {
	cp := make([]byte, len(s.data))
	copy(cp, s.data)
	return cp
}

// EqualBytes compares two byte slices for equality without allocation.
func EqualBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TrimSpaces trims leading and trailing whitespace in-place.
// Returns a slice of the same underlying array.
func TrimSpaces(b []byte) []byte {
	start := 0
	end := len(b)

	for start < end && isSpace(b[start]) {
		start++
	}
	for end > start && isSpace(b[end-1]) {
		end--
	}

	return b[start:end]
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// IndexByte returns the index of the first occurrence of c in b,
// or -1 if c is not present.
func IndexByte(b []byte, c byte) int {
	for i, v := range b {
		if v == c {
			return i
		}
	}
	return -1
}

// LastIndexByte returns the index of the last occurrence of c in b,
// or -1 if c is not present.
func LastIndexByte(b []byte, c byte) int {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] == c {
			return i
		}
	}
	return -1
}

// SplitFirst splits b at the first occurrence of sep,
// returning the part before and the part after sep.
// If sep is not found, returns b, nil.
func SplitFirst(b []byte, sep byte) ([]byte, []byte) {
	idx := IndexByte(b, sep)
	if idx < 0 {
		return b, nil
	}
	return b[:idx], b[idx+1:]
}
