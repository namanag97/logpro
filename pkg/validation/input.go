// Package validation provides input validation and sanitization.
package validation

import (
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	lferrors "github.com/logflow/logflow/pkg/errors"
)

// MaxFileSize is the maximum allowed input file size (10GB).
const MaxFileSize = 10 * 1024 * 1024 * 1024

// MaxPathLength is the maximum allowed path length.
const MaxPathLength = 4096

// MaxColumnNameLength is the maximum column name length.
const MaxColumnNameLength = 256

// MaxLineLength is the maximum line length (for CSV/JSONL).
const MaxLineLength = 10 * 1024 * 1024 // 10MB per line

// ValidateFilePath validates and sanitizes a file path.
func ValidateFilePath(path string) (string, error) {
	if path == "" {
		return "", lferrors.New(lferrors.CodeInvalidFormat, "empty file path")
	}

	// Allow stdin
	if path == "-" {
		return "-", nil
	}

	// Check length
	if len(path) > MaxPathLength {
		return "", lferrors.New(lferrors.CodeInvalidFormat, "path too long").
			WithContext("maxLength", MaxPathLength)
	}

	// Clean the path (removes .., multiple slashes, etc.)
	cleaned := filepath.Clean(path)

	// Prevent path traversal attacks
	if strings.Contains(cleaned, "..") {
		return "", lferrors.New(lferrors.CodeInvalidFormat, "path traversal not allowed")
	}

	// Convert to absolute path
	abs, err := filepath.Abs(cleaned)
	if err != nil {
		return "", lferrors.Wrap(err, lferrors.CodeInvalidFormat, "invalid path")
	}

	return abs, nil
}

// ValidateInputFile validates that an input file exists and is readable.
func ValidateInputFile(path string) error {
	if path == "-" {
		return nil // stdin is always valid
	}

	cleanPath, err := ValidateFilePath(path)
	if err != nil {
		return err
	}

	info, err := os.Stat(cleanPath)
	if os.IsNotExist(err) {
		return lferrors.FileNotFound(path)
	}
	if err != nil {
		return lferrors.Wrap(err, lferrors.CodeFileNotFound, "cannot access file")
	}

	if info.IsDir() {
		return lferrors.New(lferrors.CodeInvalidFormat, "path is a directory, expected file").
			WithContext("path", path)
	}

	if info.Size() > MaxFileSize {
		return lferrors.New(lferrors.CodeMemoryLimit, "file exceeds maximum size").
			WithContext("size", info.Size()).
			WithContext("maxSize", MaxFileSize)
	}

	// Check readability
	file, err := os.Open(cleanPath)
	if err != nil {
		if os.IsPermission(err) {
			return lferrors.Wrap(err, lferrors.CodeFilePermission, "permission denied")
		}
		return lferrors.Wrap(err, lferrors.CodeFileNotFound, "cannot open file")
	}
	file.Close()

	return nil
}

// ValidateOutputPath validates an output file path.
func ValidateOutputPath(path string) error {
	if path == "-" {
		return nil // stdout is always valid
	}

	cleanPath, err := ValidateFilePath(path)
	if err != nil {
		return err
	}

	// Check parent directory exists and is writable
	dir := filepath.Dir(cleanPath)
	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return lferrors.New(lferrors.CodeFileNotFound, "output directory does not exist").
			WithContext("directory", dir)
	}
	if err != nil {
		return lferrors.Wrap(err, lferrors.CodeFileNotFound, "cannot access output directory")
	}
	if !info.IsDir() {
		return lferrors.New(lferrors.CodeInvalidFormat, "parent path is not a directory")
	}

	// Check if file already exists
	if _, err := os.Stat(cleanPath); err == nil {
		// File exists - this is a warning, not an error
		// Could add --force flag to overwrite
	}

	return nil
}

// ValidateColumnName validates a column name.
func ValidateColumnName(name string) error {
	if name == "" {
		return lferrors.New(lferrors.CodeInvalidFormat, "empty column name")
	}

	if len(name) > MaxColumnNameLength {
		return lferrors.New(lferrors.CodeInvalidFormat, "column name too long").
			WithContext("name", name[:50]+"...").
			WithContext("maxLength", MaxColumnNameLength)
	}

	if !utf8.ValidString(name) {
		return lferrors.New(lferrors.CodeEncodingError, "column name contains invalid UTF-8")
	}

	return nil
}

// ValidateFormat validates a format string.
func ValidateFormat(format string) error {
	validFormats := map[string]bool{
		"csv":       true,
		"xes":       true,
		"json":      true,
		"jsonl":     true,
		"parquet":   true,
		"accesslog": true,
		"ocel":      true,
	}

	format = strings.ToLower(format)
	if !validFormats[format] {
		return lferrors.New(lferrors.CodeInvalidFormat, "unsupported format").
			WithContext("format", format).
			WithContext("supported", "csv, xes, json, jsonl, parquet, accesslog, ocel")
	}

	return nil
}

// ValidateCompression validates a compression type.
func ValidateCompression(compression string) error {
	validCompressions := map[string]bool{
		"none":   true,
		"snappy": true,
		"gzip":   true,
		"zstd":   true,
		"lz4":    true,
	}

	compression = strings.ToLower(compression)
	if !validCompressions[compression] {
		return lferrors.New(lferrors.CodeInvalidFormat, "unsupported compression").
			WithContext("compression", compression).
			WithContext("supported", "none, snappy, gzip, zstd, lz4")
	}

	return nil
}

// SanitizeUTF8 replaces invalid UTF-8 sequences with the replacement character.
func SanitizeUTF8(data []byte) []byte {
	if utf8.Valid(data) {
		return data
	}

	result := make([]byte, 0, len(data))
	for len(data) > 0 {
		r, size := utf8.DecodeRune(data)
		if r == utf8.RuneError && size == 1 {
			// Invalid byte - replace with replacement character
			result = append(result, 0xEF, 0xBF, 0xBD)
			data = data[1:]
		} else {
			result = append(result, data[:size]...)
			data = data[size:]
		}
	}

	return result
}

// TruncateString truncates a string to maxLen, adding "..." if truncated.
func TruncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return "..."
	}
	return s[:maxLen-3] + "..."
}

// NormalizeLineEndings converts all line endings to \n.
func NormalizeLineEndings(data []byte) []byte {
	// Fast path: check if normalization needed
	needsNorm := false
	for _, b := range data {
		if b == '\r' {
			needsNorm = true
			break
		}
	}
	if !needsNorm {
		return data
	}

	result := make([]byte, 0, len(data))
	for i := 0; i < len(data); i++ {
		if data[i] == '\r' {
			result = append(result, '\n')
			// Skip \n if \r\n
			if i+1 < len(data) && data[i+1] == '\n' {
				i++
			}
		} else {
			result = append(result, data[i])
		}
	}
	return result
}

// ValidationResult holds the result of batch validation.
type ValidationResult struct {
	Valid    bool
	Errors   []error
	Warnings []string
}

// ValidateConfig validates a complete pipeline configuration.
func ValidateConfig(inputPath, outputPath, format, compression string, columns map[string]string) *ValidationResult {
	result := &ValidationResult{Valid: true}

	// Validate input
	if err := ValidateInputFile(inputPath); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, err)
	}

	// Validate output
	if err := ValidateOutputPath(outputPath); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, err)
	}

	// Validate format
	if format != "" {
		if err := ValidateFormat(format); err != nil {
			result.Valid = false
			result.Errors = append(result.Errors, err)
		}
	}

	// Validate compression
	if err := ValidateCompression(compression); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, err)
	}

	// Validate column names
	for name, value := range columns {
		if err := ValidateColumnName(value); err != nil {
			result.Warnings = append(result.Warnings,
				"column "+name+" has unusual name: "+TruncateString(value, 50))
		}
	}

	return result
}
