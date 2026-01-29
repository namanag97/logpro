// Package ingest provides a high-performance, robust data ingestion pipeline.
// It uses heuristics to choose between fast DuckDB native paths and robust Go streaming.
package ingest

import (
"bytes"
"io"
"math"
"os"
"unicode/utf8"
)

// SampleSize is the number of bytes to read for heuristic analysis.
const SampleSize = 64 * 1024 // 64KB

// FileAnalysis contains detected characteristics of a file.
type FileAnalysis struct {
// Basic info
Size     int64
Format   Format
Encoding Encoding

// CSV-specific
Delimiter       byte
QuoteChar       byte
HasHeader       bool
EstimatedCols   int
LineEnding      LineEnding
HasQuotedFields bool

// Quality indicators
IsClean              bool
HasEmbeddedNewlines  bool
HasEncodingErrors    bool
HasRaggedRows        bool
HasMalformedQuotes   bool
NullValueStrings     []string

// Strategy recommendation
RecommendedStrategy Strategy
Confidence          float64
}

// Format represents detected file format.
type Format uint8

const (
FormatUnknown Format = iota
FormatCSV
FormatTSV
FormatJSON
FormatJSONL
FormatXES
FormatXML
FormatXLSX
FormatParquet
FormatGzip
)

func (f Format) String() string {
switch f {
case FormatCSV:
return "csv"
case FormatTSV:
return "tsv"
case FormatJSON:
return "json"
case FormatJSONL:
return "jsonl"
case FormatXES:
return "xes"
case FormatXML:
return "xml"
case FormatXLSX:
return "xlsx"
case FormatParquet:
return "parquet"
case FormatGzip:
return "gzip"
default:
return "unknown"
}
}

// Encoding represents detected character encoding.
type Encoding uint8

const (
EncodingUnknown Encoding = iota
EncodingUTF8
EncodingUTF8BOM
EncodingUTF16LE
EncodingUTF16BE
EncodingLatin1
EncodingASCII
)

func (e Encoding) String() string {
switch e {
case EncodingUTF8:
return "utf-8"
case EncodingUTF8BOM:
return "utf-8-bom"
case EncodingUTF16LE:
return "utf-16-le"
case EncodingUTF16BE:
return "utf-16-be"
case EncodingLatin1:
return "latin-1"
case EncodingASCII:
return "ascii"
default:
return "unknown"
}
}

// LineEnding represents detected line ending style.
type LineEnding uint8

const (
LineEndingUnknown LineEnding = iota
LineEndingLF               // Unix \n
LineEndingCRLF             // Windows \r\n
LineEndingCR               // Old Mac \r
LineEndingMixed            // Mixed (problematic)
)

// Strategy represents the recommended processing strategy.
type Strategy uint8

const (
StrategyFastDuckDB  Strategy = iota // Use DuckDB native readers
StrategyRobustGo                    // Use Go streaming with error recovery
StrategyStreaming                   // Use chunked streaming for large files
StrategyHybrid                      // DuckDB for read, Go for validation
)

func (s Strategy) String() string {
switch s {
case StrategyFastDuckDB:
return "fast-duckdb"
case StrategyRobustGo:
return "robust-go"
case StrategyStreaming:
return "streaming"
case StrategyHybrid:
return "hybrid"
default:
return "unknown"
}
}

// Detector analyzes files to determine optimal processing strategy.
type Detector struct {
maxSampleSize int64
}

// NewDetector creates a new file analyzer.
func NewDetector() *Detector {
return &Detector{
maxSampleSize: SampleSize,
}
}

// Analyze examines a file and returns analysis with recommended strategy.
func (d *Detector) Analyze(path string) (*FileAnalysis, error) {
f, err := os.Open(path)
if err != nil {
return nil, err
}
defer f.Close()

info, err := f.Stat()
if err != nil {
return nil, err
}

// Read sample
sampleSize := d.maxSampleSize
if info.Size() < sampleSize {
sampleSize = info.Size()
}

sample := make([]byte, sampleSize)
n, err := f.Read(sample)
if err != nil && err != io.EOF {
return nil, err
}
sample = sample[:n]

analysis := &FileAnalysis{
Size: info.Size(),
}

// Detect encoding first (affects all subsequent analysis)
analysis.Encoding = d.detectEncoding(sample)

// Detect format based on content and extension
analysis.Format = d.detectFormat(path, sample)

// Format-specific analysis
switch analysis.Format {
case FormatCSV, FormatTSV:
d.analyzeCSV(sample, analysis)
case FormatJSON, FormatJSONL:
d.analyzeJSON(sample, analysis)
case FormatXES, FormatXML:
d.analyzeXML(sample, analysis)
}

// Determine cleanliness
analysis.IsClean = !analysis.HasEncodingErrors &&
!analysis.HasMalformedQuotes &&
!analysis.HasRaggedRows &&
!analysis.HasEmbeddedNewlines

// Select strategy
analysis.RecommendedStrategy, analysis.Confidence = d.selectStrategy(analysis)

return analysis, nil
}

// detectEncoding identifies the character encoding.
func (d *Detector) detectEncoding(sample []byte) Encoding {
if len(sample) == 0 {
return EncodingUnknown
}

// Check for BOM (Byte Order Mark)
if len(sample) >= 3 && sample[0] == 0xEF && sample[1] == 0xBB && sample[2] == 0xBF {
return EncodingUTF8BOM
}
if len(sample) >= 2 {
if sample[0] == 0xFF && sample[1] == 0xFE {
return EncodingUTF16LE
}
if sample[0] == 0xFE && sample[1] == 0xFF {
return EncodingUTF16BE
}
}

// Check if valid UTF-8
if utf8.Valid(sample) {
// Check if pure ASCII
isASCII := true
for _, b := range sample {
if b > 127 {
isASCII = false
break
}
}
if isASCII {
return EncodingASCII
}
return EncodingUTF8
}

// Likely Latin-1 or another 8-bit encoding
return EncodingLatin1
}

// detectFormat identifies the file format.
func (d *Detector) detectFormat(path string, sample []byte) Format {
// Check magic bytes first
if len(sample) >= 4 {
// Parquet magic: PAR1
if bytes.HasPrefix(sample, []byte("PAR1")) {
return FormatParquet
}
// Gzip magic: 1f 8b
if sample[0] == 0x1f && sample[1] == 0x8b {
return FormatGzip
}
// XLSX magic: PK (zip archive)
if sample[0] == 0x50 && sample[1] == 0x4B {
return FormatXLSX
}
}

// Skip BOM if present
content := sample
if len(content) >= 3 && content[0] == 0xEF && content[1] == 0xBB && content[2] == 0xBF {
content = content[3:]
}

// Skip leading whitespace
content = bytes.TrimLeft(content, " \t\r\n")

// Check for XML/XES
if bytes.HasPrefix(content, []byte("<?xml")) || bytes.HasPrefix(content, []byte("<log")) {
if bytes.Contains(sample, []byte("<trace")) || bytes.Contains(sample, []byte("xes.")) {
return FormatXES
}
return FormatXML
}

// Check for JSON
if len(content) > 0 && (content[0] == '{' || content[0] == '[') {
// Check if it's JSONL (multiple JSON objects per line)
if d.isJSONL(content) {
return FormatJSONL
}
return FormatJSON
}

// Default to CSV/TSV based on content analysis
tabCount := bytes.Count(sample, []byte("\t"))
commaCount := bytes.Count(sample, []byte(","))

if tabCount > commaCount && tabCount > 0 {
return FormatTSV
}
return FormatCSV
}

// isJSONL checks if content appears to be JSONL (one JSON object per line).
func (d *Detector) isJSONL(sample []byte) bool {
lines := bytes.Split(sample, []byte("\n"))
if len(lines) < 2 {
return false
}

jsonObjects := 0
for i, line := range lines {
if i >= 5 { // Check first 5 lines
break
}
line = bytes.TrimSpace(line)
if len(line) == 0 {
continue
}
if line[0] == '{' && line[len(line)-1] == '}' {
jsonObjects++
}
}

return jsonObjects >= 2
}

// analyzeCSV performs detailed CSV analysis.
func (d *Detector) analyzeCSV(sample []byte, analysis *FileAnalysis) {
// Detect delimiter using frequency analysis
analysis.Delimiter = d.detectDelimiter(sample)

// Set format based on delimiter
if analysis.Delimiter == '\t' {
analysis.Format = FormatTSV
}

// Detect quote character
analysis.QuoteChar = d.detectQuoteChar(sample)

// Detect line ending
analysis.LineEnding = d.detectLineEnding(sample)

// Count columns from first line
analysis.EstimatedCols = d.countColumns(sample, analysis.Delimiter, analysis.QuoteChar)

// Detect header
analysis.HasHeader = d.detectHeader(sample, analysis.Delimiter, analysis.QuoteChar)

// Check for quoted fields
analysis.HasQuotedFields = bytes.IndexByte(sample, analysis.QuoteChar) >= 0

// Check for embedded newlines (in quoted fields)
analysis.HasEmbeddedNewlines = d.hasEmbeddedNewlines(sample, analysis.QuoteChar)

// Check for malformed quotes
analysis.HasMalformedQuotes = d.hasMalformedQuotes(sample, analysis.QuoteChar)

// Check for ragged rows
analysis.HasRaggedRows = d.hasRaggedRows(sample, analysis.Delimiter, analysis.QuoteChar, analysis.EstimatedCols)

// Check for encoding errors
analysis.HasEncodingErrors = !utf8.Valid(sample)

// Detect null value representations
analysis.NullValueStrings = d.detectNullValues(sample, analysis.Delimiter)
}

// detectDelimiter uses character frequency analysis to find the delimiter.
func (d *Detector) detectDelimiter(sample []byte) byte {
candidates := []byte{',', '\t', ';', '|', ':'}
type delimStats struct {
char     byte
counts   []int
variance float64
}

stats := make([]delimStats, len(candidates))

for i, delim := range candidates {
stats[i].char = delim
stats[i].counts = d.countDelimiterPerLine(sample, delim)

// Calculate variance (lower is better - consistent count per line)
if len(stats[i].counts) > 1 {
stats[i].variance = variance(stats[i].counts)
} else {
stats[i].variance = math.MaxFloat64
}
}

// Find delimiter with lowest variance and reasonable frequency
bestDelim := byte(',')
bestScore := math.MaxFloat64

for _, s := range stats {
if len(s.counts) == 0 {
continue
}

// Calculate average count
avg := mean(s.counts)
if avg < 1 {
continue // Need at least one delimiter per line
}

// Score: prefer low variance and high frequency
score := s.variance / avg
if score < bestScore {
bestScore = score
bestDelim = s.char
}
}

return bestDelim
}

// countDelimiterPerLine counts delimiter occurrences per line.
func (d *Detector) countDelimiterPerLine(sample []byte, delim byte) []int {
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

// detectQuoteChar finds the quote character used.
func (d *Detector) detectQuoteChar(sample []byte) byte {
doubleQuotes := bytes.Count(sample, []byte(`"`))
singleQuotes := bytes.Count(sample, []byte(`'`))

if doubleQuotes >= singleQuotes {
return '"'
}
return '\''
}

// detectLineEnding identifies the line ending style.
func (d *Detector) detectLineEnding(sample []byte) LineEnding {
crlfCount := bytes.Count(sample, []byte("\r\n"))
lfCount := bytes.Count(sample, []byte("\n")) - crlfCount
crCount := bytes.Count(sample, []byte("\r")) - crlfCount

if crlfCount > 0 && lfCount == 0 && crCount == 0 {
return LineEndingCRLF
}
if lfCount > 0 && crlfCount == 0 && crCount == 0 {
return LineEndingLF
}
if crCount > 0 && crlfCount == 0 && lfCount == 0 {
return LineEndingCR
}
if crlfCount > 0 || lfCount > 0 || crCount > 0 {
return LineEndingMixed
}
return LineEndingUnknown
}

// countColumns counts the number of columns in the first line.
func (d *Detector) countColumns(sample []byte, delim, quote byte) int {
// Find first line
lineEnd := bytes.IndexByte(sample, '\n')
if lineEnd < 0 {
lineEnd = len(sample)
}

line := sample[:lineEnd]
count := 1 // At least one column
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

// detectHeader heuristically determines if the first row is a header.
func (d *Detector) detectHeader(sample []byte, delim, quote byte) bool {
lines := bytes.SplitN(sample, []byte("\n"), 3)
if len(lines) < 2 {
return false
}

firstLine := lines[0]
secondLine := lines[1]

// Parse fields from both lines
firstFields := d.parseFields(firstLine, delim, quote)
secondFields := d.parseFields(secondLine, delim, quote)

if len(firstFields) == 0 || len(secondFields) == 0 {
return false
}

// Heuristics:
// 1. First row has more string-like values (no numbers)
// 2. First row has no duplicates
// 3. First row values look like column names

firstNumeric := 0
secondNumeric := 0

for _, f := range firstFields {
if d.looksNumeric(f) {
firstNumeric++
}
}
for _, f := range secondFields {
if d.looksNumeric(f) {
secondNumeric++
}
}

// If second line has more numeric values, first is likely header
if secondNumeric > firstNumeric {
return true
}

// Check if first line has unique values (headers usually unique)
seen := make(map[string]bool)
for _, f := range firstFields {
s := string(bytes.ToLower(f))
if seen[s] {
return false // Duplicate in first line, probably not header
}
seen[s] = true
}

// Check for common header patterns
for _, f := range firstFields {
s := string(bytes.ToLower(f))
if d.looksLikeHeaderName(s) {
return true
}
}

return false
}

// parseFields splits a line into fields, handling quotes.
func (d *Detector) parseFields(line []byte, delim, quote byte) [][]byte {
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
fields = append(fields, field)

return fields
}

// looksNumeric checks if a value appears to be numeric.
func (d *Detector) looksNumeric(value []byte) bool {
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

// looksLikeHeaderName checks for common header patterns.
func (d *Detector) looksLikeHeaderName(s string) bool {
headerPatterns := []string{
"id", "name", "date", "time", "timestamp", "case", "activity",
"event", "resource", "user", "type", "status", "value", "amount",
"count", "total", "start", "end", "created", "updated", "col",
}

for _, pattern := range headerPatterns {
if bytes.Contains([]byte(s), []byte(pattern)) {
return true
}
}
return false
}

// hasEmbeddedNewlines checks for newlines inside quoted fields.
func (d *Detector) hasEmbeddedNewlines(sample []byte, quote byte) bool {
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

// hasMalformedQuotes checks for unbalanced or improperly escaped quotes.
func (d *Detector) hasMalformedQuotes(sample []byte, quote byte) bool {
quoteCount := 0
for _, b := range sample {
if b == quote {
quoteCount++
}
}
// Odd number of quotes suggests malformed
return quoteCount%2 != 0
}

// hasRaggedRows checks if rows have inconsistent column counts.
func (d *Detector) hasRaggedRows(sample []byte, delim, quote byte, expectedCols int) bool {
lines := bytes.Split(sample, []byte("\n"))

// Skip last line - it may be partial (cut off by sample size)
if len(lines) > 1 {
lines = lines[:len(lines)-1]
}

raggedCount := 0
checkedRows := 0

for i, line := range lines {
if i == 0 || len(line) == 0 {
continue // Skip header and empty lines
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

// Only flag as ragged if >5% of rows are inconsistent (allow some noise)
if checkedRows == 0 {
return false
}
return float64(raggedCount)/float64(checkedRows) > 0.05
}

// detectNullValues finds common null value representations.
func (d *Detector) detectNullValues(sample []byte, delim byte) []string {
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
for pattern := range found {
result = append(result, pattern)
}
return result
}

// analyzeJSON performs JSON-specific analysis.
func (d *Detector) analyzeJSON(sample []byte, analysis *FileAnalysis) {
// Check for encoding errors
analysis.HasEncodingErrors = !utf8.Valid(sample)

// JSON is generally clean if valid UTF-8
analysis.IsClean = !analysis.HasEncodingErrors
}

// analyzeXML performs XML/XES-specific analysis.
func (d *Detector) analyzeXML(sample []byte, analysis *FileAnalysis) {
// Check for encoding errors
analysis.HasEncodingErrors = !utf8.Valid(sample)

// XML is clean if valid UTF-8
analysis.IsClean = !analysis.HasEncodingErrors
}

// selectStrategy determines the best processing strategy.
func (d *Detector) selectStrategy(analysis *FileAnalysis) (Strategy, float64) {
// Memory threshold for streaming
const streamingThreshold = 2 * 1024 * 1024 * 1024 // 2GB

// Large files need streaming
if analysis.Size > streamingThreshold {
return StrategyStreaming, 0.95
}

// Clean files use fast DuckDB path
if analysis.IsClean {
switch analysis.Format {
case FormatCSV, FormatTSV, FormatJSON, FormatJSONL, FormatParquet:
return StrategyFastDuckDB, 0.9
case FormatXES:
return StrategyRobustGo, 0.8 // XES needs Go parser
default:
return StrategyRobustGo, 0.7
}
}

// Dirty files need robust Go parser
if analysis.HasMalformedQuotes || analysis.HasEncodingErrors || analysis.HasRaggedRows {
return StrategyRobustGo, 0.85
}

// Embedded newlines can be handled by DuckDB but prefer robust
if analysis.HasEmbeddedNewlines {
return StrategyHybrid, 0.75
}

// Default to fast path
return StrategyFastDuckDB, 0.7
}

// Helper functions
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
