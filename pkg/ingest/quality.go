package ingest

import (
"bufio"
"hash"
"hash/fnv"
"io"
"math"
"os"
"sync"
"sync/atomic"
)

// QualityValidator performs streaming data quality validation.
type QualityValidator struct {
mu sync.Mutex

// Counters
totalRows   int64
totalBytes  int64
nullCounts  map[string]int64
errorRows   int64

// Checksum (FNV-1a for speed, similar to xxHash characteristics)
checksum hash.Hash64

// Cardinality estimation using HyperLogLog-style counting
cardinalityEstimators map[string]*CardinalityEstimator

// Entropy sampling
entropySamplers map[string]*EntropySampler

// Value range tracking
minValues map[string]string
maxValues map[string]string

// Configuration
sampleRate float64
}

// NewQualityValidator creates a new validator.
func NewQualityValidator() *QualityValidator {
return &QualityValidator{
nullCounts:            make(map[string]int64),
checksum:              fnv.New64a(),
cardinalityEstimators: make(map[string]*CardinalityEstimator),
entropySamplers:       make(map[string]*EntropySampler),
minValues:             make(map[string]string),
maxValues:             make(map[string]string),
sampleRate:            0.01, // Sample 1% of rows for entropy
}
}

// AddRow processes a row for quality validation.
func (q *QualityValidator) AddRow(columns []string, values [][]byte) {
q.mu.Lock()
defer q.mu.Unlock()

q.totalRows++

for i, col := range columns {
var val []byte
if i < len(values) {
val = values[i]
}

// Track nulls
if len(val) == 0 || isNullString(val) {
q.nullCounts[col]++
continue
}

// Update checksum
q.checksum.Write(val)

// Track cardinality
if _, ok := q.cardinalityEstimators[col]; !ok {
q.cardinalityEstimators[col] = NewCardinalityEstimator()
}
q.cardinalityEstimators[col].Add(val)

// Sample for entropy
if _, ok := q.entropySamplers[col]; !ok {
q.entropySamplers[col] = NewEntropySampler(10000)
}
if q.shouldSample() {
q.entropySamplers[col].Add(string(val))
}

// Track min/max
s := string(val)
if min, ok := q.minValues[col]; !ok || s < min {
q.minValues[col] = s
}
if max, ok := q.maxValues[col]; !ok || s > max {
q.maxValues[col] = s
}
}
}

// shouldSample returns true if this row should be sampled.
func (q *QualityValidator) shouldSample() bool {
// Deterministic sampling based on row count
return q.totalRows%int64(1/q.sampleRate) == 0
}

// GetReport returns the quality report.
func (q *QualityValidator) GetReport() *ValidationReport {
q.mu.Lock()
defer q.mu.Unlock()

report := &ValidationReport{
TotalRows:    q.totalRows,
TotalBytes:   q.totalBytes,
Checksum:     q.checksum.Sum64(),
ErrorRows:    q.errorRows,
ColumnStats:  make(map[string]*ColumnStats),
}

for col := range q.nullCounts {
stats := &ColumnStats{
NullCount: q.nullCounts[col],
}

if q.totalRows > 0 {
stats.NullPercentage = float64(stats.NullCount) / float64(q.totalRows) * 100
}

if est, ok := q.cardinalityEstimators[col]; ok {
stats.EstimatedCardinality = est.Count()
}

if sampler, ok := q.entropySamplers[col]; ok {
stats.Entropy = sampler.Entropy()
stats.NormalizedEntropy = sampler.NormalizedEntropy()
}

if min, ok := q.minValues[col]; ok {
stats.MinValue = min
}
if max, ok := q.maxValues[col]; ok {
stats.MaxValue = max
}

report.ColumnStats[col] = stats
}

return report
}

// ValidationReport contains quality validation results.
type ValidationReport struct {
TotalRows   int64
TotalBytes  int64
Checksum    uint64
ErrorRows   int64
ColumnStats map[string]*ColumnStats
}

// ColumnStats contains per-column statistics.
type ColumnStats struct {
NullCount            int64
NullPercentage       float64
EstimatedCardinality uint64
Entropy              float64
NormalizedEntropy    float64
MinValue             string
MaxValue             string
}

// isNullString checks if value represents null.
func isNullString(val []byte) bool {
switch string(val) {
case "", "NULL", "null", "NA", "N/A", "n/a", "None", "none", "nil", "-", "\\N":
return true
}
return false
}

// CardinalityEstimator estimates distinct value count using HyperLogLog.
type CardinalityEstimator struct {
registers []uint8
m         uint32 // Number of registers
precision uint8  // log2(m)
}

// NewCardinalityEstimator creates a new HyperLogLog estimator.
func NewCardinalityEstimator() *CardinalityEstimator {
precision := uint8(14) // 2^14 = 16384 registers
m := uint32(1) << precision

return &CardinalityEstimator{
registers: make([]uint8, m),
m:         m,
precision: precision,
}
}

// Add adds a value to the estimator.
func (c *CardinalityEstimator) Add(value []byte) {
h := fnv.New64a()
h.Write(value)
x := h.Sum64()

// Get register index (first p bits)
j := x >> (64 - c.precision)

// Count leading zeros in remaining bits
w := x << c.precision
rho := uint8(1)
for w&(1<<63) == 0 && rho < 65-c.precision {
rho++
w <<= 1
}

if rho > c.registers[j] {
c.registers[j] = rho
}
}

// Count returns the estimated cardinality.
func (c *CardinalityEstimator) Count() uint64 {
// HyperLogLog estimation
alpha := 0.7213 / (1 + 1.079/float64(c.m))

var sum float64
zeros := 0
for _, val := range c.registers {
sum += 1.0 / math.Pow(2, float64(val))
if val == 0 {
zeros++
}
}

estimate := alpha * float64(c.m*c.m) / sum

// Small range correction
if estimate <= 2.5*float64(c.m) && zeros > 0 {
estimate = float64(c.m) * math.Log(float64(c.m)/float64(zeros))
}

return uint64(estimate)
}

// EntropySampler estimates entropy using reservoir sampling.
type EntropySampler struct {
samples    []string
maxSamples int
counts     map[string]int
total      int
mu         sync.Mutex
}

// NewEntropySampler creates a new entropy sampler.
func NewEntropySampler(maxSamples int) *EntropySampler {
return &EntropySampler{
samples:    make([]string, 0, maxSamples),
maxSamples: maxSamples,
counts:     make(map[string]int),
}
}

// Add adds a value to the sampler.
func (e *EntropySampler) Add(value string) {
e.mu.Lock()
defer e.mu.Unlock()

e.total++
e.counts[value]++

// Reservoir sampling for large datasets
if len(e.samples) < e.maxSamples {
e.samples = append(e.samples, value)
}
}

// Entropy calculates Shannon entropy.
func (e *EntropySampler) Entropy() float64 {
e.mu.Lock()
defer e.mu.Unlock()

if e.total == 0 {
return 0
}

var entropy float64
for _, count := range e.counts {
if count > 0 {
p := float64(count) / float64(e.total)
entropy -= p * math.Log2(p)
}
}

return entropy
}

// NormalizedEntropy returns entropy normalized to [0, 1].
func (e *EntropySampler) NormalizedEntropy() float64 {
e.mu.Lock()
defer e.mu.Unlock()

if e.total == 0 || len(e.counts) <= 1 {
return 0
}

entropy := 0.0
for _, count := range e.counts {
if count > 0 {
p := float64(count) / float64(e.total)
entropy -= p * math.Log2(p)
}
}

// Max entropy is log2(n) where n is number of unique values
maxEntropy := math.Log2(float64(len(e.counts)))
if maxEntropy == 0 {
return 0
}

return entropy / maxEntropy
}

// FileChecksum computes xxHash-style checksum for a file.
func FileChecksum(path string) (uint64, error) {
f, err := os.Open(path)
if err != nil {
return 0, err
}
defer f.Close()

h := fnv.New64a()
buf := make([]byte, 256*1024) // 256KB buffer

for {
n, err := f.Read(buf)
if n > 0 {
h.Write(buf[:n])
}
if err == io.EOF {
break
}
if err != nil {
return 0, err
}
}

return h.Sum64(), nil
}

// StreamingChecksum computes checksum while reading.
type StreamingChecksum struct {
hash    hash.Hash64
bytes   atomic.Int64
}

// NewStreamingChecksum creates a new streaming checksum.
func NewStreamingChecksum() *StreamingChecksum {
return &StreamingChecksum{
hash: fnv.New64a(),
}
}

// Write adds bytes to the checksum.
func (s *StreamingChecksum) Write(p []byte) (int, error) {
n, err := s.hash.Write(p)
s.bytes.Add(int64(n))
return n, err
}

// Sum64 returns the checksum.
func (s *StreamingChecksum) Sum64() uint64 {
return s.hash.Sum64()
}

// Bytes returns total bytes processed.
func (s *StreamingChecksum) Bytes() int64 {
return s.bytes.Load()
}

// Reset resets the checksum.
func (s *StreamingChecksum) Reset() {
s.hash.Reset()
s.bytes.Store(0)
}

// ValidateParquet validates a Parquet file's integrity.
func ValidateParquet(path string) (*ParquetValidation, error) {
info, err := os.Stat(path)
if err != nil {
return nil, err
}

f, err := os.Open(path)
if err != nil {
return nil, err
}
defer f.Close()

// Read and validate magic bytes
header := make([]byte, 4)
if _, err := f.Read(header); err != nil {
return nil, err
}

if string(header) != "PAR1" {
return &ParquetValidation{
Valid:   false,
Message: "Invalid Parquet header (expected PAR1)",
}, nil
}

// Check footer magic
f.Seek(-4, io.SeekEnd)
footer := make([]byte, 4)
if _, err := f.Read(footer); err != nil {
return nil, err
}

if string(footer) != "PAR1" {
return &ParquetValidation{
Valid:   false,
Message: "Invalid Parquet footer (expected PAR1)",
}, nil
}

// Compute checksum
f.Seek(0, io.SeekStart)
checksum, err := FileChecksum(path)
if err != nil {
return nil, err
}

return &ParquetValidation{
Valid:    true,
FileSize: info.Size(),
Checksum: checksum,
Message:  "Parquet file is valid",
}, nil
}

// ParquetValidation contains Parquet file validation results.
type ParquetValidation struct {
Valid    bool
FileSize int64
Checksum uint64
Message  string
}

// DataProfiler generates a data profile for a file.
type DataProfiler struct {
validator *QualityValidator
columns   []string
}

// NewDataProfiler creates a new profiler.
func NewDataProfiler(columns []string) *DataProfiler {
return &DataProfiler{
validator: NewQualityValidator(),
columns:   columns,
}
}

// AddRow adds a row to the profiler.
func (p *DataProfiler) AddRow(values [][]byte) {
p.validator.AddRow(p.columns, values)
}

// Profile returns the data profile.
func (p *DataProfiler) Profile() *DataProfile {
report := p.validator.GetReport()

profile := &DataProfile{
TotalRows:     report.TotalRows,
Checksum:      report.Checksum,
ColumnCount:   len(p.columns),
ColumnDetails: make([]ColumnProfile, len(p.columns)),
}

for i, col := range p.columns {
stats, ok := report.ColumnStats[col]
if !ok {
continue
}

profile.ColumnDetails[i] = ColumnProfile{
Name:              col,
NullPercentage:    stats.NullPercentage,
Cardinality:       stats.EstimatedCardinality,
Entropy:           stats.Entropy,
NormalizedEntropy: stats.NormalizedEntropy,
MinValue:          stats.MinValue,
MaxValue:          stats.MaxValue,
}

// Determine uniqueness level
if profile.TotalRows > 0 {
uniqueRatio := float64(stats.EstimatedCardinality) / float64(profile.TotalRows)
if uniqueRatio > 0.99 {
profile.ColumnDetails[i].Uniqueness = "unique"
} else if uniqueRatio > 0.5 {
profile.ColumnDetails[i].Uniqueness = "high"
} else if uniqueRatio > 0.1 {
profile.ColumnDetails[i].Uniqueness = "medium"
} else {
profile.ColumnDetails[i].Uniqueness = "low"
}
}
}

return profile
}

// DataProfile contains a complete data profile.
type DataProfile struct {
TotalRows     int64
Checksum      uint64
ColumnCount   int
ColumnDetails []ColumnProfile
}

// ColumnProfile contains per-column profile information.
type ColumnProfile struct {
Name              string
NullPercentage    float64
Cardinality       uint64
Entropy           float64
NormalizedEntropy float64
MinValue          string
MaxValue          string
Uniqueness        string
}

// QuickProfile creates a quick profile by sampling a file.
func QuickProfile(path string, sampleRows int) (*DataProfile, error) {
analysis, err := NewDetector().Analyze(path)
if err != nil {
return nil, err
}

if analysis.Format != FormatCSV && analysis.Format != FormatTSV {
return nil, nil // Only CSV/TSV supported for now
}

f, err := os.Open(path)
if err != nil {
return nil, err
}
defer f.Close()

reader := bufio.NewReader(f)

// Read header
headerLine, err := reader.ReadBytes('\n')
if err != nil {
return nil, err
}

// Parse header
headers := make([]string, 0)
inQuote := false
start := 0
for i, b := range headerLine {
if b == '"' {
inQuote = !inQuote
} else if b == analysis.Delimiter && !inQuote {
headers = append(headers, string(headerLine[start:i]))
start = i + 1
} else if b == '\n' || b == '\r' {
if start < i {
headers = append(headers, string(headerLine[start:i]))
}
break
}
}

profiler := NewDataProfiler(headers)

// Sample rows
rowCount := 0
for rowCount < sampleRows {
line, err := reader.ReadBytes('\n')
if err == io.EOF {
break
}
if err != nil {
return nil, err
}

// Parse fields
fields := make([][]byte, 0, len(headers))
inQuote = false
start = 0
for i, b := range line {
if b == '"' {
inQuote = !inQuote
} else if b == analysis.Delimiter && !inQuote {
fields = append(fields, line[start:i])
start = i + 1
} else if b == '\n' || b == '\r' {
if start < i {
fields = append(fields, line[start:i])
}
break
}
}

profiler.AddRow(fields)
rowCount++
}

return profiler.Profile(), nil
}
