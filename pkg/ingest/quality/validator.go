// Package quality provides data quality validation.
package quality

import (
	"sync"
)

// Validator performs streaming quality validation.
type Validator struct {
	mu sync.Mutex

	totalRows  int64
	totalBytes int64
	nullCounts map[string]int64
	errorRows  int64

	checksum    *Checksum
	cardinality map[string]*CardinalityEstimator
	entropy     map[string]*EntropySampler

	minValues map[string]string
	maxValues map[string]string

	sampleRate float64
}

// NewValidator creates a validator.
func NewValidator() *Validator {
	return &Validator{
		nullCounts:  make(map[string]int64),
		cardinality: make(map[string]*CardinalityEstimator),
		entropy:     make(map[string]*EntropySampler),
		minValues:   make(map[string]string),
		maxValues:   make(map[string]string),
		checksum:    NewChecksum(),
		sampleRate:  0.01,
	}
}

// ValidateRow validates a single row.
func (v *Validator) ValidateRow(row map[string]interface{}) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.totalRows++

	for col, val := range row {
		if val == nil {
			v.nullCounts[col]++
			continue
		}

		str, ok := val.(string)
		if !ok {
			continue
		}

		// Update cardinality
		if _, exists := v.cardinality[col]; !exists {
			v.cardinality[col] = NewCardinalityEstimator()
		}
		v.cardinality[col].Add(str)

		// Update min/max
		if min, exists := v.minValues[col]; !exists || str < min {
			v.minValues[col] = str
		}
		if max, exists := v.maxValues[col]; !exists || str > max {
			v.maxValues[col] = str
		}
	}
}

// ValidateBytes updates checksum with bytes.
func (v *Validator) ValidateBytes(data []byte) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.totalBytes += int64(len(data))
	v.checksum.Update(data)
}

// Report generates the quality report.
func (v *Validator) Report() *Report {
	v.mu.Lock()
	defer v.mu.Unlock()

	report := &Report{
		TotalRows:  v.totalRows,
		TotalBytes: v.totalBytes,
		ErrorRows:  v.errorRows,
		Checksum:   v.checksum.Sum(),
		Columns:    make(map[string]*ColumnReport),
	}

	for col, nulls := range v.nullCounts {
		cr := &ColumnReport{
			Name:       col,
			NullCount:  nulls,
			NullRate:   float64(nulls) / float64(v.totalRows),
			MinValue:   v.minValues[col],
			MaxValue:   v.maxValues[col],
		}

		if est, ok := v.cardinality[col]; ok {
			cr.Cardinality = est.Estimate()
		}

		report.Columns[col] = cr
	}

	return report
}

// Report contains quality metrics.
type Report struct {
	TotalRows  int64
	TotalBytes int64
	ErrorRows  int64
	Checksum   uint64
	Columns    map[string]*ColumnReport
}

// ColumnReport contains per-column metrics.
type ColumnReport struct {
	Name        string
	NullCount   int64
	NullRate    float64
	Cardinality uint64
	Entropy     float64
	MinValue    string
	MaxValue    string
}
