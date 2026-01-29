// Package generators provides test data generation utilities.
package generators

import (
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"time"
)

// CSVGenerator generates test CSV data.
type CSVGenerator struct {
	rng *rand.Rand

	// Schema
	Columns []ColumnSpec

	// Output settings
	HasHeader   bool
	Delimiter   rune
	QuoteChar   rune
	QuoteStyle  QuoteStyle

	// Data characteristics
	NullRate    float64 // Probability of null values
	ErrorRate   float64 // Probability of malformed values
}

// ColumnSpec defines a column's generation rules.
type ColumnSpec struct {
	Name     string
	Type     ColumnType
	Nullable bool

	// For strings
	MinLength int
	MaxLength int
	Pattern   string // regex-like pattern

	// For numbers
	MinInt   int64
	MaxInt   int64
	MinFloat float64
	MaxFloat float64

	// For timestamps
	MinTime time.Time
	MaxTime time.Time

	// For enums
	Values []string
}

// ColumnType defines column data types.
type ColumnType int

const (
	TypeString ColumnType = iota
	TypeInt
	TypeFloat
	TypeBool
	TypeTimestamp
	TypeDate
	TypeEnum
)

// QuoteStyle defines quoting behavior.
type QuoteStyle int

const (
	QuoteMinimal QuoteStyle = iota
	QuoteAll
	QuoteNonNumeric
	QuoteNone
)

// NewCSVGenerator creates a CSV generator with default settings.
func NewCSVGenerator(seed int64) *CSVGenerator {
	return &CSVGenerator{
		rng:        rand.New(rand.NewSource(seed)),
		HasHeader:  true,
		Delimiter:  ',',
		QuoteChar:  '"',
		QuoteStyle: QuoteMinimal,
		NullRate:   0.01,
	}
}

// Generate writes n rows of CSV data to the writer.
func (g *CSVGenerator) Generate(w io.Writer, n int) error {
	cw := csv.NewWriter(w)
	cw.Comma = g.Delimiter

	// Write header
	if g.HasHeader {
		header := make([]string, len(g.Columns))
		for i, col := range g.Columns {
			header[i] = col.Name
		}
		if err := cw.Write(header); err != nil {
			return err
		}
	}

	// Write rows
	row := make([]string, len(g.Columns))
	for i := 0; i < n; i++ {
		for j, col := range g.Columns {
			row[j] = g.generateValue(col)
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}

	cw.Flush()
	return cw.Error()
}

func (g *CSVGenerator) generateValue(col ColumnSpec) string {
	// Check for null
	if col.Nullable && g.rng.Float64() < g.NullRate {
		return ""
	}

	// Check for error (malformed value)
	if g.ErrorRate > 0 && g.rng.Float64() < g.ErrorRate {
		return g.generateMalformed(col)
	}

	switch col.Type {
	case TypeString:
		return g.generateString(col)
	case TypeInt:
		return g.generateInt(col)
	case TypeFloat:
		return g.generateFloat(col)
	case TypeBool:
		return g.generateBool()
	case TypeTimestamp:
		return g.generateTimestamp(col)
	case TypeDate:
		return g.generateDate(col)
	case TypeEnum:
		return g.generateEnum(col)
	default:
		return g.generateString(col)
	}
}

func (g *CSVGenerator) generateString(col ColumnSpec) string {
	minLen := col.MinLength
	if minLen <= 0 {
		minLen = 5
	}
	maxLen := col.MaxLength
	if maxLen <= 0 {
		maxLen = 20
	}

	length := minLen + g.rng.Intn(maxLen-minLen+1)
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
	result := make([]byte, length)
	for i := range result {
		result[i] = chars[g.rng.Intn(len(chars))]
	}
	return string(result)
}

func (g *CSVGenerator) generateInt(col ColumnSpec) string {
	minVal := col.MinInt
	maxVal := col.MaxInt
	if maxVal == 0 {
		maxVal = 1000000
	}
	val := minVal + g.rng.Int63n(maxVal-minVal+1)
	return strconv.FormatInt(val, 10)
}

func (g *CSVGenerator) generateFloat(col ColumnSpec) string {
	minVal := col.MinFloat
	maxVal := col.MaxFloat
	if maxVal == 0 {
		maxVal = 1000000.0
	}
	val := minVal + g.rng.Float64()*(maxVal-minVal)
	return strconv.FormatFloat(val, 'f', 2, 64)
}

func (g *CSVGenerator) generateBool() string {
	if g.rng.Intn(2) == 0 {
		return "true"
	}
	return "false"
}

func (g *CSVGenerator) generateTimestamp(col ColumnSpec) string {
	minTime := col.MinTime
	if minTime.IsZero() {
		minTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	maxTime := col.MaxTime
	if maxTime.IsZero() {
		maxTime = time.Now()
	}

	delta := maxTime.Sub(minTime)
	t := minTime.Add(time.Duration(g.rng.Int63n(int64(delta))))
	return t.Format(time.RFC3339)
}

func (g *CSVGenerator) generateDate(col ColumnSpec) string {
	minTime := col.MinTime
	if minTime.IsZero() {
		minTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	maxTime := col.MaxTime
	if maxTime.IsZero() {
		maxTime = time.Now()
	}

	delta := maxTime.Sub(minTime)
	t := minTime.Add(time.Duration(g.rng.Int63n(int64(delta))))
	return t.Format("2006-01-02")
}

func (g *CSVGenerator) generateEnum(col ColumnSpec) string {
	if len(col.Values) == 0 {
		return ""
	}
	return col.Values[g.rng.Intn(len(col.Values))]
}

func (g *CSVGenerator) generateMalformed(col ColumnSpec) string {
	switch col.Type {
	case TypeInt:
		return "not_a_number"
	case TypeFloat:
		return "NaN_invalid"
	case TypeBool:
		return "maybe"
	case TypeTimestamp, TypeDate:
		return "invalid-date"
	default:
		// Embedded newline or unmatched quote
		if g.rng.Intn(2) == 0 {
			return fmt.Sprintf("value\nwith\nnewlines")
		}
		return `unmatched "quote`
	}
}

// StandardColumns returns common test column definitions.
func StandardColumns() []ColumnSpec {
	return []ColumnSpec{
		{Name: "id", Type: TypeInt, MinInt: 1, MaxInt: 1000000},
		{Name: "name", Type: TypeString, MinLength: 5, MaxLength: 30},
		{Name: "email", Type: TypeString, MinLength: 10, MaxLength: 40},
		{Name: "age", Type: TypeInt, MinInt: 18, MaxInt: 90, Nullable: true},
		{Name: "salary", Type: TypeFloat, MinFloat: 30000, MaxFloat: 200000, Nullable: true},
		{Name: "active", Type: TypeBool},
		{Name: "created_at", Type: TypeTimestamp},
		{Name: "department", Type: TypeEnum, Values: []string{"Engineering", "Sales", "Marketing", "HR", "Finance"}},
	}
}

// EventLogColumns returns process mining style columns.
func EventLogColumns() []ColumnSpec {
	return []ColumnSpec{
		{Name: "case_id", Type: TypeString, MinLength: 8, MaxLength: 12},
		{Name: "activity", Type: TypeEnum, Values: []string{
			"Submit Order", "Approve Order", "Process Payment",
			"Ship Order", "Deliver Order", "Close Order",
		}},
		{Name: "timestamp", Type: TypeTimestamp},
		{Name: "resource", Type: TypeEnum, Values: []string{
			"Alice", "Bob", "Charlie", "Diana", "Eve",
		}},
		{Name: "cost", Type: TypeFloat, MinFloat: 10, MaxFloat: 1000, Nullable: true},
	}
}
