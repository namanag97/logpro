// Package detect provides file format and quality detection.
package detect

import (
	"context"
	"io"
	"os"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// DefaultSampleSize is the default number of bytes to sample.
const DefaultSampleSize = 64 * 1024 // 64KB

// Analysis contains file analysis results.
type Analysis struct {
	// Format detection
	Format   core.Format
	Encoding Encoding

	// CSV-specific
	Delimiter       byte
	QuoteChar       byte
	HasHeader       bool
	EstimatedCols   int
	LineEnding      LineEnding
	HasQuotedFields bool

	// Quality indicators
	IsClean             bool
	HasEmbeddedNewlines bool
	HasEncodingErrors   bool
	HasRaggedRows       bool
	HasMalformedQuotes  bool

	// Null value detection
	NullValueStrings []string

	// Strategy recommendation
	RecommendedStrategy Strategy
	Confidence          float64

	// Size info
	Size int64
}

// Encoding represents character encoding.
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

// LineEnding represents line ending style.
type LineEnding uint8

const (
	LineEndingUnknown LineEnding = iota
	LineEndingLF
	LineEndingCRLF
	LineEndingCR
	LineEndingMixed
)

// Strategy represents processing strategy.
type Strategy uint8

const (
	StrategyFastDuckDB Strategy = iota
	StrategyRobustGo
	StrategyStreaming
	StrategyHybrid
)

// Detector analyzes files to determine optimal processing.
type Detector struct {
	sampleSize int64
}

// NewDetector creates a new detector.
func NewDetector() *Detector {
	return &Detector{
		sampleSize: DefaultSampleSize,
	}
}

// Analyze examines a source and returns analysis.
func (d *Detector) Analyze(ctx context.Context, source core.Source) (*Analysis, error) {
	reader, err := source.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// Read sample
	sampleSize := d.sampleSize
	if source.Size() > 0 && source.Size() < sampleSize {
		sampleSize = source.Size()
	}

	sample := make([]byte, sampleSize)
	n, err := io.ReadFull(reader, sample)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	sample = sample[:n]

	analysis := &Analysis{
		Size:   source.Size(),
		Format: source.Format(),
	}

	// Detect encoding
	analysis.Encoding = detectEncoding(sample)

	// Format-specific analysis
	switch analysis.Format {
	case core.FormatCSV, core.FormatTSV:
		analyzeCSV(sample, analysis)
	case core.FormatJSON, core.FormatJSONL:
		analyzeJSON(sample, analysis)
	case core.FormatXML, core.FormatXES:
		analyzeXML(sample, analysis)
	}

	// Determine cleanliness
	analysis.IsClean = !analysis.HasEncodingErrors &&
		!analysis.HasMalformedQuotes &&
		!analysis.HasRaggedRows &&
		!analysis.HasEmbeddedNewlines

	// Select strategy
	analysis.RecommendedStrategy, analysis.Confidence = selectStrategy(analysis)

	return analysis, nil
}

// AnalyzePath analyzes a file path.
func (d *Detector) AnalyzePath(path string) (*Analysis, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read sample
	sampleSize := d.sampleSize
	if info.Size() < sampleSize {
		sampleSize = info.Size()
	}

	sample := make([]byte, sampleSize)
	n, err := f.Read(sample)
	if err != nil && err != io.EOF {
		return nil, err
	}
	sample = sample[:n]

	analysis := &Analysis{
		Size: info.Size(),
	}

	// Detect format
	analysis.Format = detectFormat(path, sample)

	// Detect encoding
	analysis.Encoding = detectEncoding(sample)

	// Format-specific analysis
	switch analysis.Format {
	case core.FormatCSV, core.FormatTSV:
		analyzeCSV(sample, analysis)
	case core.FormatJSON, core.FormatJSONL:
		analyzeJSON(sample, analysis)
	case core.FormatXML, core.FormatXES:
		analyzeXML(sample, analysis)
	}

	// Determine cleanliness
	analysis.IsClean = !analysis.HasEncodingErrors &&
		!analysis.HasMalformedQuotes &&
		!analysis.HasRaggedRows &&
		!analysis.HasEmbeddedNewlines

	// Select strategy
	analysis.RecommendedStrategy, analysis.Confidence = selectStrategy(analysis)

	return analysis, nil
}

// selectStrategy determines the optimal strategy.
func selectStrategy(a *Analysis) (Strategy, float64) {
	// Large files need streaming
	if a.Size > 2*1024*1024*1024 {
		return StrategyStreaming, 0.95
	}

	// Clean files use DuckDB
	if a.IsClean {
		switch a.Format {
		case core.FormatCSV, core.FormatTSV, core.FormatJSON, core.FormatJSONL, core.FormatParquet:
			return StrategyFastDuckDB, 0.9
		case core.FormatXES:
			return StrategyRobustGo, 0.8
		default:
			return StrategyRobustGo, 0.7
		}
	}

	// Dirty files need robust processing
	return StrategyRobustGo, 0.85
}
