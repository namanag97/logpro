package registry

import (
	"github.com/logflow/logflow/pkg/adapters"
	"github.com/logflow/logflow/pkg/pipeline"
	"github.com/logflow/logflow/pkg/processors"
)

func init() {
	// Register Sources
	RegisterSource("csv", func(cfg pipeline.Config) (pipeline.Source, error) {
		return adapters.NewCSVSource(cfg)
	}, "csv", "txt", "tsv")

	// Register Sinks
	RegisterSink("parquet", func(cfg pipeline.Config) (pipeline.Sink, error) {
		return adapters.NewParquetSink(cfg)
	})

	// Register Processors
	RegisterProcessor("filter", processors.FilterProcessorFactory)
	RegisterProcessor("sample", processors.SampleProcessorFactory)
	RegisterProcessor("anonymize", processors.AnonymizeProcessorFactory)
}

// GetSourceForFile auto-detects format and returns appropriate source.
func GetSourceForFile(path string, cfg pipeline.Config) (pipeline.Source, error) {
	format := DetectFormat(path)
	cfg.SourceFormat = format
	cfg.SourcePath = path
	return GetSource(format, cfg)
}

// GetSinkForFile returns appropriate sink based on output path.
func GetSinkForFile(path string, cfg pipeline.Config) (pipeline.Sink, error) {
	format := DetectFormat(path)
	if format == "" {
		format = "parquet" // Default
	}
	cfg.SinkFormat = format
	cfg.SinkPath = path
	return GetSink(format, cfg)
}
