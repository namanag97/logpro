package main

import (
	"context"
	"fmt"

	"github.com/logflow/logflow/pkg/pipeline"
	"github.com/logflow/logflow/pkg/processors"
	"github.com/logflow/logflow/pkg/registry"
)

// PipelineRunner orchestrates the pipeline based on CLI flags.
type PipelineRunner struct {
	cfg pipeline.Config

	// Requested processors
	sampleRate       float64
	sampleCount      int
	stratifyBy       string
	anonymizeColumns []string
	anonymizeSalt    string
	filterRules      []processors.FilterRule
	inspect          bool
}

// NewPipelineRunner creates a runner from CLI configuration.
func NewPipelineRunner() *PipelineRunner {
	return &PipelineRunner{
		cfg: pipeline.DefaultConfig(),
	}
}

// Configure sets up the runner from CLI flags.
func (r *PipelineRunner) Configure(
	inputPath, outputPath string,
	caseIDCol, activityCol, timestampCol, resourceCol string,
	compression string,
	batchSize int,
) *PipelineRunner {
	r.cfg.SourcePath = inputPath
	r.cfg.SinkPath = outputPath
	r.cfg.CaseIDColumn = caseIDCol
	r.cfg.ActivityColumn = activityCol
	r.cfg.TimestampColumn = timestampCol
	r.cfg.ResourceColumn = resourceCol
	r.cfg.Compression = compression
	r.cfg.BatchSize = batchSize

	return r
}

// WithSampling enables sampling.
func (r *PipelineRunner) WithSampling(rate float64, count int, stratifyBy string) *PipelineRunner {
	r.sampleRate = rate
	r.sampleCount = count
	r.stratifyBy = stratifyBy
	return r
}

// WithAnonymization enables anonymization.
func (r *PipelineRunner) WithAnonymization(columns []string, salt string) *PipelineRunner {
	r.anonymizeColumns = columns
	r.anonymizeSalt = salt
	return r
}

// WithFilter adds filter rules.
func (r *PipelineRunner) WithFilter(rules []processors.FilterRule) *PipelineRunner {
	r.filterRules = rules
	return r
}

// WithInspection enables quality inspection.
func (r *PipelineRunner) WithInspection(enabled bool) *PipelineRunner {
	r.inspect = enabled
	return r
}

// Run executes the pipeline.
func (r *PipelineRunner) Run(ctx context.Context) (*PipelineResult, error) {
	// Build the pipeline
	orchestrator := pipeline.NewOrchestrator(r.cfg)

	// Get source
	source, err := registry.GetSourceForFile(r.cfg.SourcePath, r.cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create source: %w", err)
	}
	orchestrator.SetSource(source)

	// Get sink
	sink, err := registry.GetSinkForFile(r.cfg.SinkPath, r.cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create sink: %w", err)
	}
	orchestrator.SetSink(sink)

	// Add processors in order
	var qualityInspector *processors.QualityInspector

	// 1. Quality inspection (passthrough)
	if r.inspect {
		qualityInspector = processors.NewQualityInspector()
		orchestrator.AddInspector(qualityInspector)
	}

	// 2. Filtering
	if len(r.filterRules) > 0 {
		filterProc := processors.NewFilterProcessor(r.filterRules)
		orchestrator.AddProcessor(filterProc)
	}

	// 3. Anonymization
	if len(r.anonymizeColumns) > 0 {
		salt := r.anonymizeSalt
		if salt == "" {
			salt = "logflow-default-salt"
		}
		anonProc := processors.NewAnonymizeProcessor(r.anonymizeColumns, salt)
		orchestrator.AddProcessor(anonProc)
	}

	// 4. Sampling (usually last before output)
	if r.sampleRate > 0 || r.sampleCount > 0 || r.stratifyBy != "" {
		var strategy processors.SamplingStrategy

		if r.sampleCount > 0 {
			strategy = processors.NewReservoirSampling(r.sampleCount)
		} else if r.stratifyBy != "" {
			strategy = processors.NewStratifiedSampling(100, r.stratifyBy)
		} else if r.sampleRate > 0 {
			strategy = processors.NewRateSampling(r.sampleRate)
		}

		if strategy != nil {
			sampleProc := processors.NewSampleProcessor(strategy)
			orchestrator.AddProcessor(sampleProc)
		}
	}

	// Execute
	if err := orchestrator.Run(ctx); err != nil {
		return nil, err
	}

	// Collect results
	result := &PipelineResult{}

	if qualityInspector != nil {
		if report, ok := qualityInspector.Report().(*processors.QualityReport); ok {
			result.QualityReport = report
		}
	}

	return result, nil
}

// PipelineResult holds execution results.
type PipelineResult struct {
	EventsProcessed int64
	QualityReport   *processors.QualityReport
}

// --- Quick pipeline builders for common operations ---

// ConvertFile converts a file using the pipeline architecture.
func ConvertFile(ctx context.Context, inputPath, outputPath string, cfg pipeline.Config) error {
	source, err := registry.GetSourceForFile(inputPath, cfg)
	if err != nil {
		return err
	}

	sink, err := registry.GetSinkForFile(outputPath, cfg)
	if err != nil {
		return err
	}

	orchestrator := pipeline.NewOrchestrator(cfg)
	orchestrator.SetSource(source)
	orchestrator.SetSink(sink)

	return orchestrator.Run(ctx)
}

// InspectFile analyzes data quality.
func InspectFile(ctx context.Context, inputPath string, cfg pipeline.Config) (*processors.QualityReport, error) {
	source, err := registry.GetSourceForFile(inputPath, cfg)
	if err != nil {
		return nil, err
	}

	// Create a null sink that discards output
	nullSink := &NullSink{}

	inspector := processors.NewQualityInspector()

	orchestrator := pipeline.NewOrchestrator(cfg)
	orchestrator.SetSource(source)
	orchestrator.SetSink(nullSink)
	orchestrator.AddInspector(inspector)

	if err := orchestrator.Run(ctx); err != nil {
		return nil, err
	}

	report, _ := inspector.Report().(*processors.QualityReport)
	return report, nil
}

// NullSink discards all events (for inspection-only pipelines).
type NullSink struct{}

func (s *NullSink) Name() string { return "null" }
func (s *NullSink) Write(ctx context.Context, in <-chan *pipeline.Event) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-in:
			if !ok {
				return nil
			}
			// Discard
		}
	}
}
func (s *NullSink) Close() error { return nil }
