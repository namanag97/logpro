package pipe

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/logflow/logflow/internal/model"
	"github.com/logflow/logflow/pkg/parser"
	"github.com/logflow/logflow/pkg/writer"
)

// DuckDBPipeline uses DuckDB for maximum performance.
// For CSV: Uses DuckDB's native read_csv_auto (fastest path).
// For XES/JSON: Uses Go parser -> Arrow Appender -> DuckDB (hybrid approach).
type DuckDBPipeline struct {
	parserCfg parser.Config
	writerCfg writer.Config
	metadata  map[string]string
}

// NewDuckDBPipeline creates a new DuckDB-optimized pipeline.
func NewDuckDBPipeline(cfg Config) *DuckDBPipeline {
	return &DuckDBPipeline{
		parserCfg: cfg.ParserConfig,
		writerCfg: cfg.WriterConfig,
		metadata:  make(map[string]string),
	}
}

// SetMetadata sets key-value metadata for the Parquet footer.
func (p *DuckDBPipeline) SetMetadata(key, value string) {
	p.metadata[key] = value
}

// IngestCSV uses DuckDB's native CSV reader for maximum performance.
// This is the fastest path - no Go parsing involved.
func (p *DuckDBPipeline) IngestCSV(ctx context.Context, inputPath, outputPath string) (*IngestResult, error) {
	reader, err := writer.NewDuckDBCSVReader()
	if err != nil {
		return nil, fmt.Errorf("failed to create duckdb reader: %w", err)
	}
	defer reader.Close()

	// Add standard process mining metadata
	metadata := make(map[string]string)
	for k, v := range p.metadata {
		metadata[k] = v
	}
	metadata["pm:case_id"] = p.parserCfg.CaseIDColumn
	metadata["pm:activity"] = p.parserCfg.ActivityColumn
	metadata["pm:timestamp"] = p.parserCfg.TimestampColumn
	metadata["pm:resource"] = p.parserCfg.ResourceColumn

	result, err := reader.ConvertCSVToParquet(
		inputPath, outputPath,
		p.parserCfg.CaseIDColumn,
		p.parserCfg.ActivityColumn,
		p.parserCfg.TimestampColumn,
		p.parserCfg.ResourceColumn,
		p.writerCfg.Compression,
		metadata,
	)
	if err != nil {
		return nil, err
	}

	return &IngestResult{RowsWritten: result.RowsWritten}, nil
}

// IngestCSVFromStdin handles CSV input from stdin.
func (p *DuckDBPipeline) IngestCSVFromStdin(ctx context.Context, outputPath string) (*IngestResult, error) {
	reader, err := writer.NewDuckDBCSVReader()
	if err != nil {
		return nil, fmt.Errorf("failed to create duckdb reader: %w", err)
	}
	defer reader.Close()

	result, err := reader.ConvertCSVToParquetFromStdin(
		outputPath,
		p.parserCfg.CaseIDColumn,
		p.parserCfg.ActivityColumn,
		p.parserCfg.TimestampColumn,
		p.parserCfg.ResourceColumn,
		p.writerCfg.Compression,
	)
	if err != nil {
		return nil, err
	}

	return &IngestResult{RowsWritten: result.RowsWritten}, nil
}

// IngestXES uses Go parser with DuckDB Arrow Appender for XES files.
func (p *DuckDBPipeline) IngestXES(ctx context.Context, inputPath, outputPath string) (*IngestResult, error) {
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open input: %w", err)
	}
	defer inputFile.Close()

	return p.ingestWithArrowAppender(ctx, inputFile, outputPath, parser.FormatXES)
}

// IngestXESFromStdin handles XES input from stdin.
func (p *DuckDBPipeline) IngestXESFromStdin(ctx context.Context, outputPath string) (*IngestResult, error) {
	return p.ingestWithArrowAppender(ctx, os.Stdin, outputPath, parser.FormatXES)
}

// ingestWithArrowAppender uses the Go parser + DuckDB Arrow Appender path.
func (p *DuckDBPipeline) ingestWithArrowAppender(ctx context.Context, input io.Reader, outputPath string, format parser.Format) (*IngestResult, error) {
	// Create parser
	inputParser, err := parser.NewParser(format, p.parserCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create parser: %w", err)
	}

	// Create Arrow Appender writer
	arrowWriter, err := writer.NewArrowAppenderWriter(outputPath, p.writerCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create writer: %w", err)
	}

	// Add metadata
	for k, v := range p.metadata {
		arrowWriter.SetMetadata(k, v)
	}
	arrowWriter.SetMetadata("pm:case_id", "case_id")
	arrowWriter.SetMetadata("pm:activity", "activity")
	arrowWriter.SetMetadata("pm:timestamp", "timestamp")
	arrowWriter.SetMetadata("pm:resource", "resource")

	// Create event channel
	eventChan := make(chan *model.Event, 4096)
	errChan := make(chan error, 2)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Parser goroutine
	go func() {
		defer close(eventChan)
		if err := inputParser.Parse(ctx, input, eventChan); err != nil {
			select {
			case errChan <- fmt.Errorf("parser: %w", err):
			default:
			}
			cancel()
		}
	}()

	// Writer goroutine
	go func() {
		if err := arrowWriter.Write(ctx, eventChan); err != nil {
			select {
			case errChan <- fmt.Errorf("writer: %w", err):
			default:
			}
			cancel()
		}
		close(errChan)
	}()

	// Wait for completion
	for err := range errChan {
		if err != nil {
			arrowWriter.Close()
			return nil, err
		}
	}

	if err := arrowWriter.Close(); err != nil {
		return nil, err
	}

	return &IngestResult{
		RowsWritten: arrowWriter.RowsWritten(),
	}, nil
}

// Ingest routes to the appropriate ingestion method based on format.
func (p *DuckDBPipeline) Ingest(ctx context.Context, inputPath, outputPath string, format parser.Format) (*IngestResult, error) {
	isStdin := inputPath == "-"

	switch format {
	case parser.FormatCSV:
		if isStdin {
			return p.IngestCSVFromStdin(ctx, outputPath)
		}
		return p.IngestCSV(ctx, inputPath, outputPath)

	case parser.FormatXES:
		if isStdin {
			return p.IngestXESFromStdin(ctx, outputPath)
		}
		return p.IngestXES(ctx, inputPath, outputPath)

	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
}
