package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/logflow/logflow/internal/model"
	"github.com/logflow/logflow/internal/pipe"
	"github.com/logflow/logflow/pkg/diff"
	"github.com/logflow/logflow/pkg/export"
	"github.com/logflow/logflow/pkg/inspect"
	"github.com/logflow/logflow/pkg/parser"
	"github.com/logflow/logflow/pkg/pmpt"
	"github.com/logflow/logflow/pkg/transform"
	"github.com/logflow/logflow/pkg/util"
	"github.com/logflow/logflow/pkg/watch"
	"github.com/logflow/logflow/pkg/writer"
)

// Additional CLI flags
var (
	// Inspect flags
	outputFormat string
	jsonOutput   bool

	// Sample flags
	sampleRate   float64
	sampleCount  int
	stratifyBy   string

	// Export flags
	exportFormat string
	outputDir    string

	// Anonymize flags
	anonymizeColumns []string
	anonymizeSalt    string

	// Diff flags
	leftFile     string
	rightFile    string
	structuralDiff bool // Use PMPT for structural (tree-based) diff

	// Watch flags
	watchInterval time.Duration
	fullRebuild   bool

	// Batch flags
	parallelWorkers int
	batchOutputDir  string
	failFast        bool
)

var watchCmd = &cobra.Command{
	Use:   "watch <input> <output>",
	Short: "Watch a file and auto-convert on changes",
	Long: `Watch an input file for changes and automatically update the Parquet output.

This creates a real-time data pipeline from flat files. When the source file
is modified (appended to), LogFlow detects the change and updates the output.

Examples:
  logflow watch sales.csv sales.parquet
  logflow watch --interval 5s server.log events.parquet
  logflow watch --full-rebuild data.csv data.parquet`,
	Args: cobra.ExactArgs(2),
	RunE: runWatch,
}

var diffCmd = &cobra.Command{
	Use:   "diff <left> <right>",
	Short: "Compare two process logs semantically",
	Long: `Perform a semantic diff between two process mining logs.

Unlike byte-level diff, this compares:
  - Activity frequency changes
  - Case count and duration changes
  - Resource allocation changes
  - Process drift (new/removed activities)

Examples:
  logflow diff log_v1.csv log_v2.csv
  logflow diff baseline.parquet current.parquet
  logflow diff march.xes april.xes --json`,
	Args: cobra.ExactArgs(2),
	RunE: runDiff,
}

var inspectCmd = &cobra.Command{
	Use:   "inspect",
	Short: "Inspect data quality and generate reports",
	Long: `Analyze data quality metrics including completeness, uniqueness, and distribution.

Examples:
  logflow inspect -i events.csv
  logflow inspect -i events.parquet --json
  logflow inspect -i events.csv --format csv`,
	RunE: runInspect,
}

var sampleCmd = &cobra.Command{
	Use:   "sample",
	Short: "Create a random sample of the data",
	Long: `Extract a random sample using reservoir sampling or rate-based sampling.

Examples:
  logflow sample -i large.csv -o sample.parquet --rate 0.1
  logflow sample -i large.csv -o sample.parquet --count 10000
  logflow sample -i large.csv -o sample.parquet --stratify case_id`,
	RunE: runSample,
}

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export data for BI tools",
	Long: `Export process mining data to formats optimized for BI tools.

Supported formats:
  - powerbi: Star schema (Fact_Events, Dim_Cases, Dim_Activities, Dim_Resources, Dim_Time)
  - tableau: Same as powerbi
  - csv: Export Parquet back to CSV

Examples:
  logflow export -i events.parquet --format powerbi -o ./powerbi_output/
  logflow export -i events.parquet --format csv -o events_export.csv`,
	RunE: runExport,
}

var anonymizeCmd = &cobra.Command{
	Use:   "anonymize",
	Short: "Anonymize sensitive columns",
	Long: `Hash specified columns for GDPR compliance.

Examples:
  logflow anonymize -i events.csv -o anon.parquet --columns "customer_name,email"
  logflow anonymize -i events.csv -o anon.parquet --columns "org:resource" --salt "my-secret-salt"`,
	RunE: runAnonymize,
}

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Display basic statistics",
	Long: `Show summary statistics for process mining data.

Examples:
  logflow stats -i events.parquet
  logflow stats -i events.csv --json`,
	RunE: runStats,
}

var batchCmd = &cobra.Command{
	Use:   "batch <input-files...>",
	Short: "Convert multiple files in parallel",
	Long: `Convert multiple files to Parquet format using parallel processing.

Accepts glob patterns and multiple file paths. Output files are placed in
the specified output directory with .parquet extension.

Examples:
  logflow batch *.csv -o ./parquet/
  logflow batch logs/*.xes events/*.csv -o ./output/ --workers 8
  logflow batch data/**/*.csv -o ./parquet/ --fail-fast`,
	Args: cobra.MinimumNArgs(1),
	RunE: runBatch,
}

func init() {
	// Watch command flags
	watchCmd.Flags().DurationVar(&watchInterval, "interval", 500*time.Millisecond, "Debounce interval for change detection")
	watchCmd.Flags().BoolVar(&fullRebuild, "full-rebuild", false, "Always rebuild entire output (vs incremental)")
	watchCmd.Flags().StringVarP(&formatFlag, "format", "f", "", "Input format (csv, xes, json)")
	watchCmd.Flags().StringVar(&compressionFlag, "compression", "snappy", "Parquet compression")

	// Diff command flags
	diffCmd.Flags().BoolVar(&jsonOutput, "json", false, "Output report as JSON")
	diffCmd.Flags().BoolVar(&structuralDiff, "structural", false, "Use PMPT for structural process tree comparison (O(1) fingerprint check)")
	diffCmd.Flags().StringVarP(&formatFlag, "format", "f", "", "Input format (csv, xes, json)")

	// Inspect command flags
	inspectCmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input file path (required)")
	inspectCmd.Flags().StringVarP(&formatFlag, "format", "f", "", "Input format (csv, xes, json, parquet)")
	inspectCmd.Flags().BoolVar(&jsonOutput, "json", false, "Output report as JSON")
	inspectCmd.MarkFlagRequired("input")

	// Sample command flags
	sampleCmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input file path (required)")
	sampleCmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output file path (required)")
	sampleCmd.Flags().StringVarP(&formatFlag, "format", "f", "", "Input format")
	sampleCmd.Flags().Float64Var(&sampleRate, "rate", 0, "Sample rate (0.0 to 1.0)")
	sampleCmd.Flags().IntVar(&sampleCount, "count", 0, "Exact number of events to sample")
	sampleCmd.Flags().StringVar(&stratifyBy, "stratify", "", "Stratify sampling by column (e.g., case_id)")
	sampleCmd.MarkFlagRequired("input")
	sampleCmd.MarkFlagRequired("output")

	// Export command flags
	exportCmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input Parquet file (required)")
	exportCmd.Flags().StringVarP(&outputDir, "output", "o", "", "Output directory or file (required)")
	exportCmd.Flags().StringVar(&exportFormat, "format", "powerbi", "Export format (powerbi, tableau, csv)")
	exportCmd.Flags().StringVar(&compressionFlag, "compression", "snappy", "Parquet compression")
	exportCmd.MarkFlagRequired("input")
	exportCmd.MarkFlagRequired("output")

	// Anonymize command flags
	anonymizeCmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input file path (required)")
	anonymizeCmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output file path (required)")
	anonymizeCmd.Flags().StringVarP(&formatFlag, "format", "f", "", "Input format")
	anonymizeCmd.Flags().StringSliceVar(&anonymizeColumns, "columns", nil, "Columns to anonymize")
	anonymizeCmd.Flags().StringVar(&anonymizeSalt, "salt", "", "Salt for hashing (recommended for security)")
	anonymizeCmd.MarkFlagRequired("input")
	anonymizeCmd.MarkFlagRequired("output")
	anonymizeCmd.MarkFlagRequired("columns")

	// Stats command flags
	statsCmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input file path (required)")
	statsCmd.Flags().StringVarP(&formatFlag, "format", "f", "", "Input format")
	statsCmd.Flags().BoolVar(&jsonOutput, "json", false, "Output as JSON")
	statsCmd.MarkFlagRequired("input")

	// Batch command flags
	batchCmd.Flags().StringVarP(&batchOutputDir, "output", "o", "", "Output directory (required)")
	batchCmd.Flags().IntVarP(&parallelWorkers, "workers", "w", runtime.NumCPU(), "Number of parallel workers")
	batchCmd.Flags().BoolVar(&failFast, "fail-fast", false, "Stop on first error")
	batchCmd.Flags().StringVar(&compressionFlag, "compression", "snappy", "Parquet compression")
	batchCmd.Flags().StringVarP(&formatFlag, "format", "f", "", "Input format (auto-detect if not specified)")
	batchCmd.MarkFlagRequired("output")

	// Register commands
	rootCmd.AddCommand(watchCmd)
	rootCmd.AddCommand(diffCmd)
	rootCmd.AddCommand(inspectCmd)
	rootCmd.AddCommand(sampleCmd)
	rootCmd.AddCommand(exportCmd)
	rootCmd.AddCommand(anonymizeCmd)
	rootCmd.AddCommand(statsCmd)
	rootCmd.AddCommand(batchCmd)
}

func runInspect(cmd *cobra.Command, args []string) error {
	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		return fmt.Errorf("input file does not exist: %s", inputFile)
	}

	format := detectFormat(inputFile, formatFlag)
	if format == parser.FormatUnknown {
		return fmt.Errorf("unable to detect input format")
	}

	// Create parser
	parserCfg := parser.DefaultConfig()
	parserCfg.CaseIDColumn = csvCaseIDColumn
	parserCfg.ActivityColumn = csvActivityColumn
	parserCfg.TimestampColumn = csvTimestampColumn
	parserCfg.ResourceColumn = csvResourceColumn

	inputParser, err := parser.NewParser(format, parserCfg)
	if err != nil {
		return fmt.Errorf("failed to create parser: %w", err)
	}

	// Open input file
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input: %w", err)
	}
	defer file.Close()

	// Create quality analyzer
	analyzer := inspect.NewQualityAnalyzer()

	// Create event channel
	eventChan := make(chan *model.Event, 4096)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Parse and analyze
	go func() {
		defer close(eventChan)
		inputParser.Parse(ctx, file, eventChan)
	}()

	for event := range eventChan {
		analyzer.Add(event)
	}

	// Generate report
	report := analyzer.Report()

	if jsonOutput {
		jsonBytes, err := report.ToJSON()
		if err != nil {
			return err
		}
		fmt.Println(string(jsonBytes))
	} else {
		fmt.Print(report.String())

		// Print issues
		if len(report.Issues) > 0 {
			fmt.Println("\nIssues:")
			for _, issue := range report.Issues {
				fmt.Printf("  [%s] %s: %s (%d rows)\n",
					issue.Severity, issue.Category, issue.Description, issue.AffectedRows)
			}
		}

		// Print warnings
		if len(report.Warnings) > 0 {
			fmt.Println("\nWarnings:")
			for _, warning := range report.Warnings {
				fmt.Printf("  - %s\n", warning)
			}
		}

		// Print top activities
		if len(report.Distribution.TopActivities) > 0 {
			fmt.Println("\nTop Activities:")
			for i, act := range report.Distribution.TopActivities {
				if i >= 5 {
					break
				}
				fmt.Printf("  %d. %s (%d occurrences)\n", i+1, act.Activity, act.Count)
			}
		}
	}

	return nil
}

func runSample(cmd *cobra.Command, args []string) error {
	if sampleRate == 0 && sampleCount == 0 {
		return fmt.Errorf("either --rate or --count must be specified")
	}

	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		return fmt.Errorf("input file does not exist: %s", inputFile)
	}

	format := detectFormat(inputFile, formatFlag)
	if format == parser.FormatUnknown {
		return fmt.Errorf("unable to detect input format")
	}

	// Create parser
	parserCfg := parser.DefaultConfig()
	inputParser, err := parser.NewParser(format, parserCfg)
	if err != nil {
		return err
	}

	// Open input
	file, err := os.Open(inputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create sampler
	var sampler interface {
		Add(*model.Event)
		Sample() []*model.Event
	}

	if sampleCount > 0 {
		sampler = transform.NewReservoirSampler(sampleCount)
	} else {
		// Use rate sampler wrapped in a collector
		rateSampler := transform.NewRateSampler(sampleRate)
		collector := &rateSampleCollector{
			sampler: rateSampler,
			samples: make([]*model.Event, 0),
		}
		sampler = collector
	}

	// Parse and sample
	eventChan := make(chan *model.Event, 4096)
	ctx := context.Background()

	go func() {
		defer close(eventChan)
		inputParser.Parse(ctx, file, eventChan)
	}()

	totalEvents := 0
	for event := range eventChan {
		totalEvents++
		sampler.Add(event)
	}

	// Get samples
	samples := sampler.Sample()

	if verbose {
		fmt.Printf("Total events: %d\n", totalEvents)
		fmt.Printf("Sampled events: %d\n", len(samples))
	}

	// Write samples to output
	writerCfg := writer.Config{
		BatchSize:   1024,
		Compression: writer.ParseCompression(compressionFlag),
	}

	outFile, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer outFile.Close()

	parquetWriter, err := writer.NewParquetWriter(outFile, writerCfg)
	if err != nil {
		return err
	}

	// Create channel for writer
	sampleChan := make(chan *model.Event, len(samples))
	go func() {
		for _, event := range samples {
			sampleChan <- event
		}
		close(sampleChan)
	}()

	if err := parquetWriter.Write(ctx, sampleChan); err != nil {
		return err
	}

	if err := parquetWriter.Close(); err != nil {
		return err
	}

	fmt.Printf("Sampled %d events to %s\n", len(samples), outputFile)
	return nil
}

// rateSampleCollector wraps RateSampler to implement the sampler interface.
type rateSampleCollector struct {
	sampler *transform.RateSampler
	samples []*model.Event
}

func (c *rateSampleCollector) Add(event *model.Event) {
	if c.sampler.ShouldInclude() {
		// Deep copy
		cp := &model.Event{
			CaseID:    make([]byte, len(event.CaseID)),
			Activity:  make([]byte, len(event.Activity)),
			Timestamp: event.Timestamp,
			Resource:  make([]byte, len(event.Resource)),
		}
		copy(cp.CaseID, event.CaseID)
		copy(cp.Activity, event.Activity)
		copy(cp.Resource, event.Resource)
		c.samples = append(c.samples, cp)
	}
}

func (c *rateSampleCollector) Sample() []*model.Event {
	return c.samples
}

func runExport(cmd *cobra.Command, args []string) error {
	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		return fmt.Errorf("input file does not exist: %s", inputFile)
	}

	switch exportFormat {
	case "powerbi", "tableau":
		return runStarSchemaExport()
	case "csv":
		return runCSVExport()
	default:
		return fmt.Errorf("unsupported export format: %s", exportFormat)
	}
}

func runStarSchemaExport() error {
	exporter, err := export.NewStarSchemaExporter(outputDir, compressionFlag)
	if err != nil {
		return err
	}
	defer exporter.Close()

	if verbose {
		fmt.Println("Generating star schema...")
	}

	result, err := exporter.Export(inputFile)
	if err != nil {
		return fmt.Errorf("export failed: %w", err)
	}

	fmt.Printf("Star schema exported to %s:\n", result.OutputDir)
	for _, file := range result.Files() {
		fmt.Printf("  - %s\n", file)
	}

	return nil
}

func runCSVExport() error {
	// Use DuckDB to export Parquet to CSV
	reader, err := writer.NewDuckDBCSVReader()
	if err != nil {
		return err
	}
	defer reader.Close()

	// This is a simplified export - in production you'd use DuckDB COPY
	return fmt.Errorf("CSV export not yet implemented - use DuckDB CLI: duckdb -c \"COPY (SELECT * FROM '%s') TO '%s' (FORMAT CSV, HEADER)\"", inputFile, outputDir)
}

func runAnonymize(cmd *cobra.Command, args []string) error {
	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		return fmt.Errorf("input file does not exist: %s", inputFile)
	}

	format := detectFormat(inputFile, formatFlag)
	if format == parser.FormatUnknown {
		return fmt.Errorf("unable to detect input format")
	}

	// Create anonymizer
	salt := anonymizeSalt
	if salt == "" {
		salt = "logflow-default-salt"
		fmt.Println("Warning: Using default salt. For production, specify --salt")
	}
	anonymizer := transform.NewAnonymizer(salt)

	// Build column set for quick lookup
	anonColumns := make(map[string]bool)
	for _, col := range anonymizeColumns {
		anonColumns[col] = true
	}

	// Create parser
	parserCfg := parser.DefaultConfig()
	parserCfg.CaseIDColumn = csvCaseIDColumn
	parserCfg.ActivityColumn = csvActivityColumn
	parserCfg.TimestampColumn = csvTimestampColumn
	parserCfg.ResourceColumn = csvResourceColumn

	inputParser, err := parser.NewParser(format, parserCfg)
	if err != nil {
		return err
	}

	// Open input
	file, err := os.Open(inputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create output
	writerCfg := writer.Config{
		BatchSize:   1024,
		Compression: writer.ParseCompression(compressionFlag),
	}

	outFile, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer outFile.Close()

	parquetWriter, err := writer.NewParquetWriter(outFile, writerCfg)
	if err != nil {
		return err
	}

	// Create channels
	inputChan := make(chan *model.Event, 4096)
	outputChan := make(chan *model.Event, 4096)
	ctx := context.Background()

	// Parser goroutine
	go func() {
		defer close(inputChan)
		inputParser.Parse(ctx, file, inputChan)
	}()

	// Anonymize goroutine
	go func() {
		defer close(outputChan)
		for event := range inputChan {
			// Anonymize specified columns
			if anonColumns["case_id"] || anonColumns["case:concept:name"] {
				event.CaseID = anonymizer.HashBytes(event.CaseID)
			}
			if anonColumns["resource"] || anonColumns["org:resource"] {
				event.Resource = anonymizer.HashBytes(event.Resource)
			}
			// Anonymize attributes
			for i := range event.Attributes {
				if anonColumns[string(event.Attributes[i].Key)] {
					event.Attributes[i].Value = anonymizer.HashBytes(event.Attributes[i].Value)
				}
			}
			outputChan <- event
		}
	}()

	// Write
	if err := parquetWriter.Write(ctx, outputChan); err != nil {
		return err
	}

	if err := parquetWriter.Close(); err != nil {
		return err
	}

	fmt.Printf("Anonymized data written to %s\n", outputFile)
	return nil
}

func runStats(cmd *cobra.Command, args []string) error {
	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		return fmt.Errorf("input file does not exist: %s", inputFile)
	}

	format := detectFormat(inputFile, formatFlag)

	// Quick stats using line counting for CSV
	if format == parser.FormatCSV {
		file, err := os.Open(inputFile)
		if err != nil {
			return err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		lineCount := 0
		for scanner.Scan() {
			lineCount++
		}

		stat, _ := file.Stat()

		if jsonOutput {
			fmt.Printf(`{"file":"%s","size":%d,"lines":%d,"format":"%s"}%s`,
				inputFile, stat.Size(), lineCount, format, "\n")
		} else {
			fmt.Printf("File:   %s\n", inputFile)
			fmt.Printf("Size:   %s\n", humanSize(stat.Size()))
			fmt.Printf("Lines:  %d (including header)\n", lineCount)
			fmt.Printf("Format: %s\n", format)
		}
		return nil
	}

	// For other formats, do full inspection
	return runInspect(cmd, args)
}

func runDiff(cmd *cobra.Command, args []string) error {
	leftPath := args[0]
	rightPath := args[1]

	// Validate files exist
	for _, path := range []string{leftPath, rightPath} {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return fmt.Errorf("file does not exist: %s", path)
		}
	}

	// Detect format (use same format for both, or detect separately)
	leftFormat := detectFormat(leftPath, formatFlag)
	rightFormat := detectFormat(rightPath, formatFlag)

	if leftFormat == parser.FormatUnknown {
		return fmt.Errorf("unable to detect format for: %s", leftPath)
	}
	if rightFormat == parser.FormatUnknown {
		return fmt.Errorf("unable to detect format for: %s", rightPath)
	}

	// Create parsers
	parserCfg := parser.DefaultConfig()
	parserCfg.CaseIDColumn = csvCaseIDColumn
	parserCfg.ActivityColumn = csvActivityColumn
	parserCfg.TimestampColumn = csvTimestampColumn
	parserCfg.ResourceColumn = csvResourceColumn

	leftParser, err := parser.NewParser(leftFormat, parserCfg)
	if err != nil {
		return fmt.Errorf("failed to create parser for left file: %w", err)
	}

	rightParser, err := parser.NewParser(rightFormat, parserCfg)
	if err != nil {
		return fmt.Errorf("failed to create parser for right file: %w", err)
	}

	ctx := context.Background()

	// Use PMPT structural diff if requested
	if structuralDiff {
		return runStructuralDiff(ctx, leftPath, rightPath, leftParser, rightParser)
	}

	// Standard semantic diff
	analyzer := diff.NewAnalyzer()

	// Parse left file
	leftReader, leftCleanup, err := util.OpenFile(leftPath)
	if err != nil {
		return fmt.Errorf("failed to open left file: %w", err)
	}
	defer leftCleanup()

	leftChan := make(chan *model.Event, 4096)
	go func() {
		defer close(leftChan)
		leftParser.Parse(ctx, leftReader, leftChan)
	}()

	for event := range leftChan {
		analyzer.AddLeft(event)
	}

	// Parse right file
	rightReader, rightCleanup, err := util.OpenFile(rightPath)
	if err != nil {
		return fmt.Errorf("failed to open right file: %w", err)
	}
	defer rightCleanup()

	rightChan := make(chan *model.Event, 4096)
	go func() {
		defer close(rightChan)
		rightParser.Parse(ctx, rightReader, rightChan)
	}()

	for event := range rightChan {
		analyzer.AddRight(event)
	}

	// Generate report
	report := analyzer.Report()

	if jsonOutput {
		// JSON output would need marshaling - for now just show summary
		fmt.Printf(`{"left_events":%d,"right_events":%d,"left_cases":%d,"right_cases":%d,"duration_delta_pct":%.2f}`,
			report.LeftEventCount, report.RightEventCount,
			report.LeftCaseCount, report.RightCaseCount,
			report.CaseDurationDelta)
		fmt.Println()
	} else {
		fmt.Print(report.String())
	}

	return nil
}

// runStructuralDiff performs PMPT-based structural comparison.
func runStructuralDiff(ctx context.Context, leftPath, rightPath string, leftParser, rightParser parser.Parser) error {
	fmt.Println("Building Process Merkle Patricia Trees...")

	// Build left tree
	leftBuilder := pmpt.NewBuilder(pmpt.DefaultBuilderConfig())
	leftReader, leftCleanup, err := util.OpenFile(leftPath)
	if err != nil {
		return fmt.Errorf("failed to open left file: %w", err)
	}
	defer leftCleanup()

	leftChan := make(chan *model.Event, 4096)
	go func() {
		defer close(leftChan)
		leftParser.Parse(ctx, leftReader, leftChan)
	}()

	for event := range leftChan {
		leftBuilder.Add(event)
	}
	leftTree := leftBuilder.FlushAll()

	// Build right tree
	rightBuilder := pmpt.NewBuilder(pmpt.DefaultBuilderConfig())
	rightReader, rightCleanup, err := util.OpenFile(rightPath)
	if err != nil {
		return fmt.Errorf("failed to open right file: %w", err)
	}
	defer rightCleanup()

	rightChan := make(chan *model.Event, 4096)
	go func() {
		defer close(rightChan)
		rightParser.Parse(ctx, rightReader, rightChan)
	}()

	for event := range rightChan {
		rightBuilder.Add(event)
	}
	rightTree := rightBuilder.FlushAll()

	// O(1) fingerprint comparison
	fmt.Printf("\nFingerprint Check (O(1)):\n")
	fmt.Printf("  Left:  %s\n", leftTree.Fingerprint())
	fmt.Printf("  Right: %s\n", rightTree.Fingerprint())

	if leftTree.Equals(rightTree) {
		fmt.Println("\nResult: IDENTICAL PROCESSES")
		fmt.Println("The two logs have identical process structures.")
		return nil
	}

	fmt.Println("\nResult: PROCESSES DIFFER")

	// Detailed structural diff
	treeDiff := pmpt.Compare(leftTree, rightTree)

	if jsonOutput {
		fmt.Printf(`{"identical":false,"left_hash":"%s","right_hash":"%s","new_nodes":%d,"removed_nodes":%d}`,
			treeDiff.LeftRootHash.FullString(),
			treeDiff.RightRootHash.FullString(),
			len(treeDiff.NewNodes),
			len(treeDiff.RemovedNodes))
		fmt.Println()
	} else {
		fmt.Print(treeDiff.String())

		// Show tree stats
		leftStats := leftTree.Stats()
		rightStats := rightTree.Stats()

		fmt.Println("\nTree Statistics:")
		fmt.Printf("  %-20s %12s %12s\n", "", "Left", "Right")
		fmt.Printf("  %-20s %12d %12d\n", "Total Cases", leftStats.TotalCases, rightStats.TotalCases)
		fmt.Printf("  %-20s %12d %12d\n", "Total Events", leftStats.TotalEvents, rightStats.TotalEvents)
		fmt.Printf("  %-20s %12d %12d\n", "Unique Nodes", leftStats.UniqueNodes, rightStats.UniqueNodes)
		fmt.Printf("  %-20s %12d %12d\n", "Process Variants", leftStats.VariantCount, rightStats.VariantCount)
		fmt.Printf("  %-20s %12d %12d\n", "Max Depth", leftStats.MaxDepth, rightStats.MaxDepth)
	}

	return nil
}

// BatchResult holds results from a single file conversion.
type BatchResult struct {
	InputPath  string
	OutputPath string
	RowsWritten int64
	Duration    time.Duration
	Error       error
}

func runBatch(cmd *cobra.Command, args []string) error {
	// Expand glob patterns and collect all input files
	var inputFiles []string
	for _, pattern := range args {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return fmt.Errorf("invalid glob pattern %q: %w", pattern, err)
		}
		if len(matches) == 0 {
			// Try as literal path
			if _, err := os.Stat(pattern); err == nil {
				inputFiles = append(inputFiles, pattern)
			} else {
				fmt.Fprintf(os.Stderr, "Warning: no files match pattern %q\n", pattern)
			}
		} else {
			inputFiles = append(inputFiles, matches...)
		}
	}

	if len(inputFiles) == 0 {
		return fmt.Errorf("no input files found")
	}

	// Create output directory if needed
	if err := os.MkdirAll(batchOutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	fmt.Printf("Converting %d files with %d workers...\n\n", len(inputFiles), parallelWorkers)

	// Set up signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nInterrupted, stopping workers...")
		cancel()
	}()

	// Create results channel
	results := make(chan BatchResult, len(inputFiles))

	// Track progress
	var completed atomic.Int64
	var succeeded atomic.Int64
	var failed atomic.Int64
	totalFiles := int64(len(inputFiles))

	// Use errgroup with limited concurrency
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(parallelWorkers)

	startTime := time.Now()

	// Submit all files for processing
	for _, inputPath := range inputFiles {
		inputPath := inputPath // capture
		g.Go(func() error {
			result := convertFile(ctx, inputPath, batchOutputDir)
			results <- result

			done := completed.Add(1)
			if result.Error != nil {
				failed.Add(1)
				if failFast {
					return result.Error
				}
			} else {
				succeeded.Add(1)
			}

			// Progress update
			pct := float64(done) / float64(totalFiles) * 100
			fmt.Printf("\r[%3.0f%%] %d/%d files processed", pct, done, totalFiles)

			return nil
		})
	}

	// Wait for all workers
	err := g.Wait()
	close(results)

	// Collect all results
	var allResults []BatchResult
	for r := range results {
		allResults = append(allResults, r)
	}

	totalDuration := time.Since(startTime)

	// Print summary
	fmt.Printf("\r\033[K") // Clear progress line
	fmt.Println()
	fmt.Printf("=== Batch Conversion Complete ===\n\n")
	fmt.Printf("  Total files: %d\n", totalFiles)
	fmt.Printf("  Succeeded:   %d\n", succeeded.Load())
	fmt.Printf("  Failed:      %d\n", failed.Load())
	fmt.Printf("  Duration:    %v\n", totalDuration.Round(time.Millisecond))
	fmt.Printf("  Throughput:  %.1f files/sec\n", float64(totalFiles)/totalDuration.Seconds())

	// Print errors if any
	if failed.Load() > 0 {
		fmt.Println("\nErrors:")
		for _, r := range allResults {
			if r.Error != nil {
				fmt.Printf("  %s: %v\n", filepath.Base(r.InputPath), r.Error)
			}
		}
	}

	if err != nil && failFast {
		return fmt.Errorf("batch conversion failed: %w", err)
	}

	if failed.Load() > 0 {
		return fmt.Errorf("%d files failed to convert", failed.Load())
	}

	return nil
}

// convertFile converts a single file and returns the result.
func convertFile(ctx context.Context, inputPath, outputDir string) BatchResult {
	start := time.Now()

	// Determine output path
	baseName := filepath.Base(inputPath)
	baseName = strings.TrimSuffix(baseName, filepath.Ext(baseName))
	if strings.HasSuffix(baseName, ".gz") {
		baseName = strings.TrimSuffix(baseName, ".gz")
		baseName = strings.TrimSuffix(baseName, filepath.Ext(baseName))
	}
	outputPath := filepath.Join(outputDir, baseName+".parquet")

	result := BatchResult{
		InputPath:  inputPath,
		OutputPath: outputPath,
	}

	// Detect format
	format := detectFormat(inputPath, formatFlag)
	if format == parser.FormatUnknown {
		result.Error = fmt.Errorf("unable to detect format")
		result.Duration = time.Since(start)
		return result
	}

	// Create pipeline
	parserCfg := parser.DefaultConfig()
	parserCfg.CaseIDColumn = csvCaseIDColumn
	parserCfg.ActivityColumn = csvActivityColumn
	parserCfg.TimestampColumn = csvTimestampColumn
	parserCfg.ResourceColumn = csvResourceColumn

	writerCfg := writer.DefaultConfig()
	writerCfg.Compression = writer.ParseCompression(compressionFlag)

	pipeCfg := pipe.Config{
		ParserConfig:    parserCfg,
		WriterConfig:    writerCfg,
		EventBufferSize: 8192,
	}

	pipeline := pipe.NewPipeline(pipeCfg)

	// Run conversion
	pipeResult, err := pipeline.Ingest(ctx, inputPath, outputPath, format)
	if err != nil {
		result.Error = err
		result.Duration = time.Since(start)
		return result
	}

	result.RowsWritten = pipeResult.RowsWritten
	result.Duration = time.Since(start)
	return result
}

func runWatch(cmd *cobra.Command, args []string) error {
	inputPath := args[0]
	outputPath := args[1]

	// Validate input exists
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		return fmt.Errorf("input file does not exist: %s", inputPath)
	}

	format := detectFormat(inputPath, formatFlag)
	if format == parser.FormatUnknown {
		return fmt.Errorf("unable to detect format for: %s", inputPath)
	}

	// Create watcher
	w, err := watch.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create watcher: %w", err)
	}
	defer w.Close()

	// Track conversion state
	conversionCount := 0

	// Set up change handler
	w.OnChange = func(path string) error {
		conversionCount++
		start := time.Now()
		fmt.Printf("[%s] Change detected, converting... ", time.Now().Format("15:04:05"))

		// Run conversion
		ctx := context.Background()
		parserCfg := parser.DefaultConfig()
		parserCfg.CaseIDColumn = csvCaseIDColumn
		parserCfg.ActivityColumn = csvActivityColumn
		parserCfg.TimestampColumn = csvTimestampColumn
		parserCfg.ResourceColumn = csvResourceColumn

		writerCfg := writer.Config{
			BatchSize:   1024,
			Compression: writer.ParseCompression(compressionFlag),
		}

		// Open and parse
		reader, cleanup, err := util.OpenFile(inputPath)
		if err != nil {
			return err
		}
		defer cleanup()

		inputParser, err := parser.NewParser(format, parserCfg)
		if err != nil {
			return err
		}

		// Create output
		outFile, err := os.Create(outputPath)
		if err != nil {
			return err
		}
		defer outFile.Close()

		parquetWriter, err := writer.NewParquetWriter(outFile, writerCfg)
		if err != nil {
			return err
		}

		// Parse and write
		eventChan := make(chan *model.Event, 4096)
		go func() {
			defer close(eventChan)
			inputParser.Parse(ctx, reader, eventChan)
		}()

		if err := parquetWriter.Write(ctx, eventChan); err != nil {
			return err
		}
		if err := parquetWriter.Close(); err != nil {
			return err
		}

		elapsed := time.Since(start)
		fmt.Printf("done in %v\n", elapsed.Round(time.Millisecond))
		return nil
	}

	w.OnError = func(path string, err error) {
		fmt.Printf("[%s] Error: %v\n", time.Now().Format("15:04:05"), err)
	}

	// Watch the input file
	if err := w.Watch(inputPath); err != nil {
		return fmt.Errorf("failed to watch file: %w", err)
	}

	// Initial conversion
	fmt.Printf("Watching %s -> %s\n", inputPath, outputPath)
	fmt.Println("Press Ctrl+C to stop")
	fmt.Println()

	if err := w.OnChange(inputPath); err != nil {
		fmt.Printf("Initial conversion failed: %v\n", err)
	}

	// Set up signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Printf("\nStopping watch (processed %d conversions)\n", conversionCount)
		cancel()
	}()

	// Run the watcher
	return w.Run(ctx)
}
