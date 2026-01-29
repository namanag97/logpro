// LogFlow - High-performance process mining data converter
// Converts CSV, XES, and JSON files to Apache Parquet format.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/logflow/logflow/internal/pipe"
	"github.com/logflow/logflow/pkg/contract"
	"github.com/logflow/logflow/pkg/parser"
	"github.com/logflow/logflow/pkg/profile"
	"github.com/logflow/logflow/pkg/tui"
	"github.com/logflow/logflow/pkg/util"
	"github.com/logflow/logflow/pkg/writer"
)

var (
	version = "0.1.0"
	commit  = "dev"
)

// CLI flags
var (
	inputFile       string
	outputFile      string
	formatFlag      string
	compressionFlag string
	batchSize       int
	bufferSize      int
	verbose         bool
	useDuckDB       bool

	// CSV-specific flags
	csvDelimiter       string
	csvCaseIDColumn    string
	csvActivityColumn  string
	csvTimestampColumn string
	csvResourceColumn  string
	csvTimestampFormat string

	// Metadata flags
	metadataFlags []string

	// Profile flags
	profileName string

	// Contract flags
	generateContract bool
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "logflow",
	Short: "LogFlow - Convert process mining data to Parquet",
	Long: `LogFlow is a high-performance CLI tool for converting process mining data (CSV, XES, JSON) to Apache Parquet format.

Run without arguments to launch the interactive wizard.`,
	Version: fmt.Sprintf("%s (%s)", version, commit),
	RunE:    runWizard,
}

var applyCmd = &cobra.Command{
	Use:   "apply [input-file]",
	Short: "Apply a saved profile to convert a file",
	Long: `Apply a previously saved LogFlow profile to convert a file.

Looks for profiles in order:
  1. <input>.logflow.yaml (file-specific)
  2. .logflow.yaml (directory)
  3. ~/.logflow/<profile>.logflow.yaml (global)

Examples:
  logflow apply data.csv
  logflow apply --profile sales data.csv`,
	RunE: runApply,
}

var profileListCmd = &cobra.Command{
	Use:   "profiles",
	Short: "List saved profiles",
	RunE:  runProfileList,
}

var convertCmd = &cobra.Command{
	Use:   "convert",
	Short: "Convert input file to Parquet format",
	Long: `Convert process mining data files (CSV, XES, JSON) to Apache Parquet format.

Supports reading from stdin using "-" as the input path.

Examples:
  logflow convert -i events.csv -o events.parquet
  logflow convert -i trace.xes -o trace.parquet --format xes
  logflow convert -i data.csv -o data.parquet --compression zstd
  logflow convert -i data.csv -o data.parquet --duckdb
  cat events.csv | logflow convert -i - -o events.parquet --format csv
  logflow convert -i events.csv -o events.parquet --metadata "pm:source=ERP"`,
	RunE: runConvert,
}

var infoCmd = &cobra.Command{
	Use:   "info",
	Short: "Display information about an input file",
	Long:  `Analyze and display information about a process mining data file.`,
	RunE:  runInfo,
}

var schemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "Infer and display schema from a CSV file",
	Long:  `Use DuckDB's schema inference to analyze a CSV file and display column types.`,
	RunE:  runSchema,
}

func init() {
	// Global flags
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")

	// Convert command flags
	convertCmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input file path (use '-' for stdin)")
	convertCmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output Parquet file path (required)")
	convertCmd.Flags().StringVarP(&formatFlag, "format", "f", "", "Input format (csv, xes, json) - auto-detected if not specified")
	convertCmd.Flags().StringVar(&compressionFlag, "compression", "snappy", "Parquet compression (none, snappy, gzip, zstd, lz4)")
	convertCmd.Flags().IntVar(&batchSize, "batch-size", 1024, "Number of events per batch")
	convertCmd.Flags().IntVar(&bufferSize, "buffer-size", 65536, "Read buffer size in bytes")
	convertCmd.Flags().BoolVar(&useDuckDB, "duckdb", false, "Use DuckDB engine for faster CSV processing")

	// CSV-specific flags
	convertCmd.Flags().StringVar(&csvDelimiter, "delimiter", ",", "CSV field delimiter")
	convertCmd.Flags().StringVar(&csvCaseIDColumn, "case-id", "case:concept:name", "Case ID column name")
	convertCmd.Flags().StringVar(&csvActivityColumn, "activity", "concept:name", "Activity column name")
	convertCmd.Flags().StringVar(&csvTimestampColumn, "timestamp", "time:timestamp", "Timestamp column name")
	convertCmd.Flags().StringVar(&csvResourceColumn, "resource", "org:resource", "Resource column name")
	convertCmd.Flags().StringVar(&csvTimestampFormat, "timestamp-format", "2006-01-02T15:04:05.000Z07:00", "Timestamp format (Go time layout)")

	// Metadata flags
	convertCmd.Flags().StringArrayVar(&metadataFlags, "metadata", nil, "Key-value metadata for Parquet footer (format: key=value)")

	// Contract flags
	convertCmd.Flags().BoolVar(&generateContract, "contract", false, "Generate data contract (.contract.json) alongside output")

	convertCmd.MarkFlagRequired("input")
	convertCmd.MarkFlagRequired("output")

	// Info command flags
	infoCmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input file path (required)")
	infoCmd.Flags().StringVarP(&formatFlag, "format", "f", "", "Input format (csv, xes, json)")
	infoCmd.MarkFlagRequired("input")

	// Schema command flags
	schemaCmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input CSV file path (required)")
	schemaCmd.MarkFlagRequired("input")

	// Apply command flags
	applyCmd.Flags().StringVar(&profileName, "profile", "", "Profile name to use")

	// Add commands
	rootCmd.AddCommand(convertCmd)
	rootCmd.AddCommand(infoCmd)
	rootCmd.AddCommand(schemaCmd)
	rootCmd.AddCommand(applyCmd)
	rootCmd.AddCommand(profileListCmd)
}

func runConvert(cmd *cobra.Command, args []string) error {
	isStdin := inputFile == "-"

	// Validate input file exists (unless stdin)
	if !isStdin {
		if _, err := os.Stat(inputFile); os.IsNotExist(err) {
			return fmt.Errorf("input file does not exist: %s", inputFile)
		}
	}

	// Auto-detect format if not specified
	format := detectFormat(inputFile, formatFlag)
	if format == parser.FormatUnknown {
		if isStdin {
			return fmt.Errorf("format must be specified when reading from stdin (--format csv/xes/json)")
		}
		return fmt.Errorf("unable to detect input format, please specify with --format")
	}

	// Parse metadata
	metadata := parseMetadata(metadataFlags)

	if verbose {
		fmt.Printf("Input:       %s\n", inputFile)
		fmt.Printf("Output:      %s\n", outputFile)
		fmt.Printf("Format:      %s\n", format)
		fmt.Printf("Compression: %s\n", compressionFlag)
		fmt.Printf("Engine:      %s\n", engineName())
		if len(metadata) > 0 {
			fmt.Printf("Metadata:    %v\n", metadata)
		}
	}

	// Setup context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nInterrupted, cleaning up...")
		cancel()
	}()

	var result *pipe.IngestResult
	var err error

	startTime := time.Now()

	// Use DuckDB pipeline for CSV when --duckdb flag is set, or always for maximum performance
	if useDuckDB || (format == parser.FormatCSV && !isStdin) {
		result, err = runDuckDBConvert(ctx, format, metadata)
	} else {
		result, err = runArrowConvert(ctx, format, metadata)
	}

	elapsed := time.Since(startTime)

	if err != nil {
		return fmt.Errorf("conversion failed: %w", err)
	}

	// Clear progress line if shown
	tui.ClearLine()

	// Get file sizes for compression ratio
	inputStat, _ := os.Stat(inputFile)
	outputStat, _ := os.Stat(outputFile)

	inputSize := int64(0)
	outputSize := int64(0)
	if inputStat != nil {
		inputSize = inputStat.Size()
	}
	if outputStat != nil {
		outputSize = outputStat.Size()
	}

	// Print quality report with timing
	report := &tui.QualityReport{
		EventsProcessed: result.RowsWritten,
		InputSize:       inputSize,
		OutputSize:      outputSize,
		Duration:        elapsed,
	}
	tui.PrintQualityReport(report)

	// Generate data contract if requested
	if generateContract {
		cfg := contract.ContractConfig{
			CaseIDColumn:    csvCaseIDColumn,
			ActivityColumn:  csvActivityColumn,
			TimestampColumn: csvTimestampColumn,
			ResourceColumn:  csvResourceColumn,
			NullTolerance:   0.05,
			TimestampOrder:  "asc",
			ToolVersion:     version,
		}

		c, err := contract.Generate(outputFile, result.RowsWritten, cfg)
		if err != nil {
			return fmt.Errorf("failed to generate contract: %w", err)
		}

		if err := contract.WriteContract(outputFile, c); err != nil {
			return fmt.Errorf("failed to write contract: %w", err)
		}

		if verbose {
			fmt.Printf("Contract: %s\n", contract.ContractPath(outputFile))
		}
	}

	return nil
}

func runDuckDBConvert(ctx context.Context, format parser.Format, metadata map[string]string) (*pipe.IngestResult, error) {
	parserCfg := parser.Config{
		BatchSize:       batchSize,
		BufferSize:      bufferSize,
		CaseIDColumn:    csvCaseIDColumn,
		ActivityColumn:  csvActivityColumn,
		TimestampColumn: csvTimestampColumn,
		ResourceColumn:  csvResourceColumn,
		TimestampFormat: csvTimestampFormat,
		Delimiter:       csvDelimiter[0],
	}

	writerCfg := writer.DefaultConfig()
	writerCfg.BatchSize = batchSize
	writerCfg.Compression = writer.ParseCompression(compressionFlag)

	pipeCfg := pipe.Config{
		ParserConfig:    parserCfg,
		WriterConfig:    writerCfg,
		EventBufferSize: batchSize * 4,
	}

	pipeline := pipe.NewDuckDBPipeline(pipeCfg)

	// Add metadata
	for k, v := range metadata {
		pipeline.SetMetadata(k, v)
	}

	if verbose {
		fmt.Println("Processing with DuckDB engine...")
	}

	return pipeline.Ingest(ctx, inputFile, outputFile, format)
}

func runArrowConvert(ctx context.Context, format parser.Format, metadata map[string]string) (*pipe.IngestResult, error) {
	parserCfg := parser.Config{
		BatchSize:       batchSize,
		BufferSize:      bufferSize,
		CaseIDColumn:    csvCaseIDColumn,
		ActivityColumn:  csvActivityColumn,
		TimestampColumn: csvTimestampColumn,
		ResourceColumn:  csvResourceColumn,
		TimestampFormat: csvTimestampFormat,
		Delimiter:       csvDelimiter[0],
	}

	writerCfg := writer.DefaultConfig()
	writerCfg.BatchSize = batchSize
	writerCfg.Compression = writer.ParseCompression(compressionFlag)

	pipeCfg := pipe.Config{
		ParserConfig:    parserCfg,
		WriterConfig:    writerCfg,
		EventBufferSize: batchSize * 4,
	}

	pipeline := pipe.NewPipeline(pipeCfg)

	// Set progress callback for verbose mode
	if verbose {
		fmt.Println("Processing with Arrow engine...")
		pipeline.SetProgressCallback(func(stats pipe.ProgressStats) {
			tui.PrintProgress(stats.EventsProcessed, stats.EventsPerSecond, stats.ElapsedTime)
		})
	}

	isStdin := inputFile == "-"

	if isStdin {
		return pipeline.IngestFromReaderToWriter(ctx, os.Stdin, mustCreateFile(outputFile), format)
	}

	return pipeline.Ingest(ctx, inputFile, outputFile, format)
}

func runInfo(cmd *cobra.Command, args []string) error {
	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		return fmt.Errorf("input file does not exist: %s", inputFile)
	}

	format := detectFormat(inputFile, formatFlag)

	stat, err := os.Stat(inputFile)
	if err != nil {
		return err
	}

	fmt.Printf("File:   %s\n", inputFile)
	fmt.Printf("Size:   %s\n", humanSize(stat.Size()))
	fmt.Printf("Format: %s\n", format)

	return nil
}

func runSchema(cmd *cobra.Command, args []string) error {
	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		return fmt.Errorf("input file does not exist: %s", inputFile)
	}

	reader, err := writer.NewDuckDBCSVReader()
	if err != nil {
		return fmt.Errorf("failed to create schema reader: %w", err)
	}
	defer reader.Close()

	columns, err := reader.GetSchemaInfo(inputFile)
	if err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}

	fmt.Printf("Schema for %s:\n", inputFile)
	fmt.Printf("%-30s %s\n", "Column", "Type")
	fmt.Printf("%-30s %s\n", strings.Repeat("-", 30), strings.Repeat("-", 20))
	for _, col := range columns {
		fmt.Printf("%-30s %s\n", col.Name, col.Type)
	}

	return nil
}

// detectFormat determines the input format from file extension or flag.
// Handles compressed files (.gz) by stripping the compression extension first.
func detectFormat(path, formatStr string) parser.Format {
	if formatStr != "" {
		return parser.ParseFormat(formatStr)
	}

	if path == "-" {
		return parser.FormatUnknown
	}

	// Use BaseFormat to handle .gz compression transparently
	ext := util.BaseFormat(path)
	switch ext {
	case ".csv":
		return parser.FormatCSV
	case ".xes":
		return parser.FormatXES
	case ".json":
		return parser.FormatJSON
	case ".xlsx":
		return parser.FormatXLSX
	case ".parquet":
		return parser.FormatParquet
	default:
		return parser.FormatUnknown
	}
}

// parseMetadata parses key=value metadata strings.
func parseMetadata(flags []string) map[string]string {
	metadata := make(map[string]string)
	for _, flag := range flags {
		parts := strings.SplitN(flag, "=", 2)
		if len(parts) == 2 {
			metadata[parts[0]] = parts[1]
		}
	}
	return metadata
}

// engineName returns the engine name based on flags.
func engineName() string {
	if useDuckDB {
		return "DuckDB"
	}
	return "Arrow"
}

// humanSize formats a byte size in human-readable form.
func humanSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// mustCreateFile creates a file and panics on error.
func mustCreateFile(path string) *os.File {
	f, err := os.Create(path)
	if err != nil {
		panic(fmt.Sprintf("failed to create file %s: %v", path, err))
	}
	return f
}

// runWizard launches the interactive TUI wizard.
func runWizard(cmd *cobra.Command, args []string) error {
	// If subcommand was invoked, don't run wizard
	if cmd.CalledAs() != "logflow" && cmd.CalledAs() != "" {
		return nil
	}

	// Check if running in a terminal
	if fileInfo, _ := os.Stdout.Stat(); (fileInfo.Mode() & os.ModeCharDevice) == 0 {
		// Not a terminal, show help instead
		return cmd.Help()
	}

	result, err := tui.RunWizard()
	if err != nil {
		return err
	}

	if result == nil {
		// User cancelled
		return nil
	}

	// Run the conversion with wizard results
	inputFile = result.InputFile
	outputFile = result.OutputFile
	csvCaseIDColumn = result.CaseID
	csvActivityColumn = result.Activity
	csvTimestampColumn = result.Timestamp
	csvResourceColumn = result.Resource

	// Execute convert
	fmt.Println("\nStarting conversion...")
	return runConvert(cmd, args)
}

// runApply applies a saved profile to convert a file.
func runApply(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("input file required")
	}

	inputFile = args[0]

	// Find profile
	var prof *profile.Profile
	var profilePath string
	var err error

	if profileName != "" {
		// Load named profile
		profilePath, err = profile.GlobalProfilePath(profileName)
		if err != nil {
			return err
		}
		prof, err = profile.Load(profilePath)
		if err != nil {
			return fmt.Errorf("failed to load profile '%s': %w", profileName, err)
		}
	} else {
		// Auto-detect profile
		profilePath, err = profile.FindProfile(inputFile)
		if err != nil {
			return fmt.Errorf("no profile found for %s. Run 'logflow' to create one", inputFile)
		}
		prof, err = profile.Load(profilePath)
		if err != nil {
			return fmt.Errorf("failed to load profile: %w", err)
		}
		fmt.Printf("Using profile: %s\n", profilePath)
	}

	// Apply profile settings
	csvCaseIDColumn = prof.CaseID
	csvActivityColumn = prof.Activity
	csvTimestampColumn = prof.Timestamp
	csvResourceColumn = prof.Resource

	// Determine output file
	if prof.Output != "" {
		outputFile = prof.Output
	} else {
		baseName := strings.TrimSuffix(filepath.Base(inputFile), filepath.Ext(inputFile))
		outputFile = baseName + ".parquet"
	}

	if prof.Compression != "" {
		compressionFlag = prof.Compression
	}

	if prof.Delimiter != "" && len(prof.Delimiter) > 0 {
		csvDelimiter = prof.Delimiter
	}

	fmt.Printf("Converting %s -> %s\n", inputFile, outputFile)
	fmt.Printf("  Case ID:   %s\n", csvCaseIDColumn)
	fmt.Printf("  Activity:  %s\n", csvActivityColumn)
	fmt.Printf("  Timestamp: %s\n", csvTimestampColumn)
	if csvResourceColumn != "" {
		fmt.Printf("  Resource:  %s\n", csvResourceColumn)
	}
	fmt.Println()

	return runConvert(cmd, args)
}

// runProfileList lists all saved profiles.
func runProfileList(cmd *cobra.Command, args []string) error {
	profiles, err := profile.ListGlobalProfiles()
	if err != nil {
		return err
	}

	if len(profiles) == 0 {
		fmt.Println("No saved profiles found.")
		fmt.Println("Run 'logflow' to create one interactively.")
		return nil
	}

	fmt.Println("Saved profiles:")
	for _, name := range profiles {
		fmt.Printf("  - %s\n", name)
	}
	fmt.Println()
	fmt.Println("Usage: logflow apply --profile <name> <input-file>")

	return nil
}
