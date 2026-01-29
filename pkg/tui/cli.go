// Package tui provides a Claude Code-style CLI interface.
// Simple, streaming, no complex TUI - just clean prompts and output.
package tui

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/schollz/progressbar/v3"
)

// Colors (Swiss minimal)
var (
	accent    = lipgloss.Color("#FF0000")
	muted     = lipgloss.Color("#666666")
	success   = lipgloss.Color("#00CC66")
	white     = lipgloss.Color("#FFFFFF")
)

// Styles
var (
	titleStyle   = lipgloss.NewStyle().Bold(true).Foreground(white)
	accentStyle  = lipgloss.NewStyle().Foreground(accent).Bold(true)
	mutedStyle   = lipgloss.NewStyle().Foreground(muted)
	successStyle = lipgloss.NewStyle().Foreground(success).Bold(true)
	codeStyle    = lipgloss.NewStyle().Background(lipgloss.Color("#1a1a1a")).Foreground(white).Padding(0, 1)
)

// WizardResult holds the conversion configuration.
type WizardResult struct {
	InputFile  string
	OutputFile string
	CaseID     string
	Activity   string
	Timestamp  string
	Resource   string
}

// RunWizard runs the interactive CLI wizard.
func RunWizard() (*WizardResult, error) {
	reader := bufio.NewReader(os.Stdin)

	// Header
	printHeader()

	// Step 1: Get input file
	fmt.Println()
	fmt.Println(accentStyle.Render("▸ SELECT INPUT FILE"))
	fmt.Println(mutedStyle.Render("  Drag & drop a file, or type the path:"))
	fmt.Println()

	inputPath, err := promptPath(reader, "  Input: ")
	if err != nil {
		return nil, err
	}

	// Validate file exists
	info, err := os.Stat(inputPath)
	if err != nil {
		fmt.Println(accentStyle.Render("  ✗ File not found: " + inputPath))
		return nil, err
	}

	// Show file info
	format := detectFormat(inputPath)
	fmt.Println()
	fmt.Println(mutedStyle.Render("  ─────────────────────────────────────"))
	fmt.Printf("  %s %s\n", mutedStyle.Render("Format:"), titleStyle.Render(format))
	fmt.Printf("  %s %s\n", mutedStyle.Render("Size:"), titleStyle.Render(formatBytes(info.Size())))
	fmt.Println(mutedStyle.Render("  ─────────────────────────────────────"))

	// Step 2: Output path (auto-generate)
	outputPath := strings.TrimSuffix(inputPath, filepath.Ext(inputPath)) + ".parquet"
	fmt.Println()
	fmt.Printf("  %s %s\n", mutedStyle.Render("Output:"), codeStyle.Render(outputPath))

	// Step 3: Column mapping (with smart defaults)
	fmt.Println()
	fmt.Println(accentStyle.Render("▸ COLUMN MAPPING"))
	fmt.Println(mutedStyle.Render("  Press Enter to accept defaults, or type column name:"))
	fmt.Println()

	caseID, _ := promptWithDefault(reader, "  case_id", "case:concept:name")
	activity, _ := promptWithDefault(reader, "  activity", "concept:name")
	timestamp, _ := promptWithDefault(reader, "  timestamp", "time:timestamp")
	resource, _ := promptWithDefault(reader, "  resource", "org:resource")

	// Confirm
	fmt.Println()
	fmt.Println(mutedStyle.Render("  ─────────────────────────────────────"))
	fmt.Printf("  %s\n", titleStyle.Render("Ready to convert"))
	fmt.Printf("  %s → %s\n", filepath.Base(inputPath), filepath.Base(outputPath))
	fmt.Println(mutedStyle.Render("  ─────────────────────────────────────"))
	fmt.Println()

	confirm, _ := promptConfirm(reader, "  Start conversion? [Y/n]: ")
	if !confirm {
		fmt.Println(mutedStyle.Render("  Cancelled."))
		return nil, nil
	}

	return &WizardResult{
		InputFile:  inputPath,
		OutputFile: outputPath,
		CaseID:     caseID,
		Activity:   activity,
		Timestamp:  timestamp,
		Resource:   resource,
	}, nil
}

func printHeader() {
	fmt.Println()
	fmt.Println(titleStyle.Render("  LOGFLOW") + mutedStyle.Render(" v0.1.0"))
	fmt.Println(mutedStyle.Render("  High-performance process mining data converter"))
	fmt.Println()
}

func promptPath(reader *bufio.Reader, prompt string) (string, error) {
	fmt.Print(prompt)
	input, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	path := strings.TrimSpace(input)
	// Handle drag & drop (removes quotes)
	path = strings.Trim(path, "\"'")
	// Expand ~ to home dir
	if strings.HasPrefix(path, "~") {
		home, _ := os.UserHomeDir()
		path = filepath.Join(home, path[1:])
	}

	return path, nil
}

func promptWithDefault(reader *bufio.Reader, field, defaultVal string) (string, error) {
	fmt.Printf("  %s %s: ", mutedStyle.Render(field), mutedStyle.Render("["+defaultVal+"]"))
	input, err := reader.ReadString('\n')
	if err != nil {
		return defaultVal, err
	}

	input = strings.TrimSpace(input)
	if input == "" {
		return defaultVal, nil
	}
	return input, nil
}

func promptConfirm(reader *bufio.Reader, prompt string) (bool, error) {
	fmt.Print(prompt)
	input, err := reader.ReadString('\n')
	if err != nil {
		return false, err
	}

	input = strings.ToLower(strings.TrimSpace(input))
	return input == "" || input == "y" || input == "yes", nil
}

func detectFormat(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	if strings.HasSuffix(path, ".gz") {
		ext = strings.ToLower(filepath.Ext(strings.TrimSuffix(path, ".gz")))
	}
	switch ext {
	case ".csv":
		return "CSV"
	case ".xes":
		return "XES"
	case ".xlsx":
		return "XLSX"
	case ".json":
		return "JSON"
	default:
		return "Unknown"
	}
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// QualityReport for printing results.
type QualityReport struct {
	EventsProcessed int64
	InputSize       int64
	OutputSize      int64
	Duration        time.Duration
	EventsPerSecond float64
}

// PrintQualityReport prints results after conversion.
func PrintQualityReport(report *QualityReport) {
	fmt.Println()
	fmt.Println(successStyle.Render("  ✓ CONVERSION COMPLETE"))
	fmt.Println()
	fmt.Printf("  %s %s\n", mutedStyle.Render("Events:"), titleStyle.Render(formatNumber(report.EventsProcessed)))

	if report.InputSize > 0 && report.OutputSize > 0 {
		ratio := float64(report.InputSize) / float64(report.OutputSize)
		fmt.Printf("  %s %s → %s %s\n",
			mutedStyle.Render("Size:"),
			formatBytes(report.InputSize),
			formatBytes(report.OutputSize),
			successStyle.Render(fmt.Sprintf("(%.1fx compression)", ratio)))
	}

	if report.Duration > 0 {
		throughput := float64(report.EventsProcessed) / report.Duration.Seconds()
		byteThroughput := float64(report.InputSize) / report.Duration.Seconds()
		fmt.Printf("  %s %s %s\n",
			mutedStyle.Render("Time:"),
			titleStyle.Render(formatDuration(report.Duration)),
			mutedStyle.Render(fmt.Sprintf("(%s events/sec, %s/sec)", formatNumber(int64(throughput)), formatBytes(int64(byteThroughput)))))
	}
	fmt.Println()
}

// PrintProgress prints a progress update during conversion.
func PrintProgress(eventsProcessed int64, eventsPerSec float64, elapsed time.Duration) {
	fmt.Printf("\r  %s %s events %s",
		accentStyle.Render("⟳"),
		titleStyle.Render(formatNumber(eventsProcessed)),
		mutedStyle.Render(fmt.Sprintf("(%s/sec, %s)", formatNumber(int64(eventsPerSec)), formatDuration(elapsed))))
}

// ClearLine clears the current line.
func ClearLine() {
	fmt.Print("\r\033[K")
}

// formatDuration formats a duration in a human-readable way.
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
}

func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1000000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%.1fM", float64(n)/1000000)
}

// ShowProgress creates a progress bar for processing.
func ShowProgress(total int64, description string) *progressbar.ProgressBar {
	return progressbar.NewOptions64(total,
		progressbar.OptionSetDescription(description),
		progressbar.OptionSetWidth(40),
		progressbar.OptionShowBytes(false),
		progressbar.OptionShowCount(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "█",
			SaucerHead:    "█",
			SaucerPadding: "░",
			BarStart:      "",
			BarEnd:        "",
		}),
		progressbar.OptionThrottle(100*time.Millisecond),
		progressbar.OptionClearOnFinish(),
	)
}

// Spinner shows a simple loading indicator.
func Spinner(message string, done chan bool) {
	frames := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	i := 0
	for {
		select {
		case <-done:
			fmt.Printf("\r%s %s\n", successStyle.Render("✓"), message)
			return
		default:
			fmt.Printf("\r%s %s", accentStyle.Render(frames[i]), message)
			i = (i + 1) % len(frames)
			time.Sleep(80 * time.Millisecond)
		}
	}
}
