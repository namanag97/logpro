package parser

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/logflow/logflow/internal/model"
	"github.com/logflow/logflow/internal/pool"
	"github.com/xuri/excelize/v2"
)

// XLSXParser parses Excel XLSX files.
type XLSXParser struct {
	cfg        Config
	eventPool  *pool.EventPool
}

// NewXLSXParser creates a new XLSX parser.
func NewXLSXParser(cfg Config) *XLSXParser {
	return &XLSXParser{
		cfg:       cfg,
		eventPool: pool.NewEventPool(),
	}
}

// Parse reads from an Excel file and sends parsed events to out.
// Note: XLSX parsing requires random access, so this reads from a temp file if given a stream.
func (p *XLSXParser) Parse(ctx context.Context, r io.Reader, out chan<- *model.Event) error {
	// excelize needs a file path or ReadSeeker, so if we have a stream, write to temp
	var xlFile *excelize.File
	var err error

	if f, ok := r.(*os.File); ok {
		xlFile, err = excelize.OpenFile(f.Name())
	} else {
		// Read all content into memory for non-file readers
		xlFile, err = excelize.OpenReader(r)
	}
	if err != nil {
		return fmt.Errorf("failed to open xlsx: %w", err)
	}
	defer xlFile.Close()

	// Get the first sheet (or active sheet)
	sheetName := xlFile.GetSheetName(0)
	if sheetName == "" {
		sheetList := xlFile.GetSheetList()
		if len(sheetList) == 0 {
			return fmt.Errorf("no sheets found in xlsx file")
		}
		sheetName = sheetList[0]
	}

	// Get all rows using streaming reader for memory efficiency
	rows, err := xlFile.Rows(sheetName)
	if err != nil {
		return fmt.Errorf("failed to read rows: %w", err)
	}
	defer rows.Close()

	// Read header row
	if !rows.Next() {
		return fmt.Errorf("xlsx file is empty")
	}
	header, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	// Build column index map
	colIdx := make(map[string]int)
	for i, col := range header {
		colIdx[col] = i
	}

	// Find required columns
	caseIDIdx, ok := p.findColumnIndex(colIdx, p.cfg.CaseIDColumn, "case_id", "case:concept:name", "Case ID", "CaseID")
	if !ok {
		return fmt.Errorf("case ID column not found (tried: %s)", p.cfg.CaseIDColumn)
	}

	activityIdx, ok := p.findColumnIndex(colIdx, p.cfg.ActivityColumn, "activity", "concept:name", "Activity", "event")
	if !ok {
		return fmt.Errorf("activity column not found (tried: %s)", p.cfg.ActivityColumn)
	}

	timestampIdx, ok := p.findColumnIndex(colIdx, p.cfg.TimestampColumn, "timestamp", "time:timestamp", "Timestamp", "time")
	if !ok {
		return fmt.Errorf("timestamp column not found (tried: %s)", p.cfg.TimestampColumn)
	}

	resourceIdx, _ := p.findColumnIndex(colIdx, p.cfg.ResourceColumn, "resource", "org:resource", "Resource")

	// Process data rows
	rowNum := 1
	for rows.Next() {
		select {
		case <-ctx.Done():
			return ErrContextCanceled
		default:
		}

		rowNum++
		cols, err := rows.Columns()
		if err != nil {
			continue // Skip malformed rows
		}

		// Skip empty rows
		if len(cols) == 0 {
			continue
		}

		event := p.eventPool.Get()

		// Extract case ID
		if caseIDIdx < len(cols) {
			event.CaseID = []byte(cols[caseIDIdx])
		}

		// Extract activity
		if activityIdx < len(cols) {
			event.Activity = []byte(cols[activityIdx])
		}

		// Extract timestamp
		if timestampIdx < len(cols) {
			ts, err := p.parseTimestamp(cols[timestampIdx])
			if err == nil {
				event.Timestamp = ts
			}
		}

		// Extract resource (optional)
		if resourceIdx >= 0 && resourceIdx < len(cols) {
			event.Resource = []byte(cols[resourceIdx])
		}

		// Skip events with missing required fields
		if len(event.CaseID) == 0 || len(event.Activity) == 0 {
			p.eventPool.Put(event)
			continue
		}

		select {
		case out <- event:
		case <-ctx.Done():
			p.eventPool.Put(event)
			return ErrContextCanceled
		}
	}

	return nil
}

// findColumnIndex tries multiple column names and returns the first match.
func (p *XLSXParser) findColumnIndex(colIdx map[string]int, names ...string) (int, bool) {
	for _, name := range names {
		if idx, ok := colIdx[name]; ok {
			return idx, true
		}
	}
	return -1, false
}

// parseTimestamp parses various timestamp formats found in Excel.
func (p *XLSXParser) parseTimestamp(s string) (int64, error) {
	if s == "" {
		return 0, ErrInvalidTimestamp
	}

	// Try numeric Excel serial date first
	if serial, err := strconv.ParseFloat(s, 64); err == nil && serial > 1 {
		// Excel serial date: days since 1899-12-30
		// Adjust for Excel's leap year bug (1900 was not a leap year)
		days := serial
		if serial >= 60 {
			days-- // Adjust for Excel's 1900 leap year bug
		}
		t := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC).Add(time.Duration(days*24) * time.Hour)
		return t.UnixNano(), nil
	}

	// Try common date formats
	formats := []string{
		p.cfg.TimestampFormat,
		"2006-01-02T15:04:05.000Z07:00",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
		"01/02/2006 15:04:05",
		"01/02/2006",
		"02-01-2006 15:04:05",
		"02-01-2006",
		time.RFC3339,
		time.RFC3339Nano,
	}

	for _, format := range formats {
		if format == "" {
			continue
		}
		t, err := time.Parse(format, s)
		if err == nil {
			return t.UnixNano(), nil
		}
	}

	return 0, ErrInvalidTimestamp
}
