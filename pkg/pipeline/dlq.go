// Package pipeline provides Dead Letter Queue for failed records.
// DLQ prevents pipeline blockage and enables later analysis/reprocessing.
package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DLQRecord represents a failed record with full context.
type DLQRecord struct {
	// Original data
	RawData    []byte `json:"raw_data"`
	RowNumber  int64  `json:"row_number"`
	ByteOffset int64  `json:"byte_offset,omitempty"`

	// Error context
	ErrorType    string `json:"error_type"`
	ErrorMessage string `json:"error_message"`
	ErrorCode    string `json:"error_code,omitempty"`

	// Source context
	SourceFile   string `json:"source_file"`
	SourceFormat string `json:"source_format,omitempty"`
	ColumnName   string `json:"column_name,omitempty"`

	// Metadata
	Timestamp   time.Time         `json:"timestamp"`
	JobID       string            `json:"job_id,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	RetryCount  int               `json:"retry_count"`
	Recoverable bool              `json:"recoverable"`
}

// DLQWriter writes failed records to a Dead Letter Queue.
type DLQWriter struct {
	mu sync.Mutex

	outputPath string
	file       *os.File
	encoder    *json.Encoder

	// Stats
	recordCount int64
	bytesWritten int64
	startTime   time.Time

	// Configuration
	maxRecords   int64 // Max records before rotation (0 = unlimited)
	maxBytes     int64 // Max bytes before rotation (0 = unlimited)
	rotateOnFull bool
	fileIndex    int

	closed bool
}

// DLQConfig configures the Dead Letter Queue.
type DLQConfig struct {
	// OutputPath is the base path for DLQ files
	OutputPath string
	// MaxRecords before rotating to new file (0 = unlimited)
	MaxRecords int64
	// MaxBytes before rotating to new file (0 = unlimited)
	MaxBytes int64
	// RotateOnFull creates new file when limits reached
	RotateOnFull bool
}

// DefaultDLQConfig returns sensible defaults.
func DefaultDLQConfig(basePath string) DLQConfig {
	return DLQConfig{
		OutputPath:   filepath.Join(basePath, "dlq"),
		MaxRecords:   100000, // 100K records per file
		MaxBytes:     100 * 1024 * 1024, // 100MB per file
		RotateOnFull: true,
	}
}

// NewDLQWriter creates a new DLQ writer.
func NewDLQWriter(cfg DLQConfig) (*DLQWriter, error) {
	// Ensure directory exists
	if err := os.MkdirAll(cfg.OutputPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create DLQ directory: %w", err)
	}

	w := &DLQWriter{
		outputPath:   cfg.OutputPath,
		maxRecords:   cfg.MaxRecords,
		maxBytes:     cfg.MaxBytes,
		rotateOnFull: cfg.RotateOnFull,
		startTime:    time.Now(),
	}

	if err := w.openFile(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *DLQWriter) openFile() error {
	filename := fmt.Sprintf("dlq_%s_%04d.jsonl",
		time.Now().Format("20060102_150405"),
		w.fileIndex)
	path := filepath.Join(w.outputPath, filename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open DLQ file: %w", err)
	}

	w.file = file
	w.encoder = json.NewEncoder(file)
	return nil
}

// Write writes a failed record to the DLQ.
func (w *DLQWriter) Write(record DLQRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("DLQ writer is closed")
	}

	// Set timestamp if not set
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now()
	}

	// Check if rotation needed
	if w.rotateOnFull {
		if (w.maxRecords > 0 && w.recordCount >= w.maxRecords) ||
			(w.maxBytes > 0 && w.bytesWritten >= w.maxBytes) {
			if err := w.rotate(); err != nil {
				return err
			}
		}
	}

	// Write record
	if err := w.encoder.Encode(record); err != nil {
		return fmt.Errorf("failed to write DLQ record: %w", err)
	}

	w.recordCount++
	// Approximate size
	w.bytesWritten += int64(len(record.RawData) + 200)

	return nil
}

// WriteError is a convenience method to write an ErrorRecord to DLQ.
func (w *DLQWriter) WriteError(err ErrorRecord, jobID string) error {
	return w.Write(DLQRecord{
		RawData:      err.RawData,
		RowNumber:    err.RowNumber,
		ByteOffset:   err.ByteOffset,
		ErrorType:    err.ErrorType.String(),
		ErrorMessage: err.Message,
		SourceFile:   err.SourceFile,
		ColumnName:   err.Column,
		Timestamp:    err.Timestamp,
		JobID:        jobID,
		Recoverable:  isRecoverableError(err.ErrorType),
	})
}

func isRecoverableError(errType ErrorType) bool {
	switch errType {
	case ErrorTypeInvalidTimestamp, ErrorTypeInvalidType:
		return true // Data format issues might be fixed
	case ErrorTypeMalformedRow, ErrorTypeQuotingError:
		return false // Structural issues unlikely to be fixed
	default:
		return false
	}
}

func (w *DLQWriter) rotate() error {
	// Close current file
	if w.file != nil {
		w.file.Close()
	}

	// Reset counters
	w.recordCount = 0
	w.bytesWritten = 0
	w.fileIndex++

	// Open new file
	return w.openFile()
}

// Stats returns DLQ statistics.
func (w *DLQWriter) Stats() DLQStats {
	w.mu.Lock()
	defer w.mu.Unlock()

	return DLQStats{
		RecordCount:  w.recordCount,
		BytesWritten: w.bytesWritten,
		FileCount:    w.fileIndex + 1,
		Duration:     time.Since(w.startTime),
	}
}

// DLQStats contains DLQ statistics.
type DLQStats struct {
	RecordCount  int64
	BytesWritten int64
	FileCount    int
	Duration     time.Duration
}

// Flush flushes buffered data to disk.
func (w *DLQWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		return w.file.Sync()
	}
	return nil
}

// Close closes the DLQ writer.
func (w *DLQWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// DLQReader reads records from a DLQ file.
type DLQReader struct {
	file    *os.File
	decoder *json.Decoder
}

// NewDLQReader opens a DLQ file for reading.
func NewDLQReader(path string) (*DLQReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &DLQReader{
		file:    file,
		decoder: json.NewDecoder(file),
	}, nil
}

// Read reads the next record from the DLQ.
func (r *DLQReader) Read() (*DLQRecord, error) {
	var record DLQRecord
	if err := r.decoder.Decode(&record); err != nil {
		return nil, err
	}
	return &record, nil
}

// Close closes the reader.
func (r *DLQReader) Close() error {
	return r.file.Close()
}

// ListDLQFiles returns all DLQ files in a directory.
func ListDLQFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".jsonl" {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	return files, nil
}

// DLQSummary summarizes contents of a DLQ directory.
type DLQSummary struct {
	TotalRecords  int64
	TotalBytes    int64
	FileCount     int
	ErrorTypes    map[string]int64
	SourceFiles   map[string]int64
	OldestRecord  time.Time
	NewestRecord  time.Time
	RecoverableCount int64
}

// SummarizeDLQ analyzes DLQ files and returns summary.
func SummarizeDLQ(dir string) (*DLQSummary, error) {
	files, err := ListDLQFiles(dir)
	if err != nil {
		return nil, err
	}

	summary := &DLQSummary{
		ErrorTypes:  make(map[string]int64),
		SourceFiles: make(map[string]int64),
	}

	for _, path := range files {
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		summary.TotalBytes += info.Size()
		summary.FileCount++

		reader, err := NewDLQReader(path)
		if err != nil {
			continue
		}

		for {
			record, err := reader.Read()
			if err != nil {
				break
			}

			summary.TotalRecords++
			summary.ErrorTypes[record.ErrorType]++
			summary.SourceFiles[record.SourceFile]++

			if record.Recoverable {
				summary.RecoverableCount++
			}

			if summary.OldestRecord.IsZero() || record.Timestamp.Before(summary.OldestRecord) {
				summary.OldestRecord = record.Timestamp
			}
			if record.Timestamp.After(summary.NewestRecord) {
				summary.NewestRecord = record.Timestamp
			}
		}
		reader.Close()
	}

	return summary, nil
}
