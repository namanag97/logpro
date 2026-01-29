// Package compaction provides auto-compaction triggers.
// Automatically triggers compaction when conditions are met.
package compaction

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// AutoCompactionConfig configures automatic compaction triggers.
type AutoCompactionConfig struct {
	// Enabled turns on auto-compaction
	Enabled bool
	// FileCountThreshold triggers compaction when file count exceeds this
	FileCountThreshold int
	// MinFileSizeBytes triggers compaction when avg file size is below this
	MinFileSizeBytes int64
	// CheckInterval is how often to check for compaction need
	CheckInterval time.Duration
	// CompactionConfig is the config for actual compaction
	CompactionConfig Config
	// Directories to monitor
	Directories []string
	// Callback when compaction completes
	OnCompactionComplete func(result *CompactionResult)
	// Callback when compaction fails
	OnCompactionError func(dir string, err error)
}

// DefaultAutoCompactionConfig returns sensible defaults.
func DefaultAutoCompactionConfig() AutoCompactionConfig {
	return AutoCompactionConfig{
		Enabled:            true,
		FileCountThreshold: 50,                    // Compact when >50 files
		MinFileSizeBytes:   64 * 1024 * 1024,      // Compact when avg <64MB
		CheckInterval:      5 * time.Minute,
		CompactionConfig:   DefaultConfig(),
	}
}

// AutoCompactor monitors directories and triggers compaction automatically.
type AutoCompactor struct {
	mu     sync.Mutex
	cfg    AutoCompactionConfig
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	compactionsRun    int
	lastCompactionAt  time.Time
	totalSpaceSaved   int64
	totalFilesRemoved int

	running bool
}

// NewAutoCompactor creates a new auto-compactor.
func NewAutoCompactor(cfg AutoCompactionConfig) *AutoCompactor {
	ctx, cancel := context.WithCancel(context.Background())
	return &AutoCompactor{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins monitoring directories for compaction.
func (a *AutoCompactor) Start() {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return
	}
	a.running = true
	a.mu.Unlock()

	a.wg.Add(1)
	go a.monitorLoop()
}

// Stop stops the auto-compactor.
func (a *AutoCompactor) Stop() {
	a.cancel()
	a.wg.Wait()

	a.mu.Lock()
	a.running = false
	a.mu.Unlock()
}

func (a *AutoCompactor) monitorLoop() {
	defer a.wg.Done()

	ticker := time.NewTicker(a.cfg.CheckInterval)
	defer ticker.Stop()

	// Initial check
	a.checkAllDirectories()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			a.checkAllDirectories()
		}
	}
}

func (a *AutoCompactor) checkAllDirectories() {
	for _, dir := range a.cfg.Directories {
		if a.shouldCompact(dir) {
			a.runCompaction(dir)
		}
	}
}

// ShouldCompact checks if a directory needs compaction.
func (a *AutoCompactor) shouldCompact(dir string) bool {
	stats, err := a.getDirectoryStats(dir)
	if err != nil {
		return false
	}

	// Check file count threshold
	if stats.FileCount > a.cfg.FileCountThreshold {
		return true
	}

	// Check average file size
	if stats.FileCount > 0 {
		avgSize := stats.TotalBytes / int64(stats.FileCount)
		if avgSize < a.cfg.MinFileSizeBytes {
			return true
		}
	}

	return false
}

// DirectoryStats contains statistics about a directory of Parquet files.
type DirectoryStats struct {
	Directory    string
	FileCount    int
	TotalBytes   int64
	AvgFileSize  int64
	MinFileSize  int64
	MaxFileSize  int64
	OldestFile   time.Time
	NewestFile   time.Time
}

func (a *AutoCompactor) getDirectoryStats(dir string) (*DirectoryStats, error) {
	stats := &DirectoryStats{
		Directory:   dir,
		MinFileSize: -1,
	}

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(path), ".parquet") {
			return nil
		}

		stats.FileCount++
		stats.TotalBytes += info.Size()

		if stats.MinFileSize < 0 || info.Size() < stats.MinFileSize {
			stats.MinFileSize = info.Size()
		}
		if info.Size() > stats.MaxFileSize {
			stats.MaxFileSize = info.Size()
		}

		if stats.OldestFile.IsZero() || info.ModTime().Before(stats.OldestFile) {
			stats.OldestFile = info.ModTime()
		}
		if info.ModTime().After(stats.NewestFile) {
			stats.NewestFile = info.ModTime()
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	if stats.FileCount > 0 {
		stats.AvgFileSize = stats.TotalBytes / int64(stats.FileCount)
	}

	return stats, nil
}

func (a *AutoCompactor) runCompaction(dir string) {
	compactor, err := NewCompactor(a.cfg.CompactionConfig)
	if err != nil {
		if a.cfg.OnCompactionError != nil {
			a.cfg.OnCompactionError(dir, err)
		}
		return
	}
	defer compactor.Close()

	result, err := compactor.Compact(a.ctx, dir)
	if err != nil {
		if a.cfg.OnCompactionError != nil {
			a.cfg.OnCompactionError(dir, err)
		}
		return
	}

	// Update stats
	a.mu.Lock()
	a.compactionsRun++
	a.lastCompactionAt = time.Now()
	a.totalSpaceSaved += result.SpaceSavedBytes
	a.totalFilesRemoved += result.FilesProcessed - result.FilesCreated
	a.mu.Unlock()

	if a.cfg.OnCompactionComplete != nil {
		a.cfg.OnCompactionComplete(result)
	}
}

// TriggerCompaction manually triggers compaction for a directory.
func (a *AutoCompactor) TriggerCompaction(dir string) (*CompactionResult, error) {
	compactor, err := NewCompactor(a.cfg.CompactionConfig)
	if err != nil {
		return nil, err
	}
	defer compactor.Close()

	return compactor.Compact(a.ctx, dir)
}

// AddDirectory adds a directory to monitor.
func (a *AutoCompactor) AddDirectory(dir string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Check if already monitored
	for _, d := range a.cfg.Directories {
		if d == dir {
			return
		}
	}

	a.cfg.Directories = append(a.cfg.Directories, dir)
}

// RemoveDirectory removes a directory from monitoring.
func (a *AutoCompactor) RemoveDirectory(dir string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for i, d := range a.cfg.Directories {
		if d == dir {
			a.cfg.Directories = append(a.cfg.Directories[:i], a.cfg.Directories[i+1:]...)
			return
		}
	}
}

// Stats returns auto-compaction statistics.
type AutoCompactionStats struct {
	CompactionsRun    int
	LastCompactionAt  time.Time
	TotalSpaceSaved   int64
	TotalFilesRemoved int
	DirectoriesMonitored int
	Running           bool
}

// Stats returns current statistics.
func (a *AutoCompactor) Stats() AutoCompactionStats {
	a.mu.Lock()
	defer a.mu.Unlock()

	return AutoCompactionStats{
		CompactionsRun:       a.compactionsRun,
		LastCompactionAt:     a.lastCompactionAt,
		TotalSpaceSaved:      a.totalSpaceSaved,
		TotalFilesRemoved:    a.totalFilesRemoved,
		DirectoriesMonitored: len(a.cfg.Directories),
		Running:              a.running,
	}
}

// NeedsCompaction checks all directories and returns ones needing compaction.
func (a *AutoCompactor) NeedsCompaction() []string {
	var result []string
	for _, dir := range a.cfg.Directories {
		if a.shouldCompact(dir) {
			result = append(result, dir)
		}
	}
	return result
}
