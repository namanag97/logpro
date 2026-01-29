// Package watch provides file watching and incremental update capabilities.
package watch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Watcher monitors files for changes and triggers updates.
type Watcher struct {
	watcher     *fsnotify.Watcher
	files       map[string]*fileState
	mu          sync.RWMutex
	debounce    time.Duration
	OnChange    func(path string) error
	OnError     func(path string, err error)
}

type fileState struct {
	path         string
	lastModified time.Time
	size         int64
	processing   bool
}

// NewWatcher creates a new file watcher.
func NewWatcher() (*Watcher, error) {
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create watcher: %w", err)
	}

	return &Watcher{
		watcher:  fsWatcher,
		files:    make(map[string]*fileState),
		debounce: 500 * time.Millisecond,
	}, nil
}

// Watch starts watching a file for changes.
func (w *Watcher) Watch(path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("failed to resolve path: %w", err)
	}

	stat, err := os.Stat(absPath)
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	w.mu.Lock()
	w.files[absPath] = &fileState{
		path:         absPath,
		lastModified: stat.ModTime(),
		size:         stat.Size(),
	}
	w.mu.Unlock()

	// Watch the directory containing the file (fsnotify works better this way)
	dir := filepath.Dir(absPath)
	if err := w.watcher.Add(dir); err != nil {
		return fmt.Errorf("failed to watch directory: %w", err)
	}

	return nil
}

// Run starts the watch loop. Blocks until context is cancelled.
func (w *Watcher) Run(ctx context.Context) error {
	debounceTimers := make(map[string]*time.Timer)
	var timerMu sync.Mutex

	for {
		select {
		case <-ctx.Done():
			w.watcher.Close()
			return ctx.Err()

		case event, ok := <-w.watcher.Events:
			if !ok {
				return nil
			}

			// Only handle write events
			if event.Op&(fsnotify.Write|fsnotify.Create) == 0 {
				continue
			}

			absPath, err := filepath.Abs(event.Name)
			if err != nil {
				continue
			}

			// Check if this is a watched file
			w.mu.RLock()
			state, isWatched := w.files[absPath]
			w.mu.RUnlock()

			if !isWatched {
				continue
			}

			// Debounce rapid changes
			timerMu.Lock()
			if timer, exists := debounceTimers[absPath]; exists {
				timer.Stop()
			}
			debounceTimers[absPath] = time.AfterFunc(w.debounce, func() {
				w.handleChange(absPath, state)
			})
			timerMu.Unlock()

		case err, ok := <-w.watcher.Errors:
			if !ok {
				return nil
			}
			if w.OnError != nil {
				w.OnError("", err)
			}
		}
	}
}

func (w *Watcher) handleChange(path string, state *fileState) {
	w.mu.Lock()
	if state.processing {
		w.mu.Unlock()
		return
	}
	state.processing = true
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		state.processing = false
		w.mu.Unlock()
	}()

	// Check if file actually changed
	stat, err := os.Stat(path)
	if err != nil {
		if w.OnError != nil {
			w.OnError(path, err)
		}
		return
	}

	// Compare with last known state
	if stat.ModTime().Equal(state.lastModified) && stat.Size() == state.size {
		return // No actual change
	}

	// Update state
	w.mu.Lock()
	state.lastModified = stat.ModTime()
	state.size = stat.Size()
	w.mu.Unlock()

	// Trigger callback
	if w.OnChange != nil {
		if err := w.OnChange(path); err != nil {
			if w.OnError != nil {
				w.OnError(path, err)
			}
		}
	}
}

// Close stops the watcher.
func (w *Watcher) Close() error {
	return w.watcher.Close()
}

// WatchConfig holds configuration for the watch command.
type WatchConfig struct {
	InputPath    string
	OutputPath   string
	Format       string
	Interval     time.Duration
	FullRebuild  bool // If true, rebuild entire output on change; if false, try incremental
}

// IncrementalState tracks state for incremental updates.
type IncrementalState struct {
	LastOffset    int64     // Last processed byte offset
	LastRowCount  int64     // Last known row count
	LastModified  time.Time // Last modification time
	OutputPath    string    // Output file path
}

// NewIncrementalState creates a new incremental state tracker.
func NewIncrementalState(outputPath string) *IncrementalState {
	return &IncrementalState{
		OutputPath: outputPath,
	}
}

// ShouldRebuild determines if a full rebuild is needed.
func (s *IncrementalState) ShouldRebuild(stat os.FileInfo) bool {
	// Rebuild if file is smaller (truncated) or hasn't been processed yet
	if stat.Size() < s.LastOffset {
		return true
	}
	if s.LastRowCount == 0 {
		return true
	}
	return false
}

// Update updates the state after processing.
func (s *IncrementalState) Update(offset, rowCount int64, modTime time.Time) {
	s.LastOffset = offset
	s.LastRowCount = rowCount
	s.LastModified = modTime
}
