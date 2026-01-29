package sources

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// GlobSource provides multiple files matching a pattern.
type GlobSource struct {
	pattern string
	sources []*FileSource
}

// NewGlobSource creates a source from a glob pattern.
func NewGlobSource(pattern string) (*GlobSource, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid glob pattern: %w", err)
	}

	if len(matches) == 0 {
		return nil, fmt.Errorf("no files match pattern: %s", pattern)
	}

	// Sort for deterministic ordering
	sort.Strings(matches)

	sources := make([]*FileSource, 0, len(matches))
	for _, path := range matches {
		src, err := NewFileSource(path)
		if err != nil {
			continue // Skip files we can't read
		}
		sources = append(sources, src)
	}

	if len(sources) == 0 {
		return nil, fmt.Errorf("no readable files match pattern: %s", pattern)
	}

	return &GlobSource{
		pattern: pattern,
		sources: sources,
	}, nil
}

// Sources returns all matched sources.
func (g *GlobSource) Sources() []core.Source {
	result := make([]core.Source, len(g.sources))
	for i, s := range g.sources {
		result[i] = s
	}
	return result
}

// TotalSize returns total size across all sources.
func (g *GlobSource) TotalSize() int64 {
	var total int64
	for _, s := range g.sources {
		total += s.Size()
	}
	return total
}

// Count returns number of sources.
func (g *GlobSource) Count() int {
	return len(g.sources)
}

// Pattern returns the glob pattern.
func (g *GlobSource) Pattern() string {
	return g.pattern
}
