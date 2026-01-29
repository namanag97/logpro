// Package server provides job persistence for the LogFlow server.
package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// JobStore provides persistent storage for jobs.
type JobStore struct {
	mu       sync.RWMutex
	filePath string
	jobs     map[string]*Job
	dirty    bool
}

// NewJobStore creates a new job store with the given storage path.
func NewJobStore(storagePath string) (*JobStore, error) {
	// Ensure storage directory exists
	dir := filepath.Dir(storagePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	store := &JobStore{
		filePath: storagePath,
		jobs:     make(map[string]*Job),
	}

	// Load existing jobs
	if err := store.load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	// Start background save goroutine
	go store.autoSave()

	return store, nil
}

// Get retrieves a job by ID.
func (s *JobStore) Get(id string) (*Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	return job, ok
}

// Put stores a job.
func (s *JobStore) Put(job *Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
	s.dirty = true
}

// Delete removes a job.
func (s *JobStore) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.jobs, id)
	s.dirty = true
}

// List returns all jobs.
func (s *JobStore) List() []*Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]*Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

// Range iterates over all jobs.
func (s *JobStore) Range(fn func(id string, job *Job) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for id, job := range s.jobs {
		if !fn(id, job) {
			break
		}
	}
}

// Count returns the number of jobs.
func (s *JobStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.jobs)
}

// Save persists all jobs to disk.
func (s *JobStore) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked()
}

func (s *JobStore) saveLocked() error {
	if !s.dirty {
		return nil
	}

	data, err := json.MarshalIndent(s.jobs, "", "  ")
	if err != nil {
		return err
	}

	// Write atomically
	tmpPath := s.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, s.filePath); err != nil {
		os.Remove(tmpPath)
		return err
	}

	s.dirty = false
	return nil
}

func (s *JobStore) load() error {
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return json.Unmarshal(data, &s.jobs)
}

func (s *JobStore) autoSave() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		if s.dirty {
			s.saveLocked()
		}
		s.mu.Unlock()
	}
}

// Close saves and closes the store.
func (s *JobStore) Close() error {
	return s.Save()
}

// Cleanup removes old completed/failed jobs.
func (s *JobStore) Cleanup(maxAge time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	removed := 0

	for id, job := range s.jobs {
		// Only clean up finished jobs
		if job.Status != "completed" && job.Status != "failed" {
			continue
		}
		// Check if old enough
		if job.EndTime != nil && job.EndTime.Before(cutoff) {
			delete(s.jobs, id)
			removed++
			s.dirty = true
		}
	}

	return removed
}

// Stats returns storage statistics.
type JobStoreStats struct {
	Total     int
	Pending   int
	Running   int
	Completed int
	Failed    int
}

// Stats returns job statistics.
func (s *JobStore) Stats() JobStoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var stats JobStoreStats
	for _, job := range s.jobs {
		stats.Total++
		switch job.Status {
		case "pending":
			stats.Pending++
		case "running":
			stats.Running++
		case "completed":
			stats.Completed++
		case "failed":
			stats.Failed++
		}
	}
	return stats
}
