// Package server provides the HTTP API for the web UI.
package server

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/logflow/logflow/pkg/core"
	"github.com/logflow/logflow/pkg/plugins/processmining"
	"github.com/logflow/logflow/pkg/plugins/quality"
)

// Server handles HTTP requests for the web UI.
type Server struct {
	converter *core.Converter
	plugins   *core.PluginRegistry
	jobs      sync.Map // jobID -> *Job
	mux       *http.ServeMux
	staticFS  embed.FS
}

// Job represents a conversion job.
type Job struct {
	ID          string          `json:"id"`
	Status      string          `json:"status"` // pending, running, completed, failed
	InputName   string          `json:"input_name"`
	StartTime   time.Time       `json:"start_time"`
	EndTime     *time.Time      `json:"end_time,omitempty"`
	Progress    Progress        `json:"progress"`
	Result      *core.ConversionResult `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`
	Analysis    map[string]interface{} `json:"analysis,omitempty"`
}

// Progress tracks conversion progress.
type Progress struct {
	Phase       string  `json:"phase"` // uploading, converting, analyzing, complete
	Percent     float64 `json:"percent"`
	BytesRead   int64   `json:"bytes_read"`
	RowsWritten int64   `json:"rows_written"`
	Message     string  `json:"message"`
}

// NewServer creates a new HTTP server.
func NewServer(staticFS embed.FS) (*Server, error) {
	converter, err := core.NewConverter()
	if err != nil {
		return nil, err
	}

	s := &Server{
		converter: converter,
		plugins:   core.NewPluginRegistry(),
		mux:       http.NewServeMux(),
		staticFS:  staticFS,
	}

	s.setupRoutes()
	return s, nil
}

// setupRoutes configures HTTP handlers.
func (s *Server) setupRoutes() {
	// API routes
	s.mux.HandleFunc("/api/upload", s.handleUpload)
	s.mux.HandleFunc("/api/convert", s.handleConvert)
	s.mux.HandleFunc("/api/job/", s.handleJob)
	s.mux.HandleFunc("/api/schema", s.handleSchema)
	s.mux.HandleFunc("/api/plugins", s.handlePlugins)
	s.mux.HandleFunc("/api/analyze", s.handleAnalyze)
	s.mux.HandleFunc("/api/download/", s.handleDownload)

	// Static files (embedded HTML/CSS/JS)
	s.mux.HandleFunc("/", s.handleStatic)
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CORS headers for development
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	s.mux.ServeHTTP(w, r)
}

// Close releases resources.
func (s *Server) Close() error {
	return s.converter.Close()
}

// handleStatic serves the embedded web UI.
func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path == "/" {
		path = "/index.html"
	}

	// Try to read from embedded FS
	data, err := s.staticFS.ReadFile("web" + path)
	if err != nil {
		// Fallback to index.html for SPA routing
		data, err = s.staticFS.ReadFile("web/index.html")
		if err != nil {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
	}

	// Set content type
	switch filepath.Ext(path) {
	case ".html":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	case ".css":
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	case ".js":
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	case ".json":
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}

	w.Write(data)
}

// handleUpload receives file uploads.
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse multipart form (max 500MB)
	if err := r.ParseMultipartForm(500 << 20); err != nil {
		jsonError(w, "Failed to parse upload", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		jsonError(w, "No file provided", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Save to temp directory
	tempDir := os.TempDir()
	tempPath := filepath.Join(tempDir, "logflow-"+header.Filename)

	out, err := os.Create(tempPath)
	if err != nil {
		jsonError(w, "Failed to save file", http.StatusInternalServerError)
		return
	}
	defer out.Close()

	size, err := io.Copy(out, file)
	if err != nil {
		jsonError(w, "Failed to save file", http.StatusInternalServerError)
		return
	}

	// Create job
	jobID := fmt.Sprintf("job-%d", time.Now().UnixNano())
	job := &Job{
		ID:        jobID,
		Status:    "pending",
		InputName: header.Filename,
		StartTime: time.Now(),
		Progress: Progress{
			Phase:     "uploaded",
			Percent:   0,
			BytesRead: size,
			Message:   "File uploaded, ready to convert",
		},
	}
	s.jobs.Store(jobID, job)

	// Return job info and detected schema
	response := map[string]interface{}{
		"job_id":     jobID,
		"file_name":  header.Filename,
		"file_size":  size,
		"file_path":  tempPath,
	}

	// Detect schema
	schema, err := s.detectSchema(tempPath)
	if err == nil {
		response["schema"] = schema
	}

	jsonResponse(w, response)
}

// handleConvert starts conversion.
func (s *Server) handleConvert(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		JobID       string `json:"job_id"`
		FilePath    string `json:"file_path"`
		Compression string `json:"compression"`
		RunQuality  bool   `json:"run_quality"`
		PMConfig    *processmining.Config `json:"pm_config,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Get or create job
	var job *Job
	if v, ok := s.jobs.Load(req.JobID); ok {
		job = v.(*Job)
	} else {
		job = &Job{
			ID:        req.JobID,
			Status:    "pending",
			StartTime: time.Now(),
		}
		s.jobs.Store(req.JobID, job)
	}

	// Start conversion in background
	go s.runConversion(job, req.FilePath, req.Compression, req.RunQuality, req.PMConfig)

	jsonResponse(w, map[string]string{
		"job_id": job.ID,
		"status": "started",
	})
}

// runConversion performs the actual conversion.
func (s *Server) runConversion(job *Job, inputPath, compression string, runQuality bool, pmConfig *processmining.Config) {
	job.Status = "running"
	job.Progress = Progress{
		Phase:   "converting",
		Percent: 10,
		Message: "Starting conversion...",
	}

	opts := core.ConversionOptions{
		Compression: compression,
	}
	if opts.Compression == "" {
		opts.Compression = "snappy"
	}

	// Run conversion
	result, err := s.converter.Convert(context.Background(), inputPath, opts)
	if err != nil {
		job.Status = "failed"
		job.Error = err.Error()
		now := time.Now()
		job.EndTime = &now
		return
	}

	job.Result = result
	job.Progress = Progress{
		Phase:       "complete",
		Percent:     100,
		RowsWritten: result.RowCount,
		Message:     "Conversion complete",
	}

	// Run optional analysis
	job.Analysis = make(map[string]interface{})

	if runQuality {
		job.Progress.Message = "Running quality analysis..."
		qualityPlugin, err := quality.NewPlugin()
		if err == nil {
			defer qualityPlugin.Close()
			if report, err := qualityPlugin.Analyze(context.Background(), result); err == nil {
				job.Analysis["quality"] = report
			}
		}
	}

	if pmConfig != nil && pmConfig.CaseIDColumn != "" {
		job.Progress.Message = "Running process mining analysis..."
		pmPlugin, err := processmining.NewPlugin(*pmConfig)
		if err == nil {
			defer pmPlugin.Close()
			if analysis, err := pmPlugin.Analyze(context.Background(), result); err == nil {
				job.Analysis["process_mining"] = analysis
			}
		}
	}

	job.Status = "completed"
	now := time.Now()
	job.EndTime = &now
}

// handleJob returns job status.
func (s *Server) handleJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Path[len("/api/job/"):]
	if jobID == "" {
		jsonError(w, "Job ID required", http.StatusBadRequest)
		return
	}

	v, ok := s.jobs.Load(jobID)
	if !ok {
		jsonError(w, "Job not found", http.StatusNotFound)
		return
	}

	jsonResponse(w, v.(*Job))
}

// handleSchema returns schema for a file.
func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		jsonError(w, "Path required", http.StatusBadRequest)
		return
	}

	schema, err := s.detectSchema(path)
	if err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jsonResponse(w, schema)
}

// detectSchema uses DuckDB to infer schema.
func (s *Server) detectSchema(path string) (interface{}, error) {
	// Quick conversion to get schema
	opts := core.DefaultOptions()
	opts.OutputPath = path + ".schema-check.parquet"
	defer os.Remove(opts.OutputPath)

	result, err := s.converter.Convert(context.Background(), path, opts)
	if err != nil {
		return nil, err
	}

	// Auto-detect PM columns
	pmConfig := processmining.AutoDetectColumns(result.Columns)

	return map[string]interface{}{
		"columns":            result.Columns,
		"row_count":          result.RowCount,
		"detected_pm_config": pmConfig,
	}, nil
}

// handlePlugins returns available plugins.
func (s *Server) handlePlugins(w http.ResponseWriter, r *http.Request) {
	plugins := []map[string]string{
		{
			"name":        "quality",
			"description": "Analyze data quality (completeness, uniqueness)",
			"type":        "analyzer",
		},
		{
			"name":        "process-mining",
			"description": "Process mining analysis (cases, activities, variants)",
			"type":        "analyzer",
			"requires":    "column_mapping",
		},
	}
	jsonResponse(w, plugins)
}

// handleAnalyze runs analysis on existing file.
func (s *Server) handleAnalyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		FilePath string `json:"file_path"`
		Plugin   string `json:"plugin"`
		Config   json.RawMessage `json:"config,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Create a minimal result for analysis
	result := &core.ConversionResult{
		OutputPath: req.FilePath,
	}

	var analysis interface{}
	var err error

	switch req.Plugin {
	case "quality":
		plugin, e := quality.NewPlugin()
		if e != nil {
			jsonError(w, e.Error(), http.StatusInternalServerError)
			return
		}
		defer plugin.Close()
		analysis, err = plugin.Analyze(context.Background(), result)

	case "process-mining":
		var config processmining.Config
		if err := json.Unmarshal(req.Config, &config); err != nil {
			jsonError(w, "Invalid PM config", http.StatusBadRequest)
			return
		}
		plugin, e := processmining.NewPlugin(config)
		if e != nil {
			jsonError(w, e.Error(), http.StatusInternalServerError)
			return
		}
		defer plugin.Close()
		analysis, err = plugin.Analyze(context.Background(), result)

	default:
		jsonError(w, "Unknown plugin", http.StatusBadRequest)
		return
	}

	if err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jsonResponse(w, analysis)
}

// handleDownload serves converted files.
func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Path[len("/api/download/"):]
	if jobID == "" {
		http.Error(w, "Job ID required", http.StatusBadRequest)
		return
	}

	v, ok := s.jobs.Load(jobID)
	if !ok {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	job := v.(*Job)
	if job.Result == nil {
		http.Error(w, "No output file", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(job.Result.OutputPath)))
	http.ServeFile(w, r, job.Result.OutputPath)
}

// Helper functions

func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func jsonError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
