// Package rest provides a REST API for LogFlow.
package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/logflow/logflow/pkg/interfaces"
	"github.com/logflow/logflow/pkg/query/engine"
)

// Server is the REST API server.
type Server struct {
	addr          string
	queryEngine   *engine.Engine
	catalog       interfaces.Catalog
	storage       interfaces.ObjectStorage
	authenticator interfaces.Authenticator
	mux           *http.ServeMux
	server        *http.Server
}

// Config configures the server.
type Config struct {
	Addr          string
	QueryEngine   *engine.Engine
	Catalog       interfaces.Catalog
	Storage       interfaces.ObjectStorage
	Authenticator interfaces.Authenticator
}

// NewServer creates a new REST API server.
func NewServer(cfg Config) *Server {
	s := &Server{
		addr:          cfg.Addr,
		queryEngine:   cfg.QueryEngine,
		catalog:       cfg.Catalog,
		storage:       cfg.Storage,
		authenticator: cfg.Authenticator,
		mux:           http.NewServeMux(),
	}

	s.setupRoutes()
	return s
}

func (s *Server) setupRoutes() {
	// Health check
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/ready", s.handleReady)

	// API v1
	s.mux.HandleFunc("/v1/query", s.handleQuery)
	s.mux.HandleFunc("/v1/tables", s.handleTables)
	s.mux.HandleFunc("/v1/databases", s.handleDatabases)
	s.mux.HandleFunc("/v1/ingest", s.handleIngest)
}

// Start starts the server.
func (s *Server) Start() error {
	s.server = &http.Server{
		Addr:         s.addr,
		Handler:      s.mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	return s.server.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
		"time":   time.Now().Format(time.RFC3339),
	})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ready",
		"time":   time.Now().Format(time.RFC3339),
	})
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.SQL == "" {
		writeError(w, http.StatusBadRequest, "SQL query is required")
		return
	}

	result, err := s.queryEngine.Query(r.Context(), req.SQL)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Query failed: %v", err))
		return
	}
	defer result.Close()

	rows, err := result.ToMaps()
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to read results: %v", err))
		return
	}

	resp := QueryResponse{
		Columns:  result.Columns(),
		Rows:     rows,
		RowCount: int64(len(rows)),
		Duration: result.Duration().String(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleTables(w http.ResponseWriter, r *http.Request) {
	database := r.URL.Query().Get("database")
	if database == "" {
		database = "default"
	}

	switch r.Method {
	case http.MethodGet:
		tables, err := s.catalog.ListTables(r.Context(), database)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list tables: %v", err))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"database": database,
			"tables":   tables,
		})

	case http.MethodPost:
		var req CreateTableRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "Invalid request body")
			return
		}

		spec := interfaces.TableSpec{
			Database:   database,
			Name:       req.Name,
			Location:   req.Location,
			Properties: req.Properties,
		}

		if err := s.catalog.CreateTable(r.Context(), spec); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create table: %v", err))
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "created",
			"table":  req.Name,
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleDatabases(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		databases, err := s.catalog.ListDatabases(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list databases: %v", err))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"databases": databases,
		})

	case http.MethodPost:
		var req CreateDatabaseRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "Invalid request body")
			return
		}

		if err := s.catalog.CreateDatabase(r.Context(), req.Name); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create database: %v", err))
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{
			"status":   "created",
			"database": req.Name,
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Placeholder for ingest endpoint
	writeError(w, http.StatusNotImplemented, "Ingest endpoint not yet implemented")
}

func writeError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error:   http.StatusText(code),
		Message: message,
	})
}

// Request/Response types

// QueryRequest represents a query request.
type QueryRequest struct {
	SQL    string                 `json:"sql"`
	Params map[string]interface{} `json:"params,omitempty"`
	Limit  int                    `json:"limit,omitempty"`
}

// QueryResponse represents a query response.
type QueryResponse struct {
	Columns  []string                 `json:"columns"`
	Rows     []map[string]interface{} `json:"rows"`
	RowCount int64                    `json:"row_count"`
	Duration string                   `json:"duration"`
}

// CreateTableRequest represents a table creation request.
type CreateTableRequest struct {
	Name       string            `json:"name"`
	Location   string            `json:"location,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

// CreateDatabaseRequest represents a database creation request.
type CreateDatabaseRequest struct {
	Name string `json:"name"`
}

// ErrorResponse represents an error response.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}
