package server

import (
	"embed"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// emptyFS is an empty embedded filesystem for testing
var emptyFS embed.FS

func newTestServer(t *testing.T) *Server {
	t.Helper()
	s, err := NewServer(emptyFS)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	return s
}

func TestServer_Health(t *testing.T) {
	s := newTestServer(t)
	defer s.Close()

	req := httptest.NewRequest("GET", "/api/health", nil)
	w := httptest.NewRecorder()

	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Invalid JSON response: %v", err)
	}

	if resp["status"] != "ok" {
		t.Errorf("Expected status 'ok', got %v", resp["status"])
	}
}

func TestServer_Metrics(t *testing.T) {
	s := newTestServer(t)
	defer s.Close()

	req := httptest.NewRequest("GET", "/api/metrics", nil)
	w := httptest.NewRecorder()

	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Invalid JSON response: %v", err)
	}
}

func TestServer_Jobs_Empty(t *testing.T) {
	s := newTestServer(t)
	defer s.Close()

	req := httptest.NewRequest("GET", "/api/jobs", nil)
	w := httptest.NewRecorder()

	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}
}

func TestServer_Plugins(t *testing.T) {
	s := newTestServer(t)
	defer s.Close()

	req := httptest.NewRequest("GET", "/api/plugins", nil)
	w := httptest.NewRecorder()

	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}
}

func TestServer_Upload_NoFile(t *testing.T) {
	s := newTestServer(t)
	defer s.Close()

	req := httptest.NewRequest("POST", "/api/upload", nil)
	w := httptest.NewRecorder()

	s.ServeHTTP(w, req)

	// Should return error status for missing file
	if w.Code == http.StatusOK {
		t.Error("Expected error status for upload with no file")
	}
}

func TestServer_Convert_NoInput(t *testing.T) {
	s := newTestServer(t)
	defer s.Close()

	req := httptest.NewRequest("POST", "/api/convert", nil)
	w := httptest.NewRecorder()

	s.ServeHTTP(w, req)

	// Should return error for missing input
	if w.Code == http.StatusOK {
		t.Error("Expected error status for convert with no input")
	}
}
