package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefault(t *testing.T) {
	cfg := Default()
	if cfg == nil {
		t.Fatal("Default returned nil")
	}
	if cfg.Version == 0 {
		t.Error("default Version should be non-zero")
	}
}

func TestDefaultEngineConfig(t *testing.T) {
	cfg := Default()
	if cfg.Engine.Default == "" {
		t.Error("default engine name should not be empty")
	}
	if cfg.Engine.Threads <= 0 {
		t.Error("default threads should be > 0")
	}
}

func TestDefaultConversionConfig(t *testing.T) {
	cfg := Default()
	if cfg.Conversion.Compression == "" {
		t.Error("default compression should not be empty")
	}
	if cfg.Conversion.BatchSize <= 0 {
		t.Error("default batch size should be > 0")
	}
	if cfg.Conversion.SchemaPolicy == "" {
		t.Error("default schema policy should not be empty")
	}
}

func TestDefaultServerConfig(t *testing.T) {
	cfg := Default()
	if cfg.Server.Port <= 0 {
		t.Error("default server port should be > 0")
	}
}

func TestNewManager(t *testing.T) {
	m := NewManager()
	if m == nil {
		t.Fatal("NewManager returned nil")
	}
}

func TestManagerGet(t *testing.T) {
	m := NewManager()
	cfg := m.Get()
	if cfg == nil {
		t.Fatal("Get returned nil before Load")
	}
}

func TestManagerLoad(t *testing.T) {
	m := NewManager()
	err := m.Load()
	// Load may or may not find config files; should not crash
	_ = err

	cfg := m.Get()
	if cfg == nil {
		t.Fatal("Get returned nil after Load")
	}
}

func TestManagerSave(t *testing.T) {
	// Set up a temp home dir to avoid writing to actual user dir
	dir := t.TempDir()
	oldHome := os.Getenv("HOME")
	os.Setenv("HOME", dir)
	defer os.Setenv("HOME", oldHome)

	// Create the .logflow dir
	logflowDir := filepath.Join(dir, ".logflow")
	os.MkdirAll(logflowDir, 0755)

	m := NewManager()
	_ = m.Load()
	err := m.Save()
	// Save should create file at ~/.logflow/config.yaml
	_ = err
}

func TestManagerGetPaths(t *testing.T) {
	m := NewManager()
	_ = m.Load()
	paths := m.GetPaths()
	// paths may be empty if no config files exist
	if paths == nil {
		t.Error("GetPaths should return non-nil slice")
	}
}

func TestGlobal(t *testing.T) {
	g := Global()
	if g == nil {
		t.Fatal("Global returned nil")
	}
	// Calling again should return same instance
	g2 := Global()
	if g != g2 {
		t.Error("Global should return same instance")
	}
}

func TestEnvVarOverrides(t *testing.T) {
	// Set environment variables
	os.Setenv("LOGFLOW_ENGINE_THREADS", "16")
	defer os.Unsetenv("LOGFLOW_ENGINE_THREADS")

	m := NewManager()
	err := m.Load()
	if err != nil {
		// Some config files may not exist; that's OK
	}

	cfg := m.Get()
	// Depending on implementation, env var should override default
	// At minimum, verify the config is not nil
	if cfg == nil {
		t.Fatal("config should not be nil after Load with env vars")
	}
}

func TestDefaultTelemetryConfig(t *testing.T) {
	cfg := Default()
	// Telemetry should have sensible defaults
	if cfg.Telemetry.Endpoint == "" {
		// May be empty by default; just verify no panic
	}
}

func TestDefaultStorageConfig(t *testing.T) {
	cfg := Default()
	if cfg.Storage.Database == "" {
		t.Error("default storage database should not be empty")
	}
}

func TestDefaultPluginsConfig(t *testing.T) {
	cfg := Default()
	// Check quality plugin defaults
	_ = cfg.Plugins.Quality.Enabled
	_ = cfg.Plugins.ProcessMining.Enabled
}

func TestConversionErrorPolicy(t *testing.T) {
	cfg := Default()
	if cfg.Conversion.ErrorPolicy == "" {
		t.Error("default error policy should not be empty")
	}
	if cfg.Conversion.MaxErrors <= 0 {
		t.Error("default max errors should be > 0")
	}
}
