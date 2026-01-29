// Package config provides hierarchical configuration management.
// Priority: defaults < system < user < project < env < flags < api
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)


// Config holds all LogFlow configuration.
type Config struct {
	Version int `yaml:"version"`

	Engine     EngineConfig     `yaml:"engine"`
	Conversion ConversionConfig `yaml:"conversion"`
	Plugins    PluginsConfig    `yaml:"plugins"`
	Server     ServerConfig     `yaml:"server"`
	Storage    StorageConfig    `yaml:"storage"`
	Telemetry  TelemetryConfig  `yaml:"telemetry"`
}

// EngineConfig controls the conversion engine.
type EngineConfig struct {
	Default     string `yaml:"default"`      // duckdb | arrow
	MemoryLimit string `yaml:"memory_limit"` // e.g., "4GB"
	Threads     int    `yaml:"threads"`      // 0 = auto
	TempDir     string `yaml:"temp_dir"`
}

// ConversionConfig controls default conversion behavior.
type ConversionConfig struct {
	Compression  string `yaml:"compression"`    // snappy | zstd | gzip | lz4 | none
	RowGroupSize string `yaml:"row_group_size"` // e.g., "128MB"
	BatchSize    int    `yaml:"batch_size"`
}

// PluginsConfig controls plugin behavior.
type PluginsConfig struct {
	Quality        QualityPluginConfig        `yaml:"quality"`
	ProcessMining  ProcessMiningPluginConfig  `yaml:"process_mining"`
	Anonymization  AnonymizationPluginConfig  `yaml:"anonymization"`
}

// QualityPluginConfig for the quality analyzer.
type QualityPluginConfig struct {
	Enabled       bool    `yaml:"enabled"`
	NullThreshold float64 `yaml:"null_threshold"`
}

// ProcessMiningPluginConfig for PM features.
type ProcessMiningPluginConfig struct {
	Enabled    bool `yaml:"enabled"`
	AutoDetect bool `yaml:"auto_detect"`
}

// AnonymizationPluginConfig for PII hashing.
type AnonymizationPluginConfig struct {
	DefaultSalt string `yaml:"default_salt"`
}

// ServerConfig for the HTTP server.
type ServerConfig struct {
	Port          int      `yaml:"port"`
	Host          string   `yaml:"host"`
	MaxUploadSize string   `yaml:"max_upload_size"`
	CORSOrigins   []string `yaml:"cors_origins"`
}

// StorageConfig for persistence.
type StorageConfig struct {
	Database       string        `yaml:"database"`
	JobsRetention  time.Duration `yaml:"jobs_retention"`
	CacheDir       string        `yaml:"cache_dir"`
}

// TelemetryConfig for optional metrics.
type TelemetryConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Endpoint string `yaml:"endpoint"`
}

// Default returns the default configuration.
func Default() *Config {
	homeDir, _ := os.UserHomeDir()
	logflowDir := filepath.Join(homeDir, ".logflow")

	return &Config{
		Version: 1,
		Engine: EngineConfig{
			Default:     "duckdb",
			MemoryLimit: "4GB",
			Threads:     0, // auto
			TempDir:     filepath.Join(os.TempDir(), "logflow"),
		},
		Conversion: ConversionConfig{
			Compression:  "snappy",
			RowGroupSize: "128MB",
			BatchSize:    8192,
		},
		Plugins: PluginsConfig{
			Quality: QualityPluginConfig{
				Enabled:       true,
				NullThreshold: 0.5,
			},
			ProcessMining: ProcessMiningPluginConfig{
				Enabled:    false,
				AutoDetect: true,
			},
		},
		Server: ServerConfig{
			Port:          8080,
			Host:          "localhost",
			MaxUploadSize: "500MB",
			CORSOrigins:   []string{"*"},
		},
		Storage: StorageConfig{
			Database:      filepath.Join(logflowDir, "logflow.db"),
			JobsRetention: 30 * 24 * time.Hour,
			CacheDir:      filepath.Join(logflowDir, "cache"),
		},
		Telemetry: TelemetryConfig{
			Enabled: false,
		},
	}
}

// Manager handles configuration loading and merging.
type Manager struct {
	mu     sync.RWMutex
	config *Config
	paths  []string // Paths that were loaded
}

// NewManager creates a new configuration manager.
func NewManager() *Manager {
	return &Manager{
		config: Default(),
	}
}

// Load loads configuration from all sources in priority order.
func (m *Manager) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Start with defaults
	m.config = Default()

	// Load from paths in order (later overrides earlier)
	paths := m.getConfigPaths()
	for _, path := range paths {
		if err := m.loadFile(path); err != nil {
			// Ignore missing files, but log errors for existing files
			if !os.IsNotExist(err) {
				return err
			}
		} else {
			m.paths = append(m.paths, path)
		}
	}

	// Override with environment variables
	m.loadEnv()

	// Ensure directories exist
	m.ensureDirs()

	return nil
}

// getConfigPaths returns config file paths in priority order.
func (m *Manager) getConfigPaths() []string {
	var paths []string

	// System config
	if runtime.GOOS != "windows" {
		paths = append(paths, "/etc/logflow/config.yaml")
	}

	// User config
	if home, err := os.UserHomeDir(); err == nil {
		paths = append(paths, filepath.Join(home, ".logflow", "config.yaml"))
	}

	// Project config (current directory)
	if cwd, err := os.Getwd(); err == nil {
		paths = append(paths, filepath.Join(cwd, ".logflow.yaml"))
	}

	return paths
}

// loadFile loads a single config file and merges it.
func (m *Manager) loadFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var partial Config
	if err := yaml.Unmarshal(data, &partial); err != nil {
		return err
	}

	// Merge non-zero values
	m.merge(&partial)
	return nil
}

// merge merges non-zero values from src into config.
func (m *Manager) merge(src *Config) {
	// Engine
	if src.Engine.Default != "" {
		m.config.Engine.Default = src.Engine.Default
	}
	if src.Engine.MemoryLimit != "" {
		m.config.Engine.MemoryLimit = src.Engine.MemoryLimit
	}
	if src.Engine.Threads != 0 {
		m.config.Engine.Threads = src.Engine.Threads
	}
	if src.Engine.TempDir != "" {
		m.config.Engine.TempDir = src.Engine.TempDir
	}

	// Conversion
	if src.Conversion.Compression != "" {
		m.config.Conversion.Compression = src.Conversion.Compression
	}
	if src.Conversion.RowGroupSize != "" {
		m.config.Conversion.RowGroupSize = src.Conversion.RowGroupSize
	}
	if src.Conversion.BatchSize != 0 {
		m.config.Conversion.BatchSize = src.Conversion.BatchSize
	}

	// Server
	if src.Server.Port != 0 {
		m.config.Server.Port = src.Server.Port
	}
	if src.Server.Host != "" {
		m.config.Server.Host = src.Server.Host
	}
	if src.Server.MaxUploadSize != "" {
		m.config.Server.MaxUploadSize = src.Server.MaxUploadSize
	}
	if len(src.Server.CORSOrigins) > 0 {
		m.config.Server.CORSOrigins = src.Server.CORSOrigins
	}

	// Storage
	if src.Storage.Database != "" {
		m.config.Storage.Database = src.Storage.Database
	}
	if src.Storage.CacheDir != "" {
		m.config.Storage.CacheDir = src.Storage.CacheDir
	}
	if src.Storage.JobsRetention != 0 {
		m.config.Storage.JobsRetention = src.Storage.JobsRetention
	}
}

// loadEnv loads configuration from environment variables.
func (m *Manager) loadEnv() {
	// LOGFLOW_ENGINE
	if v := os.Getenv("LOGFLOW_ENGINE"); v != "" {
		m.config.Engine.Default = v
	}

	// LOGFLOW_COMPRESSION
	if v := os.Getenv("LOGFLOW_COMPRESSION"); v != "" {
		m.config.Conversion.Compression = v
	}

	// LOGFLOW_PORT
	if v := os.Getenv("LOGFLOW_PORT"); v != "" {
		var port int
		if _, err := fmt.Sscanf(v, "%d", &port); err == nil {
			m.config.Server.Port = port
		}
	}

	// LOGFLOW_DATABASE
	if v := os.Getenv("LOGFLOW_DATABASE"); v != "" {
		m.config.Storage.Database = v
	}
}

// ensureDirs creates necessary directories.
func (m *Manager) ensureDirs() {
	dirs := []string{
		filepath.Dir(m.config.Storage.Database),
		m.config.Storage.CacheDir,
		m.config.Engine.TempDir,
	}

	for _, dir := range dirs {
		os.MkdirAll(dir, 0755)
	}
}

// Get returns the current configuration.
func (m *Manager) Get() *Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

// GetPaths returns the paths that were loaded.
func (m *Manager) GetPaths() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.paths
}

// Save writes the current config to the user config file.
func (m *Manager) Save() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}

	configDir := filepath.Join(home, ".logflow")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return err
	}

	data, err := yaml.Marshal(m.config)
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(configDir, "config.yaml"), data, 0644)
}

// Global instance
var (
	globalManager *Manager
	globalOnce    sync.Once
)

// Global returns the global configuration manager.
func Global() *Manager {
	globalOnce.Do(func() {
		globalManager = NewManager()
		globalManager.Load()
	})
	return globalManager
}
