// Package contract provides data contract generation and validation for Parquet files.
package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Contract represents a data contract for a Parquet file.
type Contract struct {
	// Version of the contract schema
	Version string `json:"version"`

	// File metadata
	File FileInfo `json:"file"`

	// Schema definition
	Schema Schema `json:"schema"`

	// Service Level Objectives
	SLO SLO `json:"slo"`

	// Generation metadata
	Generated GeneratedInfo `json:"generated"`
}

// FileInfo contains information about the data file.
type FileInfo struct {
	Name      string `json:"name"`
	Path      string `json:"path"`
	Size      int64  `json:"size_bytes"`
	Signature string `json:"sha256"`
	RowCount  int64  `json:"row_count,omitempty"`
}

// Schema defines the expected structure of the data.
type Schema struct {
	Columns []Column `json:"columns"`
}

// Column represents a single column in the schema.
type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Role     string `json:"role,omitempty"` // "case_id", "activity", "timestamp", "resource", "attribute"
}

// SLO defines service level objectives for data quality.
type SLO struct {
	NullTolerance    float64 `json:"null_tolerance"`     // Max % of nulls allowed (0.0-1.0)
	TimestampOrder   string  `json:"timestamp_order"`    // "asc", "desc", "any"
	UniqueCase       bool    `json:"unique_case"`        // Expect unique case IDs per event?
	MinEvents        int64   `json:"min_events"`         // Minimum expected events
	MaxEvents        int64   `json:"max_events"`         // Maximum expected events (0 = unlimited)
	RequiredColumns  []string `json:"required_columns"`  // Columns that must be present
}

// GeneratedInfo contains metadata about contract generation.
type GeneratedInfo struct {
	Timestamp string `json:"timestamp"`
	Tool      string `json:"tool"`
	Version   string `json:"tool_version"`
}

// ContractConfig configures contract generation.
type ContractConfig struct {
	// Schema column mappings
	CaseIDColumn    string
	ActivityColumn  string
	TimestampColumn string
	ResourceColumn  string

	// SLO settings
	NullTolerance  float64
	TimestampOrder string

	// Tool info
	ToolVersion string
}

// DefaultConfig returns default contract configuration.
func DefaultConfig() ContractConfig {
	return ContractConfig{
		CaseIDColumn:    "case_id",
		ActivityColumn:  "activity",
		TimestampColumn: "timestamp",
		ResourceColumn:  "resource",
		NullTolerance:   0.05, // 5% null tolerance
		TimestampOrder:  "asc",
		ToolVersion:     "0.1.0",
	}
}

// Generate creates a contract for a Parquet file.
func Generate(parquetPath string, rowCount int64, cfg ContractConfig) (*Contract, error) {
	// Get file info
	stat, err := os.Stat(parquetPath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Calculate file hash
	hash, err := hashFile(parquetPath)
	if err != nil {
		return nil, fmt.Errorf("failed to hash file: %w", err)
	}

	absPath, _ := filepath.Abs(parquetPath)

	contract := &Contract{
		Version: "1.0",
		File: FileInfo{
			Name:      filepath.Base(parquetPath),
			Path:      absPath,
			Size:      stat.Size(),
			Signature: hash,
			RowCount:  rowCount,
		},
		Schema: Schema{
			Columns: []Column{
				{Name: cfg.CaseIDColumn, Type: "string", Nullable: false, Role: "case_id"},
				{Name: cfg.ActivityColumn, Type: "string", Nullable: false, Role: "activity"},
				{Name: cfg.TimestampColumn, Type: "int64", Nullable: false, Role: "timestamp"},
				{Name: cfg.ResourceColumn, Type: "string", Nullable: true, Role: "resource"},
			},
		},
		SLO: SLO{
			NullTolerance:   cfg.NullTolerance,
			TimestampOrder:  cfg.TimestampOrder,
			UniqueCase:      false,
			MinEvents:       0,
			MaxEvents:       0,
			RequiredColumns: []string{cfg.CaseIDColumn, cfg.ActivityColumn, cfg.TimestampColumn},
		},
		Generated: GeneratedInfo{
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Tool:      "logflow",
			Version:   cfg.ToolVersion,
		},
	}

	return contract, nil
}

// WriteContract writes the contract to a .contract.json file alongside the Parquet file.
func WriteContract(parquetPath string, contract *Contract) error {
	contractPath := ContractPath(parquetPath)

	data, err := json.MarshalIndent(contract, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal contract: %w", err)
	}

	if err := os.WriteFile(contractPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write contract: %w", err)
	}

	return nil
}

// ContractPath returns the contract file path for a given Parquet file.
func ContractPath(parquetPath string) string {
	ext := filepath.Ext(parquetPath)
	base := strings.TrimSuffix(parquetPath, ext)
	return base + ".contract.json"
}

// LoadContract loads a contract from a .contract.json file.
func LoadContract(contractPath string) (*Contract, error) {
	data, err := os.ReadFile(contractPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read contract: %w", err)
	}

	var contract Contract
	if err := json.Unmarshal(data, &contract); err != nil {
		return nil, fmt.Errorf("failed to parse contract: %w", err)
	}

	return &contract, nil
}

// Validate checks if a Parquet file matches its contract.
func Validate(parquetPath string) (*ValidationResult, error) {
	contractPath := ContractPath(parquetPath)

	contract, err := LoadContract(contractPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load contract: %w", err)
	}

	result := &ValidationResult{
		Valid:  true,
		Errors: make([]string, 0),
	}

	// Check file exists
	stat, err := os.Stat(parquetPath)
	if err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, fmt.Sprintf("file not found: %s", parquetPath))
		return result, nil
	}

	// Check file size (allow some variance for metadata changes)
	sizeDiff := float64(stat.Size()-contract.File.Size) / float64(contract.File.Size)
	if sizeDiff > 0.1 || sizeDiff < -0.1 {
		result.Warnings = append(result.Warnings, fmt.Sprintf("file size changed significantly: %d -> %d", contract.File.Size, stat.Size()))
	}

	// Check signature
	hash, err := hashFile(parquetPath)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("failed to hash file: %v", err))
		result.Valid = false
		return result, nil
	}

	if hash != contract.File.Signature {
		result.Valid = false
		result.Errors = append(result.Errors, "file signature mismatch - data has been modified")
	}

	return result, nil
}

// ValidationResult contains the results of contract validation.
type ValidationResult struct {
	Valid    bool     `json:"valid"`
	Errors   []string `json:"errors,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// hashFile calculates the SHA-256 hash of a file.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
