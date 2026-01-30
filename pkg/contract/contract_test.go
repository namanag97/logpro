package contract

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/marcboeker/go-duckdb"
)

// createTestParquet creates a minimal Parquet file for testing.
func createTestParquet(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "test.parquet")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE events AS SELECT * FROM (VALUES
			('case1', 'Register', '2024-01-01 10:00:00'::TIMESTAMP, 'Alice'),
			('case1', 'Approve',  '2024-01-01 11:00:00'::TIMESTAMP, 'Bob'),
			('case2', 'Register', '2024-01-02 09:00:00'::TIMESTAMP, 'Charlie')
		) AS t(case_id, activity, timestamp, resource)
	`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, err = db.Exec("COPY events TO '" + path + "' (FORMAT 'parquet')")
	if err != nil {
		t.Fatalf("copy to parquet: %v", err)
	}
	return path
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.NullTolerance == 0 {
		t.Error("default NullTolerance should be non-zero")
	}
	if cfg.TimestampOrder == "" {
		t.Error("default TimestampOrder should not be empty")
	}
}

func TestConfigColumn(t *testing.T) {
	cfg := DefaultConfig()
	// Default mapping should return the role itself if no mapping exists
	col := cfg.Column("nonexistent_role")
	if col == "" {
		t.Error("Column for unknown role should return the role name")
	}
}

func TestGenerate(t *testing.T) {
	dir := t.TempDir()
	path := createTestParquet(t, dir)

	cfg := DefaultConfig()
	contract, err := Generate(path, 3, cfg)
	if err != nil {
		t.Fatalf("Generate error: %v", err)
	}
	if contract == nil {
		t.Fatal("Generate returned nil")
	}
	if contract.File.Name == "" {
		t.Error("contract file name is empty")
	}
	if contract.File.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", contract.File.RowCount)
	}
	if len(contract.Schema.Columns) == 0 {
		t.Error("schema columns should not be empty")
	}
	if contract.File.Signature == "" {
		t.Error("file signature should not be empty")
	}
}

func TestWriteAndLoadContract(t *testing.T) {
	dir := t.TempDir()
	path := createTestParquet(t, dir)

	cfg := DefaultConfig()
	contract, err := Generate(path, 3, cfg)
	if err != nil {
		t.Fatalf("Generate error: %v", err)
	}

	err = WriteContract(path, contract)
	if err != nil {
		t.Fatalf("WriteContract error: %v", err)
	}

	contractPath := ContractPath(path)
	if _, err := os.Stat(contractPath); os.IsNotExist(err) {
		t.Fatalf("contract file not found at %q", contractPath)
	}

	loaded, err := LoadContract(contractPath)
	if err != nil {
		t.Fatalf("LoadContract error: %v", err)
	}

	if loaded.File.Name != contract.File.Name {
		t.Errorf("loaded name = %q, want %q", loaded.File.Name, contract.File.Name)
	}
	if loaded.File.RowCount != contract.File.RowCount {
		t.Errorf("loaded RowCount = %d, want %d", loaded.File.RowCount, contract.File.RowCount)
	}
	if loaded.File.Signature != contract.File.Signature {
		t.Errorf("loaded Signature = %q, want %q", loaded.File.Signature, contract.File.Signature)
	}
}

func TestContractPath(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"data.parquet", "data.contract.json"},
		{"/path/to/file.parquet", "/path/to/file.contract.json"},
	}
	for _, tt := range tests {
		got := ContractPath(tt.input)
		if got != tt.want {
			t.Errorf("ContractPath(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestValidateSuccess(t *testing.T) {
	dir := t.TempDir()
	path := createTestParquet(t, dir)

	cfg := DefaultConfig()
	contract, _ := Generate(path, 3, cfg)
	_ = WriteContract(path, contract)

	result, err := Validate(path)
	if err != nil {
		t.Fatalf("Validate error: %v", err)
	}
	if result == nil {
		t.Fatal("Validate returned nil")
	}
	if !result.Valid {
		t.Errorf("Validate should pass, errors: %v", result.Errors)
	}
}

func TestValidateNoContract(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "missing.parquet")
	// Create an empty file
	_ = os.WriteFile(path, []byte{}, 0644)

	_, err := Validate(path)
	if err == nil {
		t.Error("Validate without contract should return error")
	}
}

func TestValidateModifiedFile(t *testing.T) {
	dir := t.TempDir()
	path := createTestParquet(t, dir)

	cfg := DefaultConfig()
	contract, _ := Generate(path, 3, cfg)
	_ = WriteContract(path, contract)

	// Modify the parquet file after generating contract
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	_, _ = f.Write([]byte("extra data"))
	f.Close()

	result, err := Validate(path)
	if err != nil {
		t.Fatalf("Validate error: %v", err)
	}
	if result.Valid {
		t.Error("Validate should fail for modified file")
	}
}

func TestGenerateSchemaColumns(t *testing.T) {
	dir := t.TempDir()
	path := createTestParquet(t, dir)

	cfg := DefaultConfig()
	contract, err := Generate(path, 3, cfg)
	if err != nil {
		t.Fatalf("Generate error: %v", err)
	}

	// Check that columns from the parquet file are captured
	if len(contract.Schema.Columns) < 4 {
		t.Errorf("expected at least 4 columns, got %d", len(contract.Schema.Columns))
	}

	colNames := make(map[string]bool)
	for _, c := range contract.Schema.Columns {
		colNames[c.Name] = true
	}

	for _, expected := range []string{"case_id", "activity", "timestamp", "resource"} {
		if !colNames[expected] {
			t.Errorf("missing column %q in schema", expected)
		}
	}
}

func TestContractVersion(t *testing.T) {
	dir := t.TempDir()
	path := createTestParquet(t, dir)

	cfg := DefaultConfig()
	contract, _ := Generate(path, 3, cfg)

	if contract.Version == "" {
		t.Error("contract version should not be empty")
	}
}
