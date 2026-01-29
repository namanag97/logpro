package detect

import (
	"bytes"
	"path/filepath"
	"strings"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// detectFormat identifies the file format.
func detectFormat(path string, sample []byte) core.Format {
	// Check magic bytes first
	if len(sample) >= 4 {
		// Parquet: PAR1
		if bytes.HasPrefix(sample, []byte("PAR1")) {
			return core.FormatParquet
		}
		// Gzip: 1f 8b
		if sample[0] == 0x1f && sample[1] == 0x8b {
			return core.FormatGzip
		}
		// XLSX: PK (zip)
		if sample[0] == 0x50 && sample[1] == 0x4B {
			return core.FormatXLSX
		}
		// ORC: ORC
		if bytes.HasPrefix(sample, []byte("ORC")) {
			return core.FormatORC
		}
		// Avro: Obj\x01
		if bytes.HasPrefix(sample, []byte("Obj\x01")) {
			return core.FormatAvro
		}
	}

	// Try extension
	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".gz" {
		base := strings.TrimSuffix(path, ".gz")
		ext = strings.ToLower(filepath.Ext(base))
	}

	switch ext {
	case ".csv":
		return core.FormatCSV
	case ".tsv":
		return core.FormatTSV
	case ".json":
		return core.FormatJSON
	case ".jsonl", ".ndjson":
		return core.FormatJSONL
	case ".xes":
		return core.FormatXES
	case ".xml":
		return core.FormatXML
	case ".xlsx", ".xls":
		return core.FormatXLSX
	case ".parquet":
		return core.FormatParquet
	case ".orc":
		return core.FormatORC
	case ".avro":
		return core.FormatAvro
	}

	// Content-based detection
	content := sample
	if len(content) >= 3 && content[0] == 0xEF && content[1] == 0xBB && content[2] == 0xBF {
		content = content[3:]
	}
	content = bytes.TrimLeft(content, " \t\r\n")

	// XML/XES
	if bytes.HasPrefix(content, []byte("<?xml")) || bytes.HasPrefix(content, []byte("<")) {
		if bytes.Contains(sample, []byte("<trace")) || bytes.Contains(sample, []byte("xes.")) {
			return core.FormatXES
		}
		return core.FormatXML
	}

	// JSON
	if len(content) > 0 && (content[0] == '{' || content[0] == '[') {
		if isJSONL(content) {
			return core.FormatJSONL
		}
		return core.FormatJSON
	}

	// Default to CSV
	tabCount := bytes.Count(sample, []byte("\t"))
	commaCount := bytes.Count(sample, []byte(","))

	if tabCount > commaCount && tabCount > 0 {
		return core.FormatTSV
	}
	return core.FormatCSV
}

// isJSONL checks if content is JSONL.
func isJSONL(sample []byte) bool {
	lines := bytes.Split(sample, []byte("\n"))
	if len(lines) < 2 {
		return false
	}

	jsonObjects := 0
	for i, line := range lines {
		if i >= 5 {
			break
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if line[0] == '{' && line[len(line)-1] == '}' {
			jsonObjects++
		}
	}
	return jsonObjects >= 2
}
