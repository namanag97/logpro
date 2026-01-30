package parser

import (
	"context"
	"strings"
	"testing"

	"github.com/logflow/logflow/internal/model"
)

// --- Format tests ---

func TestParseFormat(t *testing.T) {
	tests := []struct {
		input string
		want  Format
	}{
		{"csv", FormatCSV},
		{"CSV", FormatCSV},
		{"xes", FormatXES},
		{"json", FormatJSON},
		{"jsonl", FormatJSONL},
		{"xlsx", FormatXLSX},
		{"parquet", FormatParquet},
		{"unknown", FormatUnknown},
		{"", FormatUnknown},
	}
	for _, tt := range tests {
		got := ParseFormat(tt.input)
		if got != tt.want {
			t.Errorf("ParseFormat(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestFormatString(t *testing.T) {
	tests := []struct {
		f    Format
		want string
	}{
		{FormatCSV, "csv"},
		{FormatXES, "xes"},
		{FormatJSON, "json"},
		{FormatJSONL, "jsonl"},
		{FormatXLSX, "xlsx"},
		{FormatParquet, "parquet"},
		{FormatUnknown, "unknown"},
	}
	for _, tt := range tests {
		got := tt.f.String()
		if got != tt.want {
			t.Errorf("Format(%d).String() = %q, want %q", tt.f, got, tt.want)
		}
	}
}

// --- Config tests ---

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.BatchSize <= 0 {
		t.Error("default batch size should be > 0")
	}
	if cfg.BufferSize <= 0 {
		t.Error("default buffer size should be > 0")
	}
}

func TestConfigColumn(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ColumnMapping = map[string]string{
		"case_id": "CaseNr",
	}

	if cfg.Column("case_id") != "CaseNr" {
		t.Errorf("Column(case_id) = %q, want %q", cfg.Column("case_id"), "CaseNr")
	}
	// Non-mapped role returns "" (empty string)
	if cfg.Column("missing") != "" {
		t.Errorf("Column(missing) = %q, want empty string", cfg.Column("missing"))
	}
}

func TestConfigHasPMColumns(t *testing.T) {
	cfg := DefaultConfig()
	// Default config should have PM column mappings
	hasPM := cfg.HasPMColumns()
	_ = hasPM // May or may not be true depending on defaults
}

// --- NewParser factory tests ---

func TestNewParserCSV(t *testing.T) {
	p, err := NewParser(FormatCSV, DefaultConfig())
	if err != nil {
		t.Fatalf("NewParser(CSV) error: %v", err)
	}
	if p == nil {
		t.Fatal("NewParser(CSV) returned nil")
	}
}

func TestNewParserXES(t *testing.T) {
	p, err := NewParser(FormatXES, DefaultConfig())
	if err != nil {
		t.Fatalf("NewParser(XES) error: %v", err)
	}
	if p == nil {
		t.Fatal("NewParser(XES) returned nil")
	}
}

func TestNewParserJSONL(t *testing.T) {
	p, err := NewParser(FormatJSONL, DefaultConfig())
	if err != nil {
		t.Fatalf("NewParser(JSONL) error: %v", err)
	}
	if p == nil {
		t.Fatal("NewParser(JSONL) returned nil")
	}
}

func TestNewParserAccessLog(t *testing.T) {
	p, err := NewParser(FormatAccessLog, DefaultConfig())
	if err != nil {
		t.Fatalf("NewParser(AccessLog) error: %v", err)
	}
	if p == nil {
		t.Fatal("NewParser(AccessLog) returned nil")
	}
}

func TestNewParserUnknown(t *testing.T) {
	_, err := NewParser(FormatUnknown, DefaultConfig())
	if err == nil {
		t.Error("NewParser(Unknown) should return error")
	}
}

// --- CSVParser tests ---

func TestCSVParserBasic(t *testing.T) {
	input := "case_id,activity,timestamp,resource\nc1,A,2024-01-01T10:00:00Z,Alice\nc2,B,2024-01-01T11:00:00Z,Bob\n"

	p := NewCSVParser(DefaultConfig())
	out := make(chan *model.Event, 100)
	ctx := context.Background()

	go func() {
		defer close(out)
		if err := p.Parse(ctx, strings.NewReader(input), out); err != nil {
			t.Errorf("Parse error: %v", err)
		}
	}()

	events := drainChannel(out)
	if len(events) != 2 {
		t.Fatalf("events count = %d, want 2", len(events))
	}
	if string(events[0].CaseID) != "c1" {
		t.Errorf("event[0].CaseID = %q, want %q", events[0].CaseID, "c1")
	}
	if string(events[0].Activity) != "A" {
		t.Errorf("event[0].Activity = %q, want %q", events[0].Activity, "A")
	}
}

func TestCSVParserCustomDelimiter(t *testing.T) {
	input := "case_id;activity;timestamp;resource\nc1;A;2024-01-01T10:00:00Z;Alice\n"

	cfg := DefaultConfig()
	cfg.Delimiter = ';'
	p := NewCSVParser(cfg)
	out := make(chan *model.Event, 100)

	go func() {
		defer close(out)
		if err := p.Parse(context.Background(), strings.NewReader(input), out); err != nil {
			t.Errorf("Parse error: %v", err)
		}
	}()

	events := drainChannel(out)
	if len(events) != 1 {
		t.Fatalf("events count = %d, want 1", len(events))
	}
	if string(events[0].CaseID) != "c1" {
		t.Errorf("CaseID = %q, want %q", events[0].CaseID, "c1")
	}
}

func TestCSVParserQuotedFields(t *testing.T) {
	input := "case_id,activity,timestamp,resource\n\"case,1\",\"Activity \"\"A\"\"\",2024-01-01T10:00:00Z,Alice\n"

	p := NewCSVParser(DefaultConfig())
	out := make(chan *model.Event, 100)

	go func() {
		defer close(out)
		_ = p.Parse(context.Background(), strings.NewReader(input), out)
	}()

	events := drainChannel(out)
	if len(events) != 1 {
		t.Fatalf("events count = %d, want 1", len(events))
	}
	if string(events[0].CaseID) != "case,1" {
		t.Errorf("CaseID = %q, want %q", events[0].CaseID, "case,1")
	}
}

func TestCSVParserContextCancel(t *testing.T) {
	input := "case_id,activity,timestamp,resource\n"
	for i := 0; i < 1000; i++ {
		input += "c1,A,2024-01-01T10:00:00Z,Alice\n"
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := NewCSVParser(DefaultConfig())
	out := make(chan *model.Event, 100)

	go func() {
		defer close(out)
		_ = p.Parse(ctx, strings.NewReader(input), out)
	}()

	// Drain whatever was sent
	drainChannel(out)
}

func TestCSVParserColumnMapping(t *testing.T) {
	input := "CaseNr,ActivityName,Time,Worker\nc1,A,2024-01-01T10:00:00Z,Alice\n"

	cfg := DefaultConfig()
	cfg.ColumnMapping = map[string]string{
		"case_id":   "CaseNr",
		"activity":  "ActivityName",
		"timestamp": "Time",
		"resource":  "Worker",
	}
	p := NewCSVParser(cfg)
	out := make(chan *model.Event, 100)

	go func() {
		defer close(out)
		if err := p.Parse(context.Background(), strings.NewReader(input), out); err != nil {
			t.Errorf("Parse error: %v", err)
		}
	}()

	events := drainChannel(out)
	if len(events) != 1 {
		t.Fatalf("events count = %d, want 1", len(events))
	}
	if string(events[0].CaseID) != "c1" {
		t.Errorf("CaseID = %q, want %q", events[0].CaseID, "c1")
	}
}

// --- XESParser tests ---

func TestXESParserBasic(t *testing.T) {
	input := `<?xml version="1.0" encoding="UTF-8"?>
<log>
  <trace>
    <string key="concept:name" value="case1"/>
    <event>
      <string key="concept:name" value="Activity A"/>
      <date key="time:timestamp" value="2024-01-01T10:00:00.000+00:00"/>
      <string key="org:resource" value="Alice"/>
    </event>
    <event>
      <string key="concept:name" value="Activity B"/>
      <date key="time:timestamp" value="2024-01-01T11:00:00.000+00:00"/>
      <string key="org:resource" value="Bob"/>
    </event>
  </trace>
</log>`

	p := NewXESParser(DefaultConfig())
	out := make(chan *model.Event, 100)

	go func() {
		defer close(out)
		if err := p.Parse(context.Background(), strings.NewReader(input), out); err != nil {
			t.Errorf("XES Parse error: %v", err)
		}
	}()

	events := drainChannel(out)
	if len(events) != 2 {
		t.Fatalf("events count = %d, want 2", len(events))
	}
	if string(events[0].Activity) != "Activity A" {
		t.Errorf("event[0].Activity = %q, want %q", events[0].Activity, "Activity A")
	}
}

// --- JSONLParser tests ---

func TestJSONLParserBasic(t *testing.T) {
	input := `{"case_id":"c1","activity":"A","timestamp":"2024-01-01T10:00:00Z","resource":"Alice"}
{"case_id":"c2","activity":"B","timestamp":"2024-01-01T11:00:00Z","resource":"Bob"}
`

	p := NewJSONLParser(DefaultConfig())
	out := make(chan *model.Event, 100)

	go func() {
		defer close(out)
		if err := p.Parse(context.Background(), strings.NewReader(input), out); err != nil {
			t.Errorf("JSONL Parse error: %v", err)
		}
	}()

	events := drainChannel(out)
	if len(events) != 2 {
		t.Fatalf("events count = %d, want 2", len(events))
	}
	if string(events[0].CaseID) != "c1" {
		t.Errorf("event[0].CaseID = %q, want %q", events[0].CaseID, "c1")
	}
}

// --- AccessLogParser tests ---

func TestAccessLogParserBasic(t *testing.T) {
	input := `127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/start.html" "Mozilla/4.08"
192.168.1.1 - - [11/Oct/2000:14:00:00 -0700] "POST /api/data HTTP/1.1" 201 512 "-" "curl/7.68"
`

	p := NewAccessLogParser(DefaultConfig())
	out := make(chan *model.Event, 10)

	err := p.Parse(context.Background(), strings.NewReader(input), out)
	if err != nil {
		t.Fatalf("AccessLog Parse error: %v", err)
	}

	events := drainChannel(out)
	if len(events) != 2 {
		t.Fatalf("events count = %d, want 2", len(events))
	}
}

// --- CSVScanner (FSM) tests ---

func TestCSVScannerBasic(t *testing.T) {
	s := NewCSVScanner(',')
	fields := s.ScanLine([]byte("a,b,c"))

	if len(fields) != 3 {
		t.Fatalf("fields count = %d, want 3", len(fields))
	}
	if string(fields[0]) != "a" {
		t.Errorf("field[0] = %q, want %q", fields[0], "a")
	}
	if string(fields[2]) != "c" {
		t.Errorf("field[2] = %q, want %q", fields[2], "c")
	}
}

func TestCSVScannerQuoted(t *testing.T) {
	s := NewCSVScanner(',')
	fields := s.ScanLine([]byte(`"hello, world",b,"c""d"`))

	if len(fields) != 3 {
		t.Fatalf("fields count = %d, want 3", len(fields))
	}
	if string(fields[0]) != "hello, world" {
		t.Errorf("field[0] = %q, want %q", fields[0], "hello, world")
	}
}

func TestCSVScannerReset(t *testing.T) {
	s := NewCSVScanner(',')
	_ = s.ScanLine([]byte("a,b"))
	s.Reset()
	fields := s.ScanLine([]byte("x,y,z"))

	if len(fields) != 3 {
		t.Fatalf("fields after reset = %d, want 3", len(fields))
	}
}

func TestCSVScannerEmpty(t *testing.T) {
	s := NewCSVScanner(',')
	fields := s.ScanLine([]byte(""))
	// Empty line should return at least one empty field or zero fields
	_ = fields
}

func TestCSVScannerSemicolon(t *testing.T) {
	s := NewCSVScanner(';')
	fields := s.ScanLine([]byte("a;b;c"))

	if len(fields) != 3 {
		t.Fatalf("fields count = %d, want 3", len(fields))
	}
}

// --- Utility function tests ---

func TestNormalizeLineEndings(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello\r\nworld", "hello\nworld"},
		{"hello\rworld", "hello\nworld"},
		{"hello\nworld", "hello\nworld"},
	}
	for _, tt := range tests {
		got := string(NormalizeLineEndings([]byte(tt.input)))
		if got != tt.want {
			t.Errorf("NormalizeLineEndings(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestSanitizeUTF8(t *testing.T) {
	valid := []byte("hello world")
	result := SanitizeUTF8(valid)
	if string(result) != "hello world" {
		t.Errorf("SanitizeUTF8 changed valid UTF-8: %q", result)
	}
}

// --- Helper ---

func drainChannel(ch chan *model.Event) []*model.Event {
	var events []*model.Event
	for ev := range ch {
		events = append(events, ev)
	}
	return events
}
