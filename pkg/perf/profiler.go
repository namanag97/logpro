// Package perf provides detailed performance profiling for conversions.
// Identifies I/O, CPU, memory, and compression bottlenecks.
package perf

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Profiler tracks detailed performance metrics.
type Profiler struct {
	// Timing breakdowns
	timings sync.Map // phase -> []time.Duration

	// Counters
	bytesRead    atomic.Int64
	bytesWritten atomic.Int64
	rowsProcessed atomic.Int64

	// I/O metrics
	readOps   atomic.Int64
	writeOps  atomic.Int64
	readWait  atomic.Int64 // nanoseconds waiting on reads
	writeWait atomic.Int64 // nanoseconds waiting on writes

	// Memory metrics
	allocations atomic.Int64
	peakMemory  atomic.Int64

	// Start time
	startTime time.Time
	phases    []PhaseRecord
	phaseMu   sync.Mutex
}

// PhaseRecord records a phase of processing.
type PhaseRecord struct {
	Name      string
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
}

// New creates a new profiler.
func New() *Profiler {
	return &Profiler{
		startTime: time.Now(),
	}
}

// StartPhase begins timing a phase.
func (p *Profiler) StartPhase(name string) func() {
	start := time.Now()
	return func() {
		duration := time.Since(start)
		p.phaseMu.Lock()
		p.phases = append(p.phases, PhaseRecord{
			Name:      name,
			StartTime: start,
			EndTime:   time.Now(),
			Duration:  duration,
		})
		p.phaseMu.Unlock()

		// Also store in map for aggregation
		if existing, ok := p.timings.Load(name); ok {
			p.timings.Store(name, append(existing.([]time.Duration), duration))
		} else {
			p.timings.Store(name, []time.Duration{duration})
		}
	}
}

// RecordRead records a read operation.
func (p *Profiler) RecordRead(bytes int64, duration time.Duration) {
	p.bytesRead.Add(bytes)
	p.readOps.Add(1)
	p.readWait.Add(int64(duration))
}

// RecordWrite records a write operation.
func (p *Profiler) RecordWrite(bytes int64, duration time.Duration) {
	p.bytesWritten.Add(bytes)
	p.writeOps.Add(1)
	p.writeWait.Add(int64(duration))
}

// RecordRows records processed rows.
func (p *Profiler) RecordRows(count int64) {
	p.rowsProcessed.Add(count)
}

// RecordAlloc records a memory allocation.
func (p *Profiler) RecordAlloc(bytes int64) {
	p.allocations.Add(bytes)

	// Update peak
	current := p.allocations.Load()
	for {
		peak := p.peakMemory.Load()
		if current <= peak || p.peakMemory.CompareAndSwap(peak, current) {
			break
		}
	}
}

// Report generates a detailed profiling report.
func (p *Profiler) Report() *Report {
	totalDuration := time.Since(p.startTime)

	report := &Report{
		TotalDuration:  totalDuration,
		BytesRead:      p.bytesRead.Load(),
		BytesWritten:   p.bytesWritten.Load(),
		RowsProcessed:  p.rowsProcessed.Load(),
		ReadOps:        p.readOps.Load(),
		WriteOps:       p.writeOps.Load(),
		ReadWaitTime:   time.Duration(p.readWait.Load()),
		WriteWaitTime:  time.Duration(p.writeWait.Load()),
		PeakMemory:     p.peakMemory.Load(),
		PhaseBreakdown: make(map[string]time.Duration),
	}

	// Calculate phase totals
	p.timings.Range(func(key, value interface{}) bool {
		name := key.(string)
		durations := value.([]time.Duration)
		var total time.Duration
		for _, d := range durations {
			total += d
		}
		report.PhaseBreakdown[name] = total
		return true
	})

	// Calculate derived metrics
	if totalDuration > 0 {
		report.ReadThroughput = float64(report.BytesRead) / totalDuration.Seconds()
		report.WriteThroughput = float64(report.BytesWritten) / totalDuration.Seconds()
		report.RowsPerSecond = float64(report.RowsProcessed) / totalDuration.Seconds()
	}

	// Calculate bottleneck
	report.Bottleneck = p.identifyBottleneck(report)

	// Get memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	report.HeapAlloc = int64(m.HeapAlloc)
	report.GCPauses = int64(m.PauseTotalNs)
	report.NumGC = int64(m.NumGC)

	return report
}

// identifyBottleneck determines the primary bottleneck.
func (p *Profiler) identifyBottleneck(report *Report) Bottleneck {
	total := report.TotalDuration

	// Calculate percentages
	readPct := float64(report.ReadWaitTime) / float64(total) * 100
	writePct := float64(report.WriteWaitTime) / float64(total) * 100
	ioPct := readPct + writePct

	// Check compression time
	var compressPct float64
	if compressTime, ok := report.PhaseBreakdown["compress"]; ok {
		compressPct = float64(compressTime) / float64(total) * 100
	}

	// Check parse time
	var parsePct float64
	if parseTime, ok := report.PhaseBreakdown["parse"]; ok {
		parsePct = float64(parseTime) / float64(total) * 100
	}

	// Determine primary bottleneck
	if ioPct > 50 {
		if readPct > writePct {
			return Bottleneck{
				Type:        "io_read",
				Percentage:  readPct,
				Description: "Read I/O is the bottleneck. Consider SSD, memory-mapping, or parallel reads.",
			}
		}
		return Bottleneck{
			Type:        "io_write",
			Percentage:  writePct,
			Description: "Write I/O is the bottleneck. Consider faster storage or async writes.",
		}
	}

	if compressPct > 30 {
		return Bottleneck{
			Type:        "compression",
			Percentage:  compressPct,
			Description: "Compression is the bottleneck. Consider snappy (faster) or parallel compression.",
		}
	}

	if parsePct > 30 {
		return Bottleneck{
			Type:        "parsing",
			Percentage:  parsePct,
			Description: "Parsing is the bottleneck. Consider DuckDB engine or parallel parsing.",
		}
	}

	return Bottleneck{
		Type:        "balanced",
		Percentage:  0,
		Description: "No single bottleneck. System is well-balanced.",
	}
}

// Report holds profiling results.
type Report struct {
	// Overall
	TotalDuration time.Duration `json:"total_duration"`

	// Throughput
	BytesRead       int64   `json:"bytes_read"`
	BytesWritten    int64   `json:"bytes_written"`
	RowsProcessed   int64   `json:"rows_processed"`
	ReadThroughput  float64 `json:"read_throughput_bytes_sec"`
	WriteThroughput float64 `json:"write_throughput_bytes_sec"`
	RowsPerSecond   float64 `json:"rows_per_second"`

	// I/O
	ReadOps       int64         `json:"read_ops"`
	WriteOps      int64         `json:"write_ops"`
	ReadWaitTime  time.Duration `json:"read_wait_time"`
	WriteWaitTime time.Duration `json:"write_wait_time"`

	// Memory
	PeakMemory int64 `json:"peak_memory"`
	HeapAlloc  int64 `json:"heap_alloc"`
	GCPauses   int64 `json:"gc_pauses_ns"`
	NumGC      int64 `json:"num_gc"`

	// Phase breakdown
	PhaseBreakdown map[string]time.Duration `json:"phase_breakdown"`

	// Analysis
	Bottleneck Bottleneck `json:"bottleneck"`
}

// Bottleneck describes the primary performance bottleneck.
type Bottleneck struct {
	Type        string  `json:"type"`
	Percentage  float64 `json:"percentage"`
	Description string  `json:"description"`
}

// String formats the report as a string.
func (r *Report) String() string {
	var s string

	s += "╔═══════════════════════════════════════════════════════════╗\n"
	s += "║                  PERFORMANCE PROFILE                       ║\n"
	s += "╚═══════════════════════════════════════════════════════════╝\n\n"

	s += "THROUGHPUT:\n"
	s += fmt.Sprintf("  Total Time:       %v\n", r.TotalDuration.Round(time.Millisecond))
	s += fmt.Sprintf("  Rows Processed:   %d (%.0f/sec)\n", r.RowsProcessed, r.RowsPerSecond)
	s += fmt.Sprintf("  Read:             %s (%.1f MB/sec)\n", formatBytes(r.BytesRead), r.ReadThroughput/(1024*1024))
	s += fmt.Sprintf("  Write:            %s (%.1f MB/sec)\n", formatBytes(r.BytesWritten), r.WriteThroughput/(1024*1024))
	s += "\n"

	s += "I/O BREAKDOWN:\n"
	s += fmt.Sprintf("  Read Wait:        %v (%.1f%%)\n", r.ReadWaitTime.Round(time.Millisecond), float64(r.ReadWaitTime)/float64(r.TotalDuration)*100)
	s += fmt.Sprintf("  Write Wait:       %v (%.1f%%)\n", r.WriteWaitTime.Round(time.Millisecond), float64(r.WriteWaitTime)/float64(r.TotalDuration)*100)
	s += fmt.Sprintf("  Read Ops:         %d\n", r.ReadOps)
	s += fmt.Sprintf("  Write Ops:        %d\n", r.WriteOps)
	s += "\n"

	s += "MEMORY:\n"
	s += fmt.Sprintf("  Peak Memory:      %s\n", formatBytes(r.PeakMemory))
	s += fmt.Sprintf("  Heap Alloc:       %s\n", formatBytes(r.HeapAlloc))
	s += fmt.Sprintf("  GC Pauses:        %v\n", time.Duration(r.GCPauses).Round(time.Millisecond))
	s += fmt.Sprintf("  GC Runs:          %d\n", r.NumGC)
	s += "\n"

	if len(r.PhaseBreakdown) > 0 {
		s += "PHASE BREAKDOWN:\n"
		// Sort phases by duration
		type phaseDur struct {
			name string
			dur  time.Duration
		}
		var phases []phaseDur
		for name, dur := range r.PhaseBreakdown {
			phases = append(phases, phaseDur{name, dur})
		}
		sort.Slice(phases, func(i, j int) bool {
			return phases[i].dur > phases[j].dur
		})
		for _, p := range phases {
			pct := float64(p.dur) / float64(r.TotalDuration) * 100
			s += fmt.Sprintf("  %-15s %v (%.1f%%)\n", p.name+":", p.dur.Round(time.Millisecond), pct)
		}
		s += "\n"
	}

	s += "═══════════════════════════════════════════════════════════\n"
	s += fmt.Sprintf("BOTTLENECK: %s\n", r.Bottleneck.Type)
	s += fmt.Sprintf("  %s\n", r.Bottleneck.Description)
	s += "═══════════════════════════════════════════════════════════\n"

	return s
}

// TheoreticalMax calculates theoretical maximum throughput.
func TheoreticalMax() TheoreticalLimits {
	// Typical hardware limits
	return TheoreticalLimits{
		NVMeReadMBps:   3500,  // NVMe SSD sequential read
		NVMeWriteMBps:  3000,  // NVMe SSD sequential write
		SATAReadMBps:   550,   // SATA SSD
		MemoryBandwidth: 50000, // DDR4/5 bandwidth MB/s
		PCIeBandwidth:  16000, // PCIe 4.0 x4
	}
}

// TheoreticalLimits holds hardware limits.
type TheoreticalLimits struct {
	NVMeReadMBps    float64 `json:"nvme_read_mbps"`
	NVMeWriteMBps   float64 `json:"nvme_write_mbps"`
	SATAReadMBps    float64 `json:"sata_read_mbps"`
	MemoryBandwidth float64 `json:"memory_bandwidth_mbps"`
	PCIeBandwidth   float64 `json:"pcie_bandwidth_mbps"`
}

// --- Profiled Reader/Writer Wrappers ---

// ProfiledReader wraps a reader to track I/O metrics.
type ProfiledReader struct {
	reader   io.Reader
	profiler *Profiler
}

// NewProfiledReader creates a profiled reader.
func NewProfiledReader(r io.Reader, p *Profiler) *ProfiledReader {
	return &ProfiledReader{reader: r, profiler: p}
}

// Read implements io.Reader with profiling.
func (r *ProfiledReader) Read(p []byte) (n int, err error) {
	start := time.Now()
	n, err = r.reader.Read(p)
	duration := time.Since(start)
	if n > 0 {
		r.profiler.RecordRead(int64(n), duration)
	}
	return
}

// ProfiledWriter wraps a writer to track I/O metrics.
type ProfiledWriter struct {
	writer   io.Writer
	profiler *Profiler
}

// NewProfiledWriter creates a profiled writer.
func NewProfiledWriter(w io.Writer, p *Profiler) *ProfiledWriter {
	return &ProfiledWriter{writer: w, profiler: p}
}

// Write implements io.Writer with profiling.
func (w *ProfiledWriter) Write(p []byte) (n int, err error) {
	start := time.Now()
	n, err = w.writer.Write(p)
	duration := time.Since(start)
	if n > 0 {
		w.profiler.RecordWrite(int64(n), duration)
	}
	return
}

// --- Disk Speed Test ---

// DiskSpeedTest measures actual disk I/O speed.
func DiskSpeedTest(path string) (*DiskSpeed, error) {
	// Write test
	testFile := path + ".speedtest"
	testSize := int64(100 * 1024 * 1024) // 100MB

	data := make([]byte, 1024*1024) // 1MB buffer
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Write test
	writeStart := time.Now()
	f, err := os.Create(testFile)
	if err != nil {
		return nil, err
	}

	written := int64(0)
	for written < testSize {
		n, err := f.Write(data)
		if err != nil {
			f.Close()
			os.Remove(testFile)
			return nil, err
		}
		written += int64(n)
	}
	f.Sync()
	f.Close()
	writeDuration := time.Since(writeStart)

	// Read test
	readStart := time.Now()
	f, err = os.Open(testFile)
	if err != nil {
		os.Remove(testFile)
		return nil, err
	}

	read := int64(0)
	for {
		n, err := f.Read(data)
		read += int64(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			f.Close()
			os.Remove(testFile)
			return nil, err
		}
	}
	f.Close()
	readDuration := time.Since(readStart)

	// Cleanup
	os.Remove(testFile)

	return &DiskSpeed{
		WriteMBps: float64(testSize) / writeDuration.Seconds() / (1024 * 1024),
		ReadMBps:  float64(read) / readDuration.Seconds() / (1024 * 1024),
		TestSize:  testSize,
	}, nil
}

// DiskSpeed holds disk speed test results.
type DiskSpeed struct {
	WriteMBps float64 `json:"write_mbps"`
	ReadMBps  float64 `json:"read_mbps"`
	TestSize  int64   `json:"test_size"`
}

func formatBytes(b int64) string {
	if b >= 1e9 {
		return fmt.Sprintf("%.1f GB", float64(b)/1e9)
	}
	if b >= 1e6 {
		return fmt.Sprintf("%.1f MB", float64(b)/1e6)
	}
	if b >= 1e3 {
		return fmt.Sprintf("%.1f KB", float64(b)/1e3)
	}
	return fmt.Sprintf("%d B", b)
}

// --- Context-based Profiling ---

type profilerKey struct{}

// WithProfiler adds a profiler to context.
func WithProfiler(ctx context.Context, p *Profiler) context.Context {
	return context.WithValue(ctx, profilerKey{}, p)
}

// FromContext gets profiler from context.
func FromContext(ctx context.Context) *Profiler {
	if p, ok := ctx.Value(profilerKey{}).(*Profiler); ok {
		return p
	}
	return nil
}
