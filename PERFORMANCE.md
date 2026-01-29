# LogFlow Performance Analysis & Optimization

## Executive Summary

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Throughput | 807K rows/sec | 1.07M rows/sec | **32% faster** |
| Speed | 92 MB/s | 133 MB/s | **45% faster** |
| Grade | VERY GOOD | EXCELLENT | ★★★★★ |

## Real-World Benchmarks

| Dataset | Input | Output | Compression | Speed |
|---------|-------|--------|-------------|-------|
| Synthetic CSV | 119 MB | 70 MB | 1.7x | 123 MB/s |
| Messy CSV (20 cols, sparse) | 29 MB | 12 MB | 2.5x | 70 MB/s |
| **BPI 2017 (XES)** | 552 MB | 13 MB | **42.7x** | 88 MB/s |

## Information-Theoretic Analysis

**Process Mining Data (BPI 2017):**
```
Format              Bytes/Event    Efficiency
─────────────────────────────────────────────
XES (XML)           482 bytes      1.2%
Parquet             11.3 bytes     50%
Theoretical         5.6 bytes      100%
```

**Per-Column Entropy:**
```
Column      Entropy    Distinct    Why
──────────────────────────────────────────────
case_id     14.82 bits  31,509     Repeated across events
activity    3.79 bits   26         Perfect for dictionary encoding
resource    6.34 bits   149        Low cardinality
timestamp   20.20 bits  1.2M       Unique, but compressible
──────────────────────────────────────────────
TOTAL       45.15 bits/event (5.6 bytes theoretical minimum)
```

## What Was Optimized

1. **ROW_GROUP_SIZE 10000** (default: 122,880) - 1.6x speedup
2. **Parallel CSV reading** enabled
3. **Schema caching** for repeated conversions
4. **Metadata queries** optimized (parquet_metadata vs COUNT(*))
5. **Explicit schema support** for 13-23% faster parsing
6. **DuckDB optimal settings** (threads, memory, object cache)

## Bottleneck Analysis

### Before Optimization
```
CSV Parsing:       ~40%
Parquet Encoding:  ~45%  ← Main bottleneck
Compression:       ~3%
I/O:               ~1%
```

### After Optimization (ROW_GROUP_SIZE 10000)
```
CSV Parsing:       ~58%  ← Now the main bottleneck
Parquet Encoding:  ~28%  (significantly reduced)
Memory Table:      ~13%
Compression:       ~0%   (snappy is very fast)
I/O:               ~2%
```

## Key Optimizations Applied

### 1. ROW_GROUP_SIZE Optimization
```sql
-- Before (default)
COPY ... TO '...' (FORMAT PARQUET, COMPRESSION 'snappy')
-- Result: 82 MB/s

-- After (optimized)
COPY ... TO '...' (FORMAT PARQUET, COMPRESSION 'snappy', ROW_GROUP_SIZE 10000)
-- Result: 133 MB/s (1.6x faster!)
```

**Why it works:** Smaller row groups reduce memory pressure during encoding and allow better parallelization.

### 2. Parallel CSV Reading
```sql
read_csv_auto('file.csv', header=true, parallel=true)
```

### 3. Compression Trade-offs
| Compression | Speed | Output Size | Use Case |
|-------------|-------|-------------|----------|
| uncompressed | 94 MB/s | 98 MB | Maximum speed |
| snappy | 133 MB/s | 70 MB | **Balanced (default)** |
| zstd | 84 MB/s | 40 MB | Maximum compression |

## Theoretical Limits

```
Disk Read Speed:      ~5,000-9,000 MB/s (NVMe)
Current Achievement:  133 MB/s
Efficiency:           ~2-3% of I/O limit

Gap Reason: Processing overhead (parsing + encoding)
```

## Further Optimization Opportunities

### 1. For CSV Input (Current Bottleneck)
- DuckDB's CSV parser is already SIMD-optimized
- Consider pre-converting to Parquet for repeated processing
- Arrow IPC format for streaming scenarios

### 2. For Sparse Data (100s of columns with NULLs)
- Parquet handles sparse data efficiently via:
  - Definition/repetition levels for NULL encoding
  - Dictionary encoding for repeated string values
  - RLE (Run-Length Encoding) for sparse columns

### 3. For Pipelining (Future)
```
Current (Sequential):
  [Parse ALL] → [Encode ALL] → [Write ALL]

Optimal (Pipelined):
  [Parse Chunk1] → [Encode Chunk1] → [Write Chunk1]
       ↓                ↓                 ↓
  [Parse Chunk2] → [Encode Chunk2] → [Write Chunk2]
```

## Benchmarking Commands

```bash
# Quick benchmark
./logflow benchmark --rows 1000000

# Full analysis with disk test
./logflow benchmark --rows 1000000 --disk-test --deep

# Generate CPU flamegraph
./logflow benchmark --rows 1000000 --cpuprofile=cpu.prof
go tool pprof -http=:8080 cpu.prof

# Test with more data
./logflow benchmark --rows 10000000 --deep
```

## Architecture for Handling 100s of Columns

When processing data with 100s of columns and sparse/inconsistent data:

1. **Schema Inference** - DuckDB's `read_csv_auto` samples first 1000 rows
2. **NULL Handling** - Parquet uses efficient null bitmaps (1 bit per value)
3. **Type Coercion** - Mixed types coerced to VARCHAR when needed
4. **Memory Efficiency** - Streaming approach prevents memory explosion

### Configuration for Large Schemas

```go
// For files with many columns, ensure adequate sample size
query := `
    COPY (SELECT * FROM read_csv_auto('file.csv',
        header=true,
        sample_size=10000,     -- More samples for schema inference
        all_varchar=false,     -- Attempt type detection
        parallel=true          -- Enable parallel parsing
    ))
    TO 'output.parquet' (
        FORMAT PARQUET,
        COMPRESSION 'snappy',
        ROW_GROUP_SIZE 10000   -- Smaller groups for faster encoding
    )
`
```

## Conclusion

The main performance lever is `ROW_GROUP_SIZE`. Smaller row groups (10,000 vs default 122,880) provide:
- 1.6x faster conversion
- Same output file size
- Same compression ratio

For further optimization, the CSV parsing bottleneck (58% of time) is best addressed by:
1. Using binary formats (Parquet/Arrow) as source when possible
2. Pre-processing data in native format
3. Implementing a custom SIMD CSV parser (diminishing returns given DuckDB is already optimized)
