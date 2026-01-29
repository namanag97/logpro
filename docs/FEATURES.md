# LogFlow Feature Inventory & User Flows

## Complete Feature Map

### Layer 1: Input (Sources)
| ID | Feature | Description | Dependencies |
|----|---------|-------------|--------------|
| I1 | CSV Parser | Parse comma/tab separated files | None |
| I2 | XES Parser | Parse IEEE XES event logs | None |
| I3 | XLSX Parser | Parse Excel spreadsheets | None |
| I4 | JSON/JSONL Parser | Parse JSON event streams | None |
| I5 | Gzip Support | Transparent .gz decompression | I1, I2, I3, I4 |
| I6 | Stdin Support | Read from pipe/stdin | I1, I2, I3, I4 |

### Layer 2: Configuration (Schema Mapping)
| ID | Feature | Description | Dependencies |
|----|---------|-------------|--------------|
| C1 | Auto-Detection | Detect file format from extension | I1-I6 |
| C2 | Schema Inference | Infer column types via DuckDB | C1 |
| C3 | Column Mapping | Map columns to case_id/activity/timestamp/resource | C2 |
| C4 | Timestamp Parsing | Configure timestamp format | C3 |
| C5 | Profile Save | Save mapping as YAML profile | C3, C4 |
| C6 | Profile Load | Apply saved profile to new file | C5 |

### Layer 3: Processing (Transformations)
| ID | Feature | Description | Dependencies |
|----|---------|-------------|--------------|
| P1 | Streaming Pipeline | Zero-alloc event streaming | C3 |
| P2 | DuckDB Engine | SIMD-optimized processing | P1 |
| P3 | Anonymization | SHA-256 hash sensitive columns | P1 |
| P4 | Sampling | Reservoir/rate-based sampling | P1 |
| P5 | Filtering | SQL-like WHERE clauses | P2 |

### Layer 4: Analysis (Intelligence)
| ID | Feature | Description | Dependencies |
|----|---------|-------------|--------------|
| A1 | Quality Inspection | Completeness, uniqueness metrics | P1 |
| A2 | Statistics | Event/case/activity counts | P1 |
| A3 | PMPT Builder | Build Process Merkle Tree | P1 |
| A4 | Interval Tree | Time-based indexing | A3 |
| A5 | Semantic Diff | Compare two logs (frequencies) | A1 |
| A6 | Structural Diff | Compare PMPT fingerprints (O(1)) | A3 |
| A7 | Bottleneck Detection | Find concurrent activities | A4 |

### Layer 5: Output (Artifacts)
| ID | Feature | Description | Dependencies |
|----|---------|-------------|--------------|
| O1 | Parquet Writer | Columnar output with compression | P1 |
| O2 | Data Contract | Generate .contract.json | O1 |
| O3 | PowerBI Export | Star schema for BI tools | O1 |
| O4 | Diff Report | Process drift report | A5, A6 |

### Layer 6: Operations (Enterprise)
| ID | Feature | Description | Dependencies |
|----|---------|-------------|--------------|
| E1 | Watch Mode | Auto-convert on file changes | O1 |
| E2 | Telemetry | OpenTelemetry traces/metrics | All |
| E3 | Circuit Breaker | Memory/concurrency protection | P1 |
| E4 | Checkpointing | Resume after failure | P1 |
| E5 | Poison Pill | Skip malicious rows | P1 |

---

## User Flows (Dependency Chains)

### Flow 1: Basic Conversion (Most Common)
```
[Select File] → [Detect Format] → [Preview Data] → [Map Columns] → [Convert] → [Show Results]
     I1-I5          C1                C2              C3            P1,O1         O1
```

### Flow 2: Quality Analysis
```
[Select File] → [Detect Format] → [Map Columns] → [Inspect] → [Show Report]
     I1-I5          C1              C3              A1,A2        A1
```

### Flow 3: Log Comparison (Diff)
```
[Select Left] → [Select Right] → [Build Trees] → [Compare] → [Show Diff]
    I1-I5          I1-I5            A3,A4          A5,A6        O4
```

### Flow 4: Profile Workflow
```
[Load Profile] → [Select File] → [Apply Mapping] → [Convert] → [Results]
      C6            I1-I5            C3              P1,O1       O1
```

### Flow 5: Watch Mode (Real-Time)
```
[Select File] → [Map Columns] → [Start Watch] → [Auto-Convert Loop]
    I1-I5          C3               E1              P1,O1
```

### Flow 6: Export to BI
```
[Select Parquet] → [Choose Format] → [Export] → [Show Files]
       I1               O3              O3          O3
```

---

## UI States (Screen Sequence)

```
┌─────────────────────────────────────────────────────────────────┐
│  1. WELCOME                                                      │
│     - Select action: Convert / Inspect / Diff / Watch / Export  │
│     - Show recent files                                          │
│     - Load profile option                                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  2. FILE SELECTION                                               │
│     - File browser with format icons                             │
│     - Drag & drop zone                                           │
│     - Recent files list                                          │
│     - Format auto-detection badge                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  3. DATA PREVIEW                                                 │
│     - First 10 rows rendered as table                            │
│     - Column type indicators                                     │
│     - Schema inference results                                   │
│     - Row/column counts                                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  4. COLUMN MAPPING                                               │
│     - Dropdown for each required field                           │
│     - Auto-suggestions based on heuristics                       │
│     - Validation indicators                                      │
│     - Timestamp format selector                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  5. PROCESSING                                                   │
│     - Progress bar with percentage                               │
│     - Events/second throughput                                   │
│     - Elapsed time                                               │
│     - LIVE TELEMETRY: Current operation, memory, errors         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  6. RESULTS                                                      │
│     - Success/failure status                                     │
│     - Compression ratio                                          │
│     - Quality metrics summary                                    │
│     - Usage examples (Python, DuckDB, PM4Py)                     │
│     - Save profile option                                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Telemetry Panel (Always Visible During Processing)

```
┌─ TELEMETRY ──────────────────────────────────────────────────────┐
│                                                                   │
│  STAGE: Parsing CSV                    [████████░░] 78%          │
│                                                                   │
│  ┌─────────────────┬─────────────────┬─────────────────┐         │
│  │ EVENTS          │ THROUGHPUT      │ MEMORY          │         │
│  │ 1,234,567       │ 125,432/sec     │ 892 MB          │         │
│  └─────────────────┴─────────────────┴─────────────────┘         │
│                                                                   │
│  ┌─────────────────┬─────────────────┬─────────────────┐         │
│  │ ELAPSED         │ ETA             │ ERRORS          │         │
│  │ 00:02:34        │ 00:00:42        │ 0               │         │
│  └─────────────────┴─────────────────┴─────────────────┘         │
│                                                                   │
│  LOG:                                                             │
│  [14:32:01] Reading row batch 1-10000                            │
│  [14:32:02] Parsed 10000 events (892 cases)                      │
│  [14:32:03] Writing Parquet row group 1                          │
│  [14:32:03] Checkpoint saved: offset=10485760                    │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```
