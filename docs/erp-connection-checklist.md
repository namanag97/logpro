# ERP Data Integration Checklists

## Architecture

LogFlow does NOT connect to SAP/Oracle. The customer extracts data on their side and sends it to LogFlow's API.

```
┌──────────────────────┐         ┌──────────────────────┐
│    CUSTOMER SIDE     │         │    LOGFLOW SIDE      │
│                      │  HTTP   │                      │
│  SAP / Oracle / etc  │────────→│  POST /api/upload    │
│  ↓                   │  file   │  ↓                   │
│  Extract to CSV/JSON │         │  Infer schema        │
│  (their problem)     │         │  Convert to Parquet  │
│                      │         │  OCEL enrichment     │
│                      │         │  Quality analysis    │
└──────────────────────┘         └──────────────────────┘
```

The checklists below cover what the CUSTOMER must do on their side to extract data, and what LOGFLOW must support on its API to receive it.

---

## Part 1: Customer-Side Checklists (Their Responsibility)

### SAP Extraction — Business

- [ ] Identify target processes (P2P, O2C, R2R) — each requires different tables
- [ ] Get executive sponsor — SAP Basis/Security won't act without management approval
- [ ] Sign NDA / data processing agreement — GDPR, SOX compliance for production data
- [ ] Clarify scope — which company codes, plants, org units
- [ ] Identify SAP landscape — ECC 6.0, S/4HANA, BW? On-prem or cloud (BTP)?
- [ ] Get SAP Basis team contact — they own SM59, user creation, transports, firewall
- [ ] Get SAP Security team contact — they create roles and authorization profiles
- [ ] Determine extraction frequency — one-time historical vs. daily delta
- [ ] Define anonymization requirements — can PII leave SAP?

### SAP Extraction — Technical

- [ ] Create dedicated System user in SU01 (never dialog, never SAP_ALL)
- [ ] Assign minimum auth objects:
  - `S_RFC` — Activity 16, RFC_TYPE=FUGR, restricted function groups
  - `S_TABU_DIS` — Activity 03 (Display only)
  - `S_TABU_NAM` — (Optional) individual table-level control
  - `S_BTCH_JOB` — if scheduling background extraction
- [ ] Configure RFC destination in SM59 — test connection
- [ ] Open firewall ports — SAP gateway 33xx, SAP Router 3299 if needed
- [ ] Verify target tables readable:
  - P2P: EKKO, EKPO, EBAN, EKBE, RSEG, RBKP, BSEG, BKPF
  - O2C: VBAK, VBAP, VBFA, BKPF
  - Metadata: DD02L, DD03L
- [ ] Test with SE37 — RFC_READ_TABLE against DD02L, use ST01 trace
- [ ] Export to CSV/JSON and deliver to LogFlow API

### Oracle Extraction — Business

- [ ] Identify variant — EBS (on-prem), Fusion Cloud, Autonomous DB
- [ ] Get DBA team contact — they create schemas and grant SELECT
- [ ] Executive approval for production read-only access
- [ ] Clarify modules — AP, AR, GL, PO, OM, INV
- [ ] Define data privacy requirements
- [ ] Determine extraction frequency
- [ ] Check licensing — Read-Only User licenses are a separate SKU

### Oracle EBS Extraction — Technical

- [ ] Create read-only database schema (never connect as APPS)
- [ ] Grant minimum: CREATE SESSION + SELECT ON specific apps.* tables
- [ ] Key tables:
  - Orders: OE_ORDER_HEADERS_ALL, OE_ORDER_LINES_ALL
  - AP: AP_INVOICES_ALL, AP_INVOICE_LINES_ALL
  - PO: PO_HEADERS_ALL, PO_LINES_ALL
  - GL: GL_JE_HEADERS, GL_JE_LINES
  - Workflow: WF_ITEM_ACTIVITY_STATUSES
- [ ] Set up SQL*Net — TNS entry, open port 1521, test with tnsping
- [ ] Export to CSV and deliver to LogFlow API

### Oracle Fusion Cloud Extraction — Technical

- [ ] Provision BICC user with ESSAdmin + ORA_ASM_APPLICATION_IMPLEMENTATION_ADMIN_ABSTRACT
- [ ] Configure external storage (UCM or OCI Object Storage)
- [ ] Set up BICC offerings per module, select only needed PVOs/columns
- [ ] Apply extraction filters (org unit, date range)
- [ ] Schedule incremental extracts during off-peak hours
- [ ] Export CSVs from storage and deliver to LogFlow API

---

## Part 2: LogFlow-Side Checklist (Our Responsibility)

### What Already Works

- [x] `POST /api/upload` — multipart file upload (500MB max)
- [x] `POST /api/convert` — async conversion to Parquet with SSE progress
- [x] `GET /api/schema` — auto-detect schema from uploaded file
- [x] `POST /api/analyze` — quality + process mining plugins
- [x] `GET /api/download/:id` — download converted Parquet
- [x] `GET /api/events` — SSE real-time progress stream
- [x] Format auto-detection — CSV, JSON, JSONL, XES, XLSX, Parquet, Access Log
- [x] Type inference — int/float/bool/timestamp/string with majority voting
- [x] OCEL 2.0 model — full E, O, EA, OA, E2O, O2O with qualifiers
- [x] OCEL enrichment — EnrichBatchWithOCEL from _id/_type hint columns
- [x] DuckDB fast path — 911K+ events/sec for clean files
- [x] Profile system — saved column mappings as YAML
- [x] S3 storage — multipart upload, checkpointing, resume

### What's Missing for Multi-Source Unification

- [ ] **Mapping endpoint** — API for user to map source columns to unified schema
  - `POST /api/sources` — register a source with its file + mapping config
  - `GET /api/sources/:id/suggest` — return mapping suggestions (column name heuristics + value distribution)
  - `PUT /api/sources/:id/mapping` — user confirms/edits the mapping
- [ ] **Manifest / project concept** — group multiple sources into one "project"
  - `POST /api/projects` — create a project with a golden schema definition
  - `POST /api/projects/:id/sources` — add sources to a project
  - `POST /api/projects/:id/unify` — trigger the join + OCEL output
- [ ] **Entity resolution endpoint** — specify join keys across sources
  - Which columns from Source A match Source B
  - Support for exact match, lookup table, and transform-then-match
- [ ] **Provenance tracking** — tag each event with its source origin after merging
- [ ] **Upload size increase** — 500MB may be too small for full ERP extracts
- [ ] **Bulk/streaming upload** — chunked transfer or S3 pre-signed URL for large files
- [ ] **Auth / API keys** — current API has no authentication (CORS is wide open)
- [ ] **Webhook callback** — notify customer system when conversion/unification completes

### SAP vs Oracle: What LogFlow Receives Is The Same

LogFlow doesn't care whether data came from SAP or Oracle. Both arrive as CSV or JSON. The difference is entirely on the customer's extraction side:

| Aspect | SAP | Oracle |
|---|---|---|
| Extraction protocol | RFC (binary) | SQL*Net / BICC REST |
| Who extracts | Customer's SAP team | Customer's DBA/Apps team |
| Output format | CSV (via RFC_READ_TABLE) | CSV (via BICC or SQL export) |
| What LogFlow sees | CSV file via /api/upload | CSV file via /api/upload |
| Column names | German abbreviations (VBELN, AEDAT, MATNR) | English (ORDER_ID, CREATION_DATE) |
| Key challenge | Mapping cryptic SAP field names | Mapping across EBS module schemas |
