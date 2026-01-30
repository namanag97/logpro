Now I have the full picture. Here's the actual wiring:

  CLI COMMANDS AND WHAT THEY ACTUALLY CALL
  =========================================

  logflow convert -i f.csv -o f.parquet
    └─→ main.go:runConvert()
         ├─ if CSV && !stdin: runDuckDBConvert()
         │    └─→ pipe.NewDuckDBPipeline()          ← internal/pipe/, DuckDB SQL
         └─ else: runArrowConvert()
              └─→ pipe.NewPipeline()                 ← internal/pipe/, parser→chan Event→writer

  logflow ingest data.csv
    └─→ ingest_cmd.go:runIngest()
         └─→ ingest.NewEngine()                      ← pkg/ingest/, FastPath/RobustPath
              ├─ FastPath  → DuckDB SQL
              └─ RobustPath → manual Arrow builders

  logflow batch *.csv -o ./out/
    └─→ commands.go:runBatch()→convertFile()
         └─→ pipe.NewPipeline()                      ← internal/pipe/, parser→chan Event→writer

  logflow watch f.csv f.parquet
    └─→ commands.go:runWatch()
         └─→ parser.Parse() → chan Event → writer    ← direct, no pipeline wrapper

  logflow diff / inspect / sample / anonymize / stats
    └─→ commands.go:run*()
         └─→ parser.Parse() → chan Event → ...       ← direct

  pipeline_runner.go (PipelineRunner)
    └─→ pipeline.NewOrchestrator()                   ← pkg/pipeline/, registry-based
         └─→ registry.GetSourceForFile()
         └─→ registry.GetSinkForFile()
         └─→ NOT CALLED BY ANY CLI COMMAND

  pkg/ingest/streaming.go (StreamingPipeline)
    └─→ Source → Decoder → chan DecodedBatch (Arrow Record) → Hooks → Sink
         └─→ NOT CALLED BY ANY CLI COMMAND (only test_e2e.go)


  FOUR PIPELINE SYSTEMS
  =====================

  1. internal/pipe/          parser → chan *model.Event → writer
     USED BY: convert (Arrow mode), batch, watch, diff, inspect, sample, anonymize

  2. internal/pipe/ (DuckDB)  DuckDB SQL COPY
     USED BY: convert (DuckDB mode, default for CSV)

  3. pkg/ingest/             Engine → FastPath (DuckDB) | RobustPath (Arrow builders)
     USED BY: ingest command only

  4. pkg/ingest/streaming.go Source → Decoder → chan arrow.Record → Sink
     USED BY: nothing (only test_e2e.go)

  5. pkg/pipeline/           Orchestrator + registry + processors
     USED BY: nothing (pipeline_runner.go exists but no CLI calls it)


  DEPENDENCY MAP (what imports what)
  ==================================

  cmd/logflow/main.go
    ├── internal/pipe           ✓ used (convert, wizard)
    ├── pkg/parser              ✓ used (format detection)
    ├── pkg/writer              ✓ used (DuckDB CSV reader for schema cmd)
    ├── pkg/tui                 ✓ used (wizard, progress)
    ├── pkg/profile             ✓ used (apply, profiles)
    ├── pkg/contract            ✓ used (data contracts)
    └── pkg/util                ✓ used (file opening)

  cmd/logflow/commands.go
    ├── internal/pipe           ✓ used (batch)
    ├── pkg/parser              ✓ used
    ├── pkg/writer              ✓ used
    ├── pkg/diff                ✓ used
    ├── pkg/export              ✓ used
    ├── pkg/inspect             ✓ used
    ├── pkg/transform           ✓ used
    ├── pkg/pmpt                ✓ used
    ├── pkg/watch               ✓ used
    └── pkg/util                ✓ used

  cmd/logflow/ingest_cmd.go
    └── pkg/ingest              ✓ used (ingest command)

  cmd/logflow/pipeline_runner.go
    ├── pkg/pipeline            ✗ NOT reachable from any CLI command
    ├── pkg/processors          ✗ NOT reachable
    └── pkg/registry            ✗ NOT reachable


  PACKAGES REACHABLE FROM CLI
  ============================
  ✓ internal/pipe, internal/model, internal/pool
  ✓ pkg/parser (+ healing, csv_fsm)
  ✓ pkg/writer
  ✓ pkg/tui
  ✓ pkg/profile
  ✓ pkg/contract
  ✓ pkg/util
  ✓ pkg/diff
  ✓ pkg/export
  ✓ pkg/inspect
  ✓ pkg/transform
  ✓ pkg/pmpt
  ✓ pkg/watch
  ✓ pkg/config
  ✓ pkg/ingest (Engine, FastPath, RobustPath, Detector, Heuristics)

  PACKAGES NOT REACHABLE FROM ANY CLI COMMAND
  =============================================
  ✗ pkg/ingest/streaming.go     (Arrow streaming pipeline)
  ✗ pkg/ingest/core/            (Source, Decoder, Sink interfaces)
  ✗ pkg/ingest/decoders/        (CSV, JSON, XES Arrow decoders)
  ✗ pkg/ingest/sinks/           (Parquet, Arrow IPC, Iceberg sinks)
  ✗ pkg/ingest/sources/         (File, HTTP, Memory, Stream sources)
  ✗ pkg/ingest/hooks/           (batch transform hooks)
  ✗ pkg/ingest/schema/          (inference, evolution, OCEL, policy)
  ✗ pkg/ingest/flow/            (backpressure, rate limiter)
  ✗ pkg/ingest/errors/          (policy, quarantine, stream errors)
  ✗ pkg/ingest/quality/         (cardinality, checksum, entropy)
  ✗ pkg/pipeline/               (orchestrator, orchestrator_v2, enterprise)
  ✗ pkg/processors/             (filter, sample, anonymize, quality)
  ✗ pkg/registry/               (source/sink/processor factory)
  ✗ pkg/adapters/               (CSV source, Parquet sink)
  ✗ pkg/ocel/                   (model, store, discovery, parquet_query)
  ✗ pkg/schema/                 (cache, policy)
  ✗ pkg/sinks/                  (iceberg)
  ✗ pkg/state/                  (store)
  ✗ pkg/hooks/                  (hooks)
  ✗ pkg/index/                  (bitmap)
  ✗ pkg/heuristic/              (optimizer)
  ✗ pkg/quality/                (validator)
  ✗ pkg/turbo/                  (converter)
  ✗ pkg/perf/                   (profiler)
  ✗ pkg/runtime/                (memory)
  ✗ pkg/validation/             (input, quality rules)
  ✗ pkg/compaction/             (auto, compactor)
  ✗ pkg/resilience/             (circuit breaker)
  ✗ pkg/lifecycle/              (shutdown)
  ✗ pkg/telemetry/              (otel, exporter)
  ✗ pkg/testing/                (generators, roundtrip)
  ✗ pkg/api/rest/               (server)
  ✗ pkg/server/                 (server, sse, storage)
  ✗ pkg/storage/                (cloud, s3, catalog, maintenance, object, table)
  ✗ pkg/checkpoint/             (checkpoint, redis, s3, backends)
  ✗ pkg/query/                  (cache, engine)
  ✗ pkg/defaults/               (alerting, auth, metrics, scheduler, tracer)
  ✗ pkg/interfaces/             (alerter, auth, catalog, metrics, scheduler, storage, tracer)

  That's it. System 1 (internal/pipe/ + pkg/parser/ + pkg/writer/) runs the CLI. System 4 (pkg/ingest/streaming.go + pkg/ingest/core/) is the Arrow-native architecture you designed. Everything in between is disconnected scaffolding. The convert command even names
   its two paths explicitly: runDuckDBConvert() and runArrowConvert() — but "Arrow" here means System 1 (internal/pipe/), not the pkg/ingest/core/ interfaces.


---
  Bug 1: convert command uses DuckDB for ALL CSVs, even when you don't ask for it

  main.go:231: if useDuckDB || (format == parser.FormatCSV && !isStdin) — the Arrow path is unreachable for CSV. The verbose output says "Engine: Arrow" then immediately "Processing with DuckDB engine..." That's a lie in the output and dead code in the Arrow path
   for CSV.

  Bug 2: convert with JSON produces 0 events

  Input: 2 JSON records. Output: Events: 0. No error. Silent data loss. The Arrow-based JSON pipeline (pipe.NewPipeline) loses all records and reports success.

  Bug 3: ingest crashes on JSON — DuckDB json extension not loaded

  INSTALL json; LOAD json; is never called. The FastPath opens DuckDB with sql.Open("duckdb", "") and immediately calls read_json_auto which doesn't exist without the extension. JSON files always fail through ingest.

  Bug 4: Embedded newlines in CSV break convert completely

  The messy CSV test showed: convert parsed the LAST LINE as the header. Error: case_id column "id" not found (available: [7 Grace 700 2025-01-06 extra_col]). DuckDB's parser mishandled the quoted newline and treated row 7 as the header row. Meanwhile ingest
  --strategy robust handled it correctly (7 rows, 2 recovered).

  Bug 5: batch, inspect, diff don't accept column mapping flags

  These commands hardcode case:concept:name, concept:name, time:timestamp from parser.DefaultConfig(). If your CSV doesn't have those exact column names, they fail. The batch command failed on all 4 example files with "required column missing." There's no
  --case-id flag on batch, inspect, or diff.

  Bug 6: Empty file produces unhelpful DuckDB error

  Both convert and ingest on a 0-byte CSV produce: CSV options could not be auto-detected. Consider setting parser options manually. — a DuckDB internal error leaking to the user.

  Bug 7: .jsonl extension not recognized

  detectFormat() in main.go has no case for .jsonl or .ndjson. Files with these extensions fail with "unable to detect input format."

  What works:
  - ingest on clean CSV → DuckDB fast path → correct Parquet (180K rows, 337K rows/sec, 7.3x compression)
  - ingest on messy CSV → robust path → correct output with recovery
  - convert on clean CSV with correct column flags → works
  - Headers-only CSV → both paths produce 0-row Parquet correctly

✻ Worked for 2m 58s

--
  The real answer: it depends on which ERP, and there is no single "just use an API" path.

  SAP ECC (the most common SAP in the wild):
  There is no REST API. No OData. The only way to get data out is RFC — a proprietary binary protocol from 1995. This is why Celonis installs a https://docs.celonis.com/en/rfc-module-overview.html plus an https://docs.celonis.com/en/on-prem-clients.html at the
  customer's site. The extractor runs SQL via RFC, dumps CSVs to SAP's filesystem, polls for them, converts to Parquet, and pushes them to the Celonis cloud. There is no HTTP endpoint on SAP ECC to call. RFC_READ_TABLE is
  https://onapsis.com/blog/sap-rfc-read-table-accessing-arbitrary-tables/ and has a 512-byte row limit.

  SAP S/4HANA (newer, some customers have it):
  This one HAS REST APIs. https://api.sap.com/products/SAPS4HANA/apis/ODATAV4 exist. You can call GET /sap/opu/odata/... over HTTPS and get JSON back. SAP is https://theobald-software.com/en/blog/sap-note-325574. This is the API path you're thinking of — it
  works, but only for S/4HANA, not ECC.

  Oracle Fusion Cloud:
  Uses https://docs.celonis.com/en/connecting-to-oracle-fusion-cloud-bicc.html. BICC extracts data from Public View Objects and dumps CSVs to OCI Object Storage or UCM. It's not a real-time API — it's a batch extraction to a storage bucket. You then pick up the
  files.

  Oracle EBS (on-premise):
  Direct database connection. SQL over SQL*Net. No REST API.

  ---
  So the realistic integration paths for LogFlow are:
  ┌────────────────────┬─────────────────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │     ERP System     │     How data actually gets out      │                                                             What LogFlow needs to support                                                             │
  ├────────────────────┼─────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ SAP ECC            │ RFC (binary, no HTTP)               │ Either build an on-prem extractor agent, or accept that customer uses middleware (MuleSoft, SAP PI/PO) to bridge RFC → HTTP and push to LogFlow's API │
  ├────────────────────┼─────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ SAP S/4HANA        │ OData REST (HTTPS + JSON)           │ LogFlow could consume OData directly, OR customer's middleware pushes to LogFlow                                                                      │
  ├────────────────────┼─────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Oracle Fusion      │ BICC → CSV files in OCI bucket      │ LogFlow reads from S3-compatible bucket, or customer pushes files                                                                                     │
  ├────────────────────┼─────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Oracle EBS         │ SQL*Net (database wire protocol)    │ Customer runs SQL export, pushes result to LogFlow's API                                                                                              │
  ├────────────────────┼─────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Any via middleware │ MuleSoft / Boomi / SAP PI/PO → HTTP │ LogFlow exposes an ingestion API that receives JSON                                                                                                   │
  └────────────────────┴─────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
  The bottom line from the research: There is no universal "connect via API" because half these systems don't have APIs. Celonis solved this by building an on-premise agent that bridges the gap. The alternative is to require the customer to use middleware to
  handle the ERP-specific extraction and push to your API.

  The question for you is: does LogFlow want to build extraction agents (like Celonis), or does LogFlow stay API-first and let the customer/middleware handle extraction?

  Sources:
  - https://docs.celonis.com/en/rfc-module-overview.html
  - https://docs.celonis.com/en/pipeline-end-to-end-overview.html
  - https://docs.celonis.com/en/on-prem-clients.html
  - https://developer.celonis.com/data-ingestion-api/getting-started/
  - https://api.sap.com/products/SAPS4HANA/apis/ODATAV4
  - https://theobald-software.com/en/blog/sap-note-325574
  - https://onapsis.com/blog/sap-rfc-read-table-accessing-arbitrary-tables/
  - https://aws.amazon.com/blogs/awsforsap/architecture-options-for-extracting-sap-data-with-aws-services/
  - https://docs.celonis.com/en/connecting-to-oracle-fusion-cloud-bicc.html

  Here's what the research actually shows about Celonis's end-to-end architecture. One correction first: Celonis does NOT host everything on the customer's network. They deprecated that model (CPM4) and moved everything to cloud. Here's the real topology:

    CUSTOMER'S NETWORK                    CELONIS CLOUD (Azure/AWS)
    ┌─────────────────────┐               ┌──────────────────────────────────┐
    │                     │               │                                  │
    │  SAP ECC / S4HANA   │               │  ┌──────────────────────────┐    │
    │  Oracle EBS         │               │  │ DATA CORE                │    │
    │  Salesforce         │               │  │ - Ingestion              │    │
    │         │           │               │  │ - In-memory DB           │    │
    │         ▼           │   outbound    │  │ - Process Query Engine   │    │
    │  ┌──────────────┐   │   HTTPS only  │  │   (PQL, 52x faster      │    │
    │  │ On-Prem      │───┼──────────────→│  │    than SQL)             │    │
    │  │ Client (OPC) │   │   polls every │  └──────────┬───────────────┘    │
    │  │              │←──┼───────────────│             │                    │
    │  │ (no logic,   │   │   job queue   │  ┌──────────▼───────────────┐    │
    │  │  just relay) │   │              │  │ PROCESS INTELLIGENCE     │    │
    │  └──────────────┘   │              │  │ GRAPH                    │    │
    │                     │              │  │ - Digital twin           │    │
    │  No inbound ports   │              │  │ - KPIs, rules, context   │    │
    │  needed             │              │  └──────────┬───────────────┘    │
    │                     │              │             │                    │
    └─────────────────────┘              │  ┌──────────▼───────────────┐    │
                                         │  │ PROCESS MINING           │    │
                                         │  │ - Visualization          │    │
                                         │  │ - Conformance checking   │    │
                                         │  │ - Root cause analysis    │    │
                                         │  └──────────┬───────────────┘    │
                                         │             │                    │
                                         │  ┌──────────▼───────────────┐    │
                                         │  │ ACTION ENGINE            │    │
                                         │  │ - Alerts to users        │    │
                                         │  │ - API calls back to SAP  │    │
                                         │  │ - RPA triggers           │    │
                                         │  │ - Automation flows       │    │
                                         │  └──────────────────────────┘    │
                                         │                                  │
                                         │  Studio, Execution Apps, UI      │
                                         └──────────────────────────────────┘

    The on-prem client is just a dumb relay — it has no logic. It polls the cloud for jobs, executes RFC/SQL locally, and pushes results back up. All processing, mining, analysis, and visualization happens in the cloud.

    ---
    Now, your architectural point about LogFlow:

    LogFlow (this repo) = the engine. The fastest way to take any messy input and produce clean Parquet with proper schema, OCEL enrichment, and quality validation. That's it. It's a library and a service, not a product.

    The product layer sits on top. That's a separate codebase that does:
    - The on-prem agent (data extraction bridge)
    - The cloud platform (data pool, query engine, mining algorithms)
    - The action layer (alerts, automation, API calls back to source systems)
    - The UI (dashboards, process maps, KPIs)

    LogFlow is the Data Core's conversion engine — the thing that turns raw extracted data into queryable Parquet at 911K events/sec. The product layer orchestrates everything else around it.

    Is that the split you're thinking about?

    Sources:
    - https://www.celonis.com/platform/process-mining/
    - https://www.celonis.com/blog/celonis-data-core-get-data-into-the-leading-process-intelligence-platform-more-easily-and-process-and-query-data-at-unparalleled-speed-and-scale
    - https://docs.celonis.com/en/it-architecture-overview.html
    - https://docs.celonis.com/en/on-prem-clients.html
    - https://assets.ctfassets.net/zmrtlfup12q3/1SJA7QMqkJMuySyzyD1V2p/c3772a04af61d9f1c4c3d9b927b87fc5/Celonis_Product_Description_Execution_Management_System_June2021.pdf
    - https://docs.celonis.com/en/cloud-operation.html
    - https://community.celonis.com/general-q-a-10/celonis-on-prem-enviroment-4787
