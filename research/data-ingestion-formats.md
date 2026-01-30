# Data Ingestion Landscape: Formats, Groups, and Conversion to Analysis-Ready Output

> Research compiled 2026-01-30 for the LogPro ingestion library.

---

## 1. The Three Structural Categories of Data

All ingestible data falls into one of three structural categories:

| Category | Schema | Examples |
|---|---|---|
| **Structured** | Fixed schema, tabular | RDBMS rows, CSV, TSV, fixed-width |
| **Semi-structured** | Self-describing / flexible schema | JSON, XML, YAML, Avro, Protobuf, log lines |
| **Unstructured** | No schema | Images, PDFs, video, audio, free-form text |

Over 80% of enterprise data is now unstructured (IDC, 2025). The library's sweet spot is structured and semi-structured data — the formats that can be *schematized* into columnar output.

---

## 2. Ingestible Formats Grouped by Domain

### Group A — Flat / Tabular Files

| Format | Encoding | Notes |
|---|---|---|
| **CSV** | Text, row | Universal lowest-common-denominator. No schema, no types. |
| **TSV** | Text, row | Tab-delimited variant of CSV. |
| **Fixed-width** | Text, row | Legacy mainframe / COBOL extracts. |
| **Excel (.xlsx/.xls)** | Binary/XML | Ubiquitous in business; multiple sheets, formulas, types. |

### Group B — Structured Serialization (Row-Oriented)

| Format | Encoding | Notes |
|---|---|---|
| **JSON** | Text | Dominant in APIs and web. Nested, no enforced schema. |
| **NDJSON / JSON Lines** | Text | One JSON object per line — streamable. |
| **XML** | Text | Legacy enterprise, SOAP, config files. Verbose. |
| **YAML** | Text | Config-oriented superset of JSON. |
| **Apache Avro** | Binary | Row-oriented, schema embedded, strong schema evolution. Kafka's native format. |
| **Protocol Buffers** | Binary | Google's serialization. Foundation of gRPC. Typed, compact. Not splittable. |
| **Thrift** | Binary | Facebook's serialization. Similar role to Protobuf. |
| **MessagePack** | Binary | Compact binary JSON. |
| **CBOR** | Binary | Concise Binary Object Representation (RFC 8949). IoT-friendly. |
| **BSON** | Binary | MongoDB's wire format. |
| **Ion** | Text/Binary | Amazon's self-describing format (used in DynamoDB exports). |

### Group C — Columnar / Analytical Formats (Output Targets)

| Format | Encoding | Notes |
|---|---|---|
| **Apache Parquet** | Binary, columnar | The de facto analysis file format. Snappy/Gzip/Zstd compression. Schema embedded. |
| **Apache ORC** | Binary, columnar | Hive-optimized. ACID support. Similar to Parquet. |
| **Apache Arrow (IPC)** | Binary, columnar | In-memory format. Zero-copy interchange between engines. |
| **Apache Iceberg** | Table format *over* Parquet/ORC/Avro | Not a file format — a metadata layer that adds ACID, schema evolution, time-travel, partition pruning on top of Parquet files. |

### Group D — Log / Observability Formats

| Format | Notes |
|---|---|
| **Syslog (RFC 5424)** | OS-level structured log standard. |
| **Common Log Format (CLF)** | Apache/nginx access logs. |
| **Combined Log Format** | CLF + referrer + user-agent. |
| **OTLP (OpenTelemetry Protocol)** | Unified wire format for metrics, traces, and logs. Protobuf-based. |
| **Prometheus exposition** | Text-based metrics scrape format. |
| **StatsD** | Simple UDP metric protocol. |
| **Windows Event Log (EVTX)** | Binary, OS-specific. |
| **Journald** | Systemd structured log format. |

### Group E — Streaming / Event Protocols

| Source | Wire Format | Notes |
|---|---|---|
| **Apache Kafka** | Avro, Protobuf, JSON, raw bytes | Distributed commit-log. Schema Registry for Avro/Protobuf. |
| **MQTT** | Binary payload (arbitrary) | Lightweight pub/sub for IoT. |
| **AMQP (RabbitMQ)** | Binary framing | Enterprise message queue. |
| **NATS / JetStream** | Binary | Cloud-native messaging. |
| **Amazon Kinesis** | JSON / binary blobs | AWS-managed streaming. |
| **Google Pub/Sub** | JSON / binary | GCP-managed streaming. |
| **Azure Event Hubs** | AMQP / Kafka-compatible | Azure-managed streaming. |
| **Webhooks** | JSON over HTTP POST | Push-based event delivery. |
| **WebSockets** | JSON / binary frames | Bidirectional real-time. |
| **Server-Sent Events** | Text stream | Unidirectional server push. |

### Group F — Database Extraction (CDC & Bulk)

| Source | Method | Notes |
|---|---|---|
| **PostgreSQL** | WAL logical replication / `COPY` | Debezium connector emits Avro/JSON change events. |
| **MySQL** | Binlog CDC / `SELECT INTO OUTFILE` | Row-based binlog is most reliable for CDC. |
| **SQL Server** | CT/CDC features / BCP | Built-in change tracking. |
| **MongoDB** | Change Streams / `mongodump` | Oplog-based. Emits BSON documents. |
| **DynamoDB** | DynamoDB Streams / S3 export (Ion) | Event-driven or bulk export. |
| **Redis** | Keyspace notifications / RDB dump | Pub/sub for change events. |
| **Generic RDBMS** | JDBC / ODBC polling | Query-based CDC (timestamp/version column). |

### Group G — File Transfer / Object Store Sources

| Source | Notes |
|---|---|
| **S3 / GCS / Azure Blob** | Object-store poll or event notification (S3 Events -> SQS/SNS). |
| **FTP / SFTP** | Legacy batch file drops. |
| **Local filesystem / NFS** | Directory watch / inotify. |
| **HTTP(S) download** | One-shot or paginated fetch. |

### Group H — Geospatial Formats

| Format | Notes |
|---|---|
| **GeoJSON** | JSON with geometry objects. Web-friendly. |
| **Shapefile (.shp)** | ESRI legacy. Multi-file. |
| **GeoParquet** | Parquet with geospatial metadata. Emerging standard. |
| **GeoTIFF / BigTIFF** | Raster imagery with coordinate reference. |
| **GRIB / GRIB2** | Weather/atmospheric gridded data. |
| **KML / KMZ** | Google Earth markup. XML-based. |
| **WKT / WKB** | Well-Known Text/Binary geometry encoding. |

### Group I — Scientific / Domain-Specific Formats

| Format | Domain | Notes |
|---|---|---|
| **HDF5** | General science, NASA, genomics | Hierarchical, self-describing, handles petabyte-scale. |
| **NetCDF (v3/v4)** | Climate, oceanography, meteorology | Array-oriented. v4 is HDF5 under the hood. |
| **FASTA** | Genomics | Reference sequences. Text. |
| **FASTQ** | Genomics | Raw sequencing reads + quality scores. |
| **BAM/SAM** | Genomics | Aligned reads. BAM is compressed binary. |
| **VCF** | Genomics | Variant calls (SNPs, indels). |
| **FITS** | Astronomy | Flexible Image Transport System. |
| **ROOT** | Particle physics | CERN's analysis format. |
| **Zarr** | Array science (ML, climate) | Chunked, compressed N-dimensional arrays. Cloud-native. |

---

## 3. Practical Connector Bundles for the Library

For a pluggable ingestion library, connectors should be organized into these 7 bundles:

```
1. FLAT-FILE       CSV, TSV, fixed-width, Excel
2. STRUCTURED-SER  JSON, NDJSON, XML, YAML, Avro, Protobuf, MsgPack, CBOR, BSON, Ion
3. LOG-OBSERVE     Syslog, CLF, OTLP, Prometheus, StatsD, journald
4. STREAM-EVENT    Kafka, MQTT, AMQP, NATS, Kinesis, Pub/Sub, Webhooks, WebSocket, SSE
5. DATABASE-CDC    Postgres WAL, MySQL binlog, Mongo change streams, JDBC poll, DynamoDB Streams
6. OBJECT-STORE    S3, GCS, Azure Blob, FTP/SFTP, local FS (with file-type detection -> delegates to 1-3)
7. DOMAIN-SPECIFIC GeoJSON/GeoParquet, HDF5/NetCDF, FASTA/FASTQ/BAM/VCF (each behind a feature flag)
```

---

## 4. Conversion Pipeline: Any Format -> Analysis-Ready Parquet/Iceberg

```
+--------------+    +---------------+    +---------------+    +----------------+
|  RAW SOURCE  |--->|  DECODER      |--->|  ARROW        |--->|  WRITER        |
|  (any fmt)   |    |  (format-     |    |  RECORD       |    |  (Parquet +    |
|              |    |   specific)   |    |  BATCHES      |    |   Iceberg      |
+--------------+    +---------------+    +---------------+    |   metadata)    |
                                                              +----------------+
```

### Pipeline Stages

| Stage | What Happens | Go Libraries |
|---|---|---|
| **Decode** | Format-specific parser converts raw bytes -> typed rows/records | `encoding/csv`, `encoding/json`, `encoding/xml`, custom Avro/Protobuf decoders |
| **Normalize** | Map to a unified in-memory representation | **Apache Arrow Go** (`github.com/apache/arrow-go`) — the canonical intermediate representation |
| **Write Parquet** | Arrow record batches -> Parquet files with compression | `github.com/apache/arrow-go/parquet` or `github.com/xitongsys/parquet-go` |
| **Write Iceberg** | Commit Parquet files into Iceberg table with catalog updates | `github.com/apache/iceberg-go` (official) or `github.com/BrobridgeOrg/go-iceberg` (CRUD-oriented) |

**Critical design insight:** Arrow is the universal pivot format. Every decoder outputs Arrow record batches, and the Parquet/Iceberg writer consumes Arrow record batches. This fully decouples input formats from output formats.

### What Iceberg Adds Over Raw Parquet

| Capability | Parquet Alone | Parquet + Iceberg |
|---|---|---|
| ACID transactions | No | Yes — concurrent writers don't corrupt the table |
| Schema evolution | No (must rewrite) | Yes — add/rename/drop columns without rewriting |
| Time travel | No | Yes — query the table as of any previous snapshot |
| Partition evolution | No (must rewrite) | Yes — change partitioning without rewriting |
| Catalog integration | No | Yes — REST, Hive, Glue, Nessie, Polaris catalogs |
| File compaction | Manual | Built-in maintenance operations |

### Parquet Best Practices

- Target file sizes between **512 MB and 1 GB** for optimal query performance.
- Use **Snappy** compression for balanced speed/ratio or **Zstd** for higher compression.
- Row group size of **128 MB** is a common default.
- Enable dictionary encoding for low-cardinality string columns.

---

## 5. Priority Ranking for Implementation

Based on real-world demand:

| Priority | Formats | Rationale |
|---|---|---|
| **P0 (Core)** | CSV, JSON/NDJSON, Avro, Protobuf, Parquet (read) | Covers 80%+ of ingestion workloads |
| **P1 (High)** | Kafka consumer, Postgres CDC, MySQL CDC, S3/GCS reader | Streaming + database = the two biggest enterprise sources |
| **P2 (Medium)** | Excel, XML, Syslog, OTLP, MQTT, MongoDB change streams | Common but less universal |
| **P3 (Low)** | GeoJSON/GeoParquet, HDF5/NetCDF, genomic formats, FITS | Domain-specific; implement behind feature flags |

---

## 6. Format Selection Quick Reference

### When to Use What (Input)

| If Your Data Is... | Use |
|---|---|
| A flat export or spreadsheet | CSV / TSV / Excel reader |
| From a REST API or web app | JSON / NDJSON decoder |
| On a Kafka topic | Kafka consumer + Avro/Protobuf/JSON decoder |
| A database that needs live sync | CDC connector (Postgres WAL, MySQL binlog) |
| From IoT sensors or devices | MQTT consumer |
| Application/system logs | Syslog / CLF / OTLP parser |
| Sitting in cloud storage | S3/GCS reader + auto-detect format |
| Scientific/research data | HDF5 / NetCDF / domain-specific reader |

### Why Parquet + Iceberg as Output

| Requirement | How It's Met |
|---|---|
| Fast analytical queries | Columnar layout, predicate pushdown, min/max statistics |
| Storage efficiency | Compression (Snappy/Zstd), encoding (dictionary, RLE, delta) |
| Schema flexibility | Iceberg schema evolution without data rewrite |
| Data governance | Iceberg snapshots, audit trail, time travel |
| Engine compatibility | Spark, Trino, DuckDB, Dremio, Snowflake, BigQuery all read Iceberg |
| Open format | No vendor lock-in; Apache-licensed |

---

## Sources

- [Structured vs Unstructured Data Guide 2025 - BizData360](https://www.bizdata360.com/structured-vs-unstructured-data-comprehensive-guide-2025/)
- [Data Ingestion Explained (2026 Guide) - Skyvia](https://skyvia.com/learn/what-is-data-ingestion)
- [Understanding Data Ingestion - DataCamp](https://www.datacamp.com/blog/data-ingestion)
- [Data File Formats for Data Engineering - Pierre Munhoz (2026)](https://medium.com/@pierre.munhoz/data-file-formats-for-data-engineering-csv-json-parquet-avro-and-more-cec8a8970904)
- [Different File Formats: Avro vs Parquet vs JSON vs XML vs Protobuf vs ORC](https://baisali-pradhan.medium.com/different-file-format-avro-vs-parquet-vs-json-vs-xml-vs-protobuf-vs-orc-5d06867e4c4f)
- [Parquet Data Format Pros and Cons 2025 - Edge Delta](https://edgedelta.com/company/blog/parquet-data-format)
- [Scaling Data Lakes: Raw Parquet to Iceberg - Dremio](https://www.dremio.com/blog/scaling-data-lakes-moving-from-raw-parquet-to-iceberg-lakehouses/)
- [Evolving the Data Lake: CSV/JSON to Parquet to Iceberg - Dremio](https://www.dremio.com/blog/evolving-the-data-lake-from-csv-json-to-parquet-to-apache-iceberg/)
- [2025/2026 Ultimate Guide to the Data Lakehouse](https://dev.to/alexmercedcoder/the-2025-2026-ultimate-guide-to-the-data-lakehouse-and-the-data-lakehouse-ecosystem-dig)
- [Parquet vs Iceberg - OLake](https://olake.io/blog/iceberg-vs-parquet-table-format-vs-file-format/)
- [CDC with Apache Iceberg - Dremio](https://www.dremio.com/blog/cdc-with-apache-iceberg/)
- [Kafka CDC Guide - Confluent](https://developer.confluent.io/courses/data-pipelines/kafka-data-ingestion-with-cdc/)
- [CDC Fundamentals - Redpanda](https://www.redpanda.com/guides/fundamentals-of-data-engineering-cdc-change-data-capture)
- [OpenTelemetry Logging Spec](https://opentelemetry.io/docs/specs/otel/logs/)
- [OpenTelemetry Logs Data Model](https://opentelemetry.io/docs/specs/otel/logs/data-model/)
- [OTLP Ingestion - Grafana](https://grafana.com/docs/opentelemetry/ingest/)
- [Data Ingestion Architecture Guide 2025 - Rivery](https://rivery.io/data-learning-center/data-ingestion-architecture-guide/)
- [Data Ingestion Methods - AWS](https://docs.aws.amazon.com/whitepapers/latest/building-data-lakes/data-ingestion-methods.html)
- [apache/iceberg-go - GitHub](https://github.com/apache/iceberg-go)
- [BrobridgeOrg/go-iceberg - GitHub](https://github.com/BrobridgeOrg/go-iceberg)
- [xitongsys/parquet-go - GitHub](https://github.com/xitongsys/parquet-go)
- [Bioinformatic File Formats Handbook](https://eriqande.github.io/eca-bioinf-handbook/bioinformatic-file-formats.html)
- [NetCDF-4/HDF5 - NASA Earthdata](https://www.earthdata.nasa.gov/about/esdis/esco/standards-practices/netcdf-4hdf5)
- [Geospatial Data Types - Geoapify](https://www.geoapify.com/different-geospatial-data-types/)
- [Data Encoding and Formats - Firas Esbai](https://www.firasesbai.com/articles/2025/05/18/data-encoding-and-formats.html)
