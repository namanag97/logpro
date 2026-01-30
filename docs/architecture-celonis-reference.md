# Architecture Reference: Celonis End-to-End vs LogFlow

## Celonis Topology (as of 2025)

Everything runs in the CLOUD except a lightweight extraction agent. Celonis deprecated their fully on-prem product (CPM4).

### Customer's Network
- On-Prem Client (OPC): dumb relay, no logic, polls cloud every ~7 sec
- Connects to SAP via RFC (JCo), Oracle via JDBC, Salesforce via API
- All connections OUTBOUND only — no inbound firewall ports needed
- Fetches data, converts to Parquet, pushes to cloud

### Celonis Cloud (Azure/AWS)
1. **Data Core** — ingestion, in-memory DB, Process Query Engine (PQL, claimed 52x faster than SQL)
2. **Process Intelligence Graph** — living digital twin, KPIs, rules, benchmarks, enterprise architecture context
3. **Process Mining** — visualization, conformance checking, root cause analysis
4. **Action Engine** — alerts, API calls back to SAP/Oracle, RPA triggers, automation flows
5. **Execution Apps** — pre-built apps for P2P, O2C, AP, AR
6. **Studio** — low-code builder for custom apps

### Deployment Options
| Option | Status |
|---|---|
| Cloud (Standard) | Active — Azure EU default |
| Private Cloud (Germany) | Available on request |
| Private Deployment | Available for sensitive IT security — contact Celonis |
| CPM4 On-Premise | **Deprecated** — no longer deployed |
| AWS Marketplace SaaS | Active |

## LogFlow's Position

LogFlow = the conversion engine. Fastest path from any messy input to clean Parquet with proper schema, OCEL enrichment, quality validation.

It is NOT the product. It is a component that a product layer sits on top of.

### What LogFlow owns (this repo)
- Format detection and auto-inference
- Multi-strategy ingestion (DuckDB fast path, Go robust path, streaming path)
- Schema inference and type coercion
- OCEL 2.0 data model and enrichment
- Parquet output with compression
- Quality analysis
- Data contracts

### What a product layer would own (separate repo, future)
- On-prem extraction agent (RFC/JDBC bridge)
- Cloud platform (data pool, query engine)
- Process mining algorithms and visualization
- Action engine (alerts, automation, API calls back to source systems)
- Multi-source unification and entity resolution
- User-facing UI and dashboards
- Auth, multi-tenancy, billing
