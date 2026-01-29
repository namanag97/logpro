# ERP Connection Preparation Checklists

## SAP Connection Preparation

### Business Checklist

- [ ] **Identify target processes** — Which processes? P2P (Purchase-to-Pay), O2C (Order-to-Cash), R2R (Record-to-Report)? Each requires different tables.
- [ ] **Get executive sponsor** — SAP Basis and Security teams won't create users or open ports without management approval.
- [ ] **Sign NDA / data processing agreement** — SAP data is often subject to data privacy (GDPR, SOX). Legal must approve extraction of production data.
- [ ] **Clarify scope boundaries** — Which company codes, plants, org units? SAP is multi-tenant — extracting everything is both unnecessary and a compliance risk.
- [ ] **Identify SAP landscape** — Which system: ECC 6.0, S/4HANA, BW? On-premise or cloud (BTP)? This determines the extraction method entirely.
- [ ] **Get SAP Basis team contact** — They own SM59 (RFC destinations), user creation, transport imports, and firewall rules. Nothing happens without them.
- [ ] **Get SAP Security team contact** — They create roles, assign authorization objects, and approve the least-privilege profile for the technical user.
- [ ] **Determine extraction frequency** — One-time historical export? Daily delta? This affects whether you need background job scheduling (S_BTCH_JOB authorization).
- [ ] **Define data retention / anonymization requirements** — Can you extract employee names? Customer IDs? Or must they be pseudonymized before leaving SAP?

### Technical Checklist

- [ ] **Create a dedicated technical user** (type: System/Communication) — Never use a dialog user. Never reuse an existing service account. Create it in SU01 with user type "System."
- [ ] **Assign minimum authorization objects:**
  - `S_RFC` — Activity 16 (Execute), RFC_TYPE=FUGR, restricted to needed function groups (SYST, RFC1, SDTX, SDIFRUNTIME)
  - `S_TABU_DIS` — Activity 03 (Display only), restrict DICBERCLS to specific table authorization groups
  - `S_TABU_NAM` — (Optional) Lock down to individual table names if security requires it
  - `S_RFCACL` — If using trusted RFC connections
  - `S_ICF` — If RFC destination protection is enabled in SM59
  - `S_BTCH_JOB` — If scheduling background extraction jobs
- [ ] **Never grant SAP_ALL** — Even for "just reading." It's an audit finding and a security risk.
- [ ] **Configure RFC destination in SM59** — Connection type 3 (ABAP), target host, system number, client number. Test connection.
- [ ] **Install RFC module** (if using extractor-style approach) — Import transport request into SAP via STMS. Deploys extraction function group.
- [ ] **Open firewall ports** — SAP gateway port 33xx (where xx = system number). If going through SAP Router, port 3299. Route string: `/H/<router_host>/S/3299/H/<sap_host>`.
- [ ] **Enable SNC** (optional but recommended) — Secure Network Communications encrypts RFC traffic. Requires SAP Crypto Library on both sides.
- [ ] **Set `auth/rfc_authority_check` profile parameter** — Must be 6+ (best: 9). Check via RZ11.
- [ ] **Verify target tables are readable:**
  - P2P: EKKO, EKPO, EBAN, EKBE, RSEG, RBKP, BSEG, BKPF
  - O2C: VBAK, VBAP, VBFA, BKPF
  - Metadata: DD02L, DD03L (table/column names)
- [ ] **Test with SE37** — Execute RFC_READ_TABLE against a small table (e.g., DD02L) to confirm the user can read. Use ST01 authorization trace to catch missing auth objects.
- [ ] **Validate extraction does NOT write** — Confirm no BAPI_* write functions are in the authorized function groups.

---

## Oracle Connection Preparation

### Business Checklist

- [ ] **Identify Oracle variant** — Oracle EBS (on-premise), Oracle Fusion Cloud, or Oracle Autonomous DB? Extraction method is completely different for each.
- [ ] **Get DBA team contact** — They create schemas, grant SELECT privileges, and manage TNS/connection strings.
- [ ] **Get Oracle Apps Admin contact** (for EBS/Fusion) — They manage application-level roles and responsibilities.
- [ ] **Executive approval for production access** — Even read-only access to APPS schema data requires sign-off.
- [ ] **Clarify scope: which modules?** — AP, AR, GL, PO, OM, INV? Each module has its own base schema and table set.
- [ ] **Define data privacy requirements** — Oracle EBS stores PII (HR, customer data). Determine what can leave the database.
- [ ] **Determine extraction frequency** — One-time vs. scheduled. For Fusion Cloud, BICC supports incremental extraction.
- [ ] **Check licensing** — Oracle EBS Read-Only User licenses are a specific SKU. Ensure extraction user doesn't trigger compliance issues.

### Technical Checklist — Oracle EBS (On-Premise)

- [ ] **Create a dedicated read-only database schema** — Never connect as APPS. `CREATE USER extraction_user IDENTIFIED BY ...`
- [ ] **Grant minimum privileges:**
  - `GRANT CREATE SESSION` — Allows login
  - `GRANT SELECT ON apps.<table>` — Per table. Never `GRANT SELECT ANY TABLE`.
  - Create synonyms in extraction schema pointing to APPS tables
- [ ] **Key tables for process mining:**
  - Orders: OE_ORDER_HEADERS_ALL, OE_ORDER_LINES_ALL
  - AP: AP_INVOICES_ALL, AP_INVOICE_LINES_ALL
  - PO: PO_HEADERS_ALL, PO_LINES_ALL
  - GL: GL_JE_HEADERS, GL_JE_LINES
  - Workflow: WF_ITEM_ACTIVITY_STATUSES (audit trail)
  - Auth: FND_LOGINS
- [ ] **Re-grant after patching** — Oracle patches can drop/recreate objects, breaking grants. Document in runbook.
- [ ] **Set up SQL*Net connectivity** — TNS entry or EZCONNECT. Open port 1521 (default listener). Test with `tnsping`.
- [ ] **Consider Data Guard** — Extract from standby/read-only replica to avoid production load.

### Technical Checklist — Oracle Fusion Cloud

- [ ] **Provision BICC user** — Requires roles: `ESSAdmin` + `ORA_ASM_APPLICATION_IMPLEMENTATION_ADMIN_ABSTRACT`.
- [ ] **Configure external storage:**
  - Option A: UCM — simpler, data stays within Oracle
  - Option B: OCI Object Storage — create bucket, API key (PEM format), IAM policies, no retention rules
- [ ] **Set up BICC offerings** — Create per module (Finance, SCM, HCM). Each contains Public View Objects (PVOs). 5,000+ available.
- [ ] **Select only needed PVOs and columns** — Don't extract all columns. Audit each VO for minimum required attributes.
- [ ] **Apply extraction filters** — Filter by org unit, date range, status to reduce volume.
- [ ] **Test extraction** — Run single offering manually. Verify CSV lands in storage. Check row counts.
- [ ] **Schedule incremental extracts** — BICC supports delta. Run during off-peak hours.

---

## Key Differences: SAP vs Oracle

| Aspect | SAP | Oracle |
|---|---|---|
| **Protocol** | RFC (binary, proprietary) | SQL*Net / REST API |
| **Extraction function** | RFC_READ_TABLE (reads any table) | BICC PVOs (pre-defined views) or direct SQL |
| **Auth model** | Authorization objects (S_RFC, S_TABU_DIS) | Database grants + application roles |
| **Cloud variant** | BTP / S/4HANA Cloud → OData APIs | Fusion Cloud → BICC → OCI Object Storage |
| **Biggest risk** | Granting too-broad RFC access | Connecting as APPS schema |
