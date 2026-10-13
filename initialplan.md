# Initial Implementation Plan: Source → Landing → Bronze

**2026-10-13 audit alignment:** Follow [the architecture revised in place](docs/architecture/architecture.md) as the single authority. This file preserves the execution-plan role and SFTP step sequence; obsolete conflicting mechanisms are corrected. No cloud, SQL, application or infrastructure implementation is performed by this documentation edit. Recoverable baseline: the full proposal quoted in annotation 1; Git b1905f3/ZIP preserve the older repository context separately.

**Purpose:** Give one chronological path for building the approved project from its current state through source delivery, Landing staging, validation, Landing committed, and Bronze Delta.

**Status:** Working implementation guide. The source choices, schedules, contracts, Landing rules, control-table definitions, and ingestion patterns remain governed by `dataset.md`, `task.md`, and `control tables.md`. This file orders that work; it does not replace those decisions.

**Scope:** Chronological execution of the consolidated architecture, preserving source→staging→committed→Bronze and coordinating trusted downstream handoffs. Detailed Silver/Gold rules, release consistency, governance, DQ and deployment are in the existing architecture, not deferred.

## 1. The end-to-end rule

Batch sources: controlled source→ADF orchestration / source-specific extractor→Landing staging→Databricks preflight→publication pending→Publication API conditional HNS directory rename→Landing committed original+bronze_input→Auto Loader AvailableNow fixed envelope→Bronze capture reconciliation→trusted Silver gate→Gold candidate/release→SQL consumer pin.

SFTP first adds supplier _upload final-file+manifest-last→platform-owned publication validation/copy→ready/_READY.json; supplier never writes ready. PostgreSQL initial/incremental and API extracts use their real controlled source endpoints, not supplier-file emulation. Only PostgreSQL WAL CDC and REES historical replay bypass file Landing: Kafka→30s raw Structured Streaming→Bronze→shared domain Silver coordinator→Gold.

Original bytes remain replay authority; Bronze is technical/source-faithful, Silver owns business transforms and Gold metric/publication correctness. No success for one layer implies whole-product success.

## 2. Current position and the immediate next step

The workbook confirms22 existing table schemas; seven metadata tables contain exported records and runtime evidence is empty. That is not proof of executed ingestion. Preserve/hash these exports and actual DDL; profile selected files and validate source mappings/contracts before migration.

Immediate gate: validate the dedicated identities/scoped UC locations and actual WC layouts, then prepare the 26-table SQL control/API migration following control tables.md. Import validated existing registration rather than manually recreating it. Correct WC layout fields using a new immutable version and Prices entity/contract mismatch; map OFF physical parts to one logical contract. Existing phase2_runtime_control_tables.sql stays historical and is not executed as migration.

Build one small actual WC sample through **Gold serving** with the shared framework and tests before expanding adapters. October 13/15/16/17/18 gates and end date are architecture Section 20; October 14 workloads needing human recovery are paused.

## 3. Prepare the execution boundary

1. Verify actual workspace/metastore, UC-compatible job compute and scoped Access Connector credentials; test cloud identity separately from Databricks user grants.
2. Retain catalog retail_de_dev and established schemas/volumes. Bind environment-specific locations/catalogs and grant account-level groups as architecture Section 17.
3. PostgreSQL/API extraction jobs use a separate dedicated Azure staging-writer principal through the Data Lake SDK, source-read/API authorization and in-memory Key Vault credentials. They cannot write committed/Bronze. Native staging writes are cloud-authorized and custom-lineage audited, not falsely claimed as UC-enforced writes. Supplier writes only _upload; publication identity owns pending/ready/rejected; ADF reads ready/writes staging; ingestion reads staging **without write** and writes pending/committed/Bronze/own checkpoints. Publication API owns final directory publish. No unrestricted Landing data role.
4. Verify positive/negative permissions on the actual compute route, host key/TLS/network/Key Vault and SQL Control API. A successful dbutils listing with an old credential is not a dedicated MI test.
5. Canonical paths include env/source/entity/delivery revision and isolated staging attempt; use architecture Section 8 exactly. No external volume wraps Delta table directories.
6. Create future job/table/checkpoint resources only during implementation; explicit envelope schema needs no separate schemas filesystem.
7. Verify source quotas, full-size expansion/framing/transfer capacity, compute cost guardrails and recoverable backup paths before full-volume admission.

**Gate:** supplier/ADF/publication/ingestion/transform/consumer privileges pass independently; invalid boundaries block data admission.

## 4. Build the shared ADF and Databricks file-delivery pattern

### Source-side readiness

Supplier receives platform-assigned ULID/revision/path before upload, freezes local bytes and calculates hashes/counts. It uploads .partial data, explicitly renames completed files, then manifest.partial→manifest.json last. A final filename or manifest flag alone cannot establish completeness. Independent bootstrap/source counts detect a self-consistent truncated export.

### SFTP publication and readiness

ADF five-minute scans invoke the Databricks publication validator for eligible manifest candidates. Missing/changing files remain pending until deadline; overdue opens incident without moving active uploads. Stable corrupt/unauthorized/invalid packages reject with evidence. Validator copies to platform-owned pending, verifies inventory/size/SHA/parsed count and basic dataset/format identity; Publication API publishes immutable ready and _READY.json. A supplier cannot modify that platform copy.

### ADF responsibilities

ADF admits one bounded identity via Control API, Binary Copies exact ready inventory to isolated staging and invokes preflight. Copy failure retries verified files/new attempt without overwriting committed or republishing ready. PostgreSQL/API are invoked extraction jobs, not SFTP transfers. ADF Get Metadata is not a checksum/count engine; Mapping Data Flow schema-drift flags do not affect Binary Copy. No infinite waiting pipeline.

### Databricks preflight and promotion

Preflight rehashes staged copies and validates the pinned source schema/layout/contract and technical quality before framing. Preserve originals and deterministic raw envelopes; known WC layouts are registered, unknown structural layouts hold. Recoverable malformed source rows retain raw/status; corrupt compressed containers or platform envelopes block.

The ingestion identity reads staging only and writes publication pending. Publication API verifies current epoch/manifest and HNS-renames the complete directory into committed. Auto Loader watches bronze_input only, never pending/staging/original/markers. Missing SQL acknowledgments are repaired from immutable manifests/Delta evidence rather than another copy or append.

## 5. Prove failure and schema-drift recovery

Execute architectureT01–T14 for the first file pattern and preserve evidence: partial/no manifest, early manifest, stable truncation/hash/count mismatch, wrong/extra paths, incomplete multipart, changed supplier file, interrupted copy, duplicate/concurrent scan, stale owner, publish-before-ack, all schema drift cases, raw type fidelity and Bronze-commit-before-ack.

Source schema compatibility is checked before committed; Bronze checks fixed-envelope/raw metadata; Silver and Gold require contract/consumer-quality and coverage gates. Same source type added in raw payload does not approve typed use. Drift activation follows versioned metadata plus downstream compatibility tests. A failed layer resumes nearest retained input; no normal checkpoint reset or repeated successful extraction.

## 6. Load batch sources in this order

| Order | Source bootstrap and processing | Trigger/budgets |
|---|---|---|
|1|Actual WC pilot with one profiled layout/sample; all four headers and independently verified count/hash; common pattern through governed Gold|Manual; standard monthly 04:00–05:00, stage30/15/15/10 min|
|2|REES46 seven CSV/GZIP monthly packages; preserve source UID for later replay|Manual historical bounds; monthly 00:00–02:00, stage90/60/60/30 min|
|3|OFF JSONL split on full records into three physical parts; one logical contract/group|Daily02:00–03:00, parts02:00/02:20/02:40;90/60/60/30 min|
|4|Amazon ten Parquet metadata files one package; reviews separate JSONL package|Metadata Sun03:00 budgets60/30/30/15; reviews daily 03:30 budgets180/120/120/30 min|
|5|Full WC seven artifacts/four verified column layouts, weather metrics|Monthly04:00–05:00;30/15/15/10 min|
|6|H&M seed/freeze/slot/shared exported snapshot; four source tables same group|Manual initial4h/2h/2h/30 min|
|7|H&M bounded commit-safe outbox incremental plus shared Silver coordinator|articles/customers00:30 daily 15/10/10/5; transactionsHH:15 hourly 20/10/10/5 min|
|8|Open Prices real /prices response pages and observation windows|Daily03:05 + Sunday reconciliation;120/30/30/15 min|

### How ADF decides when and what to ingest

Authoritative schedules are versioned metadata.pipeline_config by mode, not entity_config's legacy single cron. Full schedule/windows/SLOs are architecture Section 5. ADF scans5min inside SFTP windows, hourly late sweep7days, and exits after outcomes. It checks enforced delivery-version/request identity before admitting work, calls job APIs with bounded source scope, and evaluates real completion/evidence, not just submission response. Streams run independently and are monitored, not invoked every batch schedule.

## 7. Bronze contents and guardrails

Use the fixed technical envelope and source-specific provenance from architecture Section 7.1. Preserve source raw text/numeric lexemes/Parquet typed raw reference; Kafka raw bytes/key/headers/offsets before parsing; generated source keys and event sequence clearly marked project metadata. Original Landing bytes remain unchanged.

No Bronze business joins, currency conversion, sentinel imputation, aggregate, SCD or business-key dedup. Technical duplicate capture and operational metadata are required. Headers/data/malformed/duplicate dispositions reconcile separately. foreachBatch idempotency uses stable txnAppId/batchId; native checkpoint and Delta commits are independent from SQL audit. Explicit envelope schema requires no separate schemaLocation.

## 8. Add streaming only after batch foundations pass

### H&M PostgreSQL CDC

Actual WAL outbox capture is coordinated with the initial snapshot before unfreezing mutations; do not start an independent base-table snapshot and write competing Silver. Capture source_generation/event_id/sequence in raw Debezium envelopes; topic{env}.hm.changes.v1, six partitions, keyentity+PK. Native connector slot/offset state controls recovery. One Silver coordinator unions batch/CDC common events, protects deletes/stale versions and reconstructs all SCD events.

### REES46 Kafka replay

Topic{env}.rees46.events.v1, six partitions, shared batch record_uid; bounded source inventory and producer rate. 30s streams,14 day source retention, raw poison→Bronze then async shared DLQ30days. Checkpoints separated byenv/pipeline/targetgeneration. Offsets/retention loss trigger assessed archived replay, not latest reset. Gold session/date restatement is part of replay completion. Full source/transport/target identities and tests are architecture Section 10.3/11.1/T18–T23.

## 9. Completion gates and operating checklist

- Each selected source/version has profiled contract/layout/key and independent inventory/count/hash controls.
- Source publication, staged copy, committed envelope set and raw capture reconcile by immutable identity.
- Silver accepted/held/rejected/duplicate dispositions conserve inputs; CDC current/history/FK states verify.
- Gold candidates pass metric/dependency/freshness/security gates; serving consumer pins one release.
- All audit/recovery request and native commit/checkpoint evidence reconcile, including successful-write/missing-ack scenarios.
- T01–T34 cases executed with outcomes/evidence; actual IAM, lineage, rollback/DR and full-volume budgets measured.
- No source missing history, unmatched domain or public historical replay is mislabeled complete live production data.

Operations follow architecture Section 16.4 runbooks; current existing tables and reported setup alone do not satisfy these gates.

## 10. Recommended execution order at a glance

1. Preserve/profile actual sources and metadata; validate control SQL/API migration and IAM/network capacity.
2. Implement one shared WC pilot source→serving flow with read-only staging, original fidelity and transactional admission.
3. Prove duplicate/partial/schema/commit-ack failures through the consumer release.
4. Extend the same package adapters to REES46/OFF/Amazon/full WC; track full inventory and volume.
5. Seed H&M and reconcile initial exported-snapshot baseline with slot ready before unfreeze.
6. Enable commit-safe incremental and actual WAL CDC with one shared Silver coordinator.
7. Implement Prices observation capture/reconcile and REES bounded Kafka replay, same quality/recovery semantics.
8. Complete all downstream products, full-volume tests, governance/DR/CI deployment/canary, release evidence and runbooks.

Use architecture Section 20 dates and acceptance gates. Do not rebuild the same metadata or successful extraction for every layer.

## 11. SFTP implementation steps

This retains the established three gates: secure supplier/platform boundary; ready-publication correctness; pilot vertical slice. The pilot uses actual WC_F_2016, not store_sales.txt. Gate3 now proves downstream handoff through trusted Silver/Gold serving as well as Bronze, preventing a locally successful adapter that breaks consumers.

### 11.1 Prepare paths, identities, and control metadata

1. Verify canonical env/source/entity/revision _upload, platform pending, ready/rejected, Landing staging/pending/committed and checkpoint paths from architecture Section 8.
2. Test supplier SSH user: upload/rename only in _upload; deny ready/rejected and other filesystems.
3. Test publication MI, ADF MI and dedicated ingestion MI independently. Ingestion staging is read-only; publisher owns final rename; no unrestricted Landing data RBAC. Scope UC credentials/locations/volumes and actual compute route.
4. Verify retained src_legacy_dat_txt_sftp, ent_wc_store_sales, contract_wc_store_sales_v1 and qr_wc_f_2016_raw_v1. Profile the pilot's comma-quoted TOA5/four-header layout; register a new immutable layout/version where old metadata was incorrect.
5. Validate/import control registry into enforced SQL and API; prove unique request/revision/lease/CAS behavior. Existing Delta exports are history/projections, not a lock engine.

**Gate1:** actual identities pass positive and negative path/UC tests; job-capable compute reaches allowed endpoints.

### 11.2 Freeze the delivery and manifest contract

6. Platform Control API allocates ULID and revision1 before simulated supplier upload; communicate assigned path. A correction gets same ID/higher revision, never overwrites prior bytes.
7. Finalize local immutable artifact; record SHA-256 of actual upload bytes, size, data count excluding four headers, header count, verified format/encoding/layout and source-version control hash/count.
8. Build manifest_version1.0 TEXT with assigned identity, contract/version, source window/group, completeness and artifact inventory. Safe relative paths only; values describe finalized local bytes, not a .partial intermediate.
9. Preserve received manifest bytes/hash separately from platform observations; version source/control schemas and record no fake field facts.

### 11.3 Publish a supplier delivery to _upload

10. Supplier creates assigned revision path and uploads data as filename.partial.
11. Client waits for successful upload close, then explicitly renames to exact final name. Nothing auto-renames by SFTP.
12. After all final files complete, upload manifest.json.partial then rename manifest.json last. Supplier writes no _READY/_COMMITTED marker and cannot alter platform-ready copy.

### 11.4 Scan and validate _upload

13. Parameterized ADF pl_sftp_publish_scan runs manually for the pilot,5 min in operating window; hourly late sweep7days. Each run exits; no infinite wait.
14. List candidate manifests and obtain one Control API admission per delivery revision/manifest. A duplicate completed identity returns existing outcome.
15. Invoke Databricks publication validator with pinned run/source/entity/contract/mapping and current lease epoch.
16. Check manifest syntax/identity, exact safe paths/inventory, expected parts, format/compression/encoding, bytes/SHA, parsed data/header counts against independent bootstrap controls.
17. Missing/changing/.partial artifact is pending; compare observations before declaring stable mismatch. Two stable sizes do not prove complete content; SHA/count/source controls are mandatory.
18. Copy eligible bytes into platform-owned pending and validate the copy. Full source schema compatibility is the later staged preflight; obvious wrong-dataset/header/layout identity is checked now.

### 11.5 Route incomplete, invalid, and valid deliveries

19. Pending inside window remains AWAITING_STABILITY, no marker/data publish; next bounded scan retries.
20. Window expiry creates TIMED_OUT/late incident, no destructive movement of active upload. Valid late completion can pass gates visibly late.
21. Stable malformed/unsafe/wrong/count/hash package is REJECTED_PACKAGE; preserve rejected copy/reference and reason evidence; request corrected revision. Connection/capacity issues get retries, not data quarantine.
22. Publication API checks current owner and complete copied manifest, publishes ready under platform identity and writes _READY.json last.
23. Equal revision/hash already ready is a no-op; changed content under same revision is conflict. Missing SQL ack is repaired from ready evidence; acknowledgment to supplier retries separately.

**Gate2:** ready contains only verified original data, original manifest and platform _READY.json; supplier cannot mutate it.

### 11.6 Copy ready delivery to Landing staging

24. ADF pl_sftp_ready_to_landing finds platform ready marker/manifest, verifies identity/hash and claims bounded transfer. No candidate records NO_READY_DELIVERY, not infinite wait.
25. Binary Copy original data and manifest/evidence into staging/.../attempt=...; preserve original bytes and explicit inventory. Markers are control files, not Bronze data input.
26. Check prior committed/captured delivery evidence before copy. Already reconciled capture is skipped; staged verified objects can resume. A SQL lease expiry requires old executor termination before replacement.
27. On transfer interruption retry missing verified objects or new isolated attempt; leave ready immutable; do not overwrite committed.

### 11.7 Validate staging and promote to committed

28. Databricks preflight reads staging only, rechecks bytes/hash/count, observes actual layout/schema, compares pinned contract/fields/rules, writes drift/validation evidence and deterministic source-faithful envelopes. Preserve four headers and DATA ordinals.
29. Passing delivery writes original+bronze_input+manifest/_COMMITTED evidence into Landing pending. Publication API conditionally HNS-renames the entire directory into committed after rechecking epoch/content. Do not use dbutils.mv or partial copy into a watched prefix.
30. Breaking structure/unknown layout stays outside committed; recoverable malformed source rows are retained raw with status for Silver exclusion. Infrastructure retries preserve sound data; consumer-incompatible typed changes require contract/rollout tests before activation.
31. Ready is transport-valid; committed is capture-compatible. A complete package does not imply Silver/Gold business validity. Missing commit acknowledgment repairs from the committed manifest.

### 11.8 Load Bronze and prove recovery

32. Auto Loader reads only committed bronze_input JSONL with explicit fixed envelope, none evolution, rescued/corrupt-envelope checks, AvailableNow and unique env/pipeline/target-generation checkpoint; no separate schemas filesystem.
33. Write source-faithful Bronze operational metadata idempotently; reconcile DATA+source-malformed/header records and raw identities. No business joins/conversions/sentinel replacement.
34. Retry the delivery and crash after Delta commit before SQL ack; prove same transaction/capture identities no-op and metadata repair. Downstream Silver runs from pinned captured input, not new SFTP extraction.
35. Execute T01–T14 and relevant downstream cases: invalid manifest/hash/partials, schema evolution, duplicate/concurrent runs, copy/publish/write interruption, record fidelity; Silver weather typing/quality and Gold validated release/consumer pin. Prove recovery starts at last good layer, quarantine closure follows reconciliation.

**Gate3:** actual WC pilot reaches one validated source-faithful Bronze capture and a trusted governed serving release with complete counts/lineage; failures cannot expose half/invalid data. Full WC expands the same tested adapter, not another pipeline rewrite.

### 11.9 Document alignment before implementation

Architecture29 and control tables.md are the canonical decisions; dataset.md records selected source facts/progress; task.md is the execution/acceptance checklist. No historical supplier-ready/22-Delta-lock/direct-native-Auto-Loader advice remains active here. No SQL/notebook/pipeline/IaC implementation is changed by this audit.
