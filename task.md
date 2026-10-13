# Today’s Implementation Plan: Source → Landing → Bronze

**2026-10-13 alignment:** This checklist implements the architecture quoted in user annotation1 and strengthened in [docs/architecture/architecture.md](docs/architecture/architecture.md). The annotated proposal is the baseline; older repository-only architecture is historical. This task is documentation only, not evidence that the following gates passed.

**Status:** Approved working implementation plan.  
**Repository scope:** Documentation only. No application, infrastructure, pipeline, notebook, or CI/CD code is included in this file.  
**Scope boundary:** This plan ends at successful Delta Bronze commit and reconciliation. Detailed Silver and Gold transformations are outside today’s work.

## 1. Today’s outcome

Use the five active-day plan in architecture Section 20; target October 18,2026 EOD, with October 14 paused. Today's first gate is actual source profiling, control/IAM/capacity validation and one WC pilot **through governed serving**, including duplicate/reject/recovery evidence. Do not call Bronze-only success a complete product.

Retain selected source boundaries,26-table operational control design, source-faithful Bronze and the shared reusable package. Expand the tested pattern without independent notebook reimplementation. No resources/code are created by this documentation update.

## 2. Preconditions from the completed platform setup

Verify actual workspace/metastore, storage credentials, account groups, sibling UC external locations/volumes, tested job compute, network/Key Vault and Control API/source connectivity. Reported catalog/schema creation does not prove identity or job execution.

PostgreSQL/API extractor role has separate native SDK staging-write/source-read authorization and Key Vault references; it is not the staging-read-only Bronze ingestion identity. Supplier _upload only; publication owns pending/ready/rejected; ADF ready read/staging write; ingestion staging read-only and assigned pending/committed/Bronze/checkpoint write; transform assigned Silver/Gold; consumers serving views only. No unrestricted Landing data role. Test every actual principal with positive and negative paths; serverless/default UC success does not prove dedicated MI access. Use architecture Section 17 for governance/network/security fallback and initialplan.md Section 11 Gate1.

## 3. Authoritative source routing

The complete entity/path/target mapping is architecture Section 3.1; it is the single mapping authority.

- H&M controlled PostgreSQL: four snapshots, three mutable outbox incrementals and WAL CDC of that same outbox; sample_submission initial-only.
- SFTP: seven REES46 monthly GZIP archives, three OFF JSONL physical parts/one contract, ten Amazon metadata Parquets/one package, separate review JSONL and seven WC weather DATs.
- Prices HTTPS: /api/v1/prices raw pages; preserve old proofs registry inactive and register profiled Prices entity/contract before activation.
- Kafka: H&M WAL and REES historical replay direct raw Bronze; no file Landing/Auto Loader.

No Mongo/Cosmos or standalone Product API path. Do not invent cross-domain business keys.

## 4. Controlled availability, extraction, and SLA contract

Preserve the schedules and C/B/S/G budgets in architecture Section 5. Articles/customers daily 00:30 UTC, transactions hourlyHH:15,30s stream triggers; source supplier windows and all stage targets remain unchanged from the annotated proposal. Batch Gold is dependency-driven; CDC/replay Gold refresh5min targets end-to-productp95≤10 min.

ADF5min supplier scans within windows and hourly late sweep7days exit after outcomes; no infinite Wait loop. Availability lateness and slow processing are different incidents. Full source counts/throughput/quotas are measured before admission; fixture success is not full-volume completion.

## 5. Landing implementation contract

Use architecture Section 8 canonical env/source/entity/delivery-version/revision/attempt paths. Source bytes remain original; preflight reads staging without write, validates/frames and prepares original/bronze_input/manifest/_COMMITTED in non-scanned publication pending. Publication API conditional HNS rename exposes complete committed directory. Auto Loader reads bronze_input only.

No dbutils.mv atomicity assumption, ordinary file-by-file copy into watched prefix or marker-only completeness. Equal identity/hash destination is no-op; conflict blocks. Ledger ack repairs from immutable evidence; a transfer retry cannot overwrite committed.

## 6. SFTP supplier readiness and manifest contract

Follow initialplan.md Section 11 steps6–27 and architecture Section 8–9. Platform allocates ULID/revision before supplier upload. Supplier freezes/calculates local artifact controls, uploads .partial, explicitly renames final data, then manifest.partial→manifest.json last. Supplier never writes ready/_READY.json.

ADF orchestrates Databricks publication validation of a platform-owned copy: identity/path/inventory/format/compression/encoding/size/SHA/parsed counts and independent source-version controls. Publication API publishes ready and platform marker. ADF Binary Copies exact inventory to isolated staging.

Missing/changing inside window waits; stable wrong package rejects; overdue upload alerts without moving active files; valid late revision passes with incident. Same revision/same manifest skips; changed revision content conflicts. Transient transport failures retry sound bytes, not quarantine good data.

## 7. Source contract baseline

Preserve established source/contract/rule IDs and correct only source-semantic errors through immutable versions.

- OFF: contract_off_products_v1 and qr_off_part_001_schema_v1 apply across physical part aliases; no deleted part contracts/rules.
- WC: retain contract_wc_store_sales_v1 and qr_wc_f_2016_raw_v1; verified weather data is comma-quoted TOA5 with4headers/four layouts, not old store-sales pipe data. Existing BLTA27fields remain history; register all verified layouts in a new version.
- Prices: actual /prices schema is not proof-draft fields. Versioned registration/mapping migration precedes activation.
- H&M generated transaction_id/outbox are project fields, not source facts; other exact fields/types/keys/counts are physically profiled before approval.

source_contract_field and schema_version remain immutable per approved version/layout; observed drift never silently updates them. Typed Silver/Gold compatibility is tested before activation. Full source contract matrix: architecture Section 3.2.

## 8. Control-plane tables to create

Use the **26-table** annotated baseline extended in control tables.md; do not execute historical22-table Delta SQL as the new authoritative control store. Import validated existing metadata and preserve original exports.

| Group | Tables |
|---|---|
|Metadata11|source_config, entity_config, source_contract, source_contract_field, source_mapping, schema_version, schema_drift_event, quality_rule, pipeline_config, data_product, asset_dependency|
|Delivery4|source_delivery, delivery_manifest, delivery_artifact, validation_result|
|Operations9|ingestion_run, run_step, run_event, cursor_state, work_lease, asset_commit, product_release, recovery_request, alert_event|
|Quality1|reconciliation_result|
|Quarantine1|quarantine_event|

Dedicated platform_control PostgreSQL + authenticated Control API owns transactional state; UC Delta projections are read-only reporting. Actual Delta commits/checkpoints/Connect offsets remain native evidence. Enforced environment/revision/version keys, CAS and unique scopes replace informational Delta keys as operational concurrency. SQL lease does not itself fence stale Delta/cloud publication; old writer and in-flight publish intent must terminate/reconcile.

## 9. Bronze design

Fixed technical envelope JSONL from all batch adapters; original source bytes preserved. Kafka raw key/value bytes+headers+topic/partition/offset captured before parsing. Required metadata is source-specific: source/entity/version, delivery/artifact/ordinal/layout/contract, snapshot/group/key when applicable, CUD event/sequence/source generation, request/page/attempt, replay/correction/run and raw reference/hash.

No business joins, aggregation, SCD, currency/sentinel normalization or guessed review/order IDs. Decimal/JSON lexemes, DATheaders and Parquet tagged type/row-group refs preserve source meaning. Idempotent normal writes/checkpoints plus exact record accounting gate capture success. See architecture Section 7.

## 10. ADF, Landing, Auto Loader, and Bronze schema-drift gate

ADF orchestrates Binary Copy/jobs and records task evidence; its Mapping Data Flow schema-drift flags do not govern this path. Databricks preflight compares full staged structural schema and values against pinned contract/field/layout baseline before committed. Platform envelopes keep unapproved source values in raw_payload/original, not approved typed columns.

Auto Loader uses explicit fixed JSON envelope, none evolution, AvailableNow, strict bronze_input discovery, rescued/corrupt-envelope check and unique persistent env/pipeline/generation checkpoint. No separate schemaLocation needed. Bronze checks envelope/version/raw identities/counts; Silver/Gold enforce source typed/business and consumer quality.

All source/schema/DQ/commit/mapping/rule/code versions are pinned per run; every change produces immutable schema snapshots/drift evidence and explicit disposition. No parameter automatically approves schema evolution. architecture Section 13–14 is authoritative.

## 11. Schema-drift decision matrix

Use the complete integrated detection→classification→Landing→Bronze→quarantine→alert/control→schema version→downstream→authorization→recovery/reprocess matrix in architecture Section 13.

Covered: additive optional, unexpected, removed optional/required, missing key/field, rename, datatype widening/narrowing, precision/scale, nullability, named vs positional reorder, nested shape/cardinality, malformed schema/duplicate/case collision and semantic key/unit change. Known four WC layouts are registered variants, not four new source datasets. Typed use requires tested versioned mapping/contract, never rescue-based approval.

## 12. Schema-drift states and quarantine handling

Architecture15.6/16.1 specifies file vs record vs infrastructure containment and states. Use governed quarantine files, restricted UC record_exception, raw Kafka Bronze with async DLQ and SQL reference/evidence. Infrastructure/auth/capacity incident is not bad-data quarantine.

DETECTED→QUARANTINED/REJECTED/HELD_REFERENCE→DIAGNOSED→CORRECTED_APPROVED→REPLAY_QUEUED→REPLAYING→VALIDATED→RECONCILED→CLOSED. Failed replay returns to diagnosed with same original UID/request evidence. Steward-authorized correction and nearest-retained-layer replay preserve source identity/order; Gold recomputes affected grains. No silent edits/drops or automatic business correction.

## 13. PostgreSQL implementation sequence

1. Seed/profile four selected H&M source tables, generated SHA-256 transaction key, source generation/provenance and exact controls.
2. Enforce controlled CUD+outbox with transaction-scoped serialization before sequence allocation; deny bypass writes.
3. Pause/drain changes, establish/verify Debezium no_data slot/offset readiness and consistent exported snapshot group/H.
4. Extract bounded chunks under imported snapshot transactions; stage/preflight/pending/committed/Bronze count reconciliation.
5. Shared Silver coordinator validates four-table baseline; initialize captureH then unfreeze.
6. Run daily/hourly bounded outbox extracts alongside continuous WAL outbox Kafka capture with common event_id.
7. Apply shared ledger once, source-sequence current/delete/SCD history protections and validated Gold cut.
8. Prove rollback gaps/late commits/duplicate modes/out-of-order/delete/slot recovery and source-to-product controls; do not substitute preceding100overlap for commit-safety proof.

Full extraction/ordering/cutover protocol: architecture Section 10 and15.2.

## 14. API and Kafka implementation sequence

Prices: inspected /prices contract→bounded creation/refresh window→serial page/size100→durable raw page/attempt/hash→terminal/integrity/schema validation→committed envelopes→Auto Loader/Bronze reconcile→observed Silver/Gold. Retry10sconnect/30srequest,5 transient backoffs5s–5 min+jitter/Retry-After; auth fails closed. Mutating-pagination inconsistency starts new bounded attempt; no mixed pages or inferred source deletion.

Kafka: threeenv-qualified topics/six source partitions,14 day source/30 day DLQ; raw30s capture before classification. Preserve common source IDs separate from broker positions; DLQ idempotently exports from raw Bronze. checkpoint/Connect offsets native, SQL observations. Fail on data loss; assessed newgeneration replay if retention/checkpoint unavailable. Single-broker project is non-HA. Full contracts/recovery: architecture Section 11–12.

## 15. Today’s execution order

Follow initialplan.md Section 10–11 and architecture Section 20's unchanged active-day gates. Validate control/IAM/profile/capacity, one actual WC cross-layer serving slice, adverse tests, then same selected batch adapters/full inventories; H&M seed/baseline/incremental/WAL coordination; Prices/replay; full-volume, governance, recovery/DR and immutable CI/CD promotion. Do not recreate successful captures/metadata for downstream steps. Planned implementations are NOT_RUN until actual evidence is stored.

## 16. Definition of done for today

Today: one actual profiled WC pilot through governed serving plus duplicate/invalid/recovery evidence and validated common prerequisites/capacity plan. Final October 18 completion: all selected full source inventories/modes, source-to-product reconciliation, every required DQ/schema/recovery/IAM/lineage case, tested deployment/canary/rollback, honest measured SLO/DR and coverage limits. Gold uses validated immutable candidates and one serving manifest with retained release membership; consumers pin one release across queries. No full dataset or production availability claim without evidence.
