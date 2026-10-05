# Initial Implementation Plan: Source → Landing → Bronze

**Purpose:** Give one chronological path for building the approved project from its current state through source delivery, Landing staging, validation, Landing committed, and Bronze Delta.

**Status:** Working implementation guide. The source choices, schedules, contracts, Landing rules, control-table definitions, and ingestion patterns remain governed by `dataset.md`, `task.md`, and `control tables.md`. This file orders that work; it does not replace those decisions.

**Scope:** Source availability and bootstrap, ADF orchestration, SFTP and non-SFTP ingestion, Landing `staging` and `committed`, Auto Loader, Bronze, reconciliation, failure recovery, then streaming. Detailed Silver and Gold implementation is out of scope.

## 1. The end-to-end rule

Every Landing-based batch source follows this path:

```text
Public/local source data or source system
  → controlled source boundary
  → ADF schedule or one-time trigger
  → Landing staging
  → Databricks preflight: delivery integrity + schema + contract + quality
  → promote only a passing immutable delivery to Landing committed
  → Auto Loader AvailableNow reads committed only
  → Bronze Delta with source data + operational lineage fields
  → reconcile counts/hashes + record commit and cursor state
```

ADF does not copy directly to `committed/`. A failed, incomplete, or unapproved
delivery remains outside the committed prefix, so Auto Loader cannot ingest it.
The only approved ingestion paths that bypass ADLS Landing are PostgreSQL CDC
through Debezium/Kafka and the controlled REES46 Kafka replay. They use Kafka
offsets and durable Structured Streaming checkpoints instead.

## 2. Current position and the immediate next step

The source metadata tables have been created and populated: source, entity,
mapping, contract, contract fields, quality rules, and schema baselines. First
verify their IDs and references against the exports and `dataset.md`. In
particular, resolve the Open Food Facts parts 002/003 entity/mapping alignment
before ingesting them: all three physical deliveries use the one logical
`contract_off_products_v1` contract and its baseline.

Next, create the remaining runtime and audit tables from the canonical 22-table
inventory in `control tables.md`. The inventory is shared across sources; do
not make separate control tables per datasource.

The source metadata group has 8 tables:

- `metadata.source_config`
- `metadata.entity_config`
- `metadata.source_contract`
- `metadata.source_contract_field`
- `metadata.source_mapping`
- `metadata.schema_version`
- `metadata.schema_drift_event`
- `metadata.quality_rule`

The remaining 14 runtime, audit, quality, and quarantine tables are:

- `landing_audit.source_delivery`, `landing_audit.delivery_manifest`,
  `landing_audit.delivery_artifact`, `landing_audit.validation_result`
- `ops.ingestion_run`, `ops.run_step`, `ops.run_event`, `ops.cursor_state`,
  `ops.bronze_commit`, `ops.work_lease`, `ops.recovery_request`,
  `ops.alert_event`
- `dq.reconciliation_result`
- `quarantine.quarantine_event`

Use the exact columns and logical keys in `control tables.md`. Delta does not
enforce uniqueness for the documented logical keys; ingestion writes must
enforce idempotency and use leases/optimistic concurrency as documented.

**Gate:** All 22 tables exist in the approved Unity Catalog schemas; source,
entity, mapping, contract, field, quality-rule, and schema-baseline references
resolve unambiguously. No delivery is loaded yet.

## 3. Prepare the execution boundary

Complete these setup items before extracting project datasets:

1. Verify the Databricks Access Connector storage credential and Unity Catalog
   external location for the ADLS Gen2 Landing container.
2. Create/verify governed Unity Catalog volumes for contracts, control files,
   validation samples, and quarantine artifacts.
3. Verify separation of permissions: the Azure Data Factory managed identity
   writes only to Landing staging; Databricks validates staging, promotes to
   committed, writes Bronze/quarantine, and owns its checkpoint/schema paths.
4. Verify job-capable Databricks compute can read/write only its approved
   external locations and Unity Catalog tables.
5. Configure the Azure Storage SFTP local user and SSH key with controlled
   `/_upload` and `/ready` supplier paths.
6. Create the Landing prefixes, Bronze tables, and per-source/entity Auto
   Loader checkpoint and schema-location paths. Keep checkpoints/schema state
   outside directories Auto Loader scans.
7. Confirm secrets are in Key Vault and references, not credentials, appear
   in metadata.

Landing layout, relative to the Landing container:

```text
staging/{environment}/{source}/{entity}/delivery_id={delivery_id}/
committed/{environment}/{source}/{entity}/business_date={date}/delivery_id={delivery_id}/
quarantine/{environment}/{source}/{entity}/reason={reason_code}/delivery_id={delivery_id}/
```

**Gate:** ADF can write to a test staging location but cannot write to
committed. Databricks can read staging and publish a validated test object to
committed. Auto Loader is configured to read committed only.

## 4. Build the shared ADF and Databricks file-delivery pattern

Build the pattern once, starting with a small WC_F_2016 sample. Keep it
parameterized by the existing `source_id`, `entity_id`, `mapping_id`,
`contract_id`/version, delivery identity, and source-specific path/format.

### Source-side readiness

For each supplier-style delivery, upload to `/_upload/{delivery_id}/` using a
temporary `.partial` filename. Publish final files and `manifest.json` under
`/ready/{delivery_id}/`, then write `_READY` last. The manifest carries the
registered source/entity/delivery, contract version, exact artifact list,
format/compression, bytes, SHA-256, record count where available, and schema
fingerprint. Static public datasets are controlled bootstrap deliveries; their
simulation schedules are project run times, not claims that publishers send
new data on those schedules.

### ADF responsibilities

ADF uses a five-minute readiness poll for SFTP deliveries. Its pipeline checks
the ready marker, manifest presence/version, safe paths, and expected file
inventory, then Binary Copies original bytes into a delivery-specific Landing
staging path. It records run/delivery status and invokes Databricks preflight.
It does not approve a schema change or publish files directly to committed.

For scheduled PostgreSQL batch extraction and the API, ADF triggers the
source-specific extractor and passes the bounded window/run parameters. The
Databricks PostgreSQL/API extraction task owns source-specific snapshot,
watermark, or pagination logic where specified by `dataset.md` and `task.md`.
For CDC and Kafka replay, ADF does not transport the event data; Debezium,
Kafka, and Databricks Structured Streaming run the continuous/replay path.

The pipeline should be idempotent by delivery/run identity. An ADF retry must
not duplicate or overwrite a committed delivery. It must wait for `_READY`,
preserve source bytes, and retain failed run details in the control plane.

### Databricks preflight and promotion

Preflight reads every artifact in the delivery and checks manifest inventory,
byte counts, hashes, record counts when defined, parser validity, observed
schema fingerprint, active contract/fields, and quality rules. It writes the
observed schema/version and validation evidence to the established control
tables. A match proceeds; drift is classified by the policy in `dataset.md`
and `task.md`. A blocking result is quarantined and never promoted.

On a pass, promote the immutable delivery to the corresponding committed
prefix. Promotion is idempotent by delivery/artifact identity and hash. Only
after that Landing commit does ADF/Databricks start the Auto Loader
`AvailableNow` job for that source/entity. Auto Loader uses the normal durable
checkpoint/schema location and writes the raw source payload to Bronze with
operational lineage columns. Record `ops.bronze_commit` only after the Delta
write succeeds; advance cursors only after required reconciliation succeeds.

**Gate:** A valid sample reaches Bronze once with source payload intact and
lineage metadata populated. A retry does not duplicate it.

## 5. Prove failure and schema-drift recovery

Before full dataset loads, test these cases with controlled small deliveries:

1. No `_READY` or an incomplete manifest: remain uncommitted; no Auto Loader
   discovery.
2. Incorrect checksum/byte count: fail validation; retain evidence and
   quarantine; no Bronze write.
3. Unchanged schema: record preflight pass and promote.
4. Additive optional field: record drift and preserve as raw/rescued data only
   if allowed by the active rule; do not silently approve a typed field.
5. Missing required field, incompatible type, rename, malformed data, and
   incompatible nested change: write drift/validation/quarantine evidence;
   do not promote or advance source progress.
6. Interrupt Bronze after promotion: restart using the same normal checkpoint
   and idempotency identity; only unfinished work is committed.
7. Approve an evolution: create a new contract, field definitions, and schema
   baseline; activate the version only after references validate. Reprocess a
   held delivery through `ops.recovery_request`; never reset the normal
   checkpoint as a routine repair.

**Gate:** Every failure has a traceable run, validation, event, and recovery
record; failed deliveries cannot enter the normal Bronze path; a replay does
not duplicate committed records.

## 6. Load batch sources in this order

Finish one source at a time. For each source, complete bootstrap, source-boundary
delivery, staging, preflight, committed promotion, Auto Loader, Bronze, and
reconciliation before moving to the next. Run bootstrap deliveries manually
with ADF Trigger now; enable recurring schedules only after the path passes.

| Order | Source and preparation | Orchestration / availability | Landing and Bronze |
|---|---|---|---|
| 1 | **WC_F_2016 pilot:** profile the actual local DAT/TXT file; publish a small representative valid sample to Azure Storage SFTP with manifest and `_READY`. | ADF polls `/ready` every 5 minutes; pilot is manually triggered. The ongoing simulation schedule is monthly, first day at 04:00 UTC. | ADF staging → Databricks raw-text/schema preflight → committed → Auto Loader text/raw mode → `bronze.wc_f_2016_raw`. |
| 2 | **REES46 CSV.GZ:** download the seven monthly public files locally; upload one month per immutable SFTP delivery. Keep gzip bytes intact. | Initial files are manually released as deliveries. Operating simulation: supplier window starts first day monthly at 00:00 UTC; files ready by 02:00; ADF polls every 5 minutes. | Staging → validation of file list, gzip/CSV headers, hashes, counts, and schema → committed → Auto Loader CSV `AvailableNow` → `bronze.rees46_events_batch`. |
| 3 | **Open Food Facts JSONL:** download the public JSONL gzip archive, stream-decompress locally, split only on complete JSONL line boundaries, recompress parts, and create a checksum/count manifest for each. Publish three deliveries. | Parts are controlled deliveries at 02:00, 02:20, and 02:40 UTC in the daily 02:00–03:00 window; ADF polls every 5 minutes. | Each part stages and validates independently against the single logical `contract_off_products_v1`; passing parts commit and load with Auto Loader JSON `AvailableNow` to `bronze.off_products_jsonl`. Resolve the part mapping/entity alignment before this phase. |
| 4 | **Amazon Electronics:** download the ten Parquet metadata files as one atomic delivery and `Electronics.jsonl` as a separate reviews delivery; upload both through SFTP with separate manifests. | Metadata simulation: weekly Sunday 03:00 UTC. Reviews: daily 03:30 UTC. ADF polls every 5 minutes during the agreed supplier window. | Separate staging/preflight/commit, checkpoint, and schema location for the metadata and review entities; Auto Loader Parquet/JSON `AvailableNow` to the two corresponding Bronze tables. Do not join in Bronze. |
| 5 | **Full WC_F_2016:** after the pilot, publish the complete original dataset through SFTP and the same raw-text contract. | Monthly, first day at 04:00 UTC; ADF readiness poll every 5 minutes. | Same validated staging → committed → Auto Loader path to `bronze.wc_f_2016_raw`. Do not create typed business columns until the actual layout is verified. |
| 6 | **H&M PostgreSQL initial snapshot:** download the Kaggle CSVs locally; use a secured PostgreSQL client bulk import to seed `retail_src.articles`, `customers`, `transactions`, and `sample_submission`. Validate row counts, columns, candidate keys, and checksums. `sample_submission` is initial only. | One-time controlled snapshot; run ADF manually after PostgreSQL validation. | ADF JDBC extracts a consistent snapshot to staging; Databricks preflight validates and commits it; Auto Loader loads the four entity snapshots to their Bronze tables. Reconcile before setting the initial cursor/high sequence. |
| 7 | **H&M incremental batch:** enable the project-controlled transactional outbox for source inserts/updates/deletes. Capture a bounded `change_seq` window. | Articles/customers daily at 00:30 UTC; transactions hourly at HH:15 UTC. ADF triggers each extraction and passes the high-water mark. | Extract to Landing staging; validate, promote, Auto Loader to Bronze. Re-read the approved overlap and deduplicate by `source/entity/change_seq`. Advance `ops.cursor_state` only after Bronze commit and reconciliation. |
| 8 | **Open Prices API:** no local file upload. ADF starts the Databricks API extractor for the prior completed business window. The extractor writes each raw JSON response page plus generated manifest to staging. | Daily at 03:05 UTC; page size 100, max 2 concurrent calls, connection timeout 10s, request timeout 30s, five transient retries with exponential backoff/jitter, honor HTTP 429 `Retry-After`. | Preflight validates terminal pagination, page inventory/checksums/schema; promote pages to committed; Auto Loader JSON `AvailableNow` to `bronze.open_prices`. Advance API cursor only after all pages commit and reconcile. |

Landing SLA and Bronze SLA targets for each source remain the values in
`dataset.md`/`task.md`. The SLA clock starts from the agreed source-ready or
window boundary and completes at committed Landing or successful Bronze commit
and reconciliation, respectively. Public snapshots do not promise that the
original publishers meet these project simulation targets.

### How ADF decides when and what to ingest

- **SFTP batch:** run a five-minute poll pipeline during the controlled
  availability window. `Get Metadata`/file listing checks `/ready`; a delivery
  is eligible only when `_READY` and a valid manifest exist. Record every
  check, but copy each delivery only once based on delivery ID/revision and
  artifact identity.
- **H&M snapshot:** one-time manual trigger after the four PostgreSQL tables
  pass bootstrap validation.
- **H&M incremental:** clock-based triggers at the approved daily/hourly times.
  The pipeline reads the committed cursor, captures a bounded high-water mark,
  and passes both values to the extractor. It does not use a file-arrival
  trigger.
- **Open Prices:** daily ADF trigger starts the extractor; the extractor
  fetches pages and records pending cursor state. ADF monitors Databricks
  completion and reports success only after the end-to-end contract completes.
- **CDC and Kafka replay:** no ADF data-copy schedule. CDC runs continuously
  from PostgreSQL WAL through Debezium/Kafka into Structured Streaming. REES46
  replay starts only from a controlled replay request and has its own
  `replay_id`, consumer group, and isolated checkpoint.

For the approved clock schedules, use ADF schedule triggers. Store schedule,
availability window, timezone, source/entity, and enabled state in the existing
metadata. Use control-plane delivery/cursor state and explicit reruns for
recovery and backfill. Do not enable repeating triggers during initial testing.

## 7. Bronze contents and guardrails

Bronze is a raw source-aligned Delta layer. Preserve the source columns or raw
payload. Add only technical operational fields needed to trace, deduplicate,
parse, and replay input, such as:

```text
_source_system, _source_entity, _source_record_id,
_source_run_id, _source_delivery_id, _source_delivery_revision,
_source_file_path, _source_file_sha256,
_source_contract_version, _schema_fingerprint,
_ingestion_mode, _ingested_at_utc, _bronze_batch_id,
_record_hash, _source_event_time, _source_change_seq,
_cdc_operation, _kafka_topic, _kafka_partition, _kafka_offset,
_rescued_data, _parse_status
```

Populate only fields available for that ingestion mode; do not fabricate
Kafka, CDC, file, or event-time values for a source that does not provide them.
Preserve legitimate duplicates from source data. Prevent only duplicate
ingestion using the source/file/page/offset identities defined in
`control tables.md` and `task.md`. Do not join sources, apply reporting
aggregations, or add business transformations in Bronze. For WC_F_2016, retain
`raw_line` until its physical DAT/TXT layout is verified.

## 8. Add streaming only after batch foundations pass

### H&M PostgreSQL CDC

First complete and reconcile the initial snapshot and incremental outbox path.
Then configure PostgreSQL logical replication, a least-privilege Debezium
identity, replication slot/publication, and Kafka topics. Validate the
snapshot-to-WAL handoff before enabling continuous consumption. Databricks
Structured Streaming consumes Kafka in 30-second microbatches, writes raw CDC
envelopes to `bronze.hm_cdc_events`, tracks topic/partition offsets and source
LSNs in `ops.cursor_state`, and sends poison events to the DLQ before advancing
their offsets. CDC does not pass through ADF, Landing, or Auto Loader.

### REES46 Kafka replay

Only after the REES46 batch path works, replay a controlled subset of the
historical events through the publisher into `rees46.events.v1`. Preserve the
source event time and add replay ID, source file hash, source record number,
and publish time. Databricks Structured Streaming writes
`bronze.rees46_events_stream`; each replay uses its own consumer group and
checkpoint. This is a replay simulation, not a claim that REES46 supplies live
events. No ADF file-copy or Landing step is used for the Kafka replay path.

## 9. Completion gates and operating checklist

Do not proceed to the next source until the current source has all of these:

- A valid source contract/mapping and verified baseline schema.
- Complete source delivery/extraction evidence and idempotent delivery ID.
- Landing staging integrity and preflight result.
- A committed immutable Landing delivery, or the documented Kafka ingress
  position for streaming.
- A Bronze Delta commit with source lineage and schema version.
- Count/hash reconciliation and correctly advanced cursor/offset state.
- Verified retry/recovery behavior without duplicates or checkpoint reset.
- Alerts and clear terminal run status for pass, retryable failure, and
  quarantined failure.

Start each ingestion by inspecting the previous `ops.ingestion_run`,
`landing_audit.source_delivery`, and cursor state. Start or resume one
delivery; monitor ADF and Databricks run steps; then verify committed Landing,
Bronze commit, reconciliation, and cursor state before starting the next
delivery. For failures, use recorded failure reason and `ops.recovery_request`;
do not manually move a file into committed or edit source bytes to make a run
pass.

## 10. Recommended execution order at a glance

1. Verify the eight populated metadata tables and correct source/entity,
   contract, mapping, and schema references.
2. Create and verify the remaining 14 runtime/audit tables.
3. Finish storage identity, SFTP, Landing separation, compute, Bronze targets,
   checkpoints, and schema locations.
4. Build the WC_F_2016 sample SFTP → ADF → staging → preflight → committed →
   Auto Loader → Bronze path.
5. Prove incomplete-manifest, checksum, schema-drift, retry, and idempotency
   behavior.
6. Complete REES46 CSV, Open Food Facts JSONL, Amazon metadata/reviews, then
   full WC_F_2016 batch deliveries.
7. Provision/seed PostgreSQL and complete H&M initial snapshot, then
   incremental outbox batches.
8. Implement Open Prices API batch extraction.
9. Enable PostgreSQL CDC after snapshot handoff and incremental controls pass.
10. Run REES46 Kafka replay last.

This order gets the repeatable file/batch pattern proven first, then reuses its
contracts, audit, reconciliation, and recovery controls for database/API batch
sources. Streaming follows after source identities, Bronze idempotency, and
operational monitoring have been proven.
