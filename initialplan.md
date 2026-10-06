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
   reads only platform-published SFTP `ready/` deliveries and writes only to
   Landing staging; the Databricks Access Connector/job identity reads SFTP
   `_upload`, publishes to SFTP `ready/` or `rejected/`, validates/promotes
   Landing staging to committed, writes Bronze/quarantine, and owns its
   checkpoint/schema paths.
4. Verify job-capable Databricks compute can read/write only its approved
   external locations and Unity Catalog tables.
5. Configure the Azure Storage SFTP local user and SSH key so the supplier can
   write only under `sftp/_upload/`. Create platform-owned `sftp/ready/` and
   `sftp/rejected/` paths; the supplier must not publish to either path. Give
   the Databricks Access Connector narrowly scoped storage access to validate
   `_upload` and publish to `ready/`/`rejected/`; give ADF read access to
   `ready/` and write access to Landing staging only.
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

For each supplier-style delivery, the supplier uploads only into
`sftp/_upload/{source_id}/{entity_id}/{delivery_id}/`. Each data artifact is
uploaded under a temporary `.partial` name and renamed to its final name only
after that transfer completes. Once every listed artifact has its final name,
the supplier uploads `manifest.json.partial` and renames it to `manifest.json`
as the last delivery operation. The supplier does not write `_READY` and does
not write into `ready/` or `rejected/`.

The manifest is the supplier's declaration that the package is complete; it is
not proof that the bytes are complete. The platform independently verifies the
manifest and every artifact. It checks valid manifest JSON/version, matching
source/entity/delivery and active contract version, safe relative paths, exact
artifact inventory, no unexpected files or `.partial`/`.tmp` artifacts,
declared byte size, SHA-256, record count where defined, and declared schema
fingerprint. It checks file size and modification properties in two
observations separated by five minutes to detect a still-changing file. A file
that fails any check is never published as ready.

The manifest carries the registered source/entity/delivery, contract version,
exact artifact list, format/compression/encoding/parser settings, declared
bytes, SHA-256, record count where available, and schema fingerprint. Static
public datasets are controlled bootstrap deliveries; their simulation
schedules are project run times, not claims that publishers send new data on
those schedules.

### SFTP publication and readiness

The `sftp` container has three separate delivery areas:

```text
sftp/
├── _upload/{source_id}/{entity_id}/{delivery_id}/   # supplier writes here
├── ready/{source_id}/{entity_id}/{delivery_id}/     # platform publishes here
└── rejected/{source_id}/{entity_id}/{delivery_id}/  # platform routes rejects here
```

ADF runs a five-minute scheduled readiness scan during the configured source
availability window. It is not a continuously running 24/7 waiter. For each
delivery found in `_upload`, ADF records/discovers its presence. If
`manifest.json` is present, ADF invokes the publication-validation job. If it
is absent, ADF records `AWAITING_MANIFEST` and exits that delivery's check; the
next scheduled scan checks again. Manifest arrival does not bypass stability,
size, checksum, inventory, or contract checks.

The publication-validation job owns the publish/reject decision. ADF
orchestrates the job and acts on its durable result:

1. If `manifest.json` is absent, keep the delivery in `_upload` as
   `AWAITING_MANIFEST`; do not copy it to Landing.
2. If a manifest-listed artifact is absent, a temporary artifact remains, or
   an artifact is still changing, record `AWAITING_ARTIFACTS` or
   `AWAITING_STABILITY`; retry on the next five-minute scan.
3. If the delivery remains incomplete past its configured availability
   deadline, record `TIMED_OUT`, alert the source owner, and retain it outside
   `ready/` for investigation. Do not ingest it.
4. If the package is complete but permanently invalid (malformed manifest,
   wrong source/entity/contract, unexpected/missing artifact, size/hash/count
   mismatch, unsafe path, or disallowed schema), record `REJECTED_UPLOAD`,
   write the failure evidence and `_REJECTED.json`, and route the package to
   `sftp/rejected/`. Do not create a ready marker or start ingestion.
5. If all publication checks pass, the Databricks publication job moves the
   complete package to a temporary platform-owned directory under
   `sftp/ready/`, finalizes the delivery directory, and writes `_READY.json`
   last. The marker contains delivery ID/revision, manifest hash, publication
   run ID, and publication timestamp. Record `PUBLISHED_READY`. ADF reads this
   immutable package and copies it to Landing staging; it does not decide or
   perform the `_upload` → `ready` publication.

The supplier identity has write/list access only to `_upload`; platform
identities own `ready/` and `rejected/`. Keep the publisher's read/write
permissions scoped to the SFTP paths it needs. ADF's Landing staging write
permission remains separate from Databricks' committed, Bronze, and
quarantine permissions. Do not grant the supplier write access to Landing,
Bronze, checkpoints, schemas, or control tables.

### ADF responsibilities

ADF uses the same five-minute scheduled scan to find `_upload` deliveries for
publication validation and to find published `_READY.json` markers in `ready/`
for ingestion. It does not wait indefinitely inside a single pipeline run.
When no delivery is ready, the run records `NO_READY_DELIVERY` and exits
successfully; a missed supplier deadline is handled by the availability/SLA
monitoring rule. ADF rechecks readiness and idempotency before Binary Copying
the exact ready package (manifest and listed data artifacts) unchanged into a
delivery-specific Landing staging path. It records run/delivery status and
invokes Databricks preflight. It does not approve schema evolution or publish
files directly to Landing committed.

For scheduled PostgreSQL batch extraction and the API, ADF triggers the
source-specific extractor and passes the bounded window/run parameters. The
Databricks PostgreSQL/API extraction task owns source-specific snapshot,
watermark, or pagination logic where specified by `dataset.md` and `task.md`.
For CDC and Kafka replay, ADF does not transport the event data; Debezium,
Kafka, and Databricks Structured Streaming run the continuous/replay path.

The pipeline should be idempotent by delivery/run identity. An ADF retry must
not duplicate or overwrite a committed delivery. It requires a valid `_READY`
publication before copying, preserves source bytes, and retains failed run
details in the control plane. It checks again on the next scheduled run rather
than waiting indefinitely within a single run.

### Databricks preflight and promotion

Preflight reads every artifact in Landing staging and checks the ready marker
and manifest hash again, manifest inventory, byte counts, hashes, record
counts when defined, parser validity, observed schema fingerprint, active
contract/fields, and quality rules. Publication readiness only establishes
that the package is complete and safe to collect; it does not mean every
record is valid or that schema changes are approved. Databricks writes the
observed schema/version and validation evidence to the established control
tables. A match proceeds; drift is classified by the policy in `dataset.md`
and `task.md`. A blocking schema or delivery failure remains uncommitted and
is recorded in quarantine/audit evidence.

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

1. No manifest or a partial upload: remain in `_upload` as awaiting; no ready
   marker and no Landing/Auto Loader discovery.
2. Manifest appears before a file finishes: stability/size/hash check fails;
   retry as awaiting while incomplete, or reject after the delivery deadline
   or a definitive mismatch. No ready marker or ingestion.
3. Missing, extra, or incorrectly named file; invalid manifest; wrong
   source/entity/contract; incorrect checksum/byte count: route to
   `sftp/rejected`, retain evidence and alert; no Landing or Bronze write.
4. `_READY.json` missing or inconsistent: ADF does not copy; record the
   publication validation failure and alert/retry according to its cause.
5. Incorrect checksum/byte count detected again in Landing staging: do not
   promote; retain staged bytes and evidence and create the applicable
   quarantine event; no Bronze write.
6. Unchanged schema: record preflight pass and promote.
7. Additive optional field: record drift and preserve as raw/rescued data only
   if allowed by the active rule; do not silently approve a typed field.
8. Missing required field, incompatible type, rename, malformed data, and
   incompatible nested change: write drift/validation/quarantine evidence;
   do not promote or advance source progress.
9. Interrupt Bronze after promotion: restart using the same normal checkpoint
   and idempotency identity; only unfinished work is committed.
10. Approve an evolution: create a new contract, field definitions, and schema
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
| 1 | **WC_F_2016 pilot:** profile the actual local DAT/TXT file; upload a small representative sample to `sftp/_upload` with final data file(s) and `manifest.json` uploaded last. The platform validates and publishes `_READY.json`. | Pilot publication/ingestion is manually triggered. The ongoing simulation schedule is monthly, first day at 04:00 UTC; ADF scans every 5 minutes during the configured window. | ADF copies ready delivery to staging → Databricks raw-text/schema preflight → committed → Auto Loader text/raw mode → `bronze.wc_f_2016_raw`. |
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

- **SFTP batch:** run a five-minute scheduled scan during the controlled
  availability window. Scan `_upload` for new deliveries and manifests that
  need publication validation; scan `ready` for `_READY.json` deliveries to
  ingest. A manifest triggers a validation attempt, not automatic readiness.
  Missing files, `.partial` files, unstable size/modification properties, or
  retryable source-transfer conditions remain awaiting and are checked on a
  later scan. Permanent package/contract violations are routed to `rejected`
  with evidence and alerting. Copy only a platform-published ready delivery,
  once, based on delivery revision and artifact identity. ADF exits normally
  when there is no ready delivery; deadline monitoring handles late/missing
  arrivals.
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
metadata. The SFTP scan runs every five minutes only during each configured
availability window; it is not a 24/7 polling service. During bootstrap, use
manual ADF runs until publication, Landing, and Bronze gates pass, then enable
the configured recurring windows. Use control-plane delivery/cursor state and
explicit reruns for recovery and backfill.

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
4. Build the WC_F_2016 sample SFTP `_upload` → publication validation →
   `ready`/`_READY.json` → ADF → Landing staging → preflight → committed →
   Auto Loader → Bronze path.
5. Prove manifest-last handling, a manifest arriving while a file is still
   changing, missing/extra artifacts, checksum mismatch, schema drift, retry,
   timeout, rejection, and idempotency behavior.
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

## 11. SFTP implementation steps

Implement this sequence first with one small WC_F_2016 pilot. This section is
the execution checklist for that SFTP vertical slice; it does not ask you to
load REES46, Open Food Facts, Amazon, PostgreSQL, or the API now. The supplier
uploads only to `sftp/_upload`; the platform owns publication to `sftp/ready`
or routing to `sftp/rejected`. `manifest.json` declares that the source
considers a delivery complete, while the platform independently validates the
delivery package before accepting that declaration.

There are three separate results to prove, in order:

1. **SFTP boundary ready:** the supplier identity can write to `_upload` and
   cannot write to platform or analytics paths.
2. **Delivery published:** the platform has checked a complete package and
   written `_READY.json` in `ready`.
3. **Pilot vertical slice complete:** ADF has copied it to Landing staging,
   Databricks has validated and promoted it, and Auto Loader has committed it
   to Bronze once.

Do not build a general multi-source framework before the WC_F_2016 pilot passes
these gates. Reuse the populated metadata and control tables; create or alter
them only if the existing definitions fail a prerequisite documented here.

### 11.1 Prepare paths, identities, and control metadata

1. In the existing `sftp` container, create the platform paths:

   ```text
   _upload/{source_id}/{entity_id}/{delivery_id}/
   ready/{source_id}/{entity_id}/{delivery_id}/
   rejected/{source_id}/{entity_id}/{delivery_id}/
   ```

2. Configure the supplier SFTP local user to list, create, and write only under
   `_upload`. Do not grant supplier access to `ready`, `rejected`, Landing,
   Bronze, checkpoints, schemas, or Unity Catalog control tables.
3. Give the publication job's Databricks identity the minimum SFTP storage
   permissions needed to read `_upload` and publish to `ready`/`rejected`.
   Give ADF read access to `ready` and write access to Landing `staging` only.
   Keep Databricks' committed, Bronze, quarantine, and checkpoint permissions
   separate from ADF's staging permission.
4. In metadata, verify only the pilot's existing source
   `src_legacy_dat_txt_sftp`, entity `ent_wc_store_sales`, mapping, contract
   `contract_wc_store_sales_v1`, and quality rule
   `qr_wc_f_2016_raw_v1`. Confirm the active contract version and schema
   baseline. Do not add guessed DAT/TXT fields: the pilot preserves raw lines
   until the actual WC_F_2016 layout is profiled.
5. Verify the existing runtime/audit tables required by the pipeline are
   present. Do not recreate all 22 tables as part of SFTP setup. Use their
   existing run, delivery, artifact, validation, lease, commit, alert, and
   quarantine records as defined in `control tables.md` and `task.md`.

**Gate 1 — SFTP boundary ready:** using the supplier SFTP account, upload and
rename a harmless test file under `_upload`. Confirm a write attempt to
`ready`, `rejected`, Landing, Bronze, or control data is denied. Separately
confirm the publication job can read `_upload` and publish to `ready` and
`rejected`, ADF can read `ready` and write to Landing `staging`, and Databricks
can validate staging and write only to its approved committed/Bronze paths.
Remove the harmless test file after capturing the access-test evidence.

### 11.2 Freeze the delivery and manifest contract

6. Create a unique `delivery_id` and revision for every delivery. A corrected
   resend uses a new revision; never replace bytes inside a published delivery.
7. For each pilot artifact, record its final relative path and name, format,
   compression, encoding/parser settings only when verified, byte size,
   SHA-256, record count and counting method when defined, and schema
   fingerprint only when the raw-text contract provides one. The manifest
   identifies source, entity, delivery/revision, delivery type, contract
   ID/version, producer batch, source window/business date, and complete/final
   status. Do not invent a delimiter, header, field list, or typed schema for
   WC_F_2016. The exact record count is enforced only when the contract
   declares it.
8. Serialize the manifest using the agreed versioned JSON contract. Paths must
   be relative to the delivery directory; reject absolute paths, traversal
   segments, duplicate artifact paths, unsupported manifest versions, and
   duplicate delivery identities.
9. Once the manifest is received, preserve its original bytes and checksum in
   `landing_audit.delivery_manifest`; store one row per artifact in
   `landing_audit.delivery_artifact`, distinguishing declared from observed
   values. Record each validation outcome in
   `landing_audit.validation_result`. Before receipt, represent the delivery
   in `source_delivery` without fabricating a manifest record.

**Gate:** a known-good WC_F_2016 pilot manifest parses and references exactly
the existing source/entity/contract and the pilot artifact. Its file size,
SHA-256, and any contract-declared counts are reproducible from the local
pilot bytes.

### 11.3 Publish a supplier delivery to `_upload`

10. The supplier creates the delivery folder below `_upload` and uploads each
    artifact under a temporary name such as `file.csv.gz.partial`.
11. After each upload completes, the supplier renames it to its final name.
    This is a transfer-completion convention; the platform still independently
    checks file stability and integrity.
12. After all data artifacts have final names, the supplier uploads
    `manifest.json.partial` and renames it to `manifest.json` as the last
    operation. The supplier must not write anything else to the delivery after
    publishing the final manifest, and must never create `_READY.json`.

For the first pilot, use a small representative subset of the actual WC_F_2016
file, preserving complete physical records. Calculate manifest values from
that exact subset after it is finalized. Do not use the old `store_sales.txt`
file or its schema.

**Gate:** the test delivery contains the manifest and exactly its listed final
artifacts, with no temporary files. A manifest arriving early must not make an
in-progress file eligible. The delivery stays in `_upload` until the platform
publishes it.

### 11.4 Scan and validate `_upload`

13. Create one parameterized ADF pipeline for the pilot, `pl_sftp_publish_scan`,
    with `source_id`, `entity_id`, `mapping_id`, `contract_id`,
    `contract_version`, `availability_deadline_utc`, and `delivery_path` as
    parameters. Use a manual trigger for the pilot first; after it passes,
    attach the five-minute schedule limited to the configured availability
    window.
14. In the pipeline, list child delivery folders under the pilot `_upload`
    prefix. For each folder, register/discover its `delivery_id`, acquire the
    existing work lease, and check for the final `manifest.json`. If it is
    absent, write/retain `AWAITING_MANIFEST`, release the lease, and finish
    that delivery check successfully. Do not wait inside the pipeline run.
15. If the manifest exists, invoke one parameterized Databricks publication
    job, `job_sftp_publish_validate`, passing source/entity/mapping/contract
    identity, delivery ID/revision, manifest URI, ADF run ID, and correlation
    ID. The job returns a durable outcome and evidence references; ADF branches
    on that outcome and does not infer success from the task's process exit
    alone.
16. The publication job validates the manifest syntax/version, safe relative
    paths, source/entity/delivery and contract/version, exact inventory, no
    unexpected or temporary files, producer-declared completeness, declared
    byte sizes, SHA-256, and record count/schema fingerprint only when defined
    by the active contract. It records expected/observed values for each
    check. It does not run business transformations or silently approve
    contract evolution.
17. To catch a manifest that appears before a data upload finishes, compare
    observed file size and modification properties across two scheduled
    observations at least five minutes apart. Missing files or changing
    properties stay in an `AWAITING_*` state. ADF releases the lease and exits;
    the next scan retries. Do not sleep five minutes inside a run.
18. Keep the two validation boundaries distinct: the publication job checks
    delivery completeness and transport integrity; after ADF stages the
    delivery, Databricks preflight checks the observed schema, record parsing,
    active contract fields, schema drift, and quality rules. A syntactically
    valid complete file with invalid records is not an upload-transfer error;
    it proceeds to staging and is handled by the preflight/quarantine policy.

### 11.5 Route incomplete, invalid, and valid deliveries

19. For a missing listed file, `.partial`/`.tmp` artifact, or changing file,
    retain the package in `_upload`, record the matching `AWAITING_*` status,
    and retry on the next scan.
20. When the source availability deadline passes, mark an incomplete delivery
    `TIMED_OUT`, alert the owner, and retain the source bytes/evidence. Do not
    stage or ingest it.
21. For a permanent delivery-package error (malformed manifest, unsafe or
    duplicate path, unexpected artifact, wrong source/entity/contract, or a
    stable, definitive byte/hash/count mismatch), record `REJECTED_UPLOAD`,
    save validation/audit/alert evidence, route it to `sftp/rejected`, and
    write `_REJECTED.json`. Never create `_READY.json` for it.
22. For a passing package, the publication job creates the delivery under
    `sftp/ready`, verifies the final inventory, then writes `_READY.json` last
    with delivery/revision, manifest hash, publication run ID, and UTC
    timestamp. Record `PUBLISHED_READY`. Only the platform identity may write
    this area.
23. If the same delivery revision and manifest hash are already published,
    treat a retry as success without replacing bytes. A different hash for the
    same revision is a conflict; reject it and require a new revision.

**Gate 2 — delivery published:** prove (a) no manifest remains awaiting,
(b) early manifest/missing artifact stays awaiting, (c) stable wrong checksum
goes to `rejected` with evidence, and (d) the valid pilot appears once in
`ready` with `_READY.json` written after the verified files.

### 11.6 Copy ready delivery to Landing staging

24. Create a second ADF pipeline, `pl_sftp_ready_to_landing`, which lists only
    `sftp/ready`. For each candidate, require `_READY.json` and `manifest.json`
    and verify the delivery identity and manifest hash before starting Copy. If
    there is no candidate, record `NO_READY_DELIVERY` and succeed; deadline
    monitoring, not an empty poll, produces a late-delivery alert.
25. Configure Binary Copy with explicit source and sink paths. Copy only the
    manifest-listed data artifacts plus the manifest to
    `landing/staging/{environment}/{source}/{entity}/delivery_id={delivery_id}/`.
    Do not copy `_READY.json` as a data artifact. Preserve the bytes and source
    path; do not delete the SFTP delivery after success.
26. Before copy, check `source_delivery`/`delivery_artifact` for prior state and
    use the delivery lease. After copy, record staged URI, observed size/hash,
    ADF run ID, and copy result. A retry verifies the same staged artifact and
    never overwrites a committed delivery with different content.
27. On copy failure, mark the run/step retryable, preserve the ready source,
    release/expire the lease safely, and retry from the same delivery identity.
    When ADF reports copy success, call it `STAGED`; do not call it `COMMITTED`.

### 11.7 Validate staging and promote to committed

28. Configure `job_sftp_landing_preflight` with `run_id`, delivery/source/
    entity IDs, contract/mapping versions, manifest URI, and staging URI. It
    rechecks staged bytes and manifest, profiles observed raw-text schema
    without inventing fields, applies the active contract and quality rules,
    writes schema/validation evidence, and returns a durable PASS/FAIL result.
29. On PASS, the Databricks promotion task moves/copies the immutable delivery
    to `landing/committed/{environment}/{source}/{entity}/business_date={date}/delivery_id={delivery_id}/`,
    verifies the destination, and writes its commit marker last. Record
    `COMMITTED` only after that completes.
30. On schema/parse/contract/data failure, keep it out of `committed`; preserve
    staging and evidence, create the applicable schema-drift/quarantine
    records, and alert. Individual invalid records follow the existing
    quality-rule failure action; do not reject an otherwise complete SFTP
    transfer merely because row-level quality failed.
31. A delivery in SFTP `ready` is complete and transport-valid; only a passing
    Landing preflight can enter `committed`. Auto Loader reads committed only.

### 11.8 Load Bronze and prove recovery

32. Run Auto Loader `AvailableNow` for the WC_F_2016 committed prefix using its
    dedicated checkpoint and schema location. Confirm it does not scan SFTP,
    Landing staging, manifests, or quarantine.
33. Preserve each raw line in `bronze.wc_f_2016_raw` with the approved
    operational lineage columns. Record `ops.bronze_commit` after the Delta
    transaction succeeds; reconcile counts/hashes and update the delivery/run
    state only after reconciliation passes.
34. Repeat the same delivery run. Confirm the delivery/artifact identity and
    normal checkpoint prevent duplicate Bronze rows. Do not reset the
    checkpoint to recover this retry.
35. Test only the WC_F_2016 pilot's SFTP failure cases before using the full
    file: missing manifest; manifest arriving before file completion; missing
    or extra file; stable wrong checksum; missing or inconsistent `_READY`; ADF
    copy interruption; and Landing schema/parse failure. For each, verify the
    expected awaiting/rejected/quarantine/retryable outcome, evidence, alert,
    and that invalid deliveries do not enter committed or Bronze.

**Gate 3 — pilot vertical slice complete:** the valid WC_F_2016 sample passes
from supplier `_upload`, platform validation/publication, ADF copy to Landing
staging, Databricks preflight, Landing committed, Auto Loader, and one
reconciled Bronze Delta commit. The retry creates no duplicate. Only after this
gate passes should you publish the full WC_F_2016 delivery; remaining SFTP
datasets follow the existing source order in section 6.

### 11.9 Document alignment before implementation

The current `dataset.md` and `task.md` describe a different readiness owner:
they say the supplier publishes final files to `ready` and writes `_READY`,
while this section's approved flow gives publication to the platform after
validation. Use this section's latest approved platform-owned publication flow
for the SFTP pilot. Reconcile those older references before creating linked
services/triggers so the implementation and runbooks do not instruct the
supplier to write into a platform-owned path.
