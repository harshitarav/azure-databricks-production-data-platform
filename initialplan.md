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

Implement this sequence first with the small WC_F_2016 pilot. The supplier
uploads only to `sftp/_upload`; the platform owns publication to `sftp/ready`
or routing to `sftp/rejected`. `manifest.json` declares that the source
considers a delivery complete, while the platform independently validates the
files before accepting that declaration.

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
4. Confirm the source, entity, active mapping, contract version, field
   definitions, schema baseline, and applicable quality rules exist in the
   metadata tables. Use the existing IDs; do not create a new contract for a
   delivery revision.
5. Confirm the corresponding `landing_audit`, `ops`, `dq`, and `quarantine`
   control tables are available. Define and use the existing delivery and
   artifact idempotency identities, run/step records, lease scope, alerting,
   and terminal states from `control tables.md` and `task.md`.

**Gate:** the supplier can write a test file under `_upload` but cannot write
to `ready` or `rejected`; ADF can read `ready` and write only to Landing
`staging`; the publication job can read `_upload` and publish to its approved
paths.

### 11.2 Freeze the delivery and manifest contract

6. Create a unique `delivery_id` and revision for every delivery. A corrected
   resend uses a new revision; never replace bytes inside a published delivery.
7. For each artifact, agree on its final relative path and name, format,
   compression, encoding/parser settings, byte size, SHA-256, record count and
   counting method when available, and schema fingerprint where applicable.
   The manifest also identifies source, entity, delivery/revision, delivery
   type, contract ID/version, producer batch, source window, and final/complete
   status. The exact record count is validated only when the source contract
   declares it.
8. Serialize the manifest using the agreed versioned JSON contract. Paths must
   be relative to the delivery directory; reject absolute paths, traversal
   segments, duplicate artifact paths, unsupported manifest versions, and
   duplicate delivery identities.
9. Preserve the original manifest bytes and checksum in
   `landing_audit.delivery_manifest`; store one row per artifact in
   `landing_audit.delivery_artifact`, distinguishing declared from observed
   values. Record each validation outcome in
   `landing_audit.validation_result`.

**Gate:** a known-good test manifest parses and references exactly the intended
source/entity/contract and all artifacts. Its fields agree with the approved
contract and `control tables.md`.

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

**Gate:** the test delivery contains the manifest and exactly its listed final
artifacts, with no temporary files. A manifest arriving early must not make an
in-progress file eligible.

### 11.4 Scan and validate `_upload`

13. Configure an ADF schedule trigger to run the SFTP readiness scan every five
    minutes only during the entity's configured source-availability window.
    Bootstrap tests use a manual ADF trigger. This is scheduled polling, not a
    continuously running 24/7 pipeline.
14. At each run, create the normal `ops.ingestion_run`/`ops.run_step` audit
    records, discover delivery folders, and acquire the configured
    `ops.work_lease` so overlapping scans cannot publish the same delivery.
15. If `manifest.json` is absent, record `AWAITING_MANIFEST` and finish that
    delivery check. If it is present, invoke the parameterized Databricks
    publication-validation job with source/entity/delivery/revision, manifest
    URI, and the ADF run/correlation identifiers.
16. Validate manifest syntax/version, safe paths, source/entity/delivery and
    contract/version, exact inventory, no unexpected or temporary files, and
    declared format/parser settings. Compare observed byte sizes and SHA-256
    to declarations; compare record counts/schema fingerprint when defined by
    the contract.
17. To handle a manifest that appears before its data upload finishes, compare
    observed file size and modification properties in two observations at
    least five minutes apart. Missing artifacts or changing properties are
    retryable awaiting states; do not move, stage, or ingest the delivery.
18. Record expected/observed values, outcome, severity, decision, and evidence
    URI for every check. ADF does not treat file existence or the manifest's
    `COMPLETE` claim as sufficient readiness.

### 11.5 Route incomplete, invalid, and valid deliveries

19. For a missing manifest, missing listed file, `.partial`/`.tmp` file, or
    changing file, retain the delivery in `_upload`, record the appropriate
    `AWAITING_*` status, and retry at the next five-minute scan.
20. When the configured source-availability deadline passes, mark a still
    incomplete delivery `TIMED_OUT`, create an alert, and leave its bytes and
    evidence available for investigation. Do not ingest it.
21. For a permanent package error—malformed manifest, unsafe/duplicate path,
    unexpected artifact, wrong source/entity/contract, definitive checksum or
    count mismatch—record `REJECTED_UPLOAD`, create validation/audit/alert
    evidence, route the package to `sftp/rejected`, and write
    `_REJECTED.json`. Never create `_READY.json` for it.
22. For a passing package, publish an immutable directory under `sftp/ready`.
    The publication job copies/moves the validated manifest and artifacts,
    verifies the destination inventory, and writes `_READY.json` last with
    delivery/revision, manifest hash, publication run ID, and UTC timestamp.
    Record `PUBLISHED_READY`. Only the platform identity may write this area.
23. Make publication idempotent: if the same delivery revision and manifest
    hash are already published, treat the retry as success without replacing
    its bytes. A different hash for an existing delivery revision is a
    conflict; reject it and require a new revision.

**Gate:** incomplete test deliveries remain awaiting; a deliberately incorrect
manifest is rejected; a valid delivery reaches `ready` exactly once with its
marker written after all files.

### 11.6 Copy ready delivery to Landing staging

24. During the same configured five-minute schedule, ADF scans `ready` for
    `_READY.json`. If no ready delivery exists, record `NO_READY_DELIVERY` and
    finish successfully. Alert only when the agreed arrival deadline/SLA is
    missed.
25. Before copying, re-read `_READY.json` and `manifest.json`; verify delivery
    ID/revision/hash, listed artifact inventory, and that this delivery has not
    already been staged or committed. A missing/inconsistent marker is a
    publication failure: do not copy; record evidence and alert/retry as
    appropriate.
26. Use ADF Binary Copy to copy the original manifest and listed source bytes
    unchanged to the delivery-specific Landing `staging` prefix. Do not delete
    or mutate the source package as part of a successful copy.
27. Record observed staged paths, bytes, and checksums, plus the ADF run ID, in
    the delivery/artifact/run controls. A retry must resume or verify the same
    delivery identity without overwriting a committed delivery.

### 11.7 Validate staging and promote to committed

28. Invoke Databricks preflight against the complete staged delivery. Recheck
    manifest hash/inventory, bytes/checksums/counts, parseability, observed
    schema and fingerprint, active contract fields, schema-drift policy, and
    applicable quality rules.
29. If the delivery has an approved schema and passes blocking checks, record
    successful validation and promote the immutable staged package to the
    corresponding `landing/committed` prefix. Write the committed publication
    marker only after promotion is complete.
30. If schema, parse, contract, or data checks fail, do not promote. Preserve
    staged bytes and evidence; write `schema_drift_event` and/or
    `quarantine_event` with reason, location, validation, and recovery links;
    alert the owner. Auto Loader must not discover this delivery.
31. Distinguish package readiness from data validity: publication to SFTP
    `ready` means the transfer package is complete and matches its manifest;
    it does not approve every record or schema change. Landing `committed`
    requires the full Databricks preflight to pass.

### 11.8 Load Bronze and prove recovery

32. Configure Auto Loader to monitor only the committed prefix, with the
    existing per-source/entity schema location and durable checkpoint. Use the
    approved `AvailableNow` run pattern for these batch deliveries. It must
    never watch SFTP, Landing staging, or quarantine.
33. Write source-aligned Bronze Delta data with only the approved operational
    lineage fields. Record `ops.bronze_commit` after the Delta transaction
    succeeds; reconcile source/manifest → Landing → Bronze counts/hashes, then
    advance any cursor only after the required reconciliation passes.
34. Verify a retry after publication, after staging, and after Bronze commit.
    The same delivery/artifact identity must not be duplicated; the normal
    checkpoint must not be reset for routine recovery.
35. Run controlled failure exercises: no manifest; manifest before file
    completion; absent, extra, or partial artifact; size/hash/count mismatch;
    invalid contract/schema; missing `_READY.json`; ADF interruption during
    copy; Databricks preflight failure; and Bronze interruption. Confirm each
    case ends in an awaiting, rejected, quarantined, retryable, or committed
    state with traceable run/event/evidence and the expected alert.

**SFTP completion gate:** the WC_F_2016 pilot passes from supplier `_upload`
through platform publication, ADF Landing staging, Databricks validation,
Landing committed, Auto Loader, and one idempotent Bronze Delta commit. All
failure exercises preserve evidence and keep invalid data out of committed
Landing and Bronze.
