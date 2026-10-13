# Operational Control Tables: Source to Serving

## Purpose and scope

This is the physical operational-state specification for the consolidated [architecture](docs/architecture/architecture.md), revised in place on 2026-10-13. Original metadata/evidence concepts and columns are retained below with explicit migration additions. The active design has **26 logical tables** in the separate PostgreSQL database platform_control. One authenticated Control API owns writes; UC Delta tables are read-only reporting projections. This specification is documentation, not executed DDL.

Original 22-table export: seven populated metadata tables and no runtime evidence. Import these validated records; do not recreate source registration manually. Existing phase2_runtime_control_tables.sql is a historical Delta implementation artifact, unchanged and not the current migration. Databricks informational PK/FK declarations do not enforce relational concurrency.

Canonical SQL qualification is platform_control.<schema>.<table> (database.schema.table notation in this specification; PostgreSQL connections select database and SQL refers to schema.table). UC reporting projection qualification is retail_de_{env}.<schema>.<table>. Environment is an explicit SQL key, not a separate copy per source. Native Delta log, Spark checkpoints and Kafka Connect offsets remain authorities for their data/transport commits.

`ops.asset_commit` generalizes the historical `ops.bronze_commit`: migrate bronze_commit_id→asset_commit_id, bronze_table_fqn→target_fqn and mark layer=BRONZE. Preserve original exported evidence and provide a read-only legacy projection where necessary; this is one commit ledger, not two active authorities. Four additional concepts are pipeline_config, data_product, asset_dependency and product_release, required for multimode schedules and downstream release dependencies.

## Source schema and contract ownership

The source owner provides a versioned contract: schema or DDL, field meaning, types, nullability, required fields, keys where supported, format and encoding, delivery cadence, and change notice. For public datasets without a source team, the project owner creates and approves the initial contract from profiling the controlled source data.

The platform stores the approved contract in `metadata.source_contract` and its field rules in `metadata.source_contract_field`. The platform profiles each delivery and stores an immutable observed schema in `metadata.schema_version`, then records differences in `metadata.schema_drift_event`. An incoming schema change is not accepted merely because ADF or Auto Loader detects it. It must pass the active contract and validation rules before Landing/Bronze commit. Schema history and drift evidence are retained for audit and recovery.

## Table inventory

### Metadata schema — original 8 plus 3 integrated extensions

#### `retail_de_dev.metadata.source_config`
Purpose: configuration and ownership for each source system.

Columns: `source_id` (logical PK), `source_name`, `source_type`, `source_owner`, `owner_contact`, `default_timezone`, `data_classification`, `connection_secret_ref`, `status`, `created_at`, `updated_at`.

#### `retail_de_dev.metadata.entity_config`
Purpose: ingestion and service-level configuration for each source entity.

Columns: `entity_id` (logical PK), `source_id` (logical FK), `entity_name`, `entity_description`, `ingestion_mode`, `schedule_cron`, `availability_window_start`, `availability_window_end`, `landing_sla_minutes`, `bronze_sla_minutes`, `freshness_slo_minutes`, `watermark_policy`, `cursor_type`, `active_contract_version`, `is_enabled`, `created_at`, `updated_at`.

#### `retail_de_dev.metadata.source_contract`
Purpose: versioned contract header provided or approved by the source owner.

Columns: `contract_id` (logical PK), `source_id`, `entity_id`, `contract_version`, `contract_status`, `provider_contract_ref`, `contract_document_path`, `expected_schema_fingerprint`, `permitted_formats`, `permitted_compressions`, `checksum_algorithm`, `record_count_required`, `effective_from`, `effective_to`, `approved_by`, `approved_at`, `created_at`.

#### `retail_de_dev.metadata.source_contract_field`
Purpose: expected field-level schema and validation rules for a contract version.

Columns: `contract_field_id` (logical PK), `contract_id` (logical FK), `field_path`, `ordinal_position`, `field_name`, `expected_data_type`, `nullable`, `required`, `precision`, `scale`, `semantic_role`, `allowed_values_json`, `regex_pattern`, `min_value`, `max_value`, `is_key_candidate`, `created_at`.

#### `retail_de_dev.metadata.source_mapping`
Purpose: versioned extraction, parsing, Landing, and Bronze routing configuration.

Columns: `mapping_id` (logical PK), `source_id`, `entity_id`, `mapping_version`, `contract_id`, `extraction_spec_json`, `staging_prefix`, `landing_prefix`, `target_fqn`, `file_format`, `parser_options_json`, `checkpoint_scope`, `effective_from`, `effective_to`, `status`.

#### `retail_de_dev.metadata.schema_version`
Purpose: immutable expected or observed schema snapshots and fingerprints.

Columns: `schema_version_id` (logical PK), `source_id`, `entity_id`, `contract_id`, `schema_origin`, `schema_fingerprint`, `canonical_schema_json`, `parent_schema_version_id`, `compatibility_class`, `observed_delivery_id`, `observed_at`, `created_at`.

#### `retail_de_dev.metadata.schema_drift_event`
Purpose: audit every detected difference between the active contract and an incoming schema.

Columns: `drift_event_id` (logical PK), `source_id`, `entity_id`, `delivery_id`, `expected_schema_version_id`, `observed_schema_version_id`, `field_path`, `drift_type`, `expected_value`, `observed_value`, `severity`, `disposition`, `decision_by`, `decision_at`, `evidence_path`, `run_id`, `created_at`.

#### `retail_de_dev.metadata.quality_rule`
Purpose: versioned validation rules applied at readiness, Landing, and Bronze gates.

Columns: `rule_id` (logical PK), `source_id`, `entity_id`, `contract_id`, `rule_name`, `validation_stage`, `rule_type`, `rule_expression`, `threshold`, `severity`, `failure_action`, `effective_from`, `effective_to`, `status`.

### Landing audit schema — 4 tables

#### `retail_de_dev.landing_audit.source_delivery`
Purpose: one immutable delivery revision, snapshot extract or API request set; streaming microbatch evidence belongs in asset_commit.

Columns: `delivery_id` (logical PK), `source_id`, `entity_id`, `delivery_revision`, `delivery_type`, `producer_batch_id`, `source_window_start`, `source_window_end`, `business_date`, `manifest_uri`, `declared_schema_fingerprint`, `status`, `discovered_at`, `ready_at`, `committed_at`, `run_id`, `error_code`, `error_message`.

#### `retail_de_dev.landing_audit.delivery_manifest`
Purpose: immutable received or generated manifest and its validation status.

Columns: `manifest_id` (logical PK), `delivery_id` (logical FK), `manifest_version`, `manifest_uri`, `manifest_sha256`, `contract_id`, `contract_version`, `declared_file_count`, `declared_record_count`, `declared_schema_fingerprint`, `manifest_payload_json`, `validation_status`, `received_at`.

#### `retail_de_dev.landing_audit.delivery_artifact`
Purpose: one physical file, API page, or persisted source object within a delivery.

Columns: `artifact_id` (logical PK), `delivery_id` (logical FK), `artifact_sequence`, `artifact_type`, `source_uri`, `staging_uri`, `landing_uri`, `file_name`, `file_format`, `compression`, `declared_bytes`, `observed_bytes`, `declared_sha256`, `observed_sha256`, `declared_record_count`, `observed_record_count`, `observed_schema_version_id`, `artifact_identity`, `status`, `autoloader_discovered_at`, `bronze_committed_at`, `quarantine_reason_code`.

`artifact_identity` is the file-level idempotency identity, derived deterministically from the delivery revision, canonical source path, and content hash. The same delivery artifact must not be committed twice.

#### `retail_de_dev.landing_audit.validation_result`
Purpose: record each readiness, schema, checksum, count, format, and contract validation.

Columns: `validation_id` (logical PK), `validation_stage`, `source_id`, `entity_id`, `delivery_id`, `artifact_id`, `run_id`, `rule_id`, `validation_category`, `expected_value`, `observed_value`, `outcome`, `severity`, `decision`, `evidence_uri`, `evaluated_at`.

### Operations schema — original concepts plus product release

#### `retail_de_dev.ops.ingestion_run`
Purpose: end-to-end orchestration run and its final state.

Columns: `run_id` (logical PK), `pipeline_name`, `source_id`, `entity_id`, `ingestion_mode`, `trigger_type`, `mapping_id`, `mapping_version`, `contract_id`, `contract_version`, `schema_version_id`, `recovery_request_id`, `attempt_number`, `status`, `started_at`, `heartbeat_at`, `completed_at`, `executor_reference`, `correlation_id`, `error_code`, `error_message`.

`mapping_id`/`mapping_version`, `contract_id`/`contract_version`, and
`schema_version_id` pin the exact configuration selected when the run starts.
Do not change the run's pinned versions if a newer contract is activated while
the run is in progress. A run can cover multiple deliveries; delivery identity
is recorded at `source_delivery` and `run_step` grain rather than as one
ambiguous run-level `delivery_id`.

#### `retail_de_dev.ops.run_step`
Purpose: status, counts, timing, and errors for each step within a run.

Columns: `step_run_id` (logical PK), `run_id` (logical FK), `step_name`, `source_id`, `entity_id`, `delivery_id`, `artifact_id`, `attempt_number`, `status`, `started_at`, `completed_at`, `input_record_count`, `output_record_count`, `error_code`, `error_message`, `compute_reference`.

#### `retail_de_dev.ops.run_event`
Purpose: append-only technical event and audit log for orchestration and recovery.

Columns: `event_id` (logical PK), `run_id`, `step_run_id`, `event_timestamp`, `event_type`, `severity`, `status`, `message`, `event_payload_json`, `correlation_id`.

#### `retail_de_dev.ops.cursor_state`
Purpose: committed and pending progress for incremental batch, PostgreSQL CDC, Kafka, and API extraction.

Columns: `source_id`, `entity_id`, `cursor_type`, `partition_scope` (together logical PK), `pending_position_json`, `committed_position_json`, `last_successful_run_id`, `last_committed_at`, `status`, `row_version`, `updated_at`.

Batch capture committed position advances only after its corresponding Bronze commit and capture reconciliation. Separate Silver/Gold processing cursors advance only after their own gates; live Kafka/CDC native checkpoints/offsets are never overridden from SQL. Source-specific positions are represented by `cursor_type` and the position JSON; this single table replaces separate watermark, CDC offset, Kafka offset, and API cursor tables.

#### `retail_de_dev.ops.asset_commit`
Purpose: idempotency and commit evidence for Bronze, Silver, Gold and recovery outputs; actual Delta commits remain data authority.

Columns: `asset_commit_id` (PK after versioned migration), `source_id`, `entity_id`, `delivery_id`, `artifact_id`, `run_id`, `bronze_table_fqn`, `delta_table_version`, `ingestion_identity`, `source_change_identity_json`, `input_record_count`, `inserted_count`, `deduplicated_count`, `rejected_count`, `schema_version_id`, `outcome`, `committed_at`, `reconciliation_status`.

#### `retail_de_dev.ops.work_lease`
Purpose: avoid concurrent processing of the same delivery, entity, or stream partition.

Columns: `lease_id` (logical PK), `source_id`, `entity_id`, `work_scope`, `owner_run_id`, `lease_token`, `acquired_at`, `expires_at`, `heartbeat_at`, `status`, `row_version`.

#### `retail_de_dev.ops.recovery_request`
Purpose: controlled backfill, reprocessing, and Kafka replay requests.

Columns: `recovery_request_id` (logical PK), `recovery_type`, `source_id`, `entity_id`, `requested_scope_json`, `reason`, `requested_by`, `requested_at`, `status`, `isolated_checkpoint_path`, `target_run_id`, `completed_at`.

#### `retail_de_dev.ops.alert_event`
Purpose: auditable alert history, deduplication, acknowledgement, and resolution.

Columns: `alert_id` (logical PK), `run_id`, `delivery_id`, `artifact_id`, `drift_event_id`, `alert_type`, `severity`, `alert_message`, `deduplication_key`, `channel`, `azure_monitor_rule`, `status`, `emitted_at`, `acknowledged_by`, `acknowledged_at`, `resolved_at`.

`alert_message` preserves the human-readable alert that was emitted; detailed
technical context remains linked through the run, delivery, artifact, or drift
event and its evidence records.

### Data quality schema — 1 table

#### `retail_de_dev.dq.reconciliation_result`
Purpose: compare source, Landing, and Bronze counts or hashes for completeness.

Columns: `reconciliation_id` (logical PK), `source_id`, `entity_id`, `delivery_id`, `run_id`, `check_type`, `source_count`, `landing_count`, `bronze_count`, `source_hash`, `landing_hash`, `bronze_hash`, `variance`, `tolerance`, `outcome`, `evidence_uri`, `evaluated_at`.

### Quarantine schema — 1 table

#### `retail_de_dev.quarantine.quarantine_event`
Purpose: track rejected files, records, or deliveries and their resolution.

Columns: `quarantine_id` (logical PK), `source_id`, `entity_id`, `delivery_id`, `artifact_id`, `run_id`, `layer`, `reason_code`, `severity`, `quarantine_uri`, `original_identity`, `original_sha256`, `schema_drift_event_id`, `validation_id`, `record_count`, `status`, `disposition`, `recovery_request_id`, `quarantined_at`, `resolved_by`, `resolved_at`.

## Idempotency and failure recovery lookup

| Ingestion path | Primary evidence used to avoid duplicates and find unfinished work |
|---|---|
| SFTP and ADF files | `source_delivery`, `delivery_manifest`, `delivery_artifact`, `asset_commit` |
| Auto Loader | `delivery_artifact.artifact_identity`, its durable checkpoint, and `asset_commit.ingestion_identity` |
| PostgreSQL incremental extraction | `cursor_state.committed_position_json` and `asset_commit` |
| PostgreSQL CDC through Debezium and Kafka | `cursor_state` with the appropriate `cursor_type`, plus `asset_commit.source_change_identity_json` |
| Kafka replay | `recovery_request`, an isolated replay checkpoint, cursor position, and Bronze commit identity |
| API extraction | `source_delivery`, one `delivery_artifact` per persisted page, and `cursor_state` for page/cursor progress |

A failed run remains visible in ingestion_run/run_step with append-only run_event. Captured source state is independent from Silver/Gold progress. File/run evidence is reconciled against ADLS manifests and Delta history; stream cursor rows are native-position observations, not manual source restart overrides. Recovery itself retains its request/attempt/idempotency keys after acknowledgment loss.

## Implementation notes

- Tables are shared across sources; include `source_id` and `entity_id` on operational records where source/entity attribution applies.
- Store secrets only in Key Vault. `connection_secret_ref` is a reference, never a secret value.
- Keep schema snapshots and contract versions immutable. New approvals create new versions rather than rewriting historical evidence.
- Use append-only event and evidence records where audit history matters. Update current state with optimistic concurrency or a lease token so overlapping runs cannot silently overwrite one another.
- Maintain retention and access policies for each schema according to the project’s approved governance and data classification rules.
- These control tables support Source → Landing → Bronze. They do not define Silver or Gold transformation schemas.

## Physical schema conventions and enforced integrity

All tables add env TEXT NOT NULL CHECK dev/test/prod; registry PKs/FKs include env. ID/reference/hash/URI/status/name fields are TEXT; timestamps are TIMESTAMPTZ; business_date DATE; counts/bytes/ordinals/sequences/Delta versions BIGINT; revisions/attempts/mapping/rule versions INT; flags BOOLEAN; decimal thresholds/variance/min/max NUMERIC(38,10); *_json fields JSONB. manifest_version is TEXT (e.g. 1.0), contract_version TEXT. expected_value/observed_value remain TEXT for redacted display with metrics_json for structured numbers. Canonical schemas and manifest payloads are JSONB plus immutable original-byte URI/hash.

Each table's inherited **Columns** list above plus its **Added columns** list below is the complete active schema. Fields not listed are not implicitly invented. Names/types in the following per-table specification override an obsolete historical type. Requiredness: PK/UK constituents, env, state/version pins needed by an active step, critical identity/hash/evidence and the listed timestamps are NOT NULL; other source-specific optional fields are nullable, never populated with fabricated values. Add created_by TEXT to immutable registry/evidence rows; add updated_by TEXT and updated_at TIMESTAMPTZ + row_version BIGINT DEFAULT 0 to mutable state where not already present. run_event/validation/commit evidence are append-only; operational current state uses CAS.

Global checks: revisions/attempts/versions positive; counts/bytes nonnegative; SHA-256 exactly 64 hex normalized; end>=start; statuses from architecture Section 16.1; no decreasing committed sequence within a source generation; no active contract effective interval overlap. All FKs refer to enforced PK/UK and use RESTRICT deletion; archive/purge keeps referenced evidence. Index FK columns and common (env,status,updated_at) on queues, (env,source_id,entity_id,time) on histories. JSONB is used for bounded source-specific state, not arbitrary unvalidated code execution.

Ownership: metadata steward via reviewed migration/activation; runs/cursors/leases/deliveries and evidence Control API; release writer submits native manifest/Delta evidence to API; operators only bounded recovery actions; projections read-only. Registry/history retained project+365d; run/evidence 365d; current cursors/leases/config and active releases never aged out. Payload retention is architecture Section 17.2.4.

## Enforced per-table migration specification

### platform_control.metadata.source_config

- Grain: source_id. Enforced PK: (env,source_id). UK: (env,source_name).
- FK: none. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: status lifecycle DRAFT/ACTIVE/DISABLED; configuration activation reviewed. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: connection_secret_ref must remain secret reference; data_classification is NOT NULL before activation.

### platform_control.metadata.entity_config

- Grain: entity_id. Enforced PK: (env,entity_id). UK: (env,source_id,entity_name).
- FK: (env,source_id)→source_config. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: logical entity, not one schedule per mode. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: logical_entity_id TEXT, entity_role TEXT, source_generation TEXT; legacy ingestion_mode/schedule fields are compatibility display, authoritative mode schedule is pipeline_config.

### platform_control.metadata.source_contract

- Grain: contract_id, contract_version. Enforced PK: (env,contract_id,contract_version). UK: (env,source_id,entity_id,contract_version).
- FK: source/entity→registries. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: immutable version DRAFT/VALIDATED/ACTIVE/RETIRED; active interval changes audited. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: entity_scope_json JSONB, contract_rule_fingerprint TEXT, fingerprint_algorithm TEXT, producer_control_spec_json JSONB, retention_policy_json JSONB, acknowledgment_spec_json JSONB; permitted_formats/permitted_compressions JSONB arrays.

### platform_control.metadata.source_contract_field

- Grain: contract_field_id. Enforced PK: (env,contract_field_id). UK: (env,contract_id,contract_version,layout_id,field_path).
- FK: (env,contract_id,contract_version)→source_contract. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: immutable per contract/layout. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: contract_version TEXT, layout_id TEXT NOT NULL (common for truly common fields), units TEXT, aggregation_semantics TEXT, classification TEXT; precision/scale INT, nullable/required/is_key_candidate BOOLEAN.

### platform_control.metadata.source_mapping

- Grain: mapping_id,mapping_version. Enforced PK: (env,mapping_id,mapping_version). UK: (env,pipeline_id,mapping_version).
- FK: (env,pipeline_id,pipeline_config_version)→pipeline_config; contract_id/version→source_contract; source/entity→registries. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: immutable routing/parsing versions, activated via interval. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: pipeline_id TEXT, pipeline_config_version INT, contract_version TEXT, logical_entity_id TEXT, layout_specs_json JSONB, envelope_version INT, target_generation TEXT; old bronze_table_fqn retained for capture routing, no reuse as generic Gold destination.

### platform_control.metadata.schema_version

- Grain: schema_version_id. Enforced PK: (env,schema_version_id). UK: (env,source_id,entity_id,contract_id,contract_version,layout_id,schema_origin,schema_fingerprint,fingerprint_algorithm).
- FK: contract_id/version→source_contract; parent ID→schema_version. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: APPROVED_BASELINE/OBSERVED immutable snapshots; repeated identical schema reuses snapshot. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: contract_version TEXT, layout_id TEXT NOT NULL DEFAULT common, fingerprint_algorithm TEXT, contract_rule_fingerprint TEXT, observed_delivery_version_id TEXT; observed_delivery_id legacy display.

### platform_control.metadata.schema_drift_event

- Grain: drift_event_id. Enforced PK: (env,drift_event_id). UK: (env,delivery_version_id,field_path,drift_type,expected_schema_version_id,observed_schema_version_id).
- FK: delivery_version→source_delivery; expected/observed→schema_version; run→ingestion_run. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: 13 classification/decision/reprocess states; immutable evidence + audited decision. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: delivery_version_id TEXT, rule_version INT, downstream_impact_json JSONB, recovery_request_id TEXT, decision_reason TEXT; evidence_path unchanged.

### platform_control.metadata.quality_rule

- Grain: rule_id,rule_version. Enforced PK: (env,rule_id,rule_version). UK: (env,rule_id,rule_version).
- FK: source/entity when scoped→registries; contract/version when scoped→source_contract; product/version when scoped→data_product. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: immutable definition/effective activation, owner steward/product. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: rule_version INT, contract_version TEXT, product_id TEXT, product_version INT, layer TEXT, owner_role TEXT, parameters_json JSONB, thresholds_json JSONB, implementation_reference TEXT, corrective_action TEXT, test_id TEXT; numeric threshold is legacy simple threshold.

### platform_control.landing_audit.source_delivery

- Grain: delivery_version_id. Enforced PK: (env,delivery_version_id). UK: (env,source_id,entity_id,delivery_id,delivery_revision).
- FK: source/entity→registries; run→ingestion_run. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: one immutable revision with mutable guarded status; no delivery_id-only PK. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: delivery_version_id TEXT, source_version TEXT, source_generation TEXT, snapshot_group_id TEXT, part_index INT, expected_parts INT, manifest_sha256 TEXT, stage_attempt INT, supersedes_delivery_version_id TEXT, acknowledgment_state TEXT, acknowledgment_at TIMESTAMPTZ, completed_at TIMESTAMPTZ.

### platform_control.landing_audit.delivery_manifest

- Grain: manifest_id. Enforced PK: (env,manifest_id). UK: (env,delivery_version_id,manifest_sha256).
- FK: delivery_version→source_delivery; contract/version→source_contract. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: immutable received bytes/payload, validation status via evidence. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: delivery_version_id TEXT, snapshot_group_id TEXT, manifest_version TEXT (corrected historical INT), completeness_json JSONB; received_at required.

### platform_control.landing_audit.delivery_artifact

- Grain: artifact_id. Enforced PK: (env,artifact_id). UK: (env,delivery_version_id,relative_path); (env,artifact_identity).
- FK: delivery_version→source_delivery; observed_schema ID→schema_version. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: artifact identity = source/version/role/path/hash; a package alias links existing original rather than duplicate identity rows. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: delivery_version_id TEXT, relative_path TEXT, source_version TEXT, layout_id TEXT, record_count_method TEXT, header_count BIGINT, raw_identity TEXT, attempt INT, snapshot_group_id TEXT; per-package artifact_identity includes delivery revision; raw_identity shares source identity across batch/replay.

### platform_control.landing_audit.validation_result

- Grain: validation_id. Enforced PK: (env,validation_id). UK: (env,run_id,stage_attempt,validation_stage,subject_identity,rule_id,rule_version).
- FK: run→ingestion_run; optional delivery/artifact→audit; rule/version→quality_rule. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: append-only deterministic execution result; no successful overwrite of failed evidence. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: delivery_version_id TEXT, rule_version INT, stage_attempt INT, subject_identity TEXT, layer TEXT, product_id TEXT, input_version_json JSONB, evaluated_count BIGINT, failed_count BIGINT, metrics_json JSONB, observed_metric NUMERIC(38,10).

### platform_control.ops.ingestion_run

- Grain: run_id. Enforced PK: (env,run_id). UK: (env,request_idempotency_key).
- FK: (env,pipeline_id,pipeline_config_version)→pipeline_config; source/entity optional→registries; recovery→recovery_request; contract/mapping versions→metadata. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: 16.1 run states; group runs have entity_id nullable and entity_scope_json. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: pipeline_id TEXT, pipeline_config_version INT, request_idempotency_key TEXT, entity_scope_json JSONB, ruleset_version TEXT, code_version TEXT, source_generation TEXT, snapshot_group_id TEXT, target_generation TEXT, lease_epoch BIGINT, request_scope_json JSONB.

### platform_control.ops.run_step

- Grain: step_run_id. Enforced PK: (env,step_run_id). UK: (env,run_id,step_name,subject_identity,attempt_number).
- FK: run→ingestion_run; optional delivery/artifact→audit. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: per-step attempt, never ambiguous multi-delivery run grain. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: delivery_version_id TEXT, subject_identity TEXT, layer TEXT, checkpoint_identity TEXT, commit_evidence_json JSONB, input_version_json JSONB.

### platform_control.ops.run_event

- Grain: event_id. Enforced PK: (env,event_id). UK: (env,event_id).
- FK: run/step→operations. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: append-only trace and state history; order timestamp,event_id. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: actor TEXT, prior_state TEXT, next_state TEXT, lease_epoch BIGINT; event_payload_json redacted, no large raw payload.

### platform_control.ops.cursor_state

- Grain: source_id,entity_id,pipeline_id,progress_domain,partition_scope. Enforced PK: (env,pipeline_id,progress_domain,partition_scope). UK: same as PK.
- FK: pipeline→pipeline_config; last_successful_run→ingestion_run. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: progress_domain CAPTURE/SILVER/GOLD/NATIVE_OBSERVATION; native source state only observation; CAS increments version. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: pipeline_id TEXT, pipeline_config_version INT, progress_domain TEXT, source_generation TEXT, target_generation TEXT, pinned_bounds_json JSONB, checkpoint_identity TEXT, commit_evidence_json JSONB; source/entity nullable only group pipeline.

### platform_control.ops.asset_commit

- Grain: asset_commit_id. Enforced PK: (env,asset_commit_id). UK: (env,target_fqn,target_generation,ingestion_identity).
- FK: run→ingestion_run; optional delivery/artifact→audit; schema→schema_version. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: append-only native commit evidence; outcome distinguishes committed/validated; no distributed commit. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: layer TEXT, delivery_version_id TEXT, target_generation TEXT, txn_app_id TEXT, txn_version BIGINT, checkpoint_identity TEXT, input_versions_json JSONB, rule_versions_json JSONB, code_version TEXT, candidate_release_id TEXT, raw_identity_set_uri TEXT; source_change_identity_json JSONB.

### platform_control.ops.work_lease

- Grain: work_scope. Enforced PK: (env,work_scope). UK: (env,lease_id).
- FK: owner_run→ingestion_run. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: one row retained per scope; epoch monotonically increments; EXPIRED→SUSPECT_OWNER does not free live writer. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: lease_epoch BIGINT, executor_reference TEXT, termination_confirmed_at TIMESTAMPTZ; CAS transaction with row lock; old lease_token retained, cannot fence Delta independently.

### platform_control.ops.recovery_request

- Grain: recovery_request_id. Enforced PK: (env,recovery_request_id). UK: (env,request_idempotency_key).
- FK: source/entity optional→registries; target_run→ingestion_run. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: REQUESTED/AUTHORIZED/RUNNING/FAILED_RECOVERABLE/VALIDATING/RECONCILED/CLOSED. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: request_idempotency_key TEXT, requested_layers_json JSONB, input_versions_json JSONB, ruleset_version TEXT, target_generation TEXT, approved_by TEXT, approved_at TIMESTAMPTZ, impact_scope_json JSONB, parent_recovery_request_id TEXT, attempt INT, reconciliation_evidence_uri TEXT.

### platform_control.ops.alert_event

- Grain: alert_id. Enforced PK: (env,alert_id). UK: partial (env,deduplication_key) WHERE status is open/acknowledged.
- FK: optional run/delivery/artifact/drift→respective tables. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: OPEN/ACKNOWLEDGED/RESOLVED; failed notification does not resolve. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: delivery_version_id TEXT, first_seen TIMESTAMPTZ, last_seen TIMESTAMPTZ, occurrence_count BIGINT, runbook_id TEXT, notification_state TEXT, escalation_at TIMESTAMPTZ.

### platform_control.dq.reconciliation_result

- Grain: reconciliation_id. Enforced PK: (env,reconciliation_id). UK: (env,run_id,scope_identity,check_type,rule_version).
- FK: run→ingestion_run; optional delivery→source_delivery, product/version→data_product. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: append-only same-grain/version proof, 365d. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: delivery_version_id TEXT, product_id TEXT, product_version INT, layer TEXT, scope_identity TEXT, grain TEXT, rule_version INT, expected_count BIGINT, accepted_count BIGINT, held_count BIGINT, quarantined_count BIGINT, duplicate_identity_count BIGINT, input_versions_json JSONB, metrics_json JSONB; variance/tolerance NUMERIC(38,10).

### platform_control.quarantine.quarantine_event

- Grain: quarantine_id. Enforced PK: (env,quarantine_id). UK: (env,original_identity,layer,reason_code,rule_id,rule_version).
- FK: optional run/delivery/artifact/drift/validation/recovery→respective tables. Indexes: global FK/queue/history indexes above; PK/UK use B-tree; cursor/lease scopes indexed directly. Do not index large payload JSON unless a measured query requires it.
- State/update: 15.6.1 lifecycle, grouped exceptions reference Delta payload rows; metadata not raw store. Ownership and retention follow the physical conventions; immutable evidence is never rewritten.
- Added columns / specific type overrides: delivery_version_id TEXT, rule_id TEXT, rule_version INT, schema_version_id TEXT, source_order_json JSONB, first_seen TIMESTAMPTZ, last_seen TIMESTAMPTZ, attempts INT, diagnosis TEXT, correction_reference TEXT, corrected_by TEXT, authorized_by TEXT, replay_attempt INT, resolution TEXT, downstream_impact_json JSONB.

## Four integrated extensions

### platform_control.metadata.pipeline_config

Columns: env TEXT, pipeline_id TEXT, source_id TEXT, entity_id TEXT nullable, entity_scope_json JSONB, mode TEXT, trigger_type TEXT, schedule_cron TEXT, timezone TEXT, availability_window_json JSONB, stage_budgets_json JSONB, source_scope_json JSONB, target_spec_json JSONB, checkpoint_spec_json JSONB, retry_policy_json JSONB, concurrency_policy_json JSONB, config_version INT, status TEXT, effective_from TIMESTAMPTZ, effective_to TIMESTAMPTZ, created_at TIMESTAMPTZ, created_by TEXT.
PK (env,pipeline_id,config_version); UK same; FK source/entity→registries when applicable. Index (env,status,source_id,entity_id). Immutable config versions; active interval uniqueness enforced transactionally under config activation lock. Run references include config_version. Steward/deploy ownership; lifetime+365d. H&M entity has initial/incremental/CDC modes without overloading entity_config's single schedule.

### platform_control.metadata.data_product

Columns: env TEXT, product_id TEXT, product_version INT, product_name TEXT, owner_role TEXT, description TEXT, grain_json JSONB, metric_definitions_json JSONB, serving_spec_json JSONB, completeness_policy_json JSONB, freshness_policy_json JSONB, quality_policy_json JSONB, classification TEXT, retention_policy_json JSONB, status TEXT, effective_from TIMESTAMPTZ, effective_to TIMESTAMPTZ, approved_by TEXT, approved_at TIMESTAMPTZ, created_at TIMESTAMPTZ, created_by TEXT.
PK/UK (env,product_id,product_version); index (env,status,owner_role). Immutable product definitions; activation lock prevents overlapping active intervals. Product owner via reviewed migration; lifetime+365d.

### platform_control.metadata.asset_dependency

Columns: env TEXT, dependency_id TEXT, dependency_version INT, product_id TEXT, product_version INT, pipeline_id TEXT nullable, pipeline_config_version INT nullable, upstream_asset_fqn TEXT, downstream_asset_fqn TEXT, upstream_layer TEXT, downstream_layer TEXT, required BOOLEAN, source_group_policy_json JSONB, version_alignment_policy_json JSONB, column_mapping_json JSONB, transform_reference TEXT, quality_gate_json JSONB, status TEXT, created_at TIMESTAMPTZ, created_by TEXT.
PK (env,dependency_id,dependency_version); UK (env,product_id,product_version,upstream_asset_fqn,downstream_asset_fqn,dependency_version). FK product/version→data_product; pipeline/config→pipeline_config when set. Index (env,upstream_asset_fqn), (env,downstream_asset_fqn). Immutable definition; references UC FQNs validated by deployment, not a fake SQL FK to external UC. Owner steward/deploy; lifetime+365d.

### platform_control.ops.product_release

Columns: env TEXT, release_id TEXT, product_id TEXT, product_version INT, target_generation TEXT, run_id TEXT, lease_epoch BIGINT, input_versions_json JSONB, candidate_table_versions_json JSONB, dependency_coverage_json JSONB, metric_version TEXT, rule_versions_json JSONB, code_version TEXT, source_as_of_json JSONB, status TEXT, quality_evidence_uri TEXT, reconciliation_evidence_uri TEXT, serving_manifest_fqn TEXT, serving_manifest_delta_version BIGINT nullable, serving_manifest_sha256 TEXT nullable, supersedes_release_id TEXT nullable, created_at TIMESTAMPTZ, validated_at TIMESTAMPTZ nullable, published_at TIMESTAMPTZ nullable, acknowledged_at TIMESTAMPTZ nullable, created_by TEXT, updated_by TEXT, updated_at TIMESTAMPTZ, row_version BIGINT.
PK (env,release_id); UK (env,product_id,target_generation,run_id); FK product/version→data_product, run→ingestion_run, supersedes release→product_release. Index (env,product_id,status,created_at). ADMITTED/WRITING/DATA_COMMITTED/VALIDATED/PUBLISHED/HELD/ACK_PENDING states follow architecture Section 15.4/16.1. Native Delta serving_manifest is the actual visibility authority; this SQL row is evidence, not a second active release pointer. Release owner API; metadata365d, candidate data retention separate.

## Cross-table concurrency and migration procedure

1. Export/hash all existing Delta registry/evidence rows and DDL; retain their version/date. Workbook exports are evidence, not sufficient type validation.
2. Profile/validate fields and references. Fix WC descriptive/parser assumptions through a new immutable contract version with layout-scoped fields; keep contract_wc_store_sales_v1 as stable contract identity with versioned header/fields. Existing BLTA definitions remain history. OFF part mappings bind the single contract. Correct Prices mapping/entity through a new immutable registered Prices contract; preserve inactive proofs metadata.
3. Create enforced SQL schema through future reviewed migrations, import validated rows with env and explicit version/default mappings, and reconcile row/ID counts. Historical delivery_id-only keys migrate to delivery_version_id and revision-safe FKs. No current raw objects or metadata are dropped by this document.
4. Bootstrap active config transaction under lock; switch Control API to SQL once import/fidelity tests pass. Stop operational writes to old Delta control tables; create governed read-only projections with export_watermark. One source of truth.
5. Admission creates ingestion_run/lease/pending bounds in one SQL transaction using enforced UK and request idempotency key. Deferred circular FKs (run↔lease/recovery) are checked by transaction end; never disable referential integrity.
6. Work lease renewal CAS checks owner, row_version and epoch. Expired owner must be stopped/confirmed before writer takeover. API publication rechecks epoch and destination manifest identity; SQL fencing does not itself fence native Delta writes.
7. Data writes and acknowledgments are separate; verify Delta/ADLS/native checkpoint evidence before SQL progress CAS. Recovery from stale SQL backup rehydrates missing acknowledged evidence under global admission fence.
8. Archive evidence with manifest/hash and preserve referenced versions. Do not age/delete active cursors/checkpoints/serving cuts; retention and sensitive-data deletion follow architecture 17.2.4.

No tables, constraints, migrations, API functions or projections are created in this audit.

Publication-intent fencing: store operation/request/lease epoch/destination/manifest identity and state in the existing run_step.commit_evidence_json and run_event. Owner transfer is blocked until the old executor and any cloud publication intent terminate or reconcile. No extra table is introduced. The serving_manifest CURRENT pointer plus immutable RELEASE membership rows are one Delta transaction; ops.product_release records evidence rather than serving authorization.
Uniqueness with nullable scopes: every admission/evidence UK uses a non-null normalized subject_identity/work_scope. On the selected PostgreSQL version supporting NULLS NOT DISTINCT, use that semantics for nullable source/contract bindings; otherwise register explicit common scope values and enforce the equivalent normalized key before activation. This is an environment validation, not permission to leave duplicate nullable keys unguarded. All metadata versions referenced by an active step are explicit; multi-entity group runs keep scoped version maps and nullable singleton contract/entity fields rather than invent one shared contract.
