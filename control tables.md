# Source-to-Bronze Control Tables

## Purpose and scope

This document records the shared control-plane tables for the project’s Source → Ingestion → Landing → Bronze path. It covers PostgreSQL batch and CDC, SFTP file deliveries, Kafka streaming and replay, and API extraction. MongoDB is excluded from the project source scope.

These are operational and metadata tables, not business datasets. They are shared across sources and entities using `source_id` and `entity_id`; do not create a copy of each control table for every datasource. Business data is stored in its corresponding Bronze Delta table.

The design contains **22 tables**. The names and columns below are the canonical logical design. Where this document does not assign a physical data type, implementation must use the source contract and platform conventions. Databricks primary/foreign key declarations are logical documentation and should not be relied on as enforced uniqueness; write paths must enforce idempotency and concurrency controls.

## Source schema and contract ownership

The source owner provides a versioned contract: schema or DDL, field meaning, types, nullability, required fields, keys where supported, format and encoding, delivery cadence, and change notice. For public datasets without a source team, the project owner creates and approves the initial contract from profiling the controlled source data.

The platform stores the approved contract in `metadata.source_contract` and its field rules in `metadata.source_contract_field`. The platform profiles each delivery and stores an immutable observed schema in `metadata.schema_version`, then records differences in `metadata.schema_drift_event`. An incoming schema change is not accepted merely because ADF or Auto Loader detects it. It must pass the active contract and validation rules before Landing/Bronze commit. Schema history and drift evidence are retained for audit and recovery.

## Table inventory

### Metadata schema — 8 tables

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

Columns: `mapping_id` (logical PK), `source_id`, `entity_id`, `mapping_version`, `contract_id`, `extraction_spec_json`, `staging_prefix`, `landing_prefix`, `bronze_table_fqn`, `file_format`, `parser_options_json`, `checkpoint_scope`, `effective_from`, `effective_to`, `status`.

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
Purpose: one logical delivery, snapshot, API extraction window, or streaming microbatch.

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

### Operations schema — 8 tables

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

The committed position advances only after the corresponding Bronze commit and required reconciliation succeed. Source-specific positions are represented by `cursor_type` and the position JSON; this single table replaces separate watermark, CDC offset, Kafka offset, and API cursor tables.

#### `retail_de_dev.ops.bronze_commit`
Purpose: idempotency and commit evidence for Bronze Delta writes.

Columns: `bronze_commit_id` (logical PK), `source_id`, `entity_id`, `delivery_id`, `artifact_id`, `run_id`, `bronze_table_fqn`, `delta_table_version`, `ingestion_identity`, `source_change_identity_json`, `input_record_count`, `inserted_count`, `deduplicated_count`, `rejected_count`, `schema_version_id`, `outcome`, `committed_at`, `reconciliation_status`.

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
| SFTP and ADF files | `source_delivery`, `delivery_manifest`, `delivery_artifact`, `bronze_commit` |
| Auto Loader | `delivery_artifact.artifact_identity`, its durable checkpoint, and `bronze_commit.ingestion_identity` |
| PostgreSQL incremental extraction | `cursor_state.committed_position_json` and `bronze_commit` |
| PostgreSQL CDC through Debezium and Kafka | `cursor_state` with the appropriate `cursor_type`, plus `bronze_commit.source_change_identity_json` |
| Kafka replay | `recovery_request`, an isolated replay checkpoint, cursor position, and Bronze commit identity |
| API extraction | `source_delivery`, one `delivery_artifact` per persisted page, and `cursor_state` for page/cursor progress |

A failed run remains visible in `ingestion_run` and `run_step`; append-only details are in `run_event`. Delivery and artifact states identify work that has not completed. Failed validation has evidence in `validation_result` and, when rejected, `quarantine_event`. Watermarks and offsets are not advanced until Bronze commit and reconciliation complete.

## Implementation notes

- Tables are shared across sources; include `source_id` and `entity_id` on operational records where source/entity attribution applies.
- Store secrets only in Key Vault. `connection_secret_ref` is a reference, never a secret value.
- Keep schema snapshots and contract versions immutable. New approvals create new versions rather than rewriting historical evidence.
- Use append-only event and evidence records where audit history matters. Update current state with optimistic concurrency or a lease token so overlapping runs cannot silently overwrite one another.
- Maintain retention and access policies for each schema according to the project’s approved governance and data classification rules.
- These control tables support Source → Landing → Bronze. They do not define Silver or Gold transformation schemas.
