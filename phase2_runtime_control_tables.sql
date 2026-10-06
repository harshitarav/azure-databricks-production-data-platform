-- Phase 2 runtime, audit, reconciliation, and quarantine tables.
-- Canonical logical column design: control tables.md.
-- These are Delta tables. Logical PK/FK values must be enforced by the
-- idempotent write paths; Databricks does not enforce PK/FK uniqueness.
-- CREATE TABLE IF NOT EXISTS does not migrate an already-created table.

CREATE SCHEMA IF NOT EXISTS retail_de_dev.landing_audit;
CREATE SCHEMA IF NOT EXISTS retail_de_dev.ops;
CREATE SCHEMA IF NOT EXISTS retail_de_dev.dq;
CREATE SCHEMA IF NOT EXISTS retail_de_dev.quarantine;

-- 1. One logical source delivery, snapshot, API extraction window, or stream microbatch.
-- Logical key: delivery_id.
CREATE TABLE IF NOT EXISTS retail_de_dev.landing_audit.source_delivery (
    delivery_id                STRING NOT NULL,
    source_id                  STRING NOT NULL,
    entity_id                  STRING NOT NULL,
    delivery_revision          INT NOT NULL,
    delivery_type              STRING NOT NULL,
    producer_batch_id          STRING,
    source_window_start        TIMESTAMP,
    source_window_end          TIMESTAMP,
    business_date              DATE,
    manifest_uri               STRING,
    declared_schema_fingerprint STRING,
    status                     STRING NOT NULL,
    discovered_at              TIMESTAMP,
    ready_at                   TIMESTAMP,
    committed_at               TIMESTAMP,
    run_id                     STRING,
    error_code                 STRING,
    error_message              STRING
)
USING DELTA;

-- 2. Immutable manifest version and its validation status.
-- Logical key: manifest_id. delivery_id is a logical FK.
CREATE TABLE IF NOT EXISTS retail_de_dev.landing_audit.delivery_manifest (
    manifest_id                 STRING NOT NULL,
    delivery_id                 STRING NOT NULL,
    manifest_version            INT NOT NULL,
    manifest_uri                STRING NOT NULL,
    manifest_sha256             STRING,
    contract_id                 STRING,
    contract_version            STRING,
    declared_file_count         BIGINT,
    declared_record_count       BIGINT,
    declared_schema_fingerprint STRING,
    manifest_payload_json       STRING,
    validation_status           STRING NOT NULL,
    received_at                 TIMESTAMP NOT NULL
)
USING DELTA;

-- 3. One physical file, API page, or persisted source object.
-- Logical key: artifact_id. artifact_identity is the deterministic idempotency key.
CREATE TABLE IF NOT EXISTS retail_de_dev.landing_audit.delivery_artifact (
    artifact_id                  STRING NOT NULL,
    delivery_id                  STRING NOT NULL,
    artifact_sequence            INT NOT NULL,
    artifact_type                STRING NOT NULL,
    source_uri                   STRING,
    staging_uri                  STRING,
    landing_uri                  STRING,
    file_name                    STRING,
    file_format                  STRING,
    compression                  STRING,
    declared_bytes               BIGINT,
    observed_bytes               BIGINT,
    declared_sha256              STRING,
    observed_sha256              STRING,
    declared_record_count        BIGINT,
    observed_record_count        BIGINT,
    observed_schema_version_id   STRING,
    artifact_identity            STRING NOT NULL,
    status                       STRING NOT NULL,
    autoloader_discovered_at     TIMESTAMP,
    bronze_committed_at          TIMESTAMP,
    quarantine_reason_code       STRING
)
USING DELTA;

-- 4. One row per validation check/decision; use rule_id to trace to metadata.quality_rule.
-- Logical key: validation_id.
CREATE TABLE IF NOT EXISTS retail_de_dev.landing_audit.validation_result (
    validation_id       STRING NOT NULL,
    validation_stage   STRING NOT NULL,
    source_id           STRING NOT NULL,
    entity_id           STRING NOT NULL,
    delivery_id         STRING NOT NULL,
    artifact_id         STRING,
    run_id              STRING NOT NULL,
    rule_id             STRING,
    validation_category STRING NOT NULL,
    expected_value      STRING,
    observed_value      STRING,
    outcome             STRING NOT NULL,
    severity            STRING NOT NULL,
    decision            STRING NOT NULL,
    evidence_uri        STRING,
    evaluated_at        TIMESTAMP NOT NULL
)
USING DELTA;

-- 5. One end-to-end orchestration run.
-- Logical key: run_id.
CREATE TABLE IF NOT EXISTS retail_de_dev.ops.ingestion_run (
    run_id               STRING NOT NULL,
    pipeline_name        STRING NOT NULL,
    source_id            STRING NOT NULL,
    entity_id            STRING NOT NULL,
    ingestion_mode       STRING NOT NULL,
    trigger_type         STRING NOT NULL,
    mapping_id           STRING NOT NULL,
    mapping_version      INT NOT NULL,
    contract_id          STRING NOT NULL,
    contract_version     STRING NOT NULL,
    schema_version_id    STRING NOT NULL,
    recovery_request_id  STRING,
    attempt_number       INT NOT NULL,
    status               STRING NOT NULL,
    started_at           TIMESTAMP NOT NULL,
    heartbeat_at         TIMESTAMP,
    completed_at         TIMESTAMP,
    executor_reference   STRING,
    correlation_id       STRING,
    error_code           STRING,
    error_message        STRING
)
USING DELTA;

-- 6. One execution step/attempt in a run.
-- Logical key: step_run_id.
CREATE TABLE IF NOT EXISTS retail_de_dev.ops.run_step (
    step_run_id       STRING NOT NULL,
    run_id            STRING NOT NULL,
    step_name         STRING NOT NULL,
    source_id         STRING NOT NULL,
    entity_id         STRING NOT NULL,
    delivery_id       STRING,
    artifact_id       STRING,
    attempt_number    INT NOT NULL,
    status            STRING NOT NULL,
    started_at        TIMESTAMP,
    completed_at      TIMESTAMP,
    input_record_count  BIGINT,
    output_record_count BIGINT,
    error_code        STRING,
    error_message     STRING,
    compute_reference STRING
)
USING DELTA;

-- 7. Append-only run and step event log.
-- Logical key: event_id.
CREATE TABLE IF NOT EXISTS retail_de_dev.ops.run_event (
    event_id          STRING NOT NULL,
    run_id            STRING NOT NULL,
    step_run_id       STRING,
    event_timestamp   TIMESTAMP NOT NULL,
    event_type        STRING NOT NULL,
    severity          STRING NOT NULL,
    status            STRING,
    message           STRING,
    event_payload_json STRING,
    correlation_id    STRING
)
USING DELTA;

-- 8. Pending and committed progress for one cursor scope.
-- Logical composite key: source_id + entity_id + cursor_type + partition_scope.
CREATE TABLE IF NOT EXISTS retail_de_dev.ops.cursor_state (
    source_id                STRING NOT NULL,
    entity_id                STRING NOT NULL,
    cursor_type              STRING NOT NULL,
    partition_scope          STRING NOT NULL,
    pending_position_json    STRING,
    committed_position_json  STRING,
    last_successful_run_id   STRING,
    last_committed_at        TIMESTAMP,
    status                   STRING NOT NULL,
    row_version              BIGINT NOT NULL,
    updated_at               TIMESTAMP NOT NULL
)
USING DELTA;

-- 9. Idempotency and successful Bronze Delta commit evidence.
-- Logical key: bronze_commit_id; ingestion_identity must be unique per target/input.
CREATE TABLE IF NOT EXISTS retail_de_dev.ops.bronze_commit (
    bronze_commit_id          STRING NOT NULL,
    source_id                 STRING NOT NULL,
    entity_id                 STRING NOT NULL,
    delivery_id               STRING,
    artifact_id               STRING,
    run_id                    STRING NOT NULL,
    bronze_table_fqn          STRING NOT NULL,
    delta_table_version       BIGINT,
    ingestion_identity        STRING NOT NULL,
    source_change_identity_json STRING,
    input_record_count        BIGINT,
    inserted_count            BIGINT,
    deduplicated_count        BIGINT,
    rejected_count            BIGINT,
    schema_version_id         STRING,
    outcome                   STRING NOT NULL,
    committed_at              TIMESTAMP NOT NULL,
    reconciliation_status     STRING NOT NULL
)
USING DELTA;

-- 10. Fenced lease for exclusive source/entity/delivery/partition work.
-- Logical key: lease_id. lease_token + row_version fence stale owners.
CREATE TABLE IF NOT EXISTS retail_de_dev.ops.work_lease (
    lease_id       STRING NOT NULL,
    source_id      STRING NOT NULL,
    entity_id      STRING NOT NULL,
    work_scope     STRING NOT NULL,
    owner_run_id   STRING NOT NULL,
    lease_token    STRING NOT NULL,
    acquired_at    TIMESTAMP NOT NULL,
    expires_at     TIMESTAMP NOT NULL,
    heartbeat_at   TIMESTAMP NOT NULL,
    status         STRING NOT NULL,
    row_version    BIGINT NOT NULL
)
USING DELTA;

-- 11. Auditable backfill, reprocessing, or replay request.
-- Logical key: recovery_request_id.
CREATE TABLE IF NOT EXISTS retail_de_dev.ops.recovery_request (
    recovery_request_id    STRING NOT NULL,
    recovery_type          STRING NOT NULL,
    source_id              STRING NOT NULL,
    entity_id              STRING NOT NULL,
    requested_scope_json   STRING NOT NULL,
    reason                 STRING NOT NULL,
    requested_by           STRING NOT NULL,
    requested_at           TIMESTAMP NOT NULL,
    status                 STRING NOT NULL,
    isolated_checkpoint_path STRING,
    target_run_id          STRING,
    completed_at           TIMESTAMP
)
USING DELTA;

-- 12. Durable alert state, deduplication, acknowledgement, and resolution.
-- Logical key: alert_id. deduplication_key prevents repeated open alerts.
CREATE TABLE IF NOT EXISTS retail_de_dev.ops.alert_event (
    alert_id             STRING NOT NULL,
    run_id               STRING,
    delivery_id          STRING,
    artifact_id          STRING,
    drift_event_id       STRING,
    alert_type           STRING NOT NULL,
    severity             STRING NOT NULL,
    alert_message        STRING NOT NULL,
    deduplication_key    STRING NOT NULL,
    channel              STRING,
    azure_monitor_rule   STRING,
    status               STRING NOT NULL,
    emitted_at           TIMESTAMP NOT NULL,
    acknowledged_by      STRING,
    acknowledged_at      TIMESTAMP,
    resolved_at          TIMESTAMP
)
USING DELTA;

-- 13. Source → Landing → Bronze count/hash reconciliation.
-- Logical key: reconciliation_id.
CREATE TABLE IF NOT EXISTS retail_de_dev.dq.reconciliation_result (
    reconciliation_id STRING NOT NULL,
    source_id         STRING NOT NULL,
    entity_id         STRING NOT NULL,
    delivery_id       STRING,
    run_id            STRING NOT NULL,
    check_type        STRING NOT NULL,
    source_count      BIGINT,
    landing_count     BIGINT,
    bronze_count      BIGINT,
    source_hash       STRING,
    landing_hash      STRING,
    bronze_hash       STRING,
    variance          DECIMAL(38, 6),
    tolerance         DECIMAL(38, 6),
    outcome           STRING NOT NULL,
    evidence_uri      STRING,
    evaluated_at      TIMESTAMP NOT NULL
)
USING DELTA;

-- 14. Quarantined file, record, or delivery with traceable validation and recovery.
-- Logical key: quarantine_id.
CREATE TABLE IF NOT EXISTS retail_de_dev.quarantine.quarantine_event (
    quarantine_id       STRING NOT NULL,
    source_id           STRING NOT NULL,
    entity_id           STRING NOT NULL,
    delivery_id         STRING,
    artifact_id         STRING,
    run_id              STRING,
    layer               STRING NOT NULL,
    reason_code         STRING NOT NULL,
    severity            STRING NOT NULL,
    quarantine_uri      STRING,
    original_identity   STRING,
    original_sha256     STRING,
    schema_drift_event_id STRING,
    validation_id       STRING,
    record_count        BIGINT,
    status              STRING NOT NULL,
    disposition         STRING,
    recovery_request_id STRING,
    quarantined_at      TIMESTAMP NOT NULL,
    resolved_by         STRING,
    resolved_at         TIMESTAMP
)
USING DELTA;
