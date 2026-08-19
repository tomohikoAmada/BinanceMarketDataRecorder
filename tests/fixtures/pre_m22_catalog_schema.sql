-- Exact Catalog schema emitted by main at 8ada700 before M22.4A.
CREATE TABLE chunks (
    chunk_id TEXT PRIMARY KEY, state TEXT NOT NULL, partial_path TEXT,
    sealed_path TEXT, manifest_path TEXT,
    record_count INTEGER NOT NULL DEFAULT 0, uncompressed_bytes INTEGER,
    stored_bytes INTEGER, uncompressed_sha256 TEXT, stored_sha256 TEXT,
    created_at_utc_ns INTEGER NOT NULL, updated_at_utc_ns INTEGER NOT NULL
);
CREATE TABLE chunk_transitions (
    transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    chunk_id TEXT NOT NULL REFERENCES chunks(chunk_id), from_state TEXT,
    to_state TEXT NOT NULL, occurred_at_utc_ns INTEGER NOT NULL,
    evidence_json TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE
);
CREATE TABLE quarantined_artifacts (
    artifact_id TEXT PRIMARY KEY, relative_path TEXT NOT NULL,
    reason TEXT NOT NULL, sha256 TEXT NOT NULL,
    quarantined_at_utc_ns INTEGER NOT NULL
);
CREATE TABLE orderbook_checkpoints (
    checkpoint_id TEXT PRIMARY KEY, market TEXT NOT NULL, symbol TEXT NOT NULL,
    update_id INTEGER NOT NULL, book_hash TEXT NOT NULL,
    relative_path TEXT NOT NULL, created_at_utc_ns INTEGER NOT NULL
);
CREATE TABLE metric_batches (
    batch_id TEXT NOT NULL, utc_date TEXT NOT NULL, market TEXT NOT NULL,
    stream TEXT NOT NULL, aggregate_json TEXT NOT NULL,
    committed_at_utc_ns INTEGER NOT NULL,
    PRIMARY KEY(batch_id, utc_date, market, stream)
);
CREATE INDEX metric_batches_by_day ON metric_batches(utc_date, market, stream);
CREATE TABLE storage_targets (
    storage_id TEXT PRIMARY KEY, volume_uuid TEXT NOT NULL, volume_name TEXT,
    filesystem_type TEXT, relative_path TEXT NOT NULL, marker_nonce TEXT NOT NULL,
    registered_at_utc_ns INTEGER NOT NULL, UNIQUE(volume_uuid, relative_path)
);
CREATE TABLE storage_control (
    storage_id TEXT PRIMARY KEY REFERENCES storage_targets(storage_id),
    state TEXT NOT NULL, request_id TEXT, updated_at_utc_ns INTEGER NOT NULL,
    evidence_json TEXT NOT NULL
);
CREATE TABLE archive_transactions (
    transaction_id TEXT PRIMARY KEY,
    chunk_id TEXT NOT NULL UNIQUE REFERENCES chunks(chunk_id),
    storage_id TEXT NOT NULL, state TEXT NOT NULL, market TEXT NOT NULL,
    stream TEXT NOT NULL, source_relative_path TEXT NOT NULL,
    source_manifest_relative_path TEXT NOT NULL,
    source_manifest_sha256 TEXT NOT NULL, target_relative_path TEXT NOT NULL,
    target_temp_relative_path TEXT NOT NULL,
    external_manifest_relative_path TEXT NOT NULL, stored_bytes INTEGER NOT NULL,
    stored_sha256 TEXT NOT NULL, created_at_utc_ns INTEGER NOT NULL,
    updated_at_utc_ns INTEGER NOT NULL, verified_at_utc_ns INTEGER,
    local_deleted_at_utc_ns INTEGER, attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE INDEX archive_transactions_by_state
    ON archive_transactions(state, created_at_utc_ns, transaction_id);
CREATE TABLE archive_transaction_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id TEXT NOT NULL REFERENCES archive_transactions(transaction_id),
    from_state TEXT, to_state TEXT NOT NULL, occurred_at_utc_ns INTEGER NOT NULL,
    evidence_json TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE
);
CREATE TABLE storage_space_samples (
    sample_id TEXT PRIMARY KEY, scope_id TEXT NOT NULL, storage_id TEXT,
    observed_at_utc_ns INTEGER NOT NULL, total_bytes INTEGER NOT NULL,
    free_bytes INTEGER NOT NULL, archive_backlog_bytes INTEGER NOT NULL,
    oldest_unarchived_at_utc_ns INTEGER
);
CREATE INDEX storage_space_samples_by_scope_time
    ON storage_space_samples(scope_id, observed_at_utc_ns, sample_id);
CREATE TABLE storage_alert_state (
    scope_id TEXT PRIMARY KEY, severity TEXT NOT NULL,
    updated_at_utc_ns INTEGER NOT NULL
);
CREATE TABLE storage_alert_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT, scope_id TEXT NOT NULL,
    from_severity TEXT, to_severity TEXT NOT NULL,
    occurred_at_utc_ns INTEGER NOT NULL, evidence_json TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE
);
CREATE TABLE operational_events (
    event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL,
    occurred_at_utc_ns INTEGER NOT NULL, evidence_json TEXT NOT NULL
);
CREATE TABLE side_data_cursors (
    kind TEXT PRIMARY KEY, last_persisted_period_timestamp INTEGER NOT NULL,
    updated_at_utc_ns INTEGER NOT NULL, source_retention_window TEXT NOT NULL,
    retention_window_ms INTEGER NOT NULL
);
CREATE TABLE deployment_sessions (
    deployment_id TEXT PRIMARY KEY, reason TEXT NOT NULL, market TEXT NOT NULL,
    symbol TEXT NOT NULL, active_instance_id TEXT NOT NULL,
    active_version TEXT NOT NULL, candidate_instance_id TEXT NOT NULL,
    candidate_version TEXT NOT NULL, state TEXT NOT NULL,
    created_at_utc_ns INTEGER NOT NULL, updated_at_utc_ns INTEGER NOT NULL,
    overlap_started_at_utc_ns INTEGER, cutover_at_utc_ns INTEGER, last_error TEXT
);
CREATE TABLE deployment_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    deployment_id TEXT NOT NULL REFERENCES deployment_sessions(deployment_id),
    from_state TEXT, to_state TEXT NOT NULL, occurred_at_utc_ns INTEGER NOT NULL,
    evidence_json TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE
);
INSERT INTO chunks(
    chunk_id, state, partial_path, sealed_path, manifest_path, record_count,
    uncompressed_bytes, stored_bytes, uncompressed_sha256, stored_sha256,
    created_at_utc_ns, updated_at_utc_ns
) VALUES (
    'legacy-sealed-chunk', 'SEALED', NULL,
    'data/sealed/legacy-sealed-chunk.bmdr.zst',
    'data/manifests/legacy-sealed-chunk.manifest.json', 1, 3, 3,
    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
    1, 2
);
