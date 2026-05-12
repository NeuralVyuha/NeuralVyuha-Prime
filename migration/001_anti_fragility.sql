-- T1: Long-term Deduplication
CREATE TABLE IF NOT EXISTS alert_fingerprints (
    tenant_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    original_event_id TEXT NOT NULL,
    first_seen_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, fingerprint)
);

-- T2: Idempotency Keys
CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id TEXT NOT NULL,
    key_id TEXT NOT NULL,
    response_json TEXT,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, key_id)
);

-- T3: Entity Versioning
ALTER TABLE cases ADD COLUMN IF NOT EXISTS entity_version INT DEFAULT 1;
ALTER TABLE case_tasks ADD COLUMN IF NOT EXISTS entity_version INT DEFAULT 1;
