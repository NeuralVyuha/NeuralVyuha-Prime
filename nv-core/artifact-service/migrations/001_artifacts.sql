-- Phase Z1.1: Evidence Vault Schema
-- Run during Postgres init or as a migration

-- Enable UUID extension if not already enabled
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS artifacts (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    case_id           UUID        NOT NULL,
    tenant_id         TEXT        NOT NULL,
    filename          TEXT        NOT NULL,
    sha256            TEXT        NOT NULL,
    size              BIGINT      NOT NULL,
    minio_key         TEXT        NOT NULL,
    -- Per-file Fernet key (promote to KMS reference in Phase Z1.2)
    encryption_key_id TEXT        NOT NULL,
    created_at        BIGINT      NOT NULL
);

-- Fast sighting lookups
CREATE INDEX IF NOT EXISTS idx_artifacts_sha256     ON artifacts (sha256);
-- Tenant-scoped case listing
CREATE INDEX IF NOT EXISTS idx_artifacts_tenant_case ON artifacts (tenant_id, case_id);
