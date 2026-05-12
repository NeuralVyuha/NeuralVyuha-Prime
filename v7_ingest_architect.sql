-- ═══════════════════════════════════════════════════════════════════════════════
-- NeuralVyuha Migration: v7_ingest_architect.sql
-- Purpose: Schema for Intelligent Deduplication and Cross-Source Correlation.
-- ═══════════════════════════════════════════════════════════════════════════════

-- 1. Deduplication Configurations
-- Defines which fields to hash per source to identify "identical" events.
CREATE TABLE IF NOT EXISTS ingest_dedup_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL DEFAULT 'dev-tenant',
    source TEXT NOT NULL,
    field_paths TEXT[] NOT NULL,  -- e.g. ['data.srcip', 'rule.id']
    window_seconds INTEGER NOT NULL DEFAULT 3600,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(tenant_id, source)
);

-- 2. Correlation Rules (Tier 3)
-- Defines links between disparate sources.
CREATE TABLE IF NOT EXISTS ingest_correlation_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL DEFAULT 'dev-tenant',
    name TEXT NOT NULL,
    source_a TEXT NOT NULL,
    source_b TEXT NOT NULL,
    match_field TEXT NOT NULL, -- e.g. 'data.srcip'
    time_window INTEGER NOT NULL DEFAULT 300, -- 5 mins
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Seed Default Dedup Configurations
INSERT INTO ingest_dedup_configs (source, field_paths, window_seconds) VALUES
('wazuh', ARRAY['data.srcip', 'rule.id'], 3600),
('suricata', ARRAY['alert.signature', 'src_ip'], 3600),
('syslog', ARRAY['message', 'hostname'], 1800)
ON CONFLICT (tenant_id, source) DO NOTHING;

-- Index for fast lookup
CREATE INDEX IF NOT EXISTS idx_dedup_source ON ingest_dedup_configs(tenant_id, source);
