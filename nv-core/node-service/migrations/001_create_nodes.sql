-- Phase Z2.1: Integration Node Registry
-- Migration: 001_create_nodes.sql

CREATE TABLE IF NOT EXISTS integration_nodes (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    node_type       VARCHAR(32)   NOT NULL,
    name            VARCHAR(128)  NOT NULL,
    url             TEXT          NOT NULL,
    api_key_enc     TEXT          NOT NULL,          -- Fernet(NODE_MASTER_KEY)-encrypted API key
    tls_verify      BOOLEAN       NOT NULL DEFAULT TRUE,
    status          VARCHAR(16)   NOT NULL DEFAULT 'UNKNOWN'
                                  CHECK (status IN ('UP', 'DEGRADED', 'DOWN', 'UNKNOWN')),
    latency_ms      INTEGER,                          -- Last RTT in milliseconds
    http_status     INTEGER,                          -- Last HTTP status code from probe
    last_seen       TIMESTAMP WITH TIME ZONE,         -- Last successful probe timestamp
    last_error      TEXT,                             -- Last error message (truncated)
    probe_count     BIGINT        NOT NULL DEFAULT 0, -- Total heartbeat probes run
    created_by      VARCHAR(128),                     -- user_id of registering admin
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Fast lookup by type for dashboard queries
CREATE INDEX IF NOT EXISTS idx_nodes_type ON integration_nodes (node_type);

-- Trigger: auto-update updated_at on row change
CREATE OR REPLACE FUNCTION update_node_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_nodes_updated_at ON integration_nodes;
CREATE TRIGGER trg_nodes_updated_at
    BEFORE UPDATE ON integration_nodes
    FOR EACH ROW EXECUTE FUNCTION update_node_updated_at();
