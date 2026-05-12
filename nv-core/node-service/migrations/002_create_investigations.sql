-- Phase Z2.2: AI Case Investigation Storage
-- Migration: 002_create_investigations.sql

CREATE TABLE IF NOT EXISTS ai_case_investigations (
    id              SERIAL PRIMARY KEY,
    case_id         VARCHAR(64)   NOT NULL,
    investigation_data JSONB         NOT NULL,          -- Full chat/report JSON
    model_provider  VARCHAR(32),
    model_name      VARCHAR(64),
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Fast lookup by case_id for tab loading
CREATE INDEX IF NOT EXISTS idx_ai_investigations_case_id ON ai_case_investigations (case_id);

-- Trigger: auto-update updated_at on row change
DROP TRIGGER IF EXISTS trg_investigations_updated_at ON ai_case_investigations;
CREATE TRIGGER trg_investigations_updated_at
    BEFORE UPDATE ON ai_case_investigations
    FOR EACH ROW EXECUTE FUNCTION update_node_updated_at();
