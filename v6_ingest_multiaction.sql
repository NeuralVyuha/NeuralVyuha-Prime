-- ════════════════════════════════════════════════════════════════════════════
--  NV-INGEST v2.0 Schema Migration
--  Upgrades the ingestion control schema to support multi-action routing
-- ════════════════════════════════════════════════════════════════════════════

-- 1. Add action column to drop rules (DROP / LOG / ALERT)
ALTER TABLE ingest_drop_rules ADD COLUMN IF NOT EXISTS action VARCHAR(10) DEFAULT 'DROP';
ALTER TABLE ingest_drop_rules ADD COLUMN IF NOT EXISTS severity_override INTEGER DEFAULT NULL;
ALTER TABLE ingest_drop_rules ADD COLUMN IF NOT EXISTS priority INTEGER DEFAULT 0;

-- 2. Index for fast priority-based lookups
CREATE INDEX IF NOT EXISTS idx_drop_rules_priority ON ingest_drop_rules (tenant_id, is_active, priority DESC);

-- 3. Seed example multi-action rules
INSERT INTO ingest_drop_rules (tenant_id, field_path, operator, value, action, severity_override, priority, description)
VALUES
    ('dev-tenant', 'rule.level', 'gte', '12', 'ALERT', 4, 100, 'Wazuh Critical: Level 12+ → Immediate Alert'),
    ('dev-tenant', 'rule.level', 'lte', '3', 'DROP', NULL, 90, 'Wazuh Noise: Level ≤3 → Silent Drop'),
    ('dev-tenant', 'rule.id', 'eq', '501', 'DROP', NULL, 80, 'Drop syslog login session opened'),
    ('dev-tenant', 'description', 'contains', 'heartbeat', 'DROP', NULL, 70, 'Drop EDR heartbeat noise'),
    ('dev-tenant', 'rule.level', 'gte', '7', 'LOG', NULL, 50, 'Wazuh Moderate: Level 7-11 → Archive to MinIO')
ON CONFLICT DO NOTHING;
