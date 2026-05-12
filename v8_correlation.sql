-- ═══════════════════════════════════════════════════════════════════════════════
-- NeuralVyuha Migration: v8_correlation.sql
-- Purpose: Full schema for The Detective — Cross-Source Correlation Engine.
--          Extends Tier 3 tables with enterprise-grade fields.
-- ═══════════════════════════════════════════════════════════════════════════════

-- 1. Extend correlation rules table (full enterprise schema)
CREATE TABLE IF NOT EXISTS ingest_correlation_rules (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id          TEXT NOT NULL DEFAULT 'dev-tenant',
    name               TEXT NOT NULL,
    description        TEXT,
    source_a           TEXT NOT NULL,          -- 'wazuh', 'suricata', 'syslog', etc.
    source_b           TEXT NOT NULL,
    match_field        TEXT NOT NULL,          -- dot-notation field to correlate on
    time_window        INTEGER NOT NULL DEFAULT 300,    -- seconds
    min_count_a        INTEGER NOT NULL DEFAULT 1,      -- min events from source_a
    min_count_b        INTEGER NOT NULL DEFAULT 1,      -- min events from source_b
    action_type        TEXT NOT NULL DEFAULT 'CREATE_CASE'
                           CHECK (action_type IN ('CREATE_CASE', 'PROMOTE_ALERT', 'LOG_ONLY')),
    severity_override  INTEGER NOT NULL DEFAULT 3
                           CHECK (severity_override BETWEEN 1 AND 4),
    mitre_tactic       TEXT NOT NULL DEFAULT 'Unknown',
    is_active          BOOLEAN DEFAULT TRUE,
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Correlation Incidents — audit log of every rule that fired
CREATE TABLE IF NOT EXISTS correlation_incidents (
    id             UUID PRIMARY KEY,
    tenant_id      TEXT NOT NULL,
    rule_id        UUID REFERENCES ingest_correlation_rules(id) ON DELETE SET NULL,
    rule_name      TEXT NOT NULL,
    match_value    TEXT NOT NULL,        -- The shared IP/hostname value
    threat_score   INTEGER NOT NULL,     -- 0–100 composite score
    mitre_tactic   TEXT,
    severity       INTEGER,             -- 1–4
    action_taken   TEXT,
    event_ids      JSONB,              -- Array of all correlated event_ids
    raw_events     JSONB,              -- Full payloads snapshot (source_a + source_b)
    case_id        TEXT,               -- Back-filled by API bridge after case creation
    fired_at       BIGINT,             -- epoch ms
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for fast dashboard queries
CREATE INDEX IF NOT EXISTS idx_corr_rules_tenant  ON ingest_correlation_rules(tenant_id, is_active);
CREATE INDEX IF NOT EXISTS idx_corr_inc_tenant    ON correlation_incidents(tenant_id);
CREATE INDEX IF NOT EXISTS idx_corr_inc_fired     ON correlation_incidents(fired_at DESC);
CREATE INDEX IF NOT EXISTS idx_corr_inc_score     ON correlation_incidents(threat_score DESC);
CREATE INDEX IF NOT EXISTS idx_corr_inc_match     ON correlation_incidents(match_value);

-- 3. Seed enterprise-grade correlation rules (MITRE ATT&CK aligned)
INSERT INTO ingest_correlation_rules
    (name, description, source_a, source_b, match_field, time_window,
     min_count_a, min_count_b, action_type, severity_override, mitre_tactic)
VALUES
    (
        'Brute Force → Successful Login',
        'Detects when multiple authentication failures are followed by a successful login from the same IP. Classic credential stuffing / brute force pattern.',
        'wazuh', 'wazuh',
        'data.srcip', 300,   -- 5 minutes
        5,                   -- 5+ auth failures
        1,                   -- 1 success
        'CREATE_CASE', 4, 'Credential Access'
    ),
    (
        'Network Scan → Exploit Attempt',
        'Detects when a host performs reconnaissance (port scan/probe) followed by an exploit attempt from the same source IP.',
        'suricata', 'suricata',
        'src_ip', 600,       -- 10 minutes
        1, 1,
        'CREATE_CASE', 4, 'Initial Access'
    ),
    (
        'External Recon → Internal Auth Failure',
        'Cross-source: External scan detected by Suricata, followed by internal authentication failure in Wazuh. Suggests hand-off to insider or pivot.',
        'suricata', 'wazuh',
        'data.srcip', 900,   -- 15 minutes
        1, 3,
        'CREATE_CASE', 3, 'Lateral Movement'
    ),
    (
        'Privilege Escalation Attempt',
        'Wazuh detects privilege escalation followed by file system changes to sensitive paths.',
        'wazuh', 'wazuh',
        'data.srcip', 300,
        1, 1,
        'CREATE_CASE', 4, 'Privilege Escalation'
    ),
    (
        'Data Exfiltration Pattern',
        'Large outbound data transfer (Suricata) coinciding with file access events (Wazuh). Classic exfiltration indicator.',
        'suricata', 'wazuh',
        'src_ip', 600,
        1, 1,
        'CREATE_CASE', 4, 'Exfiltration'
    ),
    (
        'C2 Beacon Detected',
        'Repeated small HTTP POST requests to the same destination, consistent with Command and Control beaconing.',
        'suricata', 'suricata',
        'dest_ip', 120,       -- 2 minutes
        3, 1,
        'PROMOTE_ALERT', 3, 'Command and Control'
    ),
    (
        'Suspicious Persistence Install',
        'File creation in system directories (Wazuh) correlated with new cron/service entries (Syslog).',
        'wazuh', 'syslog',
        'data.srcip', 600,
        1, 1,
        'CREATE_CASE', 3, 'Persistence'
    )
ON CONFLICT DO NOTHING;
