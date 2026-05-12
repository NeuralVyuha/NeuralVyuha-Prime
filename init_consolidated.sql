-- ============================================================================
-- NeuralVyuha — Master Database Initialization Script
-- ============================================================================
-- This file creates ALL tables required by every microservice.
-- It is idempotent (safe to run multiple times on the same database).
--
-- Usage:
--   Automatic: Mounted as /docker-entrypoint-initdb.d/init.sql in docker-compose
--   Manual:    psql -U nv_user -d nv_vault -f init.sql
-- ============================================================================


-- ═══════════════════════════════════════════════════════════════════════════════
-- SERVICE: nv-case-engine (Cases, Tasks, Notes, Templates, Pages)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS cases (
    tenant_id VARCHAR(64) NOT NULL,
    case_id UUID NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    severity INT NOT NULL DEFAULT 1,
    status VARCHAR(32) NOT NULL CHECK (status IN ('OPEN', 'IN_PROGRESS', 'RESOLVED', 'CLOSED', 'DUPLICATE', 'FALSE_POSITIVE', 'TRUE_POSITIVE')),
    visibility VARCHAR(32) NOT NULL DEFAULT 'ORGANIZATION' CHECK (visibility IN ('ORGANIZATION', 'PRIVATE')),
    permitted_users TEXT[] NULL,
    created_by VARCHAR(255) NOT NULL,
    assigned_to VARCHAR(255) NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    closed_at BIGINT NULL,
    summary TEXT,
    resolution_status VARCHAR(64),
    impact_status VARCHAR(64),
    tlp VARCHAR(16) DEFAULT 'AMBER',
    pap VARCHAR(16) DEFAULT 'AMBER',
    tags TEXT[],
    flag BOOLEAN DEFAULT FALSE,
    custom_fields JSONB DEFAULT '{}'::jsonb,
    entity_version INTEGER DEFAULT 1,
    PRIMARY KEY (tenant_id, case_id)
);
CREATE INDEX IF NOT EXISTS idx_cases_tenant_status ON cases(tenant_id, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_cases_tenant_severity ON cases(tenant_id, severity, updated_at DESC);

CREATE TABLE IF NOT EXISTS case_links (
    tenant_id VARCHAR(64) NOT NULL,
    case_id UUID NOT NULL,
    target_case_id UUID NOT NULL,
    linked_by VARCHAR(255) NOT NULL,
    linked_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, case_id, target_case_id),
    FOREIGN KEY (tenant_id, case_id) REFERENCES cases(tenant_id, case_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, target_case_id) REFERENCES cases(tenant_id, case_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_case_links_tenant_target ON case_links(tenant_id, target_case_id);

CREATE TABLE IF NOT EXISTS case_tasks (
    tenant_id VARCHAR(64) NOT NULL,
    case_id UUID NOT NULL,
    task_id UUID NOT NULL,
    title TEXT NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('OPEN', 'DONE', 'CANCELLED', 'Waiting', 'InProgress', 'In Progress', 'Completed', 'Cancel', 'WAITING', 'INPROGRESS')),
    created_by VARCHAR(255) NOT NULL,
    assigned_to VARCHAR(255) NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    description TEXT,
    due_date BIGINT,
    priority INT DEFAULT 2,
    flag BOOLEAN DEFAULT FALSE,
    start_at BIGINT,
    duration BIGINT,
    PRIMARY KEY (tenant_id, case_id, task_id),
    FOREIGN KEY (tenant_id, case_id) REFERENCES cases(tenant_id, case_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_tasks_tenant_case ON case_tasks(tenant_id, case_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS case_observables (
    tenant_id VARCHAR(64) NOT NULL,
    case_id UUID NOT NULL,
    observable_id UUID NOT NULL,
    data_type VARCHAR(64) NOT NULL,
    data TEXT NOT NULL,
    message TEXT,
    tlp INT DEFAULT 2,
    pap INT DEFAULT 2,
    ioc BOOLEAN,
    sighted BOOLEAN,
    tags TEXT[],
    created_by VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, case_id, observable_id),
    FOREIGN KEY (tenant_id, case_id) REFERENCES cases(tenant_id, case_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_observables_tenant_case ON case_observables(tenant_id, case_id, created_at DESC);

CREATE TABLE IF NOT EXISTS case_ttps (
    tenant_id VARCHAR(64) NOT NULL,
    case_id UUID NOT NULL,
    ttp_id UUID NOT NULL,
    tactic VARCHAR(128) NOT NULL,
    technique_id VARCHAR(64) NOT NULL,
    technique_name VARCHAR(255) NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, case_id, ttp_id),
    FOREIGN KEY (tenant_id, case_id) REFERENCES cases(tenant_id, case_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_ttps_tenant_case ON case_ttps(tenant_id, case_id, tactic, technique_id);

CREATE TABLE IF NOT EXISTS case_notes (
    tenant_id VARCHAR(64) NOT NULL,
    case_id UUID NOT NULL,
    note_id UUID NOT NULL,
    body TEXT NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, case_id, note_id),
    FOREIGN KEY (tenant_id, case_id) REFERENCES cases(tenant_id, case_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_notes_tenant_case ON case_notes(tenant_id, case_id, created_at DESC);

CREATE TABLE IF NOT EXISTS case_alert_links (
    tenant_id VARCHAR(64) NOT NULL,
    case_id UUID NOT NULL,
    original_event_id VARCHAR(64) NOT NULL,
    linked_by VARCHAR(255) NOT NULL,
    linked_at BIGINT NOT NULL,
    link_reason TEXT NULL,
    PRIMARY KEY (tenant_id, case_id, original_event_id),
    FOREIGN KEY (tenant_id, case_id) REFERENCES cases(tenant_id, case_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_case_links_tenant_alert ON case_alert_links(tenant_id, original_event_id);
CREATE INDEX IF NOT EXISTS idx_case_links_tenant_case ON case_alert_links(tenant_id, case_id, linked_at DESC);

CREATE TABLE IF NOT EXISTS case_task_logs (
    tenant_id VARCHAR(64) NOT NULL,
    case_id UUID NOT NULL,
    task_id UUID NOT NULL,
    log_id UUID NOT NULL,
    body TEXT NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, case_id, task_id, log_id),
    FOREIGN KEY (tenant_id, case_id, task_id) REFERENCES case_tasks(tenant_id, case_id, task_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS artifacts (
    id UUID PRIMARY KEY,
    case_id UUID NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    filename TEXT NOT NULL,
    sha256 VARCHAR(64) NOT NULL,
    size BIGINT NOT NULL,
    minio_key TEXT NOT NULL,
    encryption_key_id TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    FOREIGN KEY (tenant_id, case_id) REFERENCES cases(tenant_id, case_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_artifacts_case_tenant ON artifacts(case_id, tenant_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_sha256_tenant ON artifacts(sha256, tenant_id);
CREATE INDEX IF NOT EXISTS idx_task_logs_tenant_task ON case_task_logs(tenant_id, case_id, task_id, created_at DESC);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id VARCHAR(64) NOT NULL,
    key_id VARCHAR(255) NOT NULL,
    response_json TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, key_id)
);

CREATE TABLE IF NOT EXISTS case_templates (
    tenant_id VARCHAR(64) NOT NULL,
    template_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    title_prefix VARCHAR(255) NOT NULL,
    severity INT NOT NULL DEFAULT 2,
    description TEXT,
    created_by VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, template_id)
);
CREATE INDEX IF NOT EXISTS idx_case_templates_tenant ON case_templates(tenant_id, name);

CREATE TABLE IF NOT EXISTS case_template_tasks (
    tenant_id VARCHAR(64) NOT NULL,
    template_id UUID NOT NULL,
    task_id UUID NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    PRIMARY KEY (tenant_id, template_id, task_id),
    FOREIGN KEY (tenant_id, template_id) REFERENCES case_templates(tenant_id, template_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS case_pages (
    tenant_id VARCHAR(64) NOT NULL,
    case_id UUID NOT NULL,
    page_id UUID NOT NULL,
    title VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, case_id, page_id),
    FOREIGN KEY (tenant_id, case_id) REFERENCES cases(tenant_id, case_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_case_pages_tenant_case ON case_pages(tenant_id, case_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS task_shares (
    tenant_id VARCHAR(64) NOT NULL,
    task_id UUID NOT NULL,
    organisation_name VARCHAR(255) NOT NULL,
    shared_by VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, task_id, organisation_name)
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- SERVICE: nv-correlation (Legacy & AI Correlation Logic)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS correlation_groups (
    tenant_id VARCHAR(64) NOT NULL,
    group_id VARCHAR(64) NOT NULL,
    correlation_key TEXT NOT NULL,
    rule_id VARCHAR(64) NOT NULL,
    rule_name TEXT,
    confidence VARCHAR(16) NOT NULL DEFAULT 'MEDIUM',
    status VARCHAR(16) NOT NULL DEFAULT 'OPEN',
    first_seen BIGINT NOT NULL,
    last_seen BIGINT NOT NULL,
    alert_count INT NOT NULL DEFAULT 1,
    max_severity INT NOT NULL DEFAULT 1,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    visibility_state TEXT DEFAULT 'HIDDEN',
    visibility_threshold INT DEFAULT 3,
    acknowledged_by TEXT,
    acknowledged_at BIGINT,
    dismissed_at BIGINT,
    resolved_at BIGINT,
    PRIMARY KEY (tenant_id, group_id)
);

CREATE TABLE IF NOT EXISTS correlation_group_alert_links (
    tenant_id VARCHAR(64) NOT NULL,
    group_id VARCHAR(64) NOT NULL,
    original_event_id VARCHAR(64) NOT NULL,
    linked_at BIGINT NOT NULL,
    link_reason TEXT,
    PRIMARY KEY (tenant_id, group_id, original_event_id)
);

CREATE TABLE IF NOT EXISTS correlation_rules (
    rule_id VARCHAR(64) PRIMARY KEY,
    rule_name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    confidence VARCHAR(16) NOT NULL DEFAULT 'MEDIUM',
    window_minutes INT NOT NULL DEFAULT 15,
    correlation_key_template TEXT NOT NULL,
    required_fields TEXT[] NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    definition_cel TEXT,
    name_template TEXT,
    visibility_threshold INT DEFAULT 3
);

-- AI Cluster Cache for LLM-based grouping
CREATE TABLE IF NOT EXISTS ai_correlation_cache (
    input_hash VARCHAR(64) PRIMARY KEY,
    clusters_json JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- SERVICE: nv-ingest (Drop Rules, Dedup, Extraction, Mapping)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS ingest_drop_rules (
    id SERIAL PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    field_path VARCHAR(255) NOT NULL,
    operator VARCHAR(16) NOT NULL DEFAULT 'eq',
    value TEXT NOT NULL,
    action VARCHAR(10) DEFAULT 'DROP',
    severity_override INTEGER DEFAULT NULL,
    priority INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ingest_dedup_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL DEFAULT 'dev-tenant',
    source TEXT NOT NULL,
    field_paths TEXT[] NOT NULL,
    window_seconds INTEGER NOT NULL DEFAULT 3600,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(tenant_id, source)
);

-- Tier 3 Correlation Rules (Enterprise UUID Schema)
CREATE TABLE IF NOT EXISTS ingest_correlation_rules (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id          TEXT NOT NULL DEFAULT 'dev-tenant',
    name               TEXT NOT NULL,
    description        TEXT,
    source_a           TEXT NOT NULL,
    source_b           TEXT NOT NULL,
    match_field        TEXT NOT NULL,
    time_window        INTEGER NOT NULL DEFAULT 300,
    min_count_a        INTEGER NOT NULL DEFAULT 1,
    min_count_b        INTEGER NOT NULL DEFAULT 1,
    action_type        TEXT NOT NULL DEFAULT 'CREATE_CASE'
                           CHECK (action_type IN ('CREATE_CASE', 'PROMOTE_ALERT', 'LOG_ONLY')),
    severity_override  INTEGER NOT NULL DEFAULT 3
                           CHECK (severity_override BETWEEN 1 AND 4),
    mitre_tactic       TEXT NOT NULL DEFAULT 'Unknown',
    is_active          BOOLEAN DEFAULT TRUE,
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW()
);

-- Audit log of rule firings
CREATE TABLE IF NOT EXISTS correlation_incidents (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      TEXT NOT NULL,
    rule_id        UUID REFERENCES ingest_correlation_rules(id) ON DELETE SET NULL,
    rule_name      TEXT NOT NULL,
    match_value    TEXT NOT NULL,
    threat_score   INTEGER NOT NULL DEFAULT 0,
    mitre_tactic   TEXT,
    severity       INTEGER,
    action_taken   TEXT,
    event_ids      JSONB,
    raw_events     JSONB,
    case_id        TEXT,
    fired_at       BIGINT,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingest_extraction_rules (
    id SERIAL PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    rule_name VARCHAR(255) NOT NULL,
    attribute VARCHAR(255) NOT NULL,
    regex TEXT NOT NULL,
    condition TEXT,
    priority INTEGER DEFAULT 0,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ingest_mapping_rules (
    id SERIAL PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    rule_name VARCHAR(255) NOT NULL,
    match_fields JSONB NOT NULL,
    mapping_data JSONB NOT NULL,
    priority INTEGER DEFAULT 0,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- SERVICE: nv-workflow (Definitions, Executions, Checkpoints)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS workflow_definitions (
    id SERIAL PRIMARY KEY,
    workflow_id TEXT UNIQUE NOT NULL,
    tenant_id TEXT DEFAULT 'dev-tenant',
    name TEXT NOT NULL,
    description TEXT DEFAULT '',
    trigger_config JSONB DEFAULT '{}',
    steps JSONB DEFAULT '[]',
    enabled BOOLEAN DEFAULT TRUE,
    created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
    updated_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT
);

CREATE TABLE IF NOT EXISTS workflow_executions (
    id SERIAL PRIMARY KEY,
    execution_id TEXT UNIQUE NOT NULL,
    workflow_id TEXT NOT NULL,
    tenant_id TEXT DEFAULT 'dev-tenant',
    status TEXT DEFAULT 'RUNNING',
    current_step_id TEXT,
    total_steps INT DEFAULT 0,
    started_at BIGINT,
    finished_at BIGINT,
    trigger_event JSONB DEFAULT '{}',
    step_results JSONB DEFAULT '[]',
    error TEXT,
    created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT
);

CREATE TABLE IF NOT EXISTS workflow_checkpoints (
    execution_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    tenant_id TEXT DEFAULT 'dev-tenant',
    current_step_id TEXT,
    total_steps INT DEFAULT 0,
    status TEXT DEFAULT 'RUNNING',
    context JSONB DEFAULT '{}',
    error TEXT,
    created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
    updated_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
    wait_until BIGINT DEFAULT NULL
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- SERVICE: nv-query / nv-ingest (Policies & Nodes)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS ingestion_policies (
    id SERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'dev-tenant',
    source_name TEXT NOT NULL,
    is_enabled BOOLEAN DEFAULT TRUE,
    rate_limit_eps INT DEFAULT 1000,
    max_payload_kb INT DEFAULT 1024,
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(tenant_id, source_name)
);

CREATE TABLE IF NOT EXISTS integration_nodes (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    node_type       VARCHAR(16)   NOT NULL CHECK (node_type IN ('cortex', 'misp')),
    name            VARCHAR(128)  NOT NULL,
    url             TEXT          NOT NULL,
    api_key_enc     TEXT          NOT NULL DEFAULT '',
    tls_verify      BOOLEAN       NOT NULL DEFAULT TRUE,
    status          VARCHAR(16)   NOT NULL DEFAULT 'UNKNOWN'
                                  CHECK (status IN ('UP', 'DEGRADED', 'DOWN', 'UNKNOWN')),
    latency_ms      INTEGER,
    http_status     INTEGER,
    last_seen       TIMESTAMP WITH TIME ZONE,
    last_error      TEXT,
    probe_count     BIGINT        NOT NULL DEFAULT 0,
    created_by      VARCHAR(128),
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_nodes_type ON integration_nodes (node_type);

-- Auto-update trigger for updated_at
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


-- ═══════════════════════════════════════════════════════════════════════════════
-- SEED DATA — Critical Baselines
-- ═══════════════════════════════════════════════════════════════════════════════

-- Seed default ingestion policies
INSERT INTO ingestion_policies (tenant_id, source_name) VALUES
    ('dev-tenant', 'wazuh'),
    ('dev-tenant', 'suricata'),
    ('dev-tenant', 'syslog')
ON CONFLICT DO NOTHING;

-- Seed default dedup configurations
INSERT INTO ingest_dedup_configs (source, field_paths, window_seconds) VALUES
    ('wazuh', ARRAY['data.srcip', 'rule.id'], 3600),
    ('suricata', ARRAY['alert.signature', 'src_ip'], 3600),
    ('syslog', ARRAY['message', 'hostname'], 1800)
ON CONFLICT (tenant_id, source) DO NOTHING;

-- Seed default correlation rules (MITRE ATT&CK aligned)
INSERT INTO ingest_correlation_rules
    (name, description, source_a, source_b, match_field, time_window,
     min_count_a, min_count_b, action_type, severity_override, mitre_tactic)
VALUES
    ('Brute Force → Successful Login', 'Detects auth failures followed by success.', 'wazuh', 'wazuh', 'data.srcip', 300, 5, 1, 'CREATE_CASE', 4, 'Credential Access'),
    ('Network Scan → Exploit Attempt', 'Detects recon followed by exploit.', 'suricata', 'suricata', 'src_ip', 600, 1, 1, 'CREATE_CASE', 4, 'Initial Access')
ON CONFLICT DO NOTHING;

-- Seed legacy correlation rules
INSERT INTO correlation_rules (rule_id, rule_name, enabled, confidence, window_minutes, correlation_key_template, required_fields, created_at, updated_at)
VALUES
    ('R1_HOST_RULE', 'Same host + same rule_id', TRUE, 'HIGH', 15, '{tenant_id}|{host}|{rule_id}', ARRAY['tenant_id', 'host', 'rule_id'], 1700000000000, 1700000000000),
    ('R2_USER_AUTH', 'Auth anomalies by user', TRUE, 'HIGH', 10, '{tenant_id}|{user}|{auth_type}', ARRAY['tenant_id', 'user', 'auth_type'], 1700000000000, 1700000000000)
ON CONFLICT (rule_id) DO NOTHING;

-- ═══════════════════════════════════════════════════════════════════════════════
-- DONE — Consolidated NeutralVyuha Initialization COMPLETE
-- ═══════════════════════════════════════════════════════════════════════════════
