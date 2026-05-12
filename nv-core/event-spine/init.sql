-- Phase 3D Schema: Correlation Groups (Tenant-Isolated)

CREATE TABLE IF NOT EXISTS correlation_groups (
    tenant_id VARCHAR(64) NOT NULL,
    group_id VARCHAR(64) NOT NULL,
    correlation_key TEXT NOT NULL,
    rule_id VARCHAR(64) NOT NULL,
    rule_name TEXT,
    confidence VARCHAR(16) NOT NULL CHECK (confidence IN ('LOW', 'MEDIUM', 'HIGH')),
    status VARCHAR(16) NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'CLOSED', 'MERGED')),
    first_seen BIGINT NOT NULL,
    last_seen BIGINT NOT NULL,
    alert_count INT NOT NULL DEFAULT 1,
    max_severity INT NOT NULL DEFAULT 1,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, group_id)
);

CREATE INDEX IF NOT EXISTS idx_groups_tenant_key ON correlation_groups(tenant_id, correlation_key);
CREATE INDEX IF NOT EXISTS idx_groups_tenant_updated ON correlation_groups(tenant_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_groups_tenant_status ON correlation_groups(tenant_id, status);

CREATE TABLE IF NOT EXISTS correlation_group_alert_links (
    tenant_id VARCHAR(64) NOT NULL,
    group_id VARCHAR(64) NOT NULL,
    original_event_id VARCHAR(64) NOT NULL,
    linked_at BIGINT NOT NULL,
    link_reason TEXT,
    PRIMARY KEY (tenant_id, group_id, original_event_id),
    FOREIGN KEY (tenant_id, group_id) REFERENCES correlation_groups(tenant_id, group_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_links_tenant_alert ON correlation_group_alert_links(tenant_id, original_event_id);

-- Phase 3E/3F Schema: Rule Governance

CREATE TABLE IF NOT EXISTS correlation_rules (
    rule_id VARCHAR(64) PRIMARY KEY,
    rule_name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    confidence VARCHAR(16) NOT NULL CHECK (confidence IN ('LOW', 'MEDIUM', 'HIGH')),
    window_minutes INT NOT NULL DEFAULT 15,
    correlation_key_template TEXT NOT NULL,
    required_fields TEXT[] NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

-- Seed Initial Rules
INSERT INTO correlation_rules (rule_id, rule_name, enabled, confidence, window_minutes, correlation_key_template, required_fields, created_at, updated_at)
VALUES
('R1_HOST_RULE', 'Same host + same rule_id', TRUE, 'HIGH', 15, '{tenant_id}|{host}|{rule_id}', ARRAY['tenant_id', 'host', 'rule_id'], 1700000000000, 1700000000000),
('R2_USER_AUTH', 'Auth anomalies by user', TRUE, 'HIGH', 10, '{tenant_id}|{user}|{auth_type}', ARRAY['tenant_id', 'user', 'auth_type'], 1700000000000, 1700000000000),
('R3_OBSERVABLE_HASH', 'Same observable hash', TRUE, 'MEDIUM', 30, '{tenant_id}|{observable_hash}|{observable_type}', ARRAY['tenant_id', 'observable_hash', 'observable_type'], 1700000000000, 1700000000000)
ON CONFLICT (rule_id) DO NOTHING;

-- Phase E1: Identity & RBAC

CREATE TABLE IF NOT EXISTS roles (
    role_id VARCHAR(64) PRIMARY KEY,
    description TEXT
);

CREATE TABLE IF NOT EXISTS permissions (
    permission_id VARCHAR(64) PRIMARY KEY,
    description TEXT
);

CREATE TABLE IF NOT EXISTS role_permissions (
    role_id VARCHAR(64) REFERENCES roles(role_id) ON DELETE CASCADE,
    permission_id VARCHAR(64) REFERENCES permissions(permission_id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_id)
);

-- Seed Roles
INSERT INTO roles (role_id, description) VALUES
('SOC_ANALYST', 'Standard analyst: read alerts, groups'),
('SOC_LEAD', 'Lead analyst: manage rules, user assignment'),
('RULE_ADMIN', 'Manage correlation rules'),
('TENANT_ADMIN', 'Manage tenant settings'),
('SYSTEM_ADMIN', 'Full system access')
ON CONFLICT (role_id) DO NOTHING;

-- Seed Permissions
INSERT INTO permissions (permission_id, description) VALUES
('alerts.read', 'Read alerts'),
('alerts.write', 'Create/Update alerts'),
('groups.read', 'Read groups'),
('groups.write', 'Manage groups'),
('rules.read', 'Read rules'),
('rules.write', 'Manage rules'),
('correlation.disable', 'Kill switch')
ON CONFLICT (permission_id) DO NOTHING;

-- Seed Role-Permissions
INSERT INTO role_permissions (role_id, permission_id) VALUES
('SOC_ANALYST', 'alerts.read'),
('SOC_ANALYST', 'groups.read'),
('SOC_LEAD', 'alerts.read'),
('SOC_LEAD', 'groups.read'),
('SOC_LEAD', 'rules.read'),
('RULE_ADMIN', 'rules.read'),
('RULE_ADMIN', 'rules.write'),
('RULE_ADMIN', 'correlation.disable'),
('SYSTEM_ADMIN', 'alerts.read'),
('SYSTEM_ADMIN', 'alerts.write'),
('SYSTEM_ADMIN', 'groups.read'),
('SYSTEM_ADMIN', 'groups.write'),
('SYSTEM_ADMIN', 'rules.read'),
('SYSTEM_ADMIN', 'rules.write'),
('SYSTEM_ADMIN', 'correlation.disable')
ON CONFLICT DO NOTHING;

-- Phase E3 Schema: Case Management Domain

CREATE TABLE IF NOT EXISTS cases (
    tenant_id VARCHAR(64) NOT NULL,
    case_id UUID NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    severity INT NOT NULL DEFAULT 1,
    status VARCHAR(32) NOT NULL CHECK (status IN ('OPEN', 'CLOSED')),
    visibility VARCHAR(32) NOT NULL DEFAULT 'ORGANIZATION' CHECK (visibility IN ('ORGANIZATION', 'PRIVATE')),
    permitted_users TEXT[] NULL,
    created_by VARCHAR(255) NOT NULL,
    assigned_to VARCHAR(255) NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    closed_at BIGINT NULL,
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
    status VARCHAR(32) NOT NULL CHECK (status IN ('OPEN', 'DONE', 'CANCELLED')),
    created_by VARCHAR(255) NOT NULL,
    assigned_to VARCHAR(255) NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, case_id, task_id),
    FOREIGN KEY (tenant_id, case_id) REFERENCES cases(tenant_id, case_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tasks_tenant_case ON case_tasks(tenant_id, case_id, updated_at DESC);

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

-- Phase E3.1 Schema: Task Logs & Templates

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

CREATE INDEX IF NOT EXISTS idx_task_logs_tenant_task ON case_task_logs(tenant_id, case_id, task_id, created_at DESC);

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

-- Phase 5.2 Schema: Case Pages
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

