-- NeuralVyuha V5 Product Readiness Migration
-- Consolidates all database changes for Case Status and Wiki Pages

-- 1. Update Case Status Constraint
-- Drops the legacy constraint and adds support for 'IN_PROGRESS'
ALTER TABLE cases DROP CONSTRAINT IF EXISTS cases_status_check;
ALTER TABLE cases ADD CONSTRAINT cases_status_check CHECK (status IN ('OPEN', 'CLOSED', 'IN_PROGRESS'));

-- 2. Initialize Case Wiki Pages Table
-- Ensures the table exists with optimized indexes for large-scale documentation
CREATE TABLE IF NOT EXISTS case_pages (
    tenant_id character varying(64) not null,
    case_id uuid not null,
    page_id uuid not null,
    title character varying(255) not null,
    content text not null,
    created_by character varying(255) not null,
    created_at bigint not null,
    updated_at bigint not null,
    PRIMARY KEY (tenant_id, case_id, page_id),
    CONSTRAINT fk_case_pages_case FOREIGN KEY (tenant_id, case_id) REFERENCES cases(tenant_id, case_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_case_pages_tenant_case ON case_pages (tenant_id, case_id, updated_at DESC);

-- 3. Enterprise Case Tasks Enhancement
-- Adds description, due date, and priority for robust task management
ALTER TABLE case_tasks ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE case_tasks ADD COLUMN IF NOT EXISTS due_date BIGINT;
ALTER TABLE case_tasks ADD COLUMN IF NOT EXISTS priority SMALLINT DEFAULT 2;

-- 4. Optimization: Ensure standard statuses are present for Case Details mapping
-- (No data migration needed, application logic handles adapters)


docker exec nv-postgres psql -U nv_user -d nv_vault -c "ALTER TABLE case_tasks ADD COLUMN flag BOOLEAN DEFAULT FALSE;" //for the bulk operations

docker exec nv-postgres psql -U nv_user -d nv_vault -c "CREATE TABLE IF NOT EXISTS task_shares (tenant_id VARCHAR(100) NOT NULL, task_id UUID NOT NULL, organisation_name VARCHAR(200) NOT NULL, shared_by VARCHAR(200), created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT, PRIMARY KEY (tenant_id, task_id, organisation_name));" //for the test case edit folders