-- Ingestion Control Hub: Policy Management Schema
-- Database: nv_vault | Table: ingestion_policies

CREATE TABLE IF NOT EXISTS ingestion_policies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(50) NOT NULL,
    source_name VARCHAR(50) NOT NULL,
    is_enabled BOOLEAN DEFAULT TRUE,
    rate_limit_eps INTEGER DEFAULT 100,
    max_payload_kb INTEGER DEFAULT 1024,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, source_name)
);

CREATE INDEX IF NOT EXISTS idx_ingest_policy_tenant_source ON ingestion_policies (tenant_id, source_name);

-- Seed some default policies for common sources
INSERT INTO ingestion_policies (tenant_id, source_name, is_enabled, rate_limit_eps)
VALUES 
    ('default-tenant', 'syslog', TRUE, 500),
    ('default-tenant', 'misp', TRUE, 100),
    ('default-tenant', 'sentinel', TRUE, 200),
    ('default-tenant', 'webhook', TRUE, 50)
ON CONFLICT DO NOTHING;


--docker exec -i nv-postgres psql -U nv_user -d nv_vault < v5_ingestion_policies.sql && docker exec -i nv-postgres psql -U nv_user -d nv_vault -c "INSERT INTO ingestion_policies (tenant_id, source_name, is_enabled, rate_limit_eps) VALUES ('dev-tenant', 'syslog', TRUE, 1000), ('dev-tenant', 'sentinel', TRUE, 500) ON CONFLICT DO NOTHING;"
--Get-Content v5_ingestion_policies.sql | docker exec -i nv-postgres psql -U nv_user -d nv_vault; docker exec -i nv-postgres psql -U nv_user -d nv_vault -c "INSERT INTO ingestion_policies (tenant_id, source_name, is_enabled, rate_limit_eps) VALUES ('dev-tenant', 'syslog', TRUE, 1000), ('dev-tenant', 'sentinel', TRUE, 500) ON CONFLICT DO NOTHING;"