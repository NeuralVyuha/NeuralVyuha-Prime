-- Phase 9: Observables table
CREATE TABLE IF NOT EXISTS case_observables (
    tenant_id VARCHAR(64) NOT NULL,
    case_id UUID NOT NULL,
    observable_id UUID NOT NULL,
    data_type VARCHAR(64) NOT NULL,
    data TEXT NOT NULL,
    message TEXT,
    tlp INT NOT NULL DEFAULT 2,
    ioc BOOLEAN NOT NULL DEFAULT FALSE,
    sighted BOOLEAN NOT NULL DEFAULT FALSE,
    tags TEXT[] DEFAULT '{}',
    created_by VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, case_id, observable_id),
    FOREIGN KEY (tenant_id, case_id) REFERENCES cases(tenant_id, case_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_observables_case ON case_observables(tenant_id, case_id, created_at DESC);

-- Phase 9: TTPs table (MITRE ATT&CK)
CREATE TABLE IF NOT EXISTS case_ttps (
    tenant_id VARCHAR(64) NOT NULL,
    case_id UUID NOT NULL,
    ttp_id UUID NOT NULL,
    tactic VARCHAR(128) NOT NULL,
    technique_id VARCHAR(32) NOT NULL,
    technique_name VARCHAR(255) NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, case_id, ttp_id),
    FOREIGN KEY (tenant_id, case_id) REFERENCES cases(tenant_id, case_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_ttps_case ON case_ttps(tenant_id, case_id);
