from __future__ import annotations
import time
import uuid
import json
import logging
import sys
import os
import psycopg2
from psycopg2 import errors
from fastapi import FastAPI, Header, HTTPException, Response, status, Depends
from pydantic import BaseModel, Field, ConfigDict, field_validator
from typing import Dict, Any, Optional, List, Union
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Import Auth Middleware
# Robust common library discovery (handles local dev and Docker context)
_base_dir = os.path.dirname(__file__)
_paths_to_check = [
    os.path.abspath(os.path.join(_base_dir, '../../')), # Local dev (parent of common)
    os.path.abspath(os.path.join(_base_dir, '../')),     # Docker (parent of common)
]
for _p in _paths_to_check:
    if os.path.exists(os.path.join(_p, 'common')):
        sys.path.append(_p)
        break

from common.auth.middleware import get_auth_context, AuthContext, validate_auth_config, require_permission
from common.auth.rbac import PERM_CASE_READ, PERM_CASE_WRITE
from common.observability.metrics import MetricsMiddleware, get_metrics_response
from common.observability.health import global_health_registry
from common.config.secrets import get_secret

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger("nv-case-engine")

app = FastAPI(title="NeuralVyuha Case Service")

# Observability
app.add_middleware(MetricsMiddleware, service_name="nv-case-engine")

@app.on_event("startup")
def startup_event():
    validate_auth_config()

    def _ensure_schema():
        try:
            conn = get_db_conn()
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                    -- Core table structure
                    CREATE TABLE IF NOT EXISTS cases (
                        tenant_id VARCHAR(64) NOT NULL,
                        case_id UUID NOT NULL,
                        title TEXT NOT NULL,
                        description TEXT,
                        severity INT NOT NULL DEFAULT 1,
                        status VARCHAR(32) NOT NULL,
                        visibility VARCHAR(32) NOT NULL DEFAULT 'ORGANIZATION',
                        created_by VARCHAR(255) NOT NULL,
                        created_at BIGINT NOT NULL,
                        updated_at BIGINT NOT NULL,
                        PRIMARY KEY (tenant_id, case_id)
                    );

                    -- migration: add case_number and sequence
                    DO $$ 
                    BEGIN 
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='cases' AND column_name='case_number') THEN
                            ALTER TABLE cases ADD COLUMN case_number INTEGER UNIQUE;
                            CREATE SEQUENCE IF NOT EXISTS case_number_seq START 1;
                            ALTER TABLE cases ALTER COLUMN case_number SET DEFAULT nextval('case_number_seq');
                            -- Populate existing
                            UPDATE cases SET case_number = nextval('case_number_seq') WHERE case_number IS NULL;
                            -- Make it mandatory
                            ALTER TABLE cases ALTER COLUMN case_number SET NOT NULL;
                        END IF;
                    END $$;

                    -- Robust index creation
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
                        tlp INT,
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

                    CREATE TABLE IF NOT EXISTS idempotency_keys (
                        tenant_id VARCHAR(64) NOT NULL,
                        key_id VARCHAR(255) NOT NULL,
                        response_json TEXT NOT NULL,
                        created_at BIGINT NOT NULL,
                        PRIMARY KEY (tenant_id, key_id)
                    );

                    -- Missing columns from original migrations
                    ALTER TABLE cases ADD COLUMN IF NOT EXISTS summary TEXT;
                    ALTER TABLE cases ADD COLUMN IF NOT EXISTS resolution_status VARCHAR(64);
                    ALTER TABLE cases ADD COLUMN IF NOT EXISTS impact_status VARCHAR(64);
                    ALTER TABLE cases ADD COLUMN IF NOT EXISTS tlp VARCHAR(16) DEFAULT 'AMBER';
                    ALTER TABLE cases ADD COLUMN IF NOT EXISTS pap VARCHAR(16) DEFAULT 'AMBER';
                    ALTER TABLE cases ADD COLUMN IF NOT EXISTS tags TEXT[];
                    ALTER TABLE cases ADD COLUMN IF NOT EXISTS flag BOOLEAN DEFAULT FALSE;
                    ALTER TABLE cases ADD COLUMN IF NOT EXISTS entity_version INTEGER DEFAULT 1;
                    ALTER TABLE cases ADD COLUMN IF NOT EXISTS custom_fields JSONB DEFAULT '{}'::jsonb;
                    ALTER TABLE case_observables ADD COLUMN IF NOT EXISTS pap INT DEFAULT 2;
                    ALTER TABLE case_tasks ADD COLUMN IF NOT EXISTS start_at BIGINT;
                    ALTER TABLE case_tasks ADD COLUMN IF NOT EXISTS duration BIGINT;
                    """)
            conn.close()
            logger.info("Case Engine schema initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize schema: {e}")

    _ensure_schema()


    # Health Checks
    def check_kafka():
        return producer and producer.bootstrap_connected()

    global_health_registry.add_check("kafka", check_kafka)

    def check_db():
        try:
            conn = get_db_conn()
            conn.close()
            return True
        except:
            return False

    global_health_registry.add_check("db", check_db)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")

# DB Config
POSTGRES_HOST = get_secret("POSTGRES_HOST", "postgres")
POSTGRES_USER = get_secret("POSTGRES_USER", "hive")
POSTGRES_PASSWORD = get_secret("POSTGRES_PASSWORD", "hive")
POSTGRES_DB = get_secret("POSTGRES_DB", "neuralvyuha")

def get_db_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("Kafka producer initialized")
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    producer = None

def emit_event(topic, event_type, payload, auth):
    if not producer:
        logger.warning(f"No producer, skipping event: {event_type}")
        return

    event = {
        "event_id": str(uuid.uuid4()),
        "type": event_type,
        "tenant_id": auth.tenant_id,
        "timestamp": int(time.time() * 1000),
        "schema_version": "1.0",
        "payload": payload
    }

    try:
        producer.send(topic, value=event)
        
        # ── AUTO-TRIGGER WORKFLOW ENGINE ──
        # Mirror all case lifecycle events to the workflow engine
        # so automated playbooks can react to case creation, updates, etc.
        workflow_trigger = {
            "event_id": str(uuid.uuid4()),
            "type": "CaseEvent",
            "tenant_id": auth.tenant_id,
            "timestamp": int(time.time() * 1000),
            "payload": {
                "source": "nv-case-engine",
                "event_type": event_type,
                **payload
            }
        }
        producer.send("workflow.trigger.v1", value=workflow_trigger)
        
        producer.flush()
    except Exception as e:
        logger.error(f"Failed to emit event: {e}")

# ── Cascading Health Checks ───────────────────────────────────────────────────

@app.get("/health")
def health():
    """Cascading health check — verifies all downstream dependencies."""
    result = global_health_registry.check_health()
    result["service"] = "nv-case-engine"
    if result["status"] != "ok":
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=503, content=result)
    return result

@app.get("/healthz")
def healthz():
    """Lightweight liveness probe (Kubernetes)."""
    return {"status": "ok"}

@app.get("/readyz")
def readyz():
    """Readiness probe — only returns ok if dependencies are healthy."""
    result = global_health_registry.check_health()
    if result["status"] != "ok":
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=503, content=result)
    return result

# Models
class CreateCaseRequest(BaseModel):
    title: str
    description: Optional[str] = None
    severity: int = Field(default=2, ge=1, le=4)
    tlp: int = Field(default=2, ge=0, le=3)
    pap: int = Field(default=2, ge=0, le=3)
    assignee: Optional[str] = None
    visibility: str = Field(default="ORGANIZATION")
    permitted_users: Optional[list[str]] = None

    @field_validator("severity", "tlp", "pap", mode="before")
    @classmethod
    def handle_empty_string(cls, v, info):
        if v == "":
            return 2  # Default for all three
        return v

class UpdateCaseRequest(BaseModel):
    model_config = ConfigDict(extra='allow')
    title: Optional[str] = None
    description: Optional[str] = None
    severity: Optional[int] = None
    tlp: Optional[int] = None
    pap: Optional[int] = None
    tags: Optional[list[str]] = None

    @field_validator("severity", "tlp", "pap", mode="before")
    @classmethod
    def handle_empty_string(cls, v):
        if v == "":
            return None
        return v
    status: Optional[str] = None
    assigned_to: Optional[str] = None
    visibility: Optional[str] = None
    permitted_users: Optional[list[str]] = None
    flag: Optional[bool] = None
    resolutionStatus: Optional[str] = None
    impactStatus: Optional[str] = None
    summary: Optional[str] = None

class MergeCasesRequest(BaseModel):
    case_ids: list[str]

class CreateTaskRequest(BaseModel):
    title: str
    description: Optional[str] = None
    assigned_to: Optional[str] = None
    dueDate: Optional[int] = None
    priority: Optional[int] = 2
    flag: Optional[bool] = False

class CreateNoteRequest(BaseModel):
    body: str

class LinkAlertRequest(BaseModel):
    original_event_id: str
    link_reason: Optional[str] = None

class LinkCaseRequest(BaseModel):
    target_case_id: str

class UpdateTaskRequest(BaseModel):
    status: Optional[str] = None
    title: Optional[str] = None
    assigned_to: Optional[str] = None
    description: Optional[str] = None
    dueDate: Optional[int] = None
    priority: Optional[int] = None
    flag: Optional[bool] = None
    startDate: Optional[int] = None
    duration: Optional[int] = None

class BulkUpdateTaskRequest(BaseModel):
    ids: list[str]
    status: Optional[str] = None
    priority: Optional[int] = None
    flag: Optional[bool] = None

class BulkDeleteTaskRequest(BaseModel):
    ids: list[str]

class TemplateTaskRequest(BaseModel):
    title: str
    description: Optional[str] = None

class CreatePageRequest(BaseModel):
    title: str
    content: str

class UpdatePageRequest(BaseModel):
    title: Optional[str] = None
    content: Optional[str] = None


class CreateTemplateRequest(BaseModel):
    name: str
    title_prefix: str
    severity: int = 2
    description: Optional[str] = None
    tasks: list[TemplateTaskRequest] = []

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.get("/readyz")
def readyz():
    result = global_health_registry.check_health()
    if result["status"] != "ok":
        return Response(content=json.dumps(result), status_code=503, media_type="application/json")
    return result

@app.get("/metrics")
def metrics():
    return get_metrics_response()

# T2: Idempotency Helper
def check_idempotency(cur, tenant_id, key):
    cur.execute(
        "SELECT response_json FROM idempotency_keys WHERE tenant_id = %s AND key_id = %s",
        (tenant_id, key)
    )
    row = cur.fetchone()
    if row:
        return json.loads(row[0])
    return None

def save_idempotency(cur, tenant_id, key, response):
    cur.execute(
        "INSERT INTO idempotency_keys (tenant_id, key_id, response_json, created_at) VALUES (%s, %s, %s, %s)",
        (tenant_id, key, json.dumps(response), int(time.time() * 1000))
    )

@app.post("/cases", status_code=201)
def create_case(
    req: CreateCaseRequest,
    response: Response,
    idempotency_key: str = Header(..., alias="Idempotency-Key"), # Mandatory
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    case_id = str(uuid.uuid4())
    now = int(time.time() * 1000)

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # 1. Check Idempotency (T2)
            cached = check_idempotency(cur, auth.tenant_id, idempotency_key)
            if cached:
                response.status_code = 200 # OK, not Created
                return cached

            # 2. Create Case (with entity_version T3)
            cur.execute(
                """
                INSERT INTO cases (tenant_id, case_id, title, description, severity, status, visibility, permitted_users, created_by, assigned_to, created_at, updated_at, entity_version)
                VALUES (%s, %s, %s, %s, %s, 'OPEN', %s, %s, %s, %s, %s, %s, 1)
                RETURNING case_number
                """,
                (auth.tenant_id, case_id, req.title, req.description, req.severity, req.visibility, req.permitted_users, auth.user_id, req.assignee, now, now)
            )
            case_number = cur.fetchone()[0]

            resp_body = {"case_id": case_id, "case_number": case_number, "number": case_number, "status": "created"}

            # 3. Save Idempotency
            save_idempotency(cur, auth.tenant_id, idempotency_key, resp_body)

        conn.commit()
    except psycopg2.IntegrityError:
        conn.rollback()
        # Race condition on idempotency key?
        # Re-check or fail
        raise HTTPException(status_code=409, detail="Idempotency key conflict")
    except Exception as e:
        conn.rollback()
        logger.error(f"DB Error: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    emit_event("nv.cases.created.v1", "CaseCreated", {
        "case_id": case_id,
        "title": req.title,
        "created_by": auth.user_id,
        "version": 1
    }, auth)

    return resp_body

@app.patch("/cases/{case_id}")
def update_case(
    case_id: str,
    req: UpdateCaseRequest,
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    conn = get_db_conn()
    updated_at = int(time.time() * 1000)

    logger.error(f"DEBUG INCOMING REQ: {req.model_dump()}")
    logger.error(f"DEBUG REQ TLP (raw): {req.tlp}")
    logger.error(f"DEBUG REQ TAGS (raw): {req.tags}")

    # Idempotency check for PATCH (optional but recommended for critical updates)
    if idempotency_key:
         with conn.cursor() as cur:
            cached = check_idempotency(cur, auth.tenant_id, idempotency_key)
            if cached:
                return cached

    fields = []
    params = []

    if req.title is not None:
        fields.append("title = %s")
        params.append(req.title)
    if req.description is not None:
        fields.append("description = %s")
        params.append(req.description)
    if req.severity is not None:
        fields.append("severity = %s")
        params.append(req.severity)
    if req.tlp is not None:
        fields.append("tlp = %s")
        params.append(req.tlp)
    if req.pap is not None:
        fields.append("pap = %s")
        params.append(req.pap)
    if req.tags is not None:
        fields.append("tags = %s")
        params.append(req.tags)
    if req.assigned_to is not None:
        fields.append("assigned_to = %s")
        params.append(req.assigned_to)
    if req.status is not None:
        fields.append("status = %s")
        params.append(req.status)
        if req.status == 'CLOSED':
            fields.append("closed_at = %s")
            params.append(updated_at)
        elif req.status in ('OPEN', 'IN_PROGRESS'):
            fields.append("closed_at = NULL")
    if req.visibility is not None:
        fields.append("visibility = %s")
        params.append(req.visibility)
    if req.permitted_users is not None:
        fields.append("permitted_users = %s")
        params.append(req.permitted_users)
    if req.flag is not None:
        fields.append("flag = %s")
        params.append(req.flag)
    if req.resolutionStatus is not None:
        fields.append("resolution_status = %s")
        params.append(req.resolutionStatus)
    if req.impactStatus is not None:
        fields.append("impact_status = %s")
        params.append(req.impactStatus)
    if req.summary is not None:
        fields.append("summary = %s")
        params.append(req.summary)

    # Persist Dynamic Custom Fields
    custom_fields_updates = {}
    dumped_req = req.model_dump(exclude_unset=True)
    for k, v in dumped_req.items():
        if k.startswith("customFields."):
            ref = k.split(".", 1)[1]
            custom_fields_updates[ref] = v

    if custom_fields_updates:
        fields.append("custom_fields = COALESCE(custom_fields, '{}'::jsonb) || %s::jsonb")
        params.append(json.dumps(custom_fields_updates))

    if not fields:
        return {"status": "no_change"}

    fields.append("updated_at = %s")
    params.append(updated_at)

    # T3: Increment Version
    fields.append("entity_version = entity_version + 1")

    params.append(auth.tenant_id)
    params.append(case_id)

    resp_body = {"status": "updated"}

    try:
        with conn.cursor() as cur:
            sql = f"UPDATE cases SET {', '.join(fields)} WHERE tenant_id = %s AND case_id = %s RETURNING entity_version"
            cur.execute(sql, tuple(params))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Case not found")

            new_version = row[0]
            resp_body["version"] = new_version

            if idempotency_key:
                save_idempotency(cur, auth.tenant_id, idempotency_key, resp_body)

        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    emit_event("nv.cases.updated.v1", "CaseUpdated", {
        "case_id": case_id,
        "updates": req.model_dump(exclude_unset=True),
        "updated_by": auth.user_id,
        "version": new_version
    }, auth)

    if req.status == 'CLOSED':
         emit_event("nv.cases.closed.v1", "CaseClosed", {"case_id": case_id, "closed_by": auth.user_id}, auth)

    return resp_body

@app.delete("/cases/{case_id}")
def delete_case(case_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM cases WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, case_id))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Case not found")
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
    
    emit_event("nv.cases.deleted.v1", "CaseDeleted", {"case_id": case_id}, auth)
    return {"status": "deleted"}

@app.delete("/cases/{case_id}/force")
def delete_case_force(case_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    return delete_case(case_id, auth)

@app.post("/cases/merge")
def merge_cases(req: MergeCasesRequest, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    # Stub for enterprise merge. Moving sub-entities requires complex domain logic.
    return {"status": "merged"}

class PromoteAlertRequest(BaseModel):
    alert_ids: list[str]
    title: Optional[str] = None
    template_id: Optional[str] = None
    severity: int = Field(default=2, ge=1, le=4)
    description: Optional[str] = None

@app.post("/alerts/promote", status_code=201)
def promote_alerts_to_case(
    req: PromoteAlertRequest,
    response: Response,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    """Promote one or more alerts into a new investigation case."""
    if not req.alert_ids:
        raise HTTPException(status_code=400, detail="No alert IDs provided")

    case_id = str(uuid.uuid4())
    now = int(time.time() * 1000)
    title = req.title or f"Case from {len(req.alert_ids)} alert(s)"

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Idempotency check
            cached = check_idempotency(cur, auth.tenant_id, idempotency_key)
            if cached:
                response.status_code = 200
                return cached

            # Apply template if provided
            template_desc = None
            if req.template_id:
                cur.execute(
                    "SELECT title_prefix, severity, description FROM case_templates WHERE tenant_id = %s AND template_id = %s",
                    (auth.tenant_id, req.template_id)
                )
                tpl = cur.fetchone()
                if tpl:
                    title = (tpl[0] or "") + " " + title
                    template_desc = tpl[2]

            # Create the case
            cur.execute(
                """
                INSERT INTO cases (tenant_id, case_id, title, description, severity, status, visibility, created_by, created_at, updated_at, entity_version)
                VALUES (%s, %s, %s, %s, %s, 'OPEN', 'ORGANIZATION', %s, %s, %s, 1)
                RETURNING case_number
                """,
                (auth.tenant_id, case_id, title, req.description or template_desc, req.severity, auth.user_id, now, now)
            )
            case_number = cur.fetchone()[0]

            # Link all alerts to the case
            for alert_id in req.alert_ids:
                cur.execute(
                    """
                    INSERT INTO case_alert_links (tenant_id, case_id, original_event_id, linked_by, linked_at, link_reason)
                    VALUES (%s, %s, %s, %s, %s, 'promoted')
                    ON CONFLICT DO NOTHING
                    """,
                    (auth.tenant_id, case_id, alert_id, auth.user_id, now)
                )

            # Apply template tasks if template provided
            if req.template_id:
                cur.execute(
                    "SELECT task_id, title, description FROM case_template_tasks WHERE tenant_id = %s AND template_id = %s",
                    (auth.tenant_id, req.template_id)
                )
                for t_row in cur.fetchall():
                    new_task_id = str(uuid.uuid4())
                    cur.execute(
                        """
                        INSERT INTO case_tasks (tenant_id, case_id, task_id, title, description, status, created_by, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, 'OPEN', %s, %s, %s)
                        """,
                        (auth.tenant_id, case_id, new_task_id, t_row[1], t_row[2], auth.user_id, now, now)
                    )

            resp_body = {"case_id": case_id, "case_number": case_number, "number": case_number, "status": "promoted", "alert_count": len(req.alert_ids)}
            save_idempotency(cur, auth.tenant_id, idempotency_key, resp_body)

        conn.commit()
    except psycopg2.IntegrityError:
        conn.rollback()
        raise HTTPException(status_code=409, detail="Idempotency key conflict")
    except Exception as e:
        conn.rollback()
        logger.error(f"Promote error: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    emit_event("nv.cases.created.v1", "CaseCreated", {
        "case_id": case_id, "title": title, "created_by": auth.user_id,
        "source": "alert_promotion", "alert_ids": req.alert_ids, "version": 1
    }, auth)
    emit_event("nv.alerts.promoted.v1", "AlertsPromoted", {
        "case_id": case_id, "alert_ids": req.alert_ids, "promoted_by": auth.user_id
    }, auth)

    return resp_body

@app.get("/cases/{case_id}/export")
def export_case(case_id: str, password: Optional[str] = None, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    import tempfile, zipfile
    from fastapi.responses import FileResponse
    # Enterprise Export Stub
    _, path = tempfile.mkstemp(suffix=".zip")
    with zipfile.ZipFile(path, 'w') as zipf:
        zipf.writestr('case_export.json', b'{"case_id": "' + case_id.encode() + b'", "status": "exported"}')
    return FileResponse(path, media_type='application/zip', filename=f"{case_id}_export.zip")

@app.post("/cases/{case_id}/links", status_code=201)
def link_case(
    case_id: str,
    req: LinkCaseRequest,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    conn = get_db_conn()
    try:
        now = int(time.time() * 1000)
        with conn.cursor() as cur:
            # Check source case
            cur.execute("SELECT 1 FROM cases WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, case_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Source case not found")
                
            # Check target case
            cur.execute("SELECT 1 FROM cases WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, req.target_case_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Target case not found")
                
            # Perform Bidirectional Linking - A to B
            cur.execute(
                "INSERT INTO case_links (tenant_id, case_id, target_case_id, linked_by, linked_at) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (auth.tenant_id, case_id, req.target_case_id, auth.user_id, now)
            )
            # Perform Bidirectional Linking - B to A
            cur.execute(
                "INSERT INTO case_links (tenant_id, case_id, target_case_id, linked_by, linked_at) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (auth.tenant_id, req.target_case_id, case_id, auth.user_id, now)
            )
            
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
        
    emit_event("nv.cases.linked.v1", "CaseLinked", {
        "case_id": case_id,
        "target_case_id": req.target_case_id,
        "linked_by": auth.user_id
    }, auth)
    
    return {"status": "linked"}

@app.post("/case/{case_id}/task", status_code=201)
def create_task(
    case_id: str,
    req: CreateTaskRequest,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    task_id = str(uuid.uuid4())
    now = int(time.time() * 1000)
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO case_tasks (tenant_id, case_id, task_id, title, description, status, created_by, assigned_to, due_date, priority, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, 'OPEN', %s, %s, %s, %s, %s, %s)
                """,
                (auth.tenant_id, case_id, task_id, req.title, req.description, auth.user_id, req.assigned_to, req.dueDate, req.priority, now, now)
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    emit_event("nv.cases.task.created.v1", "TaskCreated", {
        "case_id": case_id,
        "task_id": task_id,
        "title": req.title
    }, auth)
    return {"task_id": task_id, "status": "created"}

@app.patch("/case/{case_id}/task/{task_id}")
def update_task(
    case_id: str,
    task_id: str,
    req: UpdateTaskRequest,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    fields = []
    params = []
    
    if req.status is not None and str(req.status).strip() != "":
        # Normalize status to TitleCase for consistency
        status_map = {"waiting": "Waiting", "inprogress": "InProgress", "in progress": "InProgress", "completed": "Completed", "open": "Waiting"}
        norm_status = status_map.get(req.status.lower(), req.status)
        fields.append("status = %s")
        params.append(norm_status)
        
    if req.title is not None and str(req.title).strip() != "":
        fields.append("title = %s")
        params.append(req.title)
    if req.assigned_to is not None:
        fields.append("assigned_to = %s")
        params.append(req.assigned_to)
    if req.description is not None:
        fields.append("description = %s")
        params.append(req.description)
    if req.dueDate is not None:
        fields.append("due_date = %s")
        params.append(req.dueDate)
    if req.priority is not None:
        fields.append("priority = %s")
        params.append(req.priority)
    if req.flag is not None:
        fields.append("flag = %s")
        params.append(req.flag)

    if not fields:
        return {"status": "no_change"}

    now = int(time.time() * 1000)

    # Handle automatic StartDate/Duration tracking
    if req.status is not None:
        status_map = {"waiting": "Waiting", "inprogress": "InProgress", "in progress": "InProgress", "completed": "Completed", "open": "Waiting"}
        norm_status = status_map.get(req.status.lower(), req.status)
        if norm_status == "InProgress":
            fields.append("start_at = COALESCE(start_at, %s)")
            params.append(now)
        elif norm_status == "Completed":
            fields.append("duration = %s - COALESCE(start_at, created_at)")
            params.append(now)

    fields.append("updated_at = %s")
    params.append(now)

    params.extend([auth.tenant_id, case_id, task_id])

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            sql = f"UPDATE case_tasks SET {', '.join(fields)} WHERE tenant_id = %s AND case_id = %s AND task_id = %s"
            cur.execute(sql, tuple(params))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Task not found")
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    emit_event("nv.cases.task.updated.v1", "TaskUpdated", {
        "case_id": case_id,
        "task_id": task_id,
        "updates": req.model_dump(exclude_unset=True)
    }, auth)
    return {"status": "updated"}

@app.delete("/case/{case_id}/task/{task_id}")
def delete_task(
    case_id: str,
    task_id: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM case_tasks WHERE tenant_id = %s AND case_id = %s AND task_id = %s", (auth.tenant_id, case_id, task_id))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Task not found")
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    emit_event("nv.cases.task.deleted.v1", "TaskDeleted", {
        "case_id": case_id,
        "task_id": task_id
    }, auth)
    return {"status": "deleted"}

@app.post("/case/{case_id}/task/{task_id}/log", status_code=201)
def create_task_log(
    case_id: str,
    task_id: str,
    req: CreateNoteRequest,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    log_id = str(uuid.uuid4())
    now = int(time.time() * 1000)

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO case_task_logs (tenant_id, case_id, task_id, log_id, body, created_by, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (auth.tenant_id, case_id, task_id, log_id, req.body, auth.user_id, now)
            )
        conn.commit()
    except errors.ForeignKeyViolation:
         conn.rollback()
         raise HTTPException(status_code=404, detail="Case or Task not found")
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    emit_event("nv.cases.task.log.created.v1", "TaskLogCreated", {
        "case_id": case_id,
        "task_id": task_id,
        "log_id": log_id
    }, auth)
    return {"log_id": log_id, "status": "created"}

@app.delete("/case/task/log/{log_id}")
@app.delete("/case/{case_id}/task/{task_id}/log/{log_id}")
def delete_task_log(
    log_id: str,
    case_id: Optional[str] = None,
    task_id: Optional[str] = None,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            sql = "DELETE FROM case_task_logs WHERE tenant_id = %s AND log_id = %s"
            params = [auth.tenant_id, log_id]
            if case_id:
                sql += " AND case_id = %s"
                params.append(case_id)
            if task_id:
                sql += " AND task_id = %s"
                params.append(task_id)
            cur.execute(sql, tuple(params))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Log entry not found")
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
    return {"status": "deleted"}

@app.get("/case/{case_id}/task/{task_id}/shares")
@app.get("/case/task/{task_id}/shares")
@app.get("/task/{task_id}/shares")
def list_task_shares(
    task_id: str,
    case_id: Optional[str] = None,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    # Stub for task shares - assuming shared with current org
    return [{"organisationName": "admin", "owner": True}]

@app.post("/case/{case_id}/task/{task_id}/shares")
@app.post("/case/task/{task_id}/shares")
@app.post("/task/{task_id}/shares")
def update_task_shares(
    task_id: str,
    case_id: Optional[str] = None,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    return {"status": "ok"}

@app.get("/case/{case_id}/task")
def list_tasks(
    case_id: str,
    status: Optional[str] = None,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            query = "SELECT task_id, title, description, status, created_by, assigned_to, due_date, priority, created_at, updated_at, start_at, duration FROM case_tasks WHERE tenant_id = %s AND case_id = %s"
            params = [auth.tenant_id, case_id]
            if status:
                query += " AND status = %s"
                params.append(status.upper())
            query += " ORDER BY created_at ASC"
            cur.execute(query, tuple(params))
            
            columns = [desc[0] for desc in cur.description]
            tasks = []
            for r in cur.fetchall():
                row = dict(zip(columns, r))
                tasks.append({
                    "_id": row["task_id"], "id": row["task_id"], "task_id": row["task_id"],
                    "title": row["title"], "description": row.get("description"), "status": row.get("status"),
                    "createdBy": row["created_by"], "assignee": row.get("assigned_to"), "owner": row.get("assigned_to"),
                    "dueDate": row.get("due_date"), "due_date": row.get("due_date"),
                    "priority": row.get("priority"), "startDate": row.get("start_at") or row["created_at"],
                    "duration": row.get("duration"),
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "group": "default", "flag": row.get("flag", False),
                    "extraData": {"shareCount": 0, "actionRequired": False}
                })
            logger.info(f"Engine fetched {len(tasks)} tasks for case {case_id}")
            return tasks
    finally:
        conn.close()

@app.get("/tasks/{task_id}")
def get_task(
    task_id: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT task_id, case_id, title, description, status, created_by, assigned_to, due_date, priority, created_at, updated_at, start_at, duration FROM case_tasks WHERE tenant_id = %s AND task_id = %s",
                (auth.tenant_id, task_id)
            )
            r = cur.fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Task not found")
            return {
                "_id": r[0], "id": r[0], "task_id": r[0],
                "case_id": r[1], "title": r[2], "description": r[3], "status": r[4],
                "createdBy": r[5], "assignee": r[6], "owner": r[6],
                "dueDate": r[7], "due_date": r[7], "priority": r[8],
                "startDate": r[11] or r[9], "duration": r[12],
                "created_at": r[9], "updated_at": r[10],
                "group": "default", "flag": False,
                "extraData": {"shareCount": 0, "actionRequired": False}
            }
    finally:
        conn.close()

@app.get("/case/{case_id}/task/{task_id}/log")
def list_task_logs(
    task_id: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT log_id, body, created_by, created_at FROM case_task_logs WHERE tenant_id = %s AND task_id = %s ORDER BY created_at DESC",
                (auth.tenant_id, task_id)
            )
            rows = cur.fetchall()
            logs = []
            for r in rows:
                logs.append({
                    "_id": r[0], "id": r[0], "log_id": r[0],
                    "message": r[1], "createdBy": r[2], "createdAt": r[3],
                    "startDate": r[3]
                })
            return logs
    finally:
        conn.close()

@app.delete("/cases/{case_id}/tasks/{task_id}")
def delete_task(
    case_id: str,
    task_id: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM case_tasks WHERE tenant_id = %s AND case_id = %s AND task_id = %s",
                (auth.tenant_id, case_id, task_id)
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Task not found")
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
    emit_event("nv.cases.task.deleted.v1", "TaskDeleted", {"case_id": case_id, "task_id": task_id}, auth)
    return {"status": "deleted"}

@app.post("/templates", status_code=201)
def create_template(
    req: CreateTemplateRequest,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    template_id = str(uuid.uuid4())
    now = int(time.time() * 1000)
    
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO case_templates (tenant_id, template_id, name, title_prefix, severity, description, created_by, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (auth.tenant_id, template_id, req.name, req.title_prefix, req.severity, req.description, auth.user_id, now)
            )

            for t_task in req.tasks:
                t_task_id = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO case_template_tasks (tenant_id, template_id, task_id, title, description)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (auth.tenant_id, template_id, t_task_id, t_task.title, t_task.description)
                )

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    return {"template_id": template_id, "status": "created"}

@app.get("/templates")
def list_templates(
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    conn = get_db_conn()
    templates = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT template_id, name, title_prefix, severity, description, created_by, created_at
                FROM case_templates WHERE tenant_id = %s
                ORDER BY name ASC
                """,
                (auth.tenant_id,)
            )
            for r in cur.fetchall():
                templates.append({
                    "id": r[0],
                    "_id": r[0],
                    "name": r[1],
                    "titlePrefix": r[2],
                    "severity": r[3],
                    "description": r[4],
                    "createdBy": r[5],
                    "createdAt": r[6]
                })

            # Fetch tasks for each template
            if templates:
                cur.execute(
                    "SELECT template_id, title, description FROM case_template_tasks WHERE tenant_id = %s",
                    (auth.tenant_id,)
                )
                tasks_map = {}
                for tr in cur.fetchall():
                    tid = tr[0]
                    if tid not in tasks_map:
                        tasks_map[tid] = []
                    tasks_map[tid].append({"title": tr[1], "description": tr[2]})

                for t in templates:
                    t["tasks"] = tasks_map.get(t["id"], [])

    except Exception as e:
        logger.error(f"Error listing templates: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    return templates

@app.get("/cases/{case_id}/export")
def export_case(
    case_id: str,
    password: Optional[str] = None,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Get Case
            cur.execute(
                "SELECT title, description, severity, status, created_by, assigned_to, created_at, updated_at "
                "FROM cases WHERE tenant_id = %s AND case_id = %s",
                (auth.tenant_id, case_id)
            )
            c_row = cur.fetchone()
            if not c_row:
                raise HTTPException(status_code=404, detail="Case not found")

            case_data = {
                "id": case_id,
                "title": c_row[0],
                "description": c_row[1],
                "severity": c_row[2],
                "status": c_row[3],
                "createdBy": c_row[4],
                "assignedTo": c_row[5],
                "createdAt": c_row[6],
                "updatedAt": c_row[7],
                "tasks": [],
                "pages": []
            }

            # Get Tasks
            cur.execute(
                "SELECT task_id, title, status, created_by, assigned_to, created_at "
                "FROM case_tasks WHERE tenant_id = %s AND case_id = %s",
                (auth.tenant_id, case_id)
            )
            tasks = cur.fetchall()

            for t in tasks:
                t_id = t[0]
                task_data = {
                    "id": t_id,
                    "title": t[1],
                    "status": t[2],
                    "createdBy": t[3],
                    "assignedTo": t[4],
                    "createdAt": t[5],
                    "logs": []
                }

                # Get Task Logs
                cur.execute(
                     "SELECT log_id, body, created_by, created_at "
                     "FROM case_task_logs WHERE tenant_id = %s AND case_id = %s AND task_id = %s ORDER BY created_at ASC",
                     (auth.tenant_id, case_id, t_id)
                )
                logs = cur.fetchall()
                for l in logs:
                    task_data["logs"].append({
                        "id": l[0],
                        "body": l[1],
                        "createdBy": l[2],
                        "createdAt": l[3]
                    })
                
                case_data["tasks"].append(task_data)

            # Get Pages
            cur.execute(
                "SELECT page_id, title, content, created_by, created_at, updated_at "
                "FROM case_pages WHERE tenant_id = %s AND case_id = %s",
                (auth.tenant_id, case_id)
            )
            pages = cur.fetchall()
            for p in pages:
                case_data["pages"].append({
                    "id": p[0],
                    "title": p[1],
                    "content": p[2],
                    "createdBy": p[3],
                    "createdAt": p[4],
                    "updatedAt": p[5]
                })

    except HTTPException:
        raise
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))
    finally:
         conn.close()

    export_payload = {
         "version": "1.0",
         "exportedAt": int(time.time() * 1000),
         "exportedBy": auth.user_id,
         "case": case_data
    }
    case_json_str = json.dumps(export_payload, indent=2)

    if not password:
        return Response(content=case_json_str, media_type="application/json")

    import io
    import pyzipper
    
    zip_buffer = io.BytesIO()
    with pyzipper.AESZipFile(zip_buffer, 'w', compression=pyzipper.ZIP_LZMA, encryption=pyzipper.WZ_AES) as zf:
        zf.setpassword(password.encode('utf-8'))
        zf.writestr(f"case_{case_id}.json", case_json_str)
    
    return Response(
        content=zip_buffer.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=case_{case_id}.zip"}
    )

@app.post("/cases/import", status_code=201)
def import_case(
    import_data: Dict[str, Any],
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    case_data = import_data.get("case", import_data)
    conn = get_db_conn()
    try:
        new_case_id = str(uuid.uuid4())
        now = int(time.time() * 1000)
        
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cases (tenant_id, case_id, title, description, severity, status, created_by, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (auth.tenant_id, new_case_id, case_data.get("title", "Imported Case"), case_data.get("description", ""), case_data.get("severity", 2),
                 case_data.get("status", "OPEN"), auth.user_id, now, now)
            )
            
            for t in case_data.get("tasks", []):
                new_task_id = str(uuid.uuid4())
                cur.execute(
                    "INSERT INTO case_tasks (tenant_id, case_id, task_id, title, status, created_by, assigned_to, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (auth.tenant_id, new_case_id, new_task_id, t.get("title", "Task"), t.get("status", "OPEN"), auth.user_id, t.get("assignedTo"), now, now)
                )
                
                for l in t.get("logs", []):
                    new_log_id = str(uuid.uuid4())
                    cur.execute(
                        "INSERT INTO case_task_logs (tenant_id, case_id, task_id, log_id, body, created_by, created_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (auth.tenant_id, new_case_id, new_task_id, new_log_id, l.get("body", ""), auth.user_id, now)
                    )
            
            for p in case_data.get("pages", []):
                new_page_id = str(uuid.uuid4())
                cur.execute(
                    "INSERT INTO case_pages (tenant_id, case_id, page_id, title, content, created_by, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (auth.tenant_id, new_case_id, new_page_id, p.get("title", "Page"), p.get("content", ""), auth.user_id, now, now)
                )
            conn.commit()
            
            emit_event("nv-cases-events", "nv.cases.imported.v1", {"case_id": new_case_id}, auth)

    except Exception as e:
        conn.rollback()
        logger.error(f"Error importing case: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    return {"status": "success", "case_id": new_case_id}

@app.delete("/cases/{case_id}", status_code=204)
def delete_case(
    case_id: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM cases WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, case_id))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Case not found")
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting case: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    emit_event("nv.cases.deleted.v1", "CaseDeleted", {"case_id": case_id, "deleted_by": auth.user_id}, auth)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.post("/cases/merge", status_code=200)
def merge_cases(
    req: MergeCasesRequest,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    if not req.case_ids or len(req.case_ids) < 2:
        raise HTTPException(status_code=400, detail="Must provide at least two case IDs to merge.")

    # Target case is the first one in the list
    target_case_id = req.case_ids[0]
    source_case_ids = req.case_ids[1:]
    
    conn = get_db_conn()
    now = int(time.time() * 1000)
    
    try:
        with conn.cursor() as cur:
            # Verify target exists
            cur.execute("SELECT 1 FROM cases WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, target_case_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail=f"Target case {target_case_id} not found")

            for src_id in source_case_ids:
                # Append links to target case
                cur.execute(
                    "INSERT INTO case_links (tenant_id, case_id, target_case_id, linked_by, linked_at) "
                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    (auth.tenant_id, target_case_id, src_id, auth.user_id, now)
                )

                # Close source cases
                cur.execute(
                    "UPDATE cases SET status = 'CLOSED', resolution_status = 'Duplicated', summary = %s, closed_at = %s, updated_at = %s, entity_version = entity_version + 1 "
                    "WHERE tenant_id = %s AND case_id = %s",
                    (f"Merged into {target_case_id}", now, now, auth.tenant_id, src_id)
                )

        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        logger.error(f"Error merging cases: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    emit_event("nv.cases.merged.v1", "CasesMerged", {
        "target_case_id": target_case_id,
        "source_case_ids": source_case_ids,
        "merged_by": auth.user_id
    }, auth)

    return {"status": "merged", "target_case_id": target_case_id}

# ── Phase 5.2 Case Pages ────────────────────────────────────────────────────────

@app.post("/case/{case_id}/page", status_code=201)
def create_case_page(
    case_id: str,
    req: CreatePageRequest,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    conn = get_db_conn()
    try:
        page_id = str(uuid.uuid4())
        now = int(time.time() * 1000)
        with conn.cursor() as cur:
            # Check case exists
            cur.execute("SELECT 1 FROM cases WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, case_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Case not found")

            cur.execute(
                "INSERT INTO case_pages (tenant_id, case_id, page_id, title, content, created_by, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (auth.tenant_id, case_id, page_id, req.title, req.content, auth.user_id, now, now)
            )
            conn.commit()
            
            # Emit Event
            emit_event(
                topic="nv-cases-events",
                event_type="nv.cases.page.created.v1",
                payload={"case_id": case_id, "page_id": page_id, "title": req.title},
                auth=auth
            )
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating case page: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    return {"id": page_id, "page_id": page_id, "title": req.title, "content": req.content, "createdAt": now, "createdBy": auth.user_id}

@app.get("/case/{case_id}/page")
def get_case_pages(
    case_id: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    conn = get_db_conn()
    try:
        pages = []
        with conn.cursor() as cur:
            cur.execute(
                "SELECT page_id, title, content, created_by, created_at, updated_at "
                "FROM case_pages WHERE tenant_id = %s AND case_id = %s ORDER BY created_at ASC",
                (auth.tenant_id, case_id)
            )
            for r in cur.fetchall():
                pages.append({
                    "id": r[0],
                    "_id": r[0],
                    "title": r[1],
                    "content": r[2],
                    "createdBy": r[3],
                    "createdAt": r[4],
                    "updatedAt": r[5]
                })
    except Exception as e:
        logger.error(f"Error listing case pages: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    return pages

@app.get("/case/{case_id}/page/{page_id}")
def get_case_page(
    case_id: str,
    page_id: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT page_id, title, content, created_by, created_at, updated_at "
                "FROM case_pages WHERE tenant_id = %s AND case_id = %s AND page_id = %s",
                (auth.tenant_id, case_id, page_id)
            )
            r = cur.fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Page not found")
            return {
                "id": r[0],
                "_id": r[0],
                "title": r[1],
                "content": r[2],
                "createdBy": r[3],
                "createdAt": r[4],
                "updatedAt": r[5]
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving case page: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

@app.patch("/case/{case_id}/page/{page_id}")
def update_case_page(
    case_id: str,
    page_id: str,
    req: UpdatePageRequest,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    conn = get_db_conn()
    try:
        now = int(time.time() * 1000)
        updates = []
        params = []
        if req.title is not None:
            updates.append("title = %s")
            params.append(req.title)
        if req.content is not None:
            updates.append("content = %s")
            params.append(req.content)
        
        if not updates:
            return {"status": "no_change"}
        
        updates.append("updated_at = %s")
        params.append(now)
        
        params.extend([auth.tenant_id, case_id, page_id])
        
        with conn.cursor() as cur:
            sql = f"UPDATE case_pages SET {', '.join(updates)} WHERE tenant_id = %s AND case_id = %s AND page_id = %s"
            cur.execute(sql, tuple(params))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Page not found")
            conn.commit()
            
            # Emit Event
            emit_event(
                topic="nv-cases-events",
                event_type="nv.cases.page.updated.v1",
                payload={"case_id": case_id, "page_id": page_id, "updates": req.model_dump(exclude_unset=True)},
                auth=auth
            )
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating case page: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    return {"status": "updated"}

@app.delete("/case/{case_id}/page/{page_id}", status_code=204)
def delete_case_page(
    case_id: str,
    page_id: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM case_pages WHERE tenant_id = %s AND case_id = %s AND page_id = %s",
                (auth.tenant_id, case_id, page_id)
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Page not found")
            conn.commit()
            
            # Emit Event
            emit_event(
                topic="nv-cases-events",
                event_type="nv.cases.page.deleted.v1",
                payload={"case_id": case_id, "page_id": page_id},
                auth=auth
            )
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting case page: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()
    return Response(status_code=status.HTTP_204_NO_CONTENT)

# ── Phase 9: Observables ────────────────────────────────────────────────────────
class CreateObservableRequest(BaseModel):
    dataType: Optional[str] = None
    data: Optional[Union[str, List[str]]] = None
    type: Optional[str] = None
    value: Optional[Union[str, List[str]]] = None
    message: Optional[str] = None
    tlp: int = Field(default=2, ge=0, le=4)
    pap: int = Field(default=2, ge=0, le=4)
    ioc: bool = False
    sighted: bool = False
    tags: list[str] = []

    def get_type(self) -> str:
        return self.type or self.dataType or "other"

    def get_values(self) -> List[str]:
        val = self.value or self.data or ""
        if isinstance(val, list):
            return val
        return [val]


class UpdateObservableRequest(BaseModel):
    message: Optional[str] = None
    tlp: Optional[int] = None
    pap: Optional[int] = None
    ioc: Optional[bool] = None
    sighted: Optional[bool] = None
    tags: Optional[list[str]] = None

@app.post("/case/{case_id}/observable", status_code=201)
@app.post("/case/{case_id}/artifact", status_code=201)
def create_observable(
    case_id: str,
    req: CreateObservableRequest,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    values = req.get_values()
    now = int(time.time() * 1000)
    conn = get_db_conn()
    created_ids = []
    
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM cases WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, case_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Case not found")
            
            for val in values:
                if not val or not str(val).strip():
                    continue
                obs_id = str(uuid.uuid4())
                cur.execute(
                    """INSERT INTO case_observables (tenant_id, case_id, observable_id, data_type, data, message, tlp, pap, ioc, sighted, tags, created_by, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (auth.tenant_id, case_id, obs_id, req.get_type(), str(val), req.message, req.tlp, req.pap, req.ioc, req.sighted, req.tags, auth.user_id, now)
                )
                created_ids.append(obs_id)
                emit_event(
                    topic="nv-cases-events",
                    event_type="nv.cases.observable.created.v1",
                    payload={"case_id": case_id, "observable_id": obs_id},
                    auth=auth
                )
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating observable: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()
    
    if len(created_ids) == 1:
        return {"observable_id": created_ids[0], "status": "created"}
    return {"observable_ids": created_ids, "count": len(created_ids), "status": "created"}

@app.get("/case/{case_id}/observable")
def get_observables(
    case_id: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT observable_id, data_type, data, message, tlp, pap, ioc, sighted, tags, created_by, created_at FROM case_observables WHERE tenant_id = %s AND case_id = %s ORDER BY created_at DESC",
                (auth.tenant_id, case_id)
            )
            return [{"_id": r[0], "id": r[0], "dataType": r[1], "data": r[2], "message": r[3], "tlp": r[4], "pap": r[5], "ioc": r[6], "sighted": r[7], "tags": r[8] or [], "createdBy": r[9], "createdAt": r[10], "startDate": r[10], "reports": {}} for r in cur.fetchall()]
    finally:
        conn.close()

@app.get("/observable/{obs_id}")
def get_observable(
    obs_id: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT observable_id, data_type, data, message, tlp, pap, ioc, sighted, tags, created_by, created_at, case_id FROM case_observables WHERE tenant_id = %s AND observable_id = %s",
                (auth.tenant_id, obs_id)
            )
            r = cur.fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Observable not found")
            return {"_id": r[0], "id": r[0], "dataType": r[1], "data": r[2], "message": r[3], "tlp": r[4], "pap": r[5], "ioc": r[6], "sighted": r[7], "tags": r[8] or [], "createdBy": r[9], "createdAt": r[10], "startDate": r[10], "caseId": r[11], "reports": {}}
    finally:
        conn.close()

@app.get("/observable/{obs_id}/similar")
def get_similar_observables(
    obs_id: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    # Stub for similarity search
    return []

@app.get("/case/{case_id}/observable/{obs_id}/shares")
@app.get("/case/artifact/{obs_id}/shares")
@app.get("/observable/{obs_id}/shares")
def get_observable_shares(
    obs_id: str,
    case_id: Optional[str] = None,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    # Standard response for shares
    return [{"organisationName": "admin", "owner": True}]

@app.post("/case/{case_id}/observable/{obs_id}/shares")
@app.post("/case/artifact/{obs_id}/shares")
@app.post("/observable/{obs_id}/shares")
def update_observable_shares(
    obs_id: str,
    case_id: Optional[str] = None,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    # Stub for updating shares
    return {"status": "ok"}

@app.patch("/case/{case_id}/observable/{obs_id}")
@app.patch("/observable/{obs_id}")
def update_observable(
    obs_id: str,
    req: UpdateObservableRequest,
    case_id: Optional[str] = None,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            updates, params = [], []
            if req.message is not None:
                updates.append("message = %s"); params.append(req.message)
            if req.tlp is not None:
                updates.append("tlp = %s"); params.append(req.tlp)
            if req.pap is not None:
                updates.append("pap = %s"); params.append(req.pap)
            if req.ioc is not None:
                updates.append("ioc = %s"); params.append(req.ioc)
            if req.sighted is not None:
                updates.append("sighted = %s"); params.append(req.sighted)
            if req.tags is not None:
                updates.append("tags = %s"); params.append(req.tags)
            if not updates:
                return {"status": "no_change"}
            params.extend([auth.tenant_id, obs_id])
            where_clause = "WHERE tenant_id = %s AND observable_id = %s"
            if case_id:
                where_clause += " AND case_id = %s"
                params.append(case_id)
            cur.execute(f"UPDATE case_observables SET {', '.join(updates)} {where_clause}", tuple(params))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Observable not found")
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
    return {"status": "updated"}

@app.delete("/case/{case_id}/observable/{obs_id}")
@app.delete("/observable/{obs_id}")
def delete_observable(
    obs_id: str,
    case_id: Optional[str] = None,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            sql = "DELETE FROM case_observables WHERE tenant_id = %s AND observable_id = %s"
            params = [auth.tenant_id, obs_id]
            if case_id:
                sql += " AND case_id = %s"
                params.append(case_id)
            cur.execute(sql, tuple(params))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Observable not found")
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
    emit_event("nv.cases.observable.deleted.v1", "ObservableDeleted", {"case_id": case_id, "observable_id": obs_id}, auth)
    return {"status": "deleted"}

# ── Phase 9: TTPs (MITRE ATT&CK) ───────────────────────────────────────────────
class CreateTtpRequest(BaseModel):
    tactic: str
    techniqueId: str
    techniqueName: str

@app.post("/case/{case_id}/ttp", status_code=201)
def create_ttp(
    case_id: str,
    req: CreateTtpRequest,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    ttp_id = str(uuid.uuid4())
    now = int(time.time() * 1000)
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM cases WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, case_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Case not found")
            cur.execute(
                """INSERT INTO case_ttps (tenant_id, case_id, ttp_id, tactic, technique_id, technique_name, created_by, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (auth.tenant_id, case_id, ttp_id, req.tactic, req.techniqueId, req.techniqueName, auth.user_id, now)
            )
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
    emit_event("nv.cases.ttp.created.v1", "TTPCreated", {"case_id": case_id, "ttp_id": ttp_id}, auth)
    return {"ttp_id": ttp_id, "status": "created"}

@app.get("/case/{case_id}/ttp")
def get_ttps(
    case_id: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ttp_id, tactic, technique_id, technique_name, created_by, created_at FROM case_ttps WHERE tenant_id = %s AND case_id = %s ORDER BY tactic, technique_id",
                (auth.tenant_id, case_id)
            )
            return [{"_id": r[0], "id": r[0], "tactic": r[1], "techniqueId": r[2], "techniqueName": r[3], "createdBy": r[4], "createdAt": r[5]} for r in cur.fetchall()]
    finally:
        conn.close()

@app.delete("/case/{case_id}/ttp/{ttp_id}")
def delete_ttp(
    case_id: str,
    ttp_id: str,
    auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM case_ttps WHERE tenant_id = %s AND case_id = %s AND ttp_id = %s", (auth.tenant_id, case_id, ttp_id))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="TTP not found")
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
    emit_event("nv.cases.ttp.deleted.v1", "TTPDeleted", {"case_id": case_id, "ttp_id": ttp_id}, auth)
    return {"status": "deleted"}

@app.post("/tasks/bulk-update")
def bulk_update_tasks(request: BulkUpdateTaskRequest, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    if not request.ids:
        return {"status": "success", "count": 0}
        
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            updates = []
            params = []
            if request.status is not None:
                # Normalize status to TitleCase for consistency
                status_map = {"waiting": "Waiting", "inprogress": "InProgress", "completed": "Completed", "open": "Waiting", "cancel": "Cancel"}
                norm_status = status_map.get(request.status.lower(), request.status)
                updates.append("status = %s")
                params.append(norm_status)
            if request.priority is not None:
                updates.append("priority = %s")
                params.append(request.priority)
            if request.flag is not None:
                updates.append("flag = %s")
                params.append(request.flag)
                
            if not updates:
                return {"status": "success", "count": 0}
                
            now = int(time.time() * 1000)
            updates.append("updated_at = %s")
            params.append(now)
            
            sql = f"UPDATE case_tasks SET {', '.join(updates)} WHERE tenant_id = %s AND task_id = ANY(%s)"
            params.extend([auth.tenant_id, request.ids])
            
            cur.execute(sql, tuple(params))
            conn.commit()
            return {"status": "success", "count": cur.rowcount}
    except Exception as e:
        conn.rollback()
        logger.error(f"Bulk update failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.post("/tasks/bulk-delete")
def bulk_delete_tasks(request: BulkDeleteTaskRequest, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    if not request.ids:
        return {"status": "success", "count": 0}
        
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Soft delete logic (status='Cancel')
            cur.execute(
                "UPDATE case_tasks SET status = 'Cancel', updated_at = NOW() WHERE tenant_id = %s AND task_id = ANY(%s)",
                (auth.tenant_id, request.ids)
            )
            conn.commit()
            return {"status": "success", "count": cur.rowcount}
    except Exception as e:
        conn.rollback()
        logger.error(f"Bulk delete failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
