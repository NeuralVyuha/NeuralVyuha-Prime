import time
import uuid
import json
import logging
import sys
import os
import socket
import psycopg2
import hashlib
import itertools
from fastapi import FastAPI, HTTPException, Response, status, Depends, Body, Query, Request
from pydantic import BaseModel, Field, field_validator
from opensearchpy import OpenSearch
from typing import Dict, Any, Optional, List
import requests

# Robust common library discovery (handles local dev and Docker context)
_base_dir = os.path.dirname(__file__)
sys.path.append(_base_dir)  # For cel_translator (same dir as main.py)
_paths_to_check = [
    os.path.abspath(os.path.join(_base_dir, '../../')),  # Local dev
    os.path.abspath(os.path.join(_base_dir, '../')),     # Docker (/app)
]
for _p in _paths_to_check:
    if os.path.exists(os.path.join(_p, 'common')):
        sys.path.insert(0, _p)
        break

from cel_translator import cel_to_opensearch

from common.auth.middleware import get_auth_context, AuthContext, validate_auth_config, require_permission
from common.auth.rbac import PERM_ALERT_READ, PERM_ALERT_WRITE, PERM_CASE_READ, PERM_CASE_WRITE, PERM_RULE_SIMULATE, PERM_GRAPH_READ
from common.observability.metrics import MetricsMiddleware, get_metrics_response
from common.observability.health import global_health_registry

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger("query-service")

app = FastAPI(title="NeuralVyuha Query Service")

app.add_middleware(MetricsMiddleware, service_name="nv-query")

@app.on_event("startup")
def startup_event():
    validate_auth_config()
    
    def check_os():
        return os_client.ping()
        
    def check_pg():
        try:
            conn = get_db_conn()
            conn.close()
            return True
        except:
            return False
            
    global_health_registry.add_check("opensearch", check_os)
    global_health_registry.add_check("postgres", check_pg)

# Configuration
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "opensearch")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", 9200))
INDEX_ALERTS = "alerts-v1-*"
INDEX_GROUPS = "groups-v1"

# Postgres Config
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB = os.getenv("POSTGRES_DB", "nv_vault")
PG_USER = os.getenv("POSTGRES_USER", "nv_user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "nv_pass")

os_client = OpenSearch(
    hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
    http_compress=True,
    use_ssl=False
)

def get_db_conn():
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"DB Connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

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

@app.get("/health")
def health_check():
    return healthz()

@app.get("/v1/user/current")
def get_current_user(auth: AuthContext = Depends(get_auth_context)):
    return {
        "login": auth.user_id if auth.user_id != "dev-user" else "admin@neuralvyuha.local",
        "name": "Administrator",
        "organisation": "admin",
        "tenant_id": auth.tenant_id,
        "roles": auth.roles,
        "permissions": list(auth.permissions),
        "token": auth.token
    }

from pydantic import BaseModel
import jwt

class LoginRequest(BaseModel):
    user: str
    password: str
    code: Optional[str] = None

@app.post("/login")
def login(req: LoginRequest):
    # Enterprise Dev Mock
    if req.user != "admin" or req.password != "admin":
        raise HTTPException(status_code=401, detail="Invalid username or password")
        
    secret = os.getenv("JWT_SECRET_KEY", "prod-secret-change-me")
    payload = {
        "sub": "admin@neuralvyuha.local",
        "tenant_id": "dev-tenant",
        "roles": ["SYSTEM_ADMIN"],
        "exp": int(time.time()) + 3600
    }
    token = jwt.encode(payload, secret, algorithm="HS256")
    return {"status": "success", "token": token}

# ── Ingestion Control Hub API ────────────────────────────────────────────────

class IngestionPolicyUpdate(BaseModel):
    is_enabled: Optional[bool] = None
    rate_limit_eps: Optional[int] = Field(None, ge=1, le=10000)
    max_payload_kb: Optional[int] = Field(None, ge=1, le=10240)

    @field_validator("rate_limit_eps", "max_payload_kb", mode="before")
    @classmethod
    def handle_empty_string(cls, v):
        if v == "" or v is None:
            return None
        return v

@app.get("/ingest/policies")
@app.get("/api/ingest/policies")
def list_ingestion_policies(auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))):
    """List all ingestion policies for the tenant."""
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT source_name, is_enabled, rate_limit_eps, max_payload_kb, updated_at FROM ingestion_policies WHERE tenant_id = %s",
                (auth.tenant_id,)
            )
            rows = cur.fetchall()
            return [
                {
                    "source": r[0],
                    "enabled": r[1],
                    "eps": r[2],
                    "maxPayload": r[3],
                    "updatedAt": r[4]
                } for r in rows
            ]
    finally:
        conn.close()

@app.patch("/ingest/policies/{source}")
@app.patch("/api/ingest/policies/{source}")
def update_ingestion_policy(
    source: str,
    update: IngestionPolicyUpdate,
    auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))
):
    """Update a specific ingestion policy (requires admin write permissions)."""
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Check if exists
            cur.execute(
                "SELECT id FROM ingestion_policies WHERE tenant_id = %s AND source_name = %s",
                (auth.tenant_id, source)
            )
            if not cur.fetchone():
                # Create if not exists with defaults
                cur.execute(
                    "INSERT INTO ingestion_policies (tenant_id, source_name) VALUES (%s, %s)",
                    (auth.tenant_id, source)
                )
            
            # Apply updates
            fields = []
            params = []
            if update.is_enabled is not None:
                fields.append("is_enabled = %s")
                params.append(update.is_enabled)
            if update.rate_limit_eps is not None:
                fields.append("rate_limit_eps = %s")
                params.append(update.rate_limit_eps)
            if update.max_payload_kb is not None:
                fields.append("max_payload_kb = %s")
                params.append(update.max_payload_kb)
            
            if not fields:
                return {"status": "no_change"}
            
            fields.append("updated_at = CURRENT_TIMESTAMP")
            params.extend([auth.tenant_id, source])
            
            sql = f"UPDATE ingestion_policies SET {', '.join(fields)} WHERE tenant_id = %s AND source_name = %s"
            cur.execute(sql, tuple(params))
            conn.commit()
            return {"status": "updated", "source": source}
    finally:
        conn.close()

# ── Engine Control Proxy (UI → nv-ingest) ────────────────────────────────────
NV_INGEST_URL = os.getenv("NV_INGEST_URL", "http://nv-ingest:8000")

def _proxy_engine(method: str, path: str, json_body=None):
    """Forward engine control requests to nv-ingest."""
    url = f"{NV_INGEST_URL}{path}"
    try:
        if method == "GET":
            resp = requests.get(url, timeout=5)
        else:
            resp = requests.post(url, json=json_body or {}, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        raise HTTPException(status_code=502, detail="nv-ingest engine is unreachable")
    except requests.exceptions.Timeout:
        raise HTTPException(status_code=504, detail="nv-ingest engine timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Engine proxy error: {str(e)}")

@app.get("/api/ingest/engine/status")
@app.get("/ingest/engine/status")
def proxy_engine_status(auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))):
    return _proxy_engine("GET", "/engine/status")

@app.post("/api/ingest/engine/pause")
@app.post("/ingest/engine/pause")
def proxy_engine_pause(auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    return _proxy_engine("POST", "/engine/pause")

@app.post("/api/ingest/engine/resume")
@app.post("/ingest/engine/resume")
def proxy_engine_resume(auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    return _proxy_engine("POST", "/engine/resume")

@app.post("/api/ingest/engine/dry-run")
@app.post("/ingest/engine/dry-run")
async def proxy_engine_dry_run(request: Request, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    body = await request.json()
    return _proxy_engine("POST", "/engine/dry-run", body)

@app.post("/api/ingest/engine/log-level")
@app.post("/ingest/engine/log-level")
async def proxy_engine_log_level(request: Request, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    body = await request.json()
    return _proxy_engine("POST", "/engine/log-level", body)

@app.post("/api/ingest/engine/circuit-breaker")
@app.post("/ingest/engine/circuit-breaker")
async def proxy_engine_circuit_breaker(request: Request, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    body = await request.json()
    return _proxy_engine("POST", "/engine/circuit-breaker", body)

@app.get("/api/ingest/engine/metrics")
@app.get("/ingest/engine/metrics")
def proxy_engine_metrics(auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))):
    return _proxy_engine("GET", "/engine/metrics")

@app.get("/api/ingest/engine/live-stream")
@app.get("/ingest/engine/live-stream")
def proxy_engine_live_stream(auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))):
    return _proxy_engine("GET", "/engine/live-stream")

# ── Drop Rules CRUD ──────────────────────────────────────────────────────────

class DropRuleCreate(BaseModel):
    field_path: str
    operator: str = "eq"
    value: str
    action: str = "DROP"  # DROP, LOG, ALERT
    severity_override: Optional[int] = None
    priority: int = 0
    description: Optional[str] = None

    @field_validator("severity_override", "priority", mode="before")
    @classmethod
    def handle_empty_string(cls, v, info):
        if v == "":
            return None if info.field_name == "severity_override" else 0
        return v

@app.get("/api/ingest/drop-rules")
@app.get("/ingest/drop-rules")
def list_drop_rules(auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, field_path, operator, value, action, severity_override,
                          is_active, description, priority, created_at
                   FROM ingest_drop_rules WHERE tenant_id = %s
                   ORDER BY priority DESC, created_at DESC""",
                (auth.tenant_id,)
            )
            return [
                {"id": str(r[0]), "field": r[1], "operator": r[2], "value": r[3],
                 "action": r[4] or "DROP", "severityOverride": r[5],
                 "active": r[6], "description": r[7], "priority": r[8] or 0,
                 "createdAt": str(r[9])}
                for r in cur.fetchall()
            ]
    finally:
        conn.close()

@app.post("/api/ingest/drop-rules")
@app.post("/ingest/drop-rules")
def create_drop_rule(rule: DropRuleCreate, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO ingest_drop_rules
                   (tenant_id, field_path, operator, value, action, severity_override, priority, description)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (auth.tenant_id, rule.field_path, rule.operator, rule.value,
                 rule.action.upper(), rule.severity_override, rule.priority, rule.description)
            )
            row = cur.fetchone()
            conn.commit()
            return {"status": "created", "id": str(row[0]), "action": rule.action.upper()}
    finally:
        conn.close()

@app.delete("/api/ingest/drop-rules/{rule_id}")
@app.delete("/ingest/drop-rules/{rule_id}")
def delete_drop_rule(rule_id: str, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM ingest_drop_rules WHERE id = %s AND tenant_id = %s",
                (rule_id, auth.tenant_id)
            )
            conn.commit()
            return {"status": "deleted"}
    finally:
        conn.close()


# ── Deduplication CRUD ──────────────────────────────────────────────────────────

class DedupConfigCreate(BaseModel):
    source: str
    field_paths: List[str]
    window_seconds: int = 3600

@app.get("/api/ingest/dedup-configs")
@app.get("/ingest/dedup-configs")
def list_dedup_configs(auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, source, field_paths, window_seconds, is_active, created_at
                   FROM ingest_dedup_configs WHERE tenant_id = %s
                   ORDER BY source ASC""",
                (auth.tenant_id,)
            )
            return [
                {"id": str(r[0]), "source": r[1], "fields": r[2],
                 "window": r[3], "active": r[4], "createdAt": str(r[5])}
                for r in cur.fetchall()
            ]
    finally:
        conn.close()

@app.post("/api/ingest/dedup-configs")
@app.post("/ingest/dedup-configs")
def create_dedup_config(config: DedupConfigCreate, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO ingest_dedup_configs (tenant_id, source, field_paths, window_seconds)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (tenant_id, source) DO UPDATE
                   SET field_paths = EXCLUDED.field_paths,
                       window_seconds = EXCLUDED.window_seconds,
                       updated_at = NOW()
                   RETURNING id""",
                (auth.tenant_id, config.source, config.field_paths, config.window_seconds)
            )
            row = cur.fetchone()
            conn.commit()
            return {"status": "created", "id": str(row[0])}
    finally:
        conn.close()

@app.delete("/api/ingest/dedup-configs/{config_id}")
@app.delete("/ingest/dedup-configs/{config_id}")
def delete_dedup_config(config_id: str, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM ingest_dedup_configs WHERE id = %s AND tenant_id = %s",
                (config_id, auth.tenant_id)
            )
            conn.commit()
            return {"status": "deleted"}
    finally:
        conn.close()

@app.patch("/api/ingest/drop-rules/{rule_id}")
@app.patch("/ingest/drop-rules/{rule_id}")
async def toggle_drop_rule(rule_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    body = await request.json()
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ingest_drop_rules SET is_active = %s WHERE id = %s AND tenant_id = %s",
                (body.get("active", True), rule_id, auth.tenant_id)
            )
            conn.commit()
            return {"status": "updated"}
    finally:
        conn.close()


# ── Tier 3: The Detective — Correlation Rules CRUD ─────────────────────────────

class CorrelationRuleCreate(BaseModel):
    name:               str
    description:        Optional[str] = ""
    source_a:           str
    source_b:           str
    match_field:        str
    time_window:        int = 300
    min_count_a:        int = 1
    min_count_b:        int = 1
    action_type:        str = "CREATE_CASE"
    severity_override:  int = 3
    mitre_tactic:       str = "Unknown"

    @field_validator("time_window", "min_count_a", "min_count_b", "severity_override", mode="before")
    @classmethod
    def handle_empty_string(cls, v, info):
        if v == "":
            # Return defaults based on the field name
            defaults = {
                "time_window": 300,
                "min_count_a": 1,
                "min_count_b": 1,
                "severity_override": 3
            }
            return defaults.get(info.field_name, v)
        return v

@app.get("/api/ingest/correlation-rules")
@app.get("/ingest/correlation-rules")
def list_correlation_rules(auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT r.id, r.name, r.description, r.source_a, r.source_b,
                       r.match_field, r.time_window, r.min_count_a, r.min_count_b,
                       r.action_type, r.severity_override, r.mitre_tactic,
                       r.is_active, r.created_at,
                       COUNT(i.id) AS incident_count
                FROM ingest_correlation_rules r
                LEFT JOIN correlation_incidents i ON i.rule_id = r.id
                WHERE r.tenant_id = %s OR r.tenant_id = 'dev-tenant'
                GROUP BY r.id
                ORDER BY r.created_at DESC
            """, (auth.tenant_id,))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return [
                {c: (str(v) if hasattr(v, 'hex') else v) for c, v in zip(cols, row)}
                for row in rows
            ]
    finally:
        conn.close()

@app.post("/api/ingest/correlation-rules")
@app.post("/ingest/correlation-rules")
def create_correlation_rule(rule: CorrelationRuleCreate, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ingest_correlation_rules
                    (tenant_id, name, description, source_a, source_b, match_field,
                     time_window, min_count_a, min_count_b, action_type,
                     severity_override, mitre_tactic)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                auth.tenant_id, rule.name, rule.description,
                rule.source_a, rule.source_b, rule.match_field,
                rule.time_window, rule.min_count_a, rule.min_count_b,
                rule.action_type, rule.severity_override, rule.mitre_tactic,
            ))
            row = cur.fetchone()
            conn.commit()
            return {"status": "created", "id": str(row[0])}
    finally:
        conn.close()

@app.patch("/api/ingest/correlation-rules/{rule_id}")
@app.patch("/ingest/correlation-rules/{rule_id}")
async def toggle_correlation_rule(rule_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    body = await request.json()
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE ingest_correlation_rules SET is_active = %s, updated_at = NOW()
                WHERE id = %s AND (tenant_id = %s OR tenant_id = 'dev-tenant')
            """, (body.get("active", True), rule_id, auth.tenant_id))
            conn.commit()
            return {"status": "updated"}
    finally:
        conn.close()

@app.delete("/api/ingest/correlation-rules/{rule_id}")
@app.delete("/ingest/correlation-rules/{rule_id}")
def delete_correlation_rule(rule_id: str, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM ingest_correlation_rules
                WHERE id = %s AND (tenant_id = %s OR tenant_id = 'dev-tenant')
            """, (rule_id, auth.tenant_id))
            conn.commit()
            return {"status": "deleted"}
    finally:
        conn.close()

@app.get("/api/ingest/correlation-incidents")
@app.get("/ingest/correlation-incidents")
def list_correlation_incidents(
    limit: int = Query(50, le=200),
    auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))
):
    """Return the audit log of every fired correlation (The Detective's case file)."""
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, rule_id, rule_name, match_value, threat_score,
                       mitre_tactic, severity, action_taken, case_id, fired_at
                FROM correlation_incidents
                WHERE tenant_id = %s
                ORDER BY fired_at DESC
                LIMIT %s
            """, (auth.tenant_id, limit))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return [
                {c: (str(v) if hasattr(v, 'hex') else v) for c, v in zip(cols, row)}
                for row in rows
            ]
    finally:
        conn.close()


# ── Feature B: Workflow Management APIs ──────────────────────────────────────

class WorkflowCreate(BaseModel):
    name: str
    description: str = ""
    trigger_config: Dict[str, Any] = {}
    steps: List[Dict[str, Any]] = []
    enabled: bool = True

@app.post("/api/workflows", status_code=201)
@app.post("/workflows", status_code=201)
def create_workflow(wf: WorkflowCreate, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    conn = get_db_conn()
    workflow_id = str(uuid.uuid4())
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO workflow_definitions (workflow_id, tenant_id, name, description, trigger_config, steps, enabled)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
                RETURNING id
            """, (workflow_id, auth.tenant_id, wf.name, wf.description,
                  json.dumps(wf.trigger_config), json.dumps(wf.steps), wf.enabled))
            cur.fetchone()
        conn.commit()
        return {"workflow_id": workflow_id, "status": "created"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/api/workflows")
@app.get("/workflows")
def list_workflows(auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT workflow_id, name, description, trigger_config, steps, enabled, created_at
                FROM workflow_definitions WHERE tenant_id = %s ORDER BY created_at DESC
            """, (auth.tenant_id,))
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


@app.get("/api/workflows/{workflow_id}")
@app.get("/workflows/{workflow_id}")
def get_workflow(workflow_id: str, auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT workflow_id, name, description, trigger_config, steps, enabled, created_at
                FROM workflow_definitions WHERE tenant_id = %s AND workflow_id = %s
            """, (auth.tenant_id, workflow_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Workflow not found")
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


@app.delete("/api/workflows/{workflow_id}")
@app.delete("/workflows/{workflow_id}")
def delete_workflow(workflow_id: str, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM workflow_definitions WHERE tenant_id = %s AND workflow_id = %s",
                        (auth.tenant_id, workflow_id))
        conn.commit()
        return {"status": "deleted"}
    finally:
        conn.close()


@app.post("/api/workflows/{workflow_id}/trigger")
@app.post("/workflows/{workflow_id}/trigger")
async def trigger_workflow(workflow_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    """Manually trigger a workflow by publishing to the trigger topic."""
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT workflow_id FROM workflow_definitions WHERE tenant_id = %s AND workflow_id = %s AND enabled = TRUE",
                        (auth.tenant_id, workflow_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Workflow not found or disabled")
    finally:
        conn.close()
    
    # Publish trigger event to Kafka
    import requests as req_lib
    NV_INGEST_URL_LOCAL = os.getenv("NV_INGEST_URL", "http://nv-ingest:8000")
    try:
        # Direct execution approach for manual triggers
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092"),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        trigger_event = {
            "event_id": str(uuid.uuid4()),
            "type": "ManualTrigger",
            "tenant_id": auth.tenant_id,
            "timestamp": int(time.time() * 1000),
            "payload": {**body, "triggered_by": auth.user_id, "workflow_id": workflow_id}
        }
        producer.send("workflow.trigger.v1", trigger_event)
        producer.flush()
        producer.close()
        return {"status": "triggered", "workflow_id": workflow_id, "event_id": trigger_event["event_id"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to publish trigger: {e}")


@app.get("/api/workflows/executions")
@app.get("/workflows/executions")
def list_workflow_executions(
    workflow_id: str = Query(None),
    limit: int = Query(20, le=100),
    auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            query = "SELECT execution_id, workflow_id, status, total_steps, started_at, finished_at, error FROM workflow_executions WHERE tenant_id = %s"
            params = [auth.tenant_id]
            if workflow_id:
                query += " AND workflow_id = %s"
                params.append(workflow_id)
            query += " ORDER BY started_at DESC LIMIT %s"
            params.append(limit)
            cur.execute(query, tuple(params))
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()




VALID_TRANSITIONS = {
    "HIDDEN":       ["VISIBLE"],
    "VISIBLE":      ["ACKNOWLEDGED", "DISMISSED"],
    "ACKNOWLEDGED": ["RESOLVED", "DISMISSED"],
    "DISMISSED":    ["VISIBLE"],
    "RESOLVED":     [],
}

@app.get("/api/incidents")
@app.get("/incidents")
def list_incidents(
    state: str = Query(None, description="Filter by visibility_state: HIDDEN, VISIBLE, ACKNOWLEDGED, DISMISSED, RESOLVED"),
    limit: int = Query(50, le=200),
    auth: AuthContext = Depends(require_permission(PERM_ALERT_READ)),
):
    """List correlation groups (incidents) with optional state filtering."""
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            base_query = """
                SELECT group_id, correlation_key, rule_id, rule_name, confidence, status,
                       alert_count, max_severity, first_seen, last_seen,
                       COALESCE(visibility_state, 'HIDDEN') as visibility_state,
                       COALESCE(visibility_threshold, 3) as visibility_threshold,
                       acknowledged_by, acknowledged_at, dismissed_at, resolved_at
                FROM correlation_groups
                WHERE tenant_id = %s
            """
            params = [auth.tenant_id]
            if state:
                base_query += " AND COALESCE(visibility_state, 'HIDDEN') = %s"
                params.append(state.upper())
            base_query += " ORDER BY last_seen DESC LIMIT %s"
            params.append(limit)
            cur.execute(base_query, tuple(params))
            cols = [d[0] for d in cur.description]
            return [
                {c: (str(v) if hasattr(v, 'hex') else v) for c, v in zip(cols, row)}
                for row in cur.fetchall()
            ]
    finally:
        conn.close()


def _transition_incident(tenant_id: str, group_id: str, target_state: str, user_id: str, conn):
    """Perform a validated state transition on a correlation group."""
    import time as _t
    now = int(_t.time() * 1000)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(visibility_state, 'HIDDEN') FROM correlation_groups WHERE tenant_id = %s AND group_id = %s",
            (tenant_id, group_id)
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Incident not found")
        
        current_state = row[0]
        if target_state not in VALID_TRANSITIONS.get(current_state, []):
            raise HTTPException(
                status_code=409,
                detail=f"Cannot transition from {current_state} to {target_state}. Valid: {VALID_TRANSITIONS.get(current_state, [])}"
            )
        
        updates = {"visibility_state": target_state}
        if target_state == "ACKNOWLEDGED":
            updates["acknowledged_by"] = user_id
            updates["acknowledged_at"] = now
        elif target_state == "DISMISSED":
            updates["dismissed_at"] = now
        elif target_state == "RESOLVED":
            updates["resolved_at"] = now
        
        set_clauses = ", ".join(f"{k} = %s" for k in updates)
        values = list(updates.values()) + [tenant_id, group_id]
        cur.execute(
            f"UPDATE correlation_groups SET {set_clauses} WHERE tenant_id = %s AND group_id = %s",
            tuple(values)
        )
    conn.commit()
    return {"status": "transitioned", "group_id": group_id, "from": current_state, "to": target_state}


@app.patch("/api/incidents/{group_id}/acknowledge")
@app.patch("/incidents/{group_id}/acknowledge")
def acknowledge_incident(group_id: str, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    conn = get_db_conn()
    try:
        return _transition_incident(auth.tenant_id, group_id, "ACKNOWLEDGED", auth.user_id, conn)
    finally:
        conn.close()


@app.patch("/api/incidents/{group_id}/dismiss")
@app.patch("/incidents/{group_id}/dismiss")
def dismiss_incident(group_id: str, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    conn = get_db_conn()
    try:
        return _transition_incident(auth.tenant_id, group_id, "DISMISSED", auth.user_id, conn)
    finally:
        conn.close()


@app.patch("/api/incidents/{group_id}/resolve")
@app.patch("/incidents/{group_id}/resolve")
def resolve_incident(group_id: str, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    conn = get_db_conn()
    try:
        return _transition_incident(auth.tenant_id, group_id, "RESOLVED", auth.user_id, conn)
    finally:
        conn.close()


# ── Tier 5: The Hunter — Threat Hunting API ──────────────────────────────────

@app.get("/api/hunt/search")
@app.get("/hunt/search")
def hunt_search(
    q: str = Query("*", description="Lucene/KQL query string"),
    size: int = Query(25, ge=1, le=200),
    from_: int = Query(0, ge=0, alias="from"),
    sort_field: str = Query("timestamp", description="Field to sort by"),
    sort_order: str = Query("desc", description="asc or desc"),
    time_from: int = Query(None, description="Epoch ms start"),
    time_to: int   = Query(None, description="Epoch ms end"),
    source: str    = Query(None, description="Filter by source (wazuh, suricata, syslog)"),
    cel_filter: str = Query(None, description="CEL expression for server-side filtering, e.g. 'severity > 3'"),
    auth: AuthContext = Depends(require_permission(PERM_ALERT_READ)),
):
    """Full-text search across all indexed events with pagination, time range, and source filters."""
    import time as _t
    now_ms = int(_t.time() * 1000)
    filters = [{"term": {"tenant_id": auth.tenant_id}}]

    # Time range filter
    t_from = time_from or (now_ms - 24 * 3600 * 1000)  # default 24h
    t_to   = time_to   or now_ms
    filters.append({"range": {"timestamp": {"gte": t_from, "lte": t_to}}})

    # Source filter
    if source:
        filters.append({"term": {"payload.source": source}})

    # CEL-to-OpenSearch filter (Feature C)
    if cel_filter:
        cel_query = cel_to_opensearch(cel_filter)
        filters.append(cel_query)

    body = {
        "query": {
            "bool": {
                "filter": filters,
                "must": [{"query_string": {"query": q, "default_field": "*", "lenient": True}}] if q and q != "*" else []
            }
        },
        "from": from_,
        "size": size,
        "sort": [{sort_field: {"order": sort_order, "unmapped_type": "long"}}],
        "highlight": {
            "fields": {"*": {}},
            "fragment_size": 150,
            "number_of_fragments": 2,
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"],
        }
    }

    try:
        t0 = _t.time()
        resp = os_client.search(body=body, index=INDEX_ALERTS)
        took = int((_t.time() - t0) * 1000)

        hits = []
        for h in resp["hits"]["hits"]:
            src = h["_source"]
            src["_id"] = h["_id"]
            src["_highlights"] = h.get("highlight", {})
            hits.append(src)

        return {
            "total": resp["hits"]["total"]["value"],
            "took_ms": took,
            "hits": hits,
        }
    except Exception as e:
        err = str(e)
        if "index_not_found_exception" in err:
            return {"total": 0, "took_ms": 0, "hits": []}
        raise HTTPException(status_code=500, detail=err)


@app.get("/api/hunt/histogram")
@app.get("/hunt/histogram")
def hunt_histogram(
    q: str = Query("*"),
    time_from: int = Query(None),
    time_to: int   = Query(None),
    interval: str  = Query("auto", description="auto, minute, hour, day"),
    cel_filter: str = Query(None, description="CEL expression for server-side filtering"),
    auth: AuthContext = Depends(require_permission(PERM_ALERT_READ)),
):
    """Time-bucketed event counts for the Threat Hunter histogram chart."""
    import time as _t
    now_ms = int(_t.time() * 1000)
    t_from = time_from or (now_ms - 24 * 3600 * 1000)
    t_to   = time_to   or now_ms

    # Auto-detect best interval
    span_h = (t_to - t_from) / 3600000
    if interval == "auto":
        if span_h <= 1:
            cal_interval = "minute"
        elif span_h <= 48:
            cal_interval = "hour"
        else:
            cal_interval = "day"
    else:
        cal_interval = interval

    filters = [
        {"term": {"tenant_id": auth.tenant_id}},
        {"range": {"timestamp": {"gte": t_from, "lte": t_to}}},
    ]

    # CEL-to-OpenSearch filter (Feature C)
    if cel_filter:
        filters.append(cel_to_opensearch(cel_filter))

    body = {
        "query": {
            "bool": {
                "filter": filters,
                "must": [{"query_string": {"query": q, "default_field": "*", "lenient": True}}] if q and q != "*" else []
            }
        },
        "size": 0,
        "aggs": {
            "timeline": {
                "date_histogram": {
                    "field": "timestamp",
                    "calendar_interval": cal_interval,
                    "min_doc_count": 0,
                    "extended_bounds": {"min": t_from, "max": t_to},
                }
            }
        }
    }

    try:
        resp = os_client.search(body=body, index=INDEX_ALERTS)
        buckets = [
            {"time": b["key"], "time_str": b.get("key_as_string", ""), "count": b["doc_count"]}
            for b in resp["aggregations"]["timeline"]["buckets"]
        ]
        return {"interval": cal_interval, "buckets": buckets}
    except Exception as e:
        err = str(e)
        if "index_not_found_exception" in err:
            return {"interval": cal_interval, "buckets": []}
        raise HTTPException(status_code=500, detail=err)


@app.get("/api/hunt/fields")
@app.get("/hunt/fields")
def hunt_fields(
    field: str = Query(..., description="Dot-notation field name, e.g. payload.source"),
    q: str = Query("*"),
    time_from: int = Query(None),
    time_to: int   = Query(None),
    top_n: int     = Query(10, ge=1, le=50),
    auth: AuthContext = Depends(require_permission(PERM_ALERT_READ)),
):
    """Return top-N values for a given field (sidebar breakdown)."""
    import time as _t
    now_ms = int(_t.time() * 1000)
    t_from = time_from or (now_ms - 24 * 3600 * 1000)
    t_to   = time_to   or now_ms

    filters = [
        {"term": {"tenant_id": auth.tenant_id}},
        {"range": {"timestamp": {"gte": t_from, "lte": t_to}}},
    ]

    # Use .keyword sub-field for text fields
    agg_field = field + ".keyword" if not field.endswith(".keyword") else field

    body = {
        "query": {
            "bool": {
                "filter": filters,
                "must": [{"query_string": {"query": q, "default_field": "*", "lenient": True}}] if q and q != "*" else []
            }
        },
        "size": 0,
        "aggs": {
            "top_values": {
                "terms": {"field": agg_field, "size": top_n}
            }
        }
    }

    try:
        resp = os_client.search(body=body, index=INDEX_ALERTS)
        values = [
            {"key": b["key"], "count": b["doc_count"]}
            for b in resp["aggregations"]["top_values"]["buckets"]
        ]
        return {"field": field, "values": values}
    except Exception as e:
        err = str(e)
        if "index_not_found_exception" in err:
            return {"field": field, "values": []}
        raise HTTPException(status_code=500, detail=err)


# ── Feature E: Faceted Search (Dynamic Filters) ──────────────────────────────

# Well-known facet fields for auto-discovery
DEFAULT_FACET_FIELDS = [
    "payload.source", "payload.severity", "payload.rule.level",
    "payload.agent.name", "payload.rule.description",
    "decision", "schema_version",
]

@app.get("/api/hunt/facets")
@app.get("/hunt/facets")
def hunt_facets(
    time_from: int = Query(None),
    time_to: int   = Query(None),
    q: str = Query("*"),
    cel_filter: str = Query(None, description="CEL expression"),
    auth: AuthContext = Depends(require_permission(PERM_ALERT_READ)),
):
    """Auto-discover facets: return top values for key fields."""
    import time as _t
    now_ms = int(_t.time() * 1000)
    t_from = time_from or (now_ms - 24 * 3600 * 1000)
    t_to = time_to or now_ms

    filters = [
        {"term": {"tenant_id": auth.tenant_id}},
        {"range": {"timestamp": {"gte": t_from, "lte": t_to}}},
    ]
    if cel_filter:
        filters.append(cel_to_opensearch(cel_filter))

    # Build multi-aggregation query
    aggs = {}
    for field in DEFAULT_FACET_FIELDS:
        agg_name = field.replace(".", "_")
        agg_field = field + ".keyword" if not field.endswith(".keyword") else field
        aggs[agg_name] = {"terms": {"field": agg_field, "size": 10}}

    # Also discover dynamic fields from index mapping
    try:
        mapping = os_client.indices.get_mapping(index=INDEX_ALERTS)
        for idx_name, idx_data in mapping.items():
            props = idx_data.get("mappings", {}).get("properties", {})
            payload_props = props.get("payload", {}).get("properties", {})
            for field_name, field_def in payload_props.items():
                full_path = f"payload.{field_name}"
                if full_path not in DEFAULT_FACET_FIELDS:
                    agg_name = full_path.replace(".", "_")
                    if field_def.get("type") == "keyword" or "keyword" in str(field_def.get("fields", {})):
                        aggs[agg_name] = {"terms": {"field": full_path + ".keyword", "size": 5}}
            break  # Only need first index
    except Exception:
        pass  # Mapping discovery is best-effort

    body = {
        "query": {
            "bool": {
                "filter": filters,
                "must": [{"query_string": {"query": q, "default_field": "*", "lenient": True}}] if q and q != "*" else []
            }
        },
        "size": 0,
        "aggs": aggs
    }

    try:
        resp = os_client.search(body=body, index=INDEX_ALERTS)
        facets = []
        for agg_name, agg_data in resp.get("aggregations", {}).items():
            field_name = agg_name.replace("_", ".", agg_name.count("_") - 0)
            # Reconstruct field name from agg_name
            buckets = agg_data.get("buckets", [])
            if buckets:  # Only return facets that have data
                facets.append({
                    "field": agg_name.replace("_", "."),
                    "values": [{"key": b["key"], "count": b["doc_count"]} for b in buckets]
                })
        return {"facets": facets, "total_facets": len(facets)}
    except Exception as e:
        err = str(e)
        if "index_not_found_exception" in err:
            return {"facets": [], "total_facets": 0}
        raise HTTPException(status_code=500, detail=err)


@app.get("/api/hunt/facet/{field:path}")
@app.get("/hunt/facet/{field:path}")
def hunt_facet_detail(
    field: str,
    time_from: int = Query(None),
    time_to: int   = Query(None),
    q: str = Query("*"),
    top_n: int = Query(25, ge=1, le=100),
    cel_filter: str = Query(None),
    auth: AuthContext = Depends(require_permission(PERM_ALERT_READ)),
):
    """Return detailed top-N breakdown for a specific field."""
    import time as _t
    now_ms = int(_t.time() * 1000)
    t_from = time_from or (now_ms - 24 * 3600 * 1000)
    t_to = time_to or now_ms

    filters = [
        {"term": {"tenant_id": auth.tenant_id}},
        {"range": {"timestamp": {"gte": t_from, "lte": t_to}}},
    ]
    if cel_filter:
        filters.append(cel_to_opensearch(cel_filter))

    agg_field = field + ".keyword" if not field.endswith(".keyword") else field

    body = {
        "query": {
            "bool": {
                "filter": filters,
                "must": [{"query_string": {"query": q, "default_field": "*", "lenient": True}}] if q and q != "*" else []
            }
        },
        "size": 0,
        "aggs": {
            "breakdown": {"terms": {"field": agg_field, "size": top_n}},
            "unique_count": {"cardinality": {"field": agg_field}}
        }
    }

    try:
        resp = os_client.search(body=body, index=INDEX_ALERTS)
        values = [{"key": b["key"], "count": b["doc_count"]} for b in resp["aggregations"]["breakdown"]["buckets"]]
        unique = resp["aggregations"]["unique_count"]["value"]
        return {"field": field, "unique_values": unique, "top_values": values}
    except Exception as e:
        err = str(e)
        if "index_not_found_exception" in err:
            return {"field": field, "unique_values": 0, "top_values": []}
        raise HTTPException(status_code=500, detail=err)


@app.get("/status")
def get_status():
    """Return real-time health status of all platform services."""

    def probe_tcp(host, port, timeout=2):
        """Return 'UP' if TCP connection succeeds within timeout, else 'DOWN'."""
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return "UP"
        except Exception:
            return "DOWN"

    def probe_http(url, timeout=2):
        """Return 'UP' if GET request returns 200, else 'DOWN'."""
        try:
            r = requests.get(url, timeout=timeout)
            return "UP" if r.status_code == 200 else "DOWN"
        except Exception:
            return "DOWN"

    def probe_opensearch():
        try:
            ok = os_client.ping()
            return "UP" if ok else "DOWN"
        except Exception:
            return "DOWN"

    def probe_postgres():
        try:
            conn = get_db_conn()
            conn.close()
            return "UP"
        except Exception:
            return "DOWN"

    def probe_minio():
        minio_host = os.getenv("MINIO_HOST", "minio")
        minio_port = int(os.getenv("MINIO_PORT", 9000))
        return probe_tcp(minio_host, minio_port)

    def probe_redis():
        redis_host = os.getenv("REDIS_HOST", "redis")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        return probe_tcp(redis_host, redis_port)

    def probe_redpanda():
        redpanda_host = os.getenv("REDPANDA_HOST", "redpanda")
        # In slim compose, redpanda:29092 is standard internal
        return probe_tcp(redpanda_host, 29092)

    services = {
        "nv-query":   "UP",
        "opensearch": probe_opensearch(),
        "postgres":   probe_postgres(),
        "minio":      probe_minio(),
        "redis":      probe_redis(),
        "redpanda":   probe_redpanda(),
        # Internal Microservices (Real-time HTTP health checks where available)
        "nv-ingest":      probe_http("http://nv-ingest:8000/healthz"),
        "nv-dedup":       probe_tcp("nv-dedup", 9001), # metrics only
        "nv-correlation": probe_http("http://nv-correlation:9004/healthz"),
        "nv-case-engine": probe_http("http://nv-case-engine:8000/healthz"),
        "nv-workflow":    probe_http("http://nv-workflow:8000/healthz"),
        "socket-service": probe_http("http://nv-socket-service:8000/healthz"),
    }

    # Fetch connector status from integration_nodes table (Phase Z2.1 Integration)
    connectors_data = {
        "cortex": { "enabled": False, "status": "NOT_CONFIGURED", "servers": [] },
        "misp":   { "enabled": False, "status": "NOT_CONFIGURED", "servers": [] }
    }
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT node_type, status, name FROM integration_nodes WHERE node_type IN ('cortex', 'misp')")
            for row in cur.fetchall():
                ntype = str(row[0])
                status = str(row[1])
                name = str(row[2])
                
                if ntype in connectors_data:
                    target = connectors_data[ntype]
                    target["enabled"] = True
                    # If multiple servers exist, report DEGRADED if any are DOWN
                    if status != "UP":
                        target["status"] = status
                    elif target["status"] in ("UNKNOWN", "NOT_CONFIGURED"):
                        target["status"] = "UP"
                    
                    if not isinstance(target.get("servers"), list):
                        target["servers"] = []
                    target["servers"].append(name)
        conn.close()
    except Exception:
        pass # Best effort

    return {
        "version": "4.1.24-NV-ZENITH",
        "versions": {
            "NeuralVyuha": "1.0.0-ZENITH"
        },
        "services": services,
        "connectors": connectors_data,
        "config": {
            "ssoAutoLogin": False
        }
    }

# ── Legacy NeuralVyuha v4 Compatibility Stub ────────────────────────────────────
# The original NeuralVyuha Angular frontend sends streaming queries to /v0/query.
# These are now handled by the unified legacy_v0_v1_query handler below.
from fastapi import Request as FARequest

_EMPTY_RESPONSES = {
    "caseTemplates":       [],
    "getOrganisation":     {"name": "admin", "description": "NeuralVyuha Org"},
    "countCases":          {"count": 0},
    "countAlerts":         {"count": 0},
    "customFields":        [],
    "taxonomies":          [],
    "tags":                [],
    "users":               [],
    "dashboards":          [],
    "observableTypes":     [],
    "listOrganisation":    [],
    "getUser":             {"login": "admin", "name": "Administrator"},
}

# NOTE: The old /v0/query stub has been merged into legacy_v0_v1_query (see below)



@app.get("/alerts")
def search_alerts(
    q: str = Query(None, description="Simple query string"),
    size: int = 20,
    from_: int = 0,
    auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))
):
    query_body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"tenant_id": auth.tenant_id}}
                ]
            }
        },
        "from": from_,
        "size": size,
        "sort": [{"timestamp": "desc"}]
    }

    if q:
        query_body["query"]["bool"]["must"] = [
            {"query_string": {"query": q}}
        ]

    try:
        response = os_client.search(body=query_body, index=INDEX_ALERTS)
        return {
            "total": response["hits"]["total"]["value"],
            "hits": [hit["_source"] for hit in response["hits"]["hits"]]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/alerts/stats")
def get_alert_stats(
    auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))
):
    """Get alert counts grouped by status and severity for the current tenant."""
    query_body = {
        "query": {"bool": {"filter": [
            {"term": {"tenant_id": auth.tenant_id}}
        ]}},
        "size": 0,
        "aggs": {
            "by_status": {
                "terms": {"field": "status", "size": 20, "missing": "New"}
            },
            "by_severity": {
                "terms": {"field": "payload.severity", "size": 10}
            },
            "total": {
                "value_count": {"field": "tenant_id"}
            }
        }
    }

    try:
        resp = os_client.search(body=query_body, index=INDEX_ALERTS)
        status_counts = {b["key"]: b["doc_count"] for b in resp["aggregations"]["by_status"]["buckets"]}
        severity_counts = {b["key"]: b["doc_count"] for b in resp["aggregations"]["by_severity"]["buckets"]}
        total = resp["aggregations"]["total"]["value"]

        return {
            "total": total,
            "by_status": status_counts,
            "by_severity": severity_counts,
            "new": status_counts.get("New", 0),
            "updated": status_counts.get("Updated", 0),
            "imported": status_counts.get("Imported", 0),
            "ignored": status_counts.get("Ignored", 0),
        }
    except Exception as e:
        err_str = str(e)
        if "index_not_found_exception" in err_str or isinstance(e, KeyError):
            return {"total": 0, "by_status": {}, "by_severity": {}, "new": 0, "updated": 0, "imported": 0, "ignored": 0}
        raise HTTPException(status_code=500, detail=err_str)


@app.get("/alerts/timeline")
def get_alert_timeline(
    days: int = Query(14, ge=1, le=90),
    auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))
):
    """Return daily alert counts for the last N days using date_histogram aggregation."""
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - (days * 86400 * 1000)

    query_body = {
        "query": {"bool": {"filter": [
            {"term": {"tenant_id": auth.tenant_id}},
            {"range": {"timestamp": {"gte": from_ms, "lte": now_ms}}}
        ]}},
        "size": 0,
        "aggs": {
            "daily": {
                "date_histogram": {
                    "field": "timestamp",
                    "calendar_interval": "day",
                    "format": "yyyy-MM-dd",
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": from_ms,
                        "max": now_ms
                    }
                },
                "aggs": {
                    "by_severity": {
                        "terms": {"field": "payload.severity", "size": 10}
                    }
                }
            }
        }
    }

    try:
        resp = os_client.search(body=query_body, index=INDEX_ALERTS)
        buckets = resp["aggregations"]["daily"]["buckets"]

        timeline = []
        for b in buckets:
            sev = {str(s["key"]): s["doc_count"] for s in b.get("by_severity", {}).get("buckets", [])}
            timeline.append({
                "date": b["key_as_string"],
                "count": b["doc_count"],
                "critical": sev.get("4", 0),
                "high": sev.get("3", 0),
                "medium": sev.get("2", 0),
                "low": sev.get("1", 0),
            })

        return {"days": days, "timeline": timeline}
    except Exception as e:
        err_str = str(e)
        if "index_not_found_exception" in err_str or isinstance(e, KeyError):
            return {"days": days, "timeline": []}
        raise HTTPException(status_code=500, detail=err_str)

@app.post("/alerts/correlate")
@app.post("/api/alerts/correlate")
async def correlate_alerts(auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    """
    AI Correlation Engine: Fetches unassigned alerts and suggests cases based on LLM clustering.
    Phase 2 Implementation.
    """
    try:
        query = {
            "size": 50,
            "query": {
                "bool": {
                    "must_not": [
                        {"exists": {"field": "case_id"}},
                        {"term": {"status.keyword": "Closed"}},
                        {"term": {"status": "Closed"}}
                    ]
                }
            },
            "sort": [{"_createdAt": "desc"}]
        }
        res = os_client.search(index=INDEX_ALERTS, body=query)
        alerts = res['hits']['hits']
        
        if not alerts:
            return {"clusters": []}
            
        alert_ids = sorted([a['_id'] for a in alerts])
        input_hash = hashlib.sha256(",".join(alert_ids).encode("utf-8")).hexdigest()
        
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT clusters_json FROM ai_correlation_cache WHERE input_hash = %s", (input_hash,))
                row = cur.fetchone()
                if row:
                    clusters = row[0]
                    # Decorate cached clusters
                    for cluster in clusters:
                        cluster["alerts"] = [a['_source'] | {'id': a['_id']} for a in alerts if a['_id'] in cluster.get("alert_ids", [])]
                    return {"clusters": clusters, "total_analyzed": len(alerts), "cached": True}
        except Exception as e:
            logger.error(f"Cache read error: {e}")
        finally:
            conn.close()
            
        alert_summary = []
        for a in alerts:
            src = a['_source']
            alert_summary.append({
                "id": a['_id'],
                "title": src.get('title'),
                "description": src.get('description'),
                "source": src.get('source'),
                "severity": src.get('severity')
            })
            
        import httpx
        async with httpx.AsyncClient(timeout=30.0) as client:
            node_url = os.environ.get("NODE_SERVICE_URL", "http://nv-node-service:8085")
            cred_res = await client.get(f"{node_url}/internal/vault/credentials/gemini")
            if cred_res.status_code != 200:
                return {"error": "AI Engine (Gemini) missing credentials in Integration Hub."}
            
            creds = cred_res.json()
            api_key = creds.get("api_key")
            
            if not api_key:
                return {"error": "AI Engine API key is empty."}
                
            prompt = f"""You are NeuralVyuha AI Correlation Engine.
Your task is to group the following unassigned alerts into logical 'Candidate Cases' based on similarity.

IDENTITY MAPPING & TOPOLOGY INFERENCE:
- Cross-reference Source IPs, Destination IPs, hostnames, and 'service' fields across different alerts.
- If multiple alerts involve the same underlying services or IPs, treat them as a connected topology and group them together as a single incident.
- Consider lateral movement attack patterns where the destination of one alert is the source of another.

Output raw JSON only with format: [{{"title": "Suggested Case Title", "description": "Why these are linked (mention any shared topology or IPs)", "severity": 2, "alert_ids": ["id1", "id2"]}}]. 
Alerts to analyze: {json.dumps(alert_summary)}"""
            
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"response_mime_type": "application/json"}
            }
            
            gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"
            llm_res = await client.post(gemini_url, json=payload)
            
            if llm_res.status_code != 200:
                logger.error(f"LLM correlation failed: {llm_res.text}")
                return {"error": "LLM correlation request failed.", "details": llm_res.text}
                
            llm_data = llm_res.json()
            txt = llm_data["candidates"][0]["content"]["parts"][0]["text"]
            clusters = json.loads(txt)
            
            # Save raw clusters to cache
            conn = get_db_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO ai_correlation_cache (input_hash, clusters_json) VALUES (%s, %s) ON CONFLICT (input_hash) DO NOTHING",
                        (input_hash, json.dumps(clusters))
                    )
                conn.commit()
            except Exception as e:
                logger.error(f"Cache write error: {e}")
            finally:
                conn.close()
            
            # Decorate the clusters with actual alert data for the UI
            for cluster in clusters:
                cluster["alerts"] = [a for a in alert_summary if a["id"] in cluster.get("alert_ids", [])]
                
            return {"clusters": clusters, "total_analyzed": len(alerts), "cached": False}
    except Exception as e:
        logger.error(f"Error in correlate_alerts: {e}")
        return {"error": str(e)}

@app.post("/alerts/clusters/commit")
@app.post("/api/alerts/clusters/commit")
async def commit_correlation_cluster(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    """
    Takes an AI requested cluster and converts it to a case.
    Re-uses the proxy_promote_alerts core backend route.
    """
    body = await request.json()
    return _forward_request("POST", "/alerts/promote", request, auth, json_body=body)


@app.get("/alerts/{alert_id}")
def get_alert(alert_id: str, auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))):
    query_body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"tenant_id": auth.tenant_id}},
                    {"ids": {"values": [alert_id]}}
                ]
            }
        }
    }

    response = os_client.search(body=query_body, index=INDEX_ALERTS)
    if not response["hits"]["hits"]:
        raise HTTPException(status_code=404, detail="Alert not found")

    return response["hits"]["hits"][0]["_source"]

# ── Alert Mutations ─────────────────────────────────────────────────────────
import re as _re
import asyncio

_IOC_PATTERNS = {
    "ips": _re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b'),
    "hashes": _re.compile(r'\b[a-fA-F0-9]{32,64}\b'),
    "domains": _re.compile(r'\b(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}\b'),
    "urls": _re.compile(r'https?://[^\s\"\'>]+'),
}

@app.patch("/alerts/{alert_id}")
def update_alert(
    alert_id: str,
    updates: Dict[str, Any] = Body(...),
    auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))
):
    """Update alert fields (status, read, follow) in OpenSearch."""
    allowed_fields = {"status", "read", "follow", "tags"}
    filtered = {k: v for k, v in updates.items() if k in allowed_fields}
    if not filtered:
        return {"status": "no_change"}

    # Verify alert belongs to tenant
    query_body = {
        "query": {"bool": {"filter": [
            {"term": {"tenant_id": auth.tenant_id}},
            {"ids": {"values": [alert_id]}}
        ]}}
    }
    resp = os_client.search(body=query_body, index=INDEX_ALERTS)
    if not resp["hits"]["hits"]:
        raise HTTPException(status_code=404, detail="Alert not found")

    doc_index = resp["hits"]["hits"][0]["_index"]
    doc_id = resp["hits"]["hits"][0]["_id"]

    filtered["updated_at"] = int(time.time() * 1000)
    filtered["updated_by"] = auth.user_id

    try:
        os_client.update(index=doc_index, id=doc_id, body={"doc": filtered})
        return {"status": "updated", "alert_id": alert_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/alerts/bulk/status")
def bulk_update_alert_status(
    payload: Dict[str, Any] = Body(...),
    auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))
):
    """Bulk update status/read for multiple alerts."""
    ids = payload.get("ids", [])
    if not ids:
        raise HTTPException(status_code=400, detail="No alert IDs provided")

    update_fields = {}
    if "status" in payload:
        update_fields["status"] = payload["status"]
    if "read" in payload:
        update_fields["read"] = payload["read"]
    if "follow" in payload:
        update_fields["follow"] = payload["follow"]

    if not update_fields:
        return {"status": "no_change", "updated": 0}

    update_fields["updated_at"] = int(time.time() * 1000)
    update_fields["updated_by"] = auth.user_id

    # Use OpenSearch _update_by_query for efficiency
    query_body = {
        "query": {"bool": {"filter": [
            {"term": {"tenant_id": auth.tenant_id}},
            {"ids": {"values": ids}}
        ]}},
        "script": {
            "source": "; ".join([f"ctx._source.{k} = params.{k}" for k in update_fields]),
            "params": update_fields,
            "lang": "painless"
        }
    }

    try:
        resp = os_client.update_by_query(index=INDEX_ALERTS, body=query_body)
        return {"status": "updated", "updated": resp.get("updated", 0)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/alerts/{alert_id}")
def delete_alert(
    alert_id: str,
    auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))
):
    """Soft-delete an alert by setting status=Deleted."""
    query_body = {
        "query": {"bool": {"filter": [
            {"term": {"tenant_id": auth.tenant_id}},
            {"ids": {"values": [alert_id]}}
        ]}}
    }
    resp = os_client.search(body=query_body, index=INDEX_ALERTS)
    if not resp["hits"]["hits"]:
        raise HTTPException(status_code=404, detail="Alert not found")

    doc_index = resp["hits"]["hits"][0]["_index"]
    doc_id = resp["hits"]["hits"][0]["_id"]

    try:
        os_client.update(index=doc_index, id=doc_id, body={
            "doc": {"status": "Deleted", "deleted_at": int(time.time() * 1000), "deleted_by": auth.user_id}
        })
        return {"status": "deleted", "alert_id": alert_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/alerts/{alert_id}/similar")
def get_similar_alerts(
    alert_id: str,
    auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))
):
    """Find alerts with matching observables (fingerprint-based similarity)."""
    # Fetch the source alert
    query_body = {
        "query": {"bool": {"filter": [
            {"term": {"tenant_id": auth.tenant_id}},
            {"ids": {"values": [alert_id]}}
        ]}}
    }
    resp = os_client.search(body=query_body, index=INDEX_ALERTS)
    if not resp["hits"]["hits"]:
        raise HTTPException(status_code=404, detail="Alert not found")

    source_alert = resp["hits"]["hits"][0]["_source"]
    payload = source_alert.get("payload", source_alert)

    # Build similarity query from observable fields
    should_clauses = []
    for field in ["payload.source", "payload.type", "payload.sourceRef"]:
        val = payload.get(field.split(".")[-1])
        if val:
            should_clauses.append({"match": {field: val}})

    # Also match on extracted IOC fields
    for field in ["src_ip", "dst_ip", "file_hash", "domain"]:
        val = payload.get(field) or source_alert.get(field)
        if val:
            should_clauses.append({"term": {field: val}})

    if not should_clauses:
        return {"total": 0, "similar": []}

    sim_query = {
        "query": {"bool": {
            "filter": [{"term": {"tenant_id": auth.tenant_id}}],
            "must_not": [{"ids": {"values": [alert_id]}}],
            "should": should_clauses,
            "minimum_should_match": 1
        }},
        "size": 20,
        "sort": [{"timestamp": "desc"}]
    }

    try:
        sim_resp = os_client.search(body=sim_query, index=INDEX_ALERTS)
        return {
            "total": sim_resp["hits"]["total"]["value"],
            "similar": [hit["_source"] for hit in sim_resp["hits"]["hits"]]
        }
    except Exception as e:
        return {"total": 0, "similar": []}


@app.get("/alerts/{alert_id}/iocs")
def get_alert_iocs(
    alert_id: str,
    auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))
):
    """Extract IOCs (IPs, hashes, domains, URLs) from alert payload."""
    query_body = {
        "query": {"bool": {"filter": [
            {"term": {"tenant_id": auth.tenant_id}},
            {"ids": {"values": [alert_id]}}
        ]}}
    }
    resp = os_client.search(body=query_body, index=INDEX_ALERTS)
    if not resp["hits"]["hits"]:
        raise HTTPException(status_code=404, detail="Alert not found")

    source = resp["hits"]["hits"][0]["_source"]
    # Flatten the entire alert into a text blob for regex scanning
    text_blob = json.dumps(source)

    iocs = {}
    for ioc_type, pattern in _IOC_PATTERNS.items():
        matches = set(pattern.findall(text_blob))
        # Filter out common false positives
        if ioc_type == "domains":
            matches = {m for m in matches if "." in m and not m.endswith(".js") and not m.endswith(".css") and not m.endswith(".html")}
        if ioc_type == "ips":
            matches = {m for m in matches if not m.startswith("0.") and not m.startswith("127.")}
        iocs[ioc_type] = sorted(matches)

    # Also include explicit observable fields from the alert
    for field, ioc_type in [("src_ip", "ips"), ("dst_ip", "ips"), ("file_hash", "hashes"), ("domain", "domains")]:
        val = source.get(field) or source.get("payload", {}).get(field)
        if val and val not in iocs.get(ioc_type, []):
            iocs.setdefault(ioc_type, []).append(val)

    iocs["total"] = sum(len(v) for v in iocs.values() if isinstance(v, list))
    return iocs


@app.post("/alerts/promote")
async def proxy_promote_alerts(request: Request, auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))):
    """Proxy promote-to-case request to nv-case-engine."""
    return _forward_request("POST", "/alerts/promote", request, auth)



@app.get("/groups")
def search_groups(
    q: str = Query(None, description="Query string for groups"),
    status: Optional[str] = Query(None, regex="^(OPEN|CLOSED|MERGED)$"),
    size: int = 20,
    from_: int = 0,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    query_body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"tenant_id": auth.tenant_id}}
                ]
            }
        },
        "from": from_,
        "size": size,
        "sort": [{"last_seen": "desc"}]
    }

    if status:
        query_body["query"]["bool"]["filter"].append({"term": {"status": status}})

    if q:
        query_body["query"]["bool"]["must"] = [
            {"query_string": {"query": q}}
        ]

    try:
        response = os_client.search(body=query_body, index=INDEX_GROUPS)
        return {
            "total": response["hits"]["total"]["value"],
            "hits": [hit["_source"] for hit in response["hits"]["hits"]]
        }
    except Exception as e:
        if "index_not_found_exception" in str(e):
             return {"total": 0, "hits": []}
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/groups/{group_id}")
def get_group(group_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    doc_id = f"{auth.tenant_id}:{group_id}"
    try:
        response = os_client.get(index=INDEX_GROUPS, id=doc_id)
        if response["_source"]["tenant_id"] != auth.tenant_id:
             raise HTTPException(status_code=404, detail="Group not found")

        group_data = response["_source"]
        rule_id = group_data.get("rule_id")
        if rule_id:
            conn = get_db_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT rule_name, confidence, window_minutes FROM correlation_rules WHERE rule_id = %s", (rule_id,))
                    row = cur.fetchone()
                    if row:
                        group_data["_rule_metadata"] = {
                            "name": row[0],
                            "confidence": row[1],
                            "window": row[2]
                        }
            except:
                pass
            finally:
                conn.close()

        return group_data
    except Exception as e:
        if "index_not_found_exception" in str(e) or "NotFoundError" in str(e):
            raise HTTPException(status_code=404, detail="Group not found")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/groups/{group_id}/alerts")
def get_group_alerts(group_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    conn = get_db_conn()
    links = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT original_event_id, linked_at, link_reason FROM correlation_group_alert_links WHERE tenant_id = %s AND group_id = %s ORDER BY linked_at ASC",
                (auth.tenant_id, group_id)
            )
            rows = cur.fetchall()
            for r in rows:
                links.append({"id": r[0], "linked_at": r[1], "reason": r[2]})
    finally:
        conn.close()

    if not links:
        return {"total": 0, "hits": []}

    alert_ids = [l["id"] for l in links]
    os_docs = {}
    try:
        query_body = {
            "query": {
                "ids": {
                    "values": alert_ids
                }
            },
            "size": len(alert_ids)
        }
        resp = os_client.search(body=query_body, index=INDEX_ALERTS)
        for hit in resp["hits"]["hits"]:
            source = hit["_source"]
            if source.get("tenant_id") == auth.tenant_id:
                os_docs[hit["_id"]] = source

    except Exception as e:
        print(f"Failed to fetch alert details: {e}")

    results = []
    for link in links:
        aid = link["id"]
        detail = os_docs.get(aid, {})
        if detail:
            detail["_link_info"] = {
                "linked_at": link["linked_at"],
                "reason": link["reason"]
            }
            results.append(detail)

    return {"total": len(results), "hits": results}

@app.get("/rules")
def list_rules(auth: AuthContext = Depends(require_permission(PERM_RULE_SIMULATE))):
    # Assuming rules are global read, but authenticated
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT rule_id, rule_name, enabled, confidence, window_minutes, correlation_key_template, required_fields FROM correlation_rules ORDER BY rule_id")
            rows = cur.fetchall()
            rules = []
            for r in rows:
                rules.append({
                    "rule_id": r[0],
                    "rule_name": r[1],
                    "enabled": r[2],
                    "confidence": r[3],
                    "window_minutes": r[4],
                    "template": r[5],
                    "required_fields": r[6]
                })
            return {"total": len(rules), "rules": rules}
    finally:
        conn.close()

@app.post("/rules/simulate")
def simulate_rules(
    alert_payload: Dict[str, Any] = Body(...),
    auth: AuthContext = Depends(require_permission(PERM_RULE_SIMULATE)) # Require Auth, implicit Tenant
):
    conn = get_db_conn()
    rules = []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT rule_id, rule_name, enabled, confidence, window_minutes, correlation_key_template, required_fields FROM correlation_rules WHERE enabled = TRUE")
            rows = cur.fetchall()
            for r in rows:
                rules.append({
                    "rule_id": r[0],
                    "rule_name": r[1],
                    "confidence": r[3],
                    "window_minutes": r[4],
                    "template": r[5],
                    "required_fields": r[6]
                })
    finally:
        conn.close()

    timestamp = int(time.time() * 1000)
    matches = []

    context = {"tenant_id": [auth.tenant_id]} # Force tenant from auth
    for k, v in alert_payload.items():
        if isinstance(v, list):
            context[k] = v
        else:
            context[k] = [v]

    for rule in rules:
        iterables = {}
        missing = False
        for field in rule['required_fields']:
            if field not in context or not context[field]:
                missing = True
                break
            iterables[field] = context[field]

        if missing:
            continue

        keys = list(iterables.keys())
        values_product = itertools.product(*(iterables[k] for k in keys))

        for combination in values_product:
            local_ctx = dict(zip(keys, combination))
            try:
                key = rule['template'].format(**local_ctx)

                window_ms = rule['window_minutes'] * 60 * 1000
                window_idx = int(timestamp / window_ms) if window_ms > 0 else 0
                raw_id = f"{auth.tenant_id}:{rule['rule_id']}:{key}:{window_idx}"
                group_id = hashlib.sha256(raw_id.encode('utf-8')).hexdigest()

                matches.append({
                    "rule_id": rule['rule_id'],
                    "rule_name": rule['rule_name'],
                    "correlation_key": key,
                    "group_id": group_id,
                    "window_idx": window_idx
                })
            except:
                continue

    return {"matches": matches}

# E3: Case Domain Read Endpoints (Proxied to Query API for unified read surface)
# Implementation: Direct Postgres Read
@app.get("/cases")
def list_cases(
    status: Optional[str] = None,
    severity: Optional[int] = None,
    limit: int = 20,
    offset: int = 0,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            query = "SELECT case_id, title, status, severity, created_at, updated_at, assigned_to, flag, closed_at, resolution_status, impact_status, summary, tlp, pap, tags, case_number FROM cases WHERE tenant_id = %s"
            params = [auth.tenant_id]
            
            # Visibility Filtering
            if "SYSTEM_ADMIN" not in auth.roles:
                query += " AND (visibility = 'ORGANIZATION' OR created_by = %s OR assigned_to = %s OR %s = ANY(permitted_users))"
                params.extend([auth.user_id, auth.user_id, auth.user_id])
            
            if status:
                status = status.upper()
                query += " AND status = %s"
                params.append(status)
            if severity:
                query += " AND severity = %s"
                params.append(severity)
            
            # Count Total (with visibility rules applied)
            count_query = "SELECT COUNT(*) FROM cases WHERE tenant_id = %s"
            if "SYSTEM_ADMIN" not in auth.roles:
                count_query += " AND (visibility = 'ORGANIZATION' OR created_by = %s OR assigned_to = %s OR %s = ANY(permitted_users))"
            if status:
                count_query += " AND status = %s"
            if severity:
                count_query += " AND severity = %s"
                
            cur.execute(count_query, tuple(params))
            total = cur.fetchone()[0]

            query += " ORDER BY updated_at DESC LIMIT %s OFFSET %s"
            params.append(limit)
            params.append(offset)
            
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            
            cases = []
            for r in rows:
                cases.append({
                    "case_id": r[0],
                    "_id": r[0],
                    "id": r[0],
                    "title": r[1],
                    "status": r[2],
                    "severity": r[3],
                    "created_at": r[4],
                    "updated_at": r[5],
                    "assigned_to": r[6],
                    "flag": r[7] or False,
                    "closed_at": r[8],
                    "resolution_status": r[9],
                    "impact_status": r[10],
                    "summary": r[11],
                    "tlp": r[12],
                    "pap": r[13],
                    "tags": r[14] or [],
                    "case_number": r[15],
                    "number": r[15]
                })
            
            return {"total": total, "cases": cases}
    finally:
        conn.close()

@app.get("/cases/{case_id}")
def get_case(case_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT title, description, severity, status, visibility, permitted_users, created_by, assigned_to, created_at, updated_at, closed_at, flag, resolution_status, impact_status, summary, custom_fields, tlp, pap, tags, case_number FROM cases WHERE tenant_id = %s AND case_id = %s",
                (auth.tenant_id, case_id)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Case not found")
            
            visibility = row[4]
            permitted_users = row[5] or []
            created_by = row[6]
            assigned_to = row[7]

            # Visibility Check for Single Read
            if visibility == 'PRIVATE' and "SYSTEM_ADMIN" not in auth.roles:
                 if auth.user_id not in permitted_users and auth.user_id != created_by and auth.user_id != assigned_to:
                     raise HTTPException(status_code=403, detail="Access denied: Private Case")

            # Transform dict custom_fields back to array of objects for the UI
            custom_fields_dict = row[15] or {}
            custom_fields_arr = []
            for ref, val in custom_fields_dict.items():
                # Derive schema mapping back to UI object
                cf_type = list(val.keys())[0] if val and isinstance(val, dict) else "string"
                custom_fields_arr.append({
                    "_id": ref,
                    "id": ref,
                    "name": ref,
                    "reference": ref,
                    "type": cf_type,
                    "value": val
                })

            return {
                "case_id": case_id,
                "_id": case_id,
                "id": case_id,
                "title": row[0],
                "description": row[1],
                "severity": row[2],
                "status": row[3],
                "visibility": visibility,
                "permitted_users": permitted_users,
                "created_by": created_by,
                "assigned_to": assigned_to,
                "created_at": row[8],
                "updated_at": row[9],
                "closed_at": row[10],
                "flag": row[11] or False,
                "resolution_status": row[12],
                "impact_status": row[13],
                "summary": row[14],
                "customFields": custom_fields_arr,
                "tlp": row[16],
                "pap": row[17],
                "tags": row[18] or [],
                "case_number": row[19],
                "number": row[19]
            }
    finally:
        conn.close()

@app.get("/case/{case_id}/links")
@app.get("/cases/{case_id}/links")
@app.get("/api/case/{case_id}/links")
@app.get("/api/cases/{case_id}/links")
def get_case_links(case_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    conn = get_db_conn()
    links = []
    try:
        with conn.cursor() as cur:
            # First, check case access
            cur.execute("SELECT visibility, permitted_users, created_by, assigned_to FROM cases WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, case_id))
            c_row = cur.fetchone()
            if not c_row:
                raise HTTPException(status_code=404, detail="Case not found")
            
            visibility, permitted_users, created_by, assigned_to = c_row
            if visibility == 'PRIVATE' and "SYSTEM_ADMIN" not in auth.roles:
                 if auth.user_id not in (permitted_users or []) and auth.user_id != created_by and auth.user_id != assigned_to:
                     raise HTTPException(status_code=403, detail="Access denied: Private Case")

            cur.execute(
                """
                SELECT l.target_case_id, c.title, c.status, l.linked_by, l.linked_at 
                FROM case_links l
                JOIN cases c ON l.tenant_id = c.tenant_id AND l.target_case_id = c.case_id
                WHERE l.tenant_id = %s AND l.case_id = %s
                """,
                (auth.tenant_id, case_id)
            )
            for r in cur.fetchall():
                links.append({
                    "caseId": r[0],
                    "_id": r[0],
                    "target_case_id": r[0],
                    "title": r[1] or "Unknown Case",
                    "status": r[2],
                    "linked_by": r[3],
                    "linked_at": r[4],
                    "linkedWith": [] # Emulate zero shared observables strictly for UI array validation
                })
    finally:
        conn.close()
    return links

@app.get("/cases/{case_id}/timeline")
def get_case_timeline(case_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    conn = get_db_conn()
    timeline = []
    try:
        with conn.cursor() as cur:
            # Verify case exists
            cur.execute("SELECT 1 FROM cases WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, case_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Case not found")

            # Get Tasks
            cur.execute("SELECT task_id, title, status, created_by, created_at FROM case_tasks WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, case_id))
            for r in cur.fetchall():
                timeline.append({"type": "task", "id": r[0], "title": r[1], "status": r[2], "user": r[3], "ts": r[4]})
            
            # Get Notes
            cur.execute("SELECT note_id, body, created_by, created_at FROM case_notes WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, case_id))
            for r in cur.fetchall():
                timeline.append({"type": "note", "id": r[0], "body": r[1], "user": r[2], "ts": r[3]})
                
            # Get Links (Observables)
            cur.execute("SELECT original_event_id, linked_by, linked_at, link_reason FROM case_alert_links WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, case_id))
            for r in cur.fetchall():
                timeline.append({"type": "link", "alert_id": r[0], "user": r[1], "ts": r[2], "reason": r[3]})
                
    finally:
        conn.close()
    
    timeline.sort(key=lambda x: x['ts'], reverse=True)
    return {"case_id": case_id, "timeline": timeline}

# ── Tasks Read Endpoints ────────────────────────────────────────────────────────
@app.get("/case/{case_id}/task")
@app.get("/cases/{case_id}/tasks")
@app.get("/case/{case_id}/tasks")
@app.get("/cases/{case_id}/task")
@app.get("/api/case/{case_id}/tasks")
@app.get("/api/cases/{case_id}/tasks")
def list_case_tasks(case_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM cases WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, case_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Case not found")
            cur.execute(
                "SELECT task_id, title, status, created_by, assigned_to, created_at, updated_at, description, due_date, priority FROM case_tasks WHERE tenant_id = %s AND case_id = %s ORDER BY created_at ASC",
                (auth.tenant_id, case_id)
            )
            columns = [desc[0] for desc in cur.description]
            tasks = []
            for r in cur.fetchall():
                row = dict(zip(columns, r))
                db_status_up = row.get("status", "").upper() if row.get("status") else ""
                if db_status_up in ["OPEN", "WAITING"]:
                    status = "Waiting"
                elif db_status_up in ["INPROGRESS", "IN PROGRESS"]:
                    status = "In Progress"
                elif db_status_up == "COMPLETED":
                    status = "Completed"
                else:
                    status = row.get("status", "Waiting")
                
                tasks.append({
                    "_id": row["task_id"], "id": row["task_id"], "task_id": row["task_id"],
                    "title": row["title"], "status": status,
                    "createdBy": row["created_by"], "assignee": row["assigned_to"], "owner": row["assigned_to"],
                    "startDate": row["created_at"], "created_at": row["created_at"], "updated_at": row["updated_at"],
                    "description": row["description"], "dueDate": row["due_date"], "priority": row["priority"],
                    "group": "default", "flag": False,
                    "extraData": {"shareCount": 0, "actionRequired": False}
                })
            logger.info(f"Fetched {len(tasks)} tasks for case {case_id}")
            return tasks
    finally:
        conn.close()

@app.get("/tasks/{task_id}")
def get_single_task(task_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT task_id, case_id, title, status, created_by, assigned_to, created_at, updated_at, description, due_date, priority FROM case_tasks WHERE tenant_id = %s AND task_id = %s",
                (auth.tenant_id, task_id)
            )
            r = cur.fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Task not found")
            db_status = r[3].upper() if r[3] else ""
            if db_status in ["OPEN", "WAITING"]:
                status = "Waiting"
            elif db_status in ["INPROGRESS", "IN PROGRESS"]:
                status = "InProgress"
            elif db_status == "COMPLETED":
                status = "Completed"
            else:
                status = r[3] or "Waiting"
            return {
                "_id": r[0], "id": r[0], "task_id": r[0], "case_id": r[1],
                "title": r[2], "status": status,
                "createdBy": r[4], "assignee": r[5], "owner": r[5],
                "startDate": r[6], "created_at": r[6], "updated_at": r[7],
                "description": r[8], "dueDate": r[9], "priority": r[10],
                "group": "default", "flag": False,
                "extraData": {"shareCount": 0, "actionRequired": False}
            }
    finally:
        conn.close()

@app.get("/tasks/{task_id}/logs")
def get_task_logs(task_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT log_id, body, created_by, created_at FROM case_task_logs WHERE tenant_id = %s AND task_id = %s ORDER BY created_at DESC",
                (auth.tenant_id, task_id)
            )
            return [{"_id": r[0], "id": r[0], "message": r[1], "createdBy": r[2], "createdAt": r[3], "startDate": r[3]} for r in cur.fetchall()]
    finally:
        conn.close()

# ── Pages Proxy (direct Postgres) ───────────────────────────────────────────────
@app.get("/case/{case_id}/page")
def get_case_pages(case_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT page_id, title, content, created_by, created_at FROM case_pages WHERE tenant_id = %s AND case_id = %s ORDER BY created_at ASC",
                (auth.tenant_id, case_id)
            )
            return [{"_id": r[0], "id": r[0], "title": r[1], "content": r[2], "createdBy": r[3], "createdAt": r[4]} for r in cur.fetchall()]
    finally:
        conn.close()

# ── Observables Proxy (Forward to Case Engine) ──────────────────────────────────
@app.get("/case/{id}/observable")
@app.get("/cases/{id}/observables")
@app.get("/case/{id}/observables")
@app.get("/cases/{id}/observable")
@app.get("/api/cases/{id}/observables")
@app.get("/case/{id}/artifact")
@app.get("/case/{id}/artifacts")
@app.get("/cases/{id}/artifacts")
@app.get("/cases/{id}/artifact")
@app.get("/api/case/{id}/artifacts")
@app.get("/api/case/{id}/artifact")
@app.get("/api/cases/{id}/artifacts")
async def proxy_list_observables_consolidated(id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return _forward_request("GET", f"/case/{id}/observable", request, auth)

# ── TTPs (new table, direct Postgres) ───────────────────────────────────────────
@app.delete("/case/{case_id}/task/{task_id}")
@app.delete("/api/case/{case_id}/task/{task_id}")
@app.delete("/case/{case_id}/tasks/{task_id}")
@app.delete("/api/case/{case_id}/tasks/{task_id}")
async def proxy_delete_case_task(case_id: str, task_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    return _forward_request("DELETE", f"/case/{case_id}/task/{task_id}", request, auth)

# --- Simulated Responders (Enterprise Integration Hub) ---
@app.get("/api/connector/cortex/responder/case_task/{task_id}")
async def proxy_get_case_task_responders(task_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    # Simulated responders for enterprise UX
    return [
        {"id": "ai-enrichment", "name": "AI Task Enrichment", "description": "Automatically enrich task with context using LLM.", "version": "1.0"},
        {"id": "security-validation", "name": "Security Policy Validation", "description": "Validate task against internal security policies.", "version": "1.1"},
        {"id": "team-notifier", "name": "Notification (Slack/Teams)", "description": "Notify the relevant team about this task.", "version": "2.0"}
    ]

@app.post("/api/connector/cortex/action")
async def proxy_run_cortex_action(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    logger.info(f"SIMULATED RESPONDER ACTION started: {body} by {auth.user_id}")
    # Simulate a small delay
    await asyncio.sleep(0.5)
    return {
        "status": "Success",
        "data": {
            "responderId": body.get("responderId"),
            "responderName": "Enterprise Responder",
            "jobId": str(uuid.uuid4())
        }
    }

@app.get("/case/{case_id}/ttp")
@app.get("/cases/{case_id}/ttps")
@app.get("/case/{case_id}/ttps")
@app.get("/cases/{case_id}/ttp")
@app.get("/api/case/{case_id}/ttps")
@app.get("/api/case/{case_id}/ttp")
@app.get("/api/cases/{case_id}/ttps")
def list_ttps(case_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
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


# E3 Legacy Mapping for UI compatibility
@app.get("/api/case/{case_id}/links")
def legacy_get_case_links(case_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return get_case_links(case_id, auth)


# ─── E8.1 Visual Vyuha — Graph Traversal API ──────────────────────────────────
#
# GET /graph/case/{case_id}
#
# Builds a node-link graph seeded from one Case:
#   • Collects every observable linked to the case.
#   • Finds all SAME-TENANT cases that share those observables.
#   • Cross-tenant hits are anonymized ("ANON" node, no IDs exposed).
#   • Hard cap of MAX_GRAPH_NODES to protect canvas performance.
#
# Response schema:
#   { nodes: [{id, label, node_type, tenant_id}], edges: [{source, target, label, observable}],
#     truncated: bool }
#
# Node types: "case" | "ip" | "file" | "user" | "hash" | "domain" | "cross_tenant"


@app.get("/observable/type")
def list_observable_types(auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    """Return common observable types for the legacy UI."""
    types = ["ip", "domain", "url", "hash", "user", "other", "file", "fqdn", "hostname", "mail", "mail_subject", "registry", "regexp", "filepath"]
    return [{"name": t, "label": t.capitalize()} for t in types]

@app.get("/api/observable/type")
def list_observable_types_legacy(auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return list_observable_types(auth)

@app.get("/v1/describe/_all")
@app.get("/v0/describe/_all")
@app.get("/api/v1/describe/_all")
@app.get("/api/v0/describe/_all")
async def legacy_describe_all(auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    """Stub for legacy field description API."""
    return {
        "case": {"attributes": {}},
        "task": {"attributes": {}},
        "observable": {"attributes": {}},
        "ttp": {"attributes": {}},
        "page": {"attributes": {}},
        "procedure": {"attributes": {}},
        "observableTypes": [
            {"name": "ip", "label": "IP Address"},
            {"name": "domain", "label": "Domain"},
            {"name": "url", "label": "URL"},
            {"name": "hash", "label": "Hash"},
            {"name": "user", "label": "User"},
            {"name": "other", "label": "Other"}
        ],
        "customFields": []
    }

@app.post("/v1/describe/_all")
@app.post("/v0/describe/_all")
@app.post("/api/v1/describe/_all")
@app.post("/api/v0/describe/_all")
async def legacy_describe_all_post(auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return {
        "case": {"attributes": {}},
        "task": {"attributes": {}},
        "observable": {"attributes": {}},
        "ttp": {"attributes": {}},
        "page": {"attributes": {}},
        "observableTypes": [
            {"name": "ip", "label": "IP Address"},
            {"name": "domain", "label": "Domain"},
            {"name": "url", "label": "URL"},
            {"name": "hash", "label": "Hash"},
            {"name": "user", "label": "User"},
            {"name": "other", "label": "Other"}
        ],
        "customFields": []
    }

MAX_GRAPH_NODES = 500


def _get_case_observables(cur, tenant_id: str, case_id: str):
    """Returns [(observable_value, observable_type)] linked to the case."""
    observables = []

    # Pull from case_alert_links → observable info stored in linked alerts (OpenSearch)
    cur.execute(
        "SELECT original_event_id FROM case_alert_links WHERE tenant_id = %s AND case_id = %s",
        (tenant_id, case_id)
    )
    alert_ids = [r[0] for r in cur.fetchall()]
    return alert_ids  # We'll resolve observables from alert payloads via OpenSearch


def _get_artifact_hashes(cur, tenant_id: str, case_id: str):
    """Returns [(sha256, filename)] for artifacts attached to the case."""
    try:
        cur.execute(
            "SELECT sha256, filename FROM artifacts WHERE tenant_id = %s AND case_id = %s",
            (tenant_id, case_id)
        )
        return cur.fetchall()
    except Exception:
        return []


def _resolve_alert_observables(alert_ids: list, tenant_id: str) -> list:
    """Fetch observable fields from OpenSearch alert docs for graph construction."""
    if not alert_ids:
        return []
    try:
        resp = os_client.search(
            body={
                "query": {"ids": {"values": alert_ids}},
                "size": len(alert_ids),
                "_source": ["tenant_id", "src_ip", "dst_ip", "file_hash", "username", "domain"]
            },
            index=INDEX_ALERTS
        )
        observables = []
        for hit in resp["hits"]["hits"]:
            src = hit["_source"]
            if src.get("tenant_id") != tenant_id:
                continue  # Defence-in-depth tenant check
            if src.get("src_ip"):
                observables.append({"value": src["src_ip"], "type": "ip"})
            if src.get("dst_ip"):
                observables.append({"value": src["dst_ip"], "type": "ip"})
            if src.get("file_hash"):
                observables.append({"value": src["file_hash"], "type": "hash"})
            if src.get("username"):
                observables.append({"value": src["username"], "type": "user"})
            if src.get("domain"):
                observables.append({"value": src["domain"], "type": "domain"})
        return observables
    except Exception as e:
        logger.warning(f"graph: failed to resolve alert observables: {e}")
        return []


def _find_cases_sharing_observable(cur, own_tenant_id: str, obs_value: str, obs_type: str, exclude_case_id: str) -> list:
    """
    Find cases that share an observable (by IP/hash/user/domain).
    Returns list of dicts with {case_id, tenant_id, title}.
    Cross-tenant cases have title/case_id anonymized.
    """
    # Map observable type to field in alerts index (we'll do this via OpenSearch)
    field_map = {"ip": ["src_ip", "dst_ip"], "hash": ["file_hash"], "user": ["username"], "domain": ["domain"]}
    fields = field_map.get(obs_type, [])
    if not fields:
        return []

    # Build OS query: match observable in any relevant field
    should_clauses = [{"term": {f: obs_value}} for f in fields]
    try:
        resp = os_client.search(
            body={
                "query": {"bool": {"should": should_clauses, "minimum_should_match": 1}},
                "size": 100,
                "_source": ["tenant_id", "case_id"]
            },
            index=INDEX_ALERTS
        )
    except Exception as e:
        logger.warning(f"graph: observable search failed: {e}")
        return []

    results = []
    seen_cases = set()
    for hit in resp["hits"]["hits"]:
        src = hit["_source"]
        c_id = src.get("case_id")
        if not c_id or c_id == exclude_case_id or c_id in seen_cases:
            continue
        seen_cases.add(c_id)
        t_id = src.get("tenant_id", "")

        if t_id == own_tenant_id:
            # Fetch case title from Postgres
            try:
                cur.execute("SELECT title FROM cases WHERE tenant_id = %s AND case_id = %s", (t_id, c_id))
                row = cur.fetchone()
                title = row[0] if row else c_id
            except Exception:
                title = c_id
            results.append({"case_id": c_id, "tenant_id": t_id, "title": title, "cross_tenant": False})
        else:
            # TENANT_PRIVACY: anonymize cross-tenant hit — no IDs, no names
            results.append({
                "case_id": f"ANON_{hashlib.sha256(c_id.encode()).hexdigest()[:8]}",
                "tenant_id": "REDACTED",
                "title": "Seen in another tenant",
                "cross_tenant": True
            })
    return results


@app.get("/graph/case/{case_id}")
def get_case_graph(
    case_id: str,
    auth: AuthContext = Depends(require_permission(PERM_GRAPH_READ))
):
    """
    Visual Vyuha Graph API — returns a node-link graph seeded from a Case.

    Response: { nodes: [...], edges: [...], truncated: bool }
    Node types: case | ip | file | user | hash | domain | cross_tenant
    Node colours are assigned by the UI (Red=case, Blue=ip/domain, Green=file/hash, Yellow=user).
    """
    conn = get_db_conn()
    nodes: dict = {}   # node_id -> node dict
    edges: list = []
    truncated = False

    try:
        with conn.cursor() as cur:
            # 1. Verify seed case exists and belongs to tenant
            cur.execute(
                "SELECT title, severity, status FROM cases WHERE tenant_id = %s AND case_id = %s",
                (auth.tenant_id, case_id)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Case not found")

            seed_title, seed_severity, seed_status = row[0], row[1], row[2]

            # Add seed case node
            nodes[case_id] = {
                "id": case_id,
                "label": seed_title,
                "node_type": "case",
                "severity": seed_severity,
                "status": seed_status,
                "tenant_id": auth.tenant_id
            }

            # 2. Collect linked alert IDs
            alert_ids = _get_case_observables(cur, auth.tenant_id, case_id)

            # 3. Resolve observables from alert docs
            observables = _resolve_alert_observables(alert_ids, auth.tenant_id)

            # 4. Add artifact hashes as observables
            artifact_hashes = _get_artifact_hashes(cur, auth.tenant_id, case_id)
            for (sha256, filename) in artifact_hashes:
                observables.append({"value": sha256, "type": "hash", "label": filename or sha256[:16]})

            # 5. For each observable: create observable node + find sibling cases
            seen_edges = set()

            for obs in observables:
                obs_value = obs["value"]
                obs_type = obs["type"]
                obs_label = obs.get("label", obs_value)
                obs_node_id = f"{obs_type}:{obs_value}"

                if len(nodes) >= MAX_GRAPH_NODES:
                    truncated = True
                    break

                # Add observable node if needed
                if obs_node_id not in nodes:
                    nodes[obs_node_id] = {
                        "id": obs_node_id,
                        "label": obs_label,
                        "node_type": obs_type,
                        "tenant_id": auth.tenant_id
                    }

                # Edge: seed case → observable
                edge_key = f"{case_id}|{obs_node_id}"
                if edge_key not in seen_edges:
                    edges.append({
                        "source": case_id,
                        "target": obs_node_id,
                        "label": "has_observable",
                        "observable": obs_value
                    })
                    seen_edges.add(edge_key)

                # 6. Find sibling cases sharing this observable
                sibling_cases = _find_cases_sharing_observable(
                    cur, auth.tenant_id, obs_value, obs_type, case_id
                )

                for sibling in sibling_cases:
                    sibling_id = sibling["case_id"]

                    if len(nodes) >= MAX_GRAPH_NODES:
                        truncated = True
                        break

                    if sibling_id not in nodes:
                        node_type = "cross_tenant" if sibling["cross_tenant"] else "case"
                        nodes[sibling_id] = {
                            "id": sibling_id,
                            "label": sibling["title"],
                            "node_type": node_type,
                            "tenant_id": sibling["tenant_id"]
                        }

                    # Edge: sibling case → observable (or direct sibling→seed edge)
                    sibling_obs_key = f"{sibling_id}|{obs_node_id}"
                    if sibling_obs_key not in seen_edges:
                        edges.append({
                            "source": sibling_id,
                            "target": obs_node_id,
                            "label": "shares_observable",
                            "observable": obs_value
                        })
                        seen_edges.add(sibling_obs_key)

    finally:
        conn.close()

    logger.info(
        f"graph: case={case_id} tenant={auth.tenant_id} "
        f"nodes={len(nodes)} edges={len(edges)} truncated={truncated}"
    )

    return {
        "seed_case_id": case_id,
        "nodes": list(nodes.values()),
        "edges": edges,
        "truncated": truncated,
        "node_count": len(nodes),
        "edge_count": len(edges)
    }

# ── Default Legacy Query Fallback ──────────────────────────────────────────────

# ── Phase 4 Proxy Routes for Case Management ────────────────────────────────────
NV_CASE_ENGINE_URL = os.getenv("NV_CASE_ENGINE_URL", "http://nv-case-engine:8000")

def _forward_request(method, path, request: Request, auth: Any, json_body=None):
    try:
        user_id = "None"
        tenant_id = ""
        
        if auth:
            # Pydantic V2 model_dump or V1 dict()
            auth_data = {}
            if hasattr(auth, "model_dump"):
                auth_data = auth.model_dump()
            elif hasattr(auth, "dict"):
                auth_data = auth.dict()
            
            user_id = auth_data.get("user_id") or getattr(auth, "user_id", "None")
            tenant_id = auth_data.get("tenant_id") or getattr(auth, "tenant_id", "")

        logger.info(f"Proxying {method} {path} for user {user_id}")
        headers = {
            "Authorization": request.headers.get("Authorization", ""),
            "X-User-ID": str(user_id),
            "X-Tenant-ID": str(tenant_id),
        }
        # Forward Idempotency-Key if present
        idem_key = request.headers.get("Idempotency-Key") or request.headers.get("idempotency-key")
        if idem_key:
            headers["Idempotency-Key"] = idem_key
        else:
            import uuid
            headers["Idempotency-Key"] = str(uuid.uuid4())
        
        url = f"{NV_CASE_ENGINE_URL}{path}"
        logger.info(f"Connecting to: {url}")
        
        resp = requests.request(
            method, 
            url, 
            headers=headers, 
            json=json_body, 
            params=dict(request.query_params),
            timeout=10
        )
        logger.info(f"Proxy {url} returned status {resp.status_code}")
        return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type", "application/json"))
    except Exception as e:
        import traceback
        error_msg = f"CRITICAL: Error proxying to case engine: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        logger.error(error_msg)
        raise HTTPException(status_code=502, detail=f"Case Engine Gateway Error: {str(e)}")

@app.post("/cases")
async def proxy_create_case(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("POST", "/cases", request, auth, json_body=body)

@app.patch("/cases/{case_id}")
@app.patch("/case/{case_id}")
@app.patch("/api/case/{case_id}")
async def proxy_update_case(case_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    status_val = body.get("status")
    if status_val:
        if status_val.lower() == "resolved":
            body["status"] = "CLOSED"
        elif status_val.lower() in ["inprogress", "waiting"]:
            body["status"] = "OPEN"
    
    # Check if we have anything to update for a legacy UI patch 
    if not body:
        return {}

    return _forward_request("PATCH", f"/cases/{case_id}", request, auth, json_body=body)

@app.post("/cases/{case_id}/links")
async def proxy_link_case(case_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("POST", f"/cases/{case_id}/links", request, auth, json_body=body)

@app.post("/case/{case_id}/task")
async def proxy_create_task(case_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("POST", f"/case/{case_id}/task", request, auth, json_body=body)

@app.get("/case/{case_id}/task")
def list_case_tasks(case_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    logger.info(f"LIST TASKS: case_id={case_id} tenant={auth.tenant_id}")
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM cases WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, case_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Case not found")
            cur.execute(
                "SELECT task_id, title, status, created_by, assigned_to, created_at, updated_at, description, due_date, priority FROM case_tasks WHERE tenant_id = %s AND case_id = %s ORDER BY created_at ASC",
                (auth.tenant_id, case_id)
            )
            columns = [desc[0] for desc in cur.description]
            tasks = []
            for r in cur.fetchall():
                row = dict(zip(columns, r))
                db_status_up = row.get("status", "").upper() if row.get("status") else ""
                if db_status_up in ["OPEN", "WAITING"]:
                    status = "Waiting"
                elif db_status_up in ["INPROGRESS", "IN PROGRESS"]:
                    status = "In Progress"
                elif db_status_up == "COMPLETED":
                    status = "Completed"
                else:
                    status = row.get("status", "Waiting")
                
                tasks.append({
                    "_id": row["task_id"], "id": row["task_id"], "task_id": row["task_id"],
                    "title": row["title"], "status": status,
                    "createdBy": row["created_by"], "assignee": row["assigned_to"], "owner": row["assigned_to"],
                    "startDate": row["created_at"], "created_at": row["created_at"], "updated_at": row["updated_at"],
                    "description": row["description"], "dueDate": row["due_date"], "priority": row["priority"],
                    "group": "default", "flag": False,
                    "extraData": {"shareCount": 0, "actionRequired": False}
                })
            logger.info(f"FETCHED {len(tasks)} tasks for case {case_id}")
            return tasks
    finally:
        conn.close()

@app.delete("/case/{case_id}/task/{task_id}")
@app.delete("/api/case/{case_id}/task/{task_id}")
@app.delete("/case/{case_id}/tasks/{task_id}")
@app.delete("/api/case/{case_id}/tasks/{task_id}")
async def proxy_delete_case_task(case_id: str, task_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    return _forward_request("DELETE", f"/case/{case_id}/task/{task_id}", request, auth)

# --- Simulated Responders (Enterprise Integration Hub) ---
@app.get("/connector/cortex/responder/case/{case_id}")
@app.get("/api/connector/cortex/responder/case/{case_id}")
async def proxy_get_case_responders(case_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return [
        {"id": "ai-enrichment", "name": "AI Case Triage", "description": "Automatically triage case with context using LLM.", "version": "1.0"},
        {"id": "team-notifier", "name": "Notification (Slack/Teams)", "description": "Notify the relevant team about this case.", "version": "2.0"}
    ]

@app.get("/connector/cortex/responder/case_task/{task_id}")
@app.get("/api/connector/cortex/responder/case_task/{task_id}")
async def proxy_get_case_task_responders(task_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    # Simulated responders for enterprise UX
    return [
        {"id": "ai-enrichment", "name": "AI Task Enrichment", "description": "Automatically enrich task with context using LLM.", "version": "1.0"},
        {"id": "security-validation", "name": "Security Policy Validation", "description": "Validate task against internal security policies.", "version": "1.1"},
        {"id": "team-notifier", "name": "Notification (Slack/Teams)", "description": "Notify the relevant team about this task.", "version": "2.0"}
    ]

@app.post("/connector/cortex/action")
@app.post("/api/connector/cortex/action")
async def proxy_run_cortex_action(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    logger.info(f"SIMULATED RESPONDER ACTION started: {body} by {auth.user_id}")
    # Simulate a small delay
    await asyncio.sleep(0.5)
    return {
        "status": "Success",
        "data": {
            "responderId": body.get("responderId"),
            "responderName": "Enterprise Responder",
            "jobId": str(uuid.uuid4())
        }
    }

@app.patch("/case/{case_id}/task/{task_id}")
@app.patch("/cases/{case_id}/task/{task_id}")
@app.patch("/api/case/{case_id}/task/{task_id}")
@app.patch("/api/cases/{case_id}/task/{task_id}")
@app.patch("/case/task/{task_id}")
@app.patch("/api/case/task/{task_id}")
async def proxy_update_task(task_id: str, request: Request, case_id: str = None, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    if not case_id:
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT case_id FROM case_tasks WHERE tenant_id = %s AND task_id = %s", (auth.tenant_id, task_id))
                row = cur.fetchone()
                if row:
                    case_id = str(row[0])
                else:
                    raise HTTPException(status_code=404, detail="Task not found")
        finally:
            conn.close()
    
    logger.info(f"PROXY UPDATE TASK: case_id={case_id} task_id={task_id} body={body}")
    return _forward_request("PATCH", f"/case/{case_id}/task/{task_id}", request, auth, json_body=body)

@app.post("/case/task/{task_id}/log")
@app.post("/api/case/task/{task_id}/log")
async def proxy_create_task_log(task_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    # Support multipart/form-data for attachments
    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" in content_type:
        form = await request.form()
        body_json = form.get("_json", "{}")
        try:
            body = json.loads(body_json)
        except Exception:
            body = {}
        # Ignore actual file proxying for now, just save text log to prevent UI crash
    else:
        body = await request.json()

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT case_id FROM case_tasks WHERE tenant_id = %s AND task_id = %s", (auth.tenant_id, task_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Task not found")
            case_id = str(row[0])
    finally:
        conn.close()
    # Map UI 'message' field to engine 'body' field
    if "message" in body and "body" not in body:
        body["body"] = body.pop("message")
    return _forward_request("POST", f"/case/{case_id}/task/{task_id}/log", request, auth, json_body=body)

# NvApiSrv sends POST /api/tasks/{taskId}/logs (plural, no 'case/' prefix)
@app.post("/tasks/{task_id}/logs")
@app.post("/api/tasks/{task_id}/logs")
async def proxy_create_task_log_nv(task_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT case_id FROM case_tasks WHERE tenant_id = %s AND task_id = %s", (auth.tenant_id, task_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Task not found")
            case_id = str(row[0])
    finally:
        conn.close()
    # Map UI 'message' field to engine 'body' field
    if "message" in body and "body" not in body:
        body["body"] = body.pop("message")
    return _forward_request("POST", f"/case/{case_id}/task/{task_id}/log", request, auth, json_body=body)

# Case-level shares
@app.get("/case/{case_id}/shares")
@app.get("/api/case/{case_id}/shares")
async def get_case_shares(case_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    """Return case-level shares (used by task sharing modal to filter available orgs)."""
    return [{"organisationName": "admin", "owner": True}]


@app.get("/case/{case_id}/task/{task_id}/shares")
@app.get("/api/case/{case_id}/task/{task_id}/shares")
async def get_task_shares(case_id: str, task_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    """Return task-level shares — stub returns owner org."""
    return [{"organisationName": "admin", "owner": True}]

@app.post("/case/task/{task_id}/shares")
@app.post("/api/case/task/{task_id}/shares")
@app.post("/case/{case_id}/task/{task_id}/shares")
@app.post("/api/case/{case_id}/task/{task_id}/shares")
async def add_task_shares(task_id: str, request: Request, case_id: Optional[str] = None, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    """Stub: accept share updates without persisting."""
    return {"status": "ok"}

@app.delete("/task/{task_id}/shares")
@app.delete("/api/task/{task_id}/shares")
async def remove_task_shares(task_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    """Stub: accept share removal without persisting."""
    return {"status": "ok"}


# ── Task Action Required Stubs ───────────────────────────────────────────────
@app.put("/v1/task/{task_id}/actionRequired/{org}")
@app.put("/api/v1/task/{task_id}/actionRequired/{org}")
async def mark_task_action_required(task_id: str, org: str, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    return {"status": "ok"}

@app.put("/v1/task/{task_id}/actionDone/{org}")
@app.put("/api/v1/task/{task_id}/actionDone/{org}")
async def mark_task_action_done(task_id: str, org: str, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    return {"status": "ok"}

@app.get("/v1/task/{task_id}/actionRequired")
@app.get("/api/v1/task/{task_id}/actionRequired")
async def get_task_action_required(task_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return {}


@app.post("/case/template")
async def proxy_create_template(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("POST", "/templates", request, auth, json_body=body)

@app.get("/case/template")
async def proxy_list_templates(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    resp = _forward_request("GET", "/templates", request, auth)
    # The legacy UI expects a raw array of templates
    if hasattr(resp, "body"):
        try:
            data = json.loads(resp.body)
            if "templates" in data:
                return data["templates"]
        except Exception:
            pass
    elif isinstance(resp, dict) and "templates" in resp:
        return resp["templates"]
    return resp

@app.get("/case/{case_id}/export")
async def proxy_export_case(case_id: str, request: Request, password: Optional[str] = None, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    url = f"/cases/{case_id}/export"
    if password:
        url += f"?password={password}"
    return _forward_request("GET", url, request, auth)


@app.post("/cases")
@app.post("/api/cases")
async def proxy_create_case(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("POST", "/cases", request, auth, json_body=body)

@app.post("/alerts/promote")
@app.post("/api/alerts/promote")
async def proxy_promote_alerts(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("POST", "/alerts/promote", request, auth, json_body=body)

@app.delete("/cases/{case_id}")
@app.delete("/case/{case_id}")
@app.delete("/api/case/{case_id}")
async def proxy_delete_case(case_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    return _forward_request("DELETE", f"/cases/{case_id}", request, auth)

@app.delete("/case/{case_id}/force")
@app.delete("/api/case/{case_id}/force")
async def proxy_delete_case_force(case_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    return _forward_request("DELETE", f"/cases/{case_id}/force", request, auth)

@app.post("/cases/merge/{case_ids}")
async def proxy_merge_cases(case_ids: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    # case_ids is a comma separated string from the UI
    ids_list = [c.strip() for c in case_ids.split(",") if c.strip()]
    body = {"case_ids": ids_list}
    return _forward_request("POST", "/cases/merge", request, auth, json_body=body)

@app.post("/case/import")
async def proxy_import_case(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("POST", "/cases/import", request, auth, json_body=body)

@app.post("/case/{case_id}/page")
@app.post("/api/case/{case_id}/page")
async def proxy_create_case_page(case_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("POST", f"/case/{case_id}/page", request, auth, json_body=body)

@app.get("/case/{case_id}/page")
@app.get("/api/case/{case_id}/page")
async def proxy_list_case_pages(case_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return _forward_request("GET", f"/case/{case_id}/page", request, auth)

@app.get("/case/{case_id}/page/{page_id}")
@app.get("/api/case/{case_id}/page/{page_id}")
async def proxy_get_case_page(case_id: str, page_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return _forward_request("GET", f"/case/{case_id}/page/{page_id}", request, auth)

@app.patch("/case/{case_id}/page/{page_id}")
@app.patch("/api/case/{case_id}/page/{page_id}")
async def proxy_update_case_page(case_id: str, page_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("PATCH", f"/case/{case_id}/page/{page_id}", request, auth, json_body=body)

@app.delete("/case/{case_id}/page/{page_id}")
@app.delete("/api/case/{case_id}/page/{page_id}")
async def proxy_delete_case_page(case_id: str, page_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    return _forward_request("DELETE", f"/case/{case_id}/page/{page_id}", request, auth)

@app.post("/case/{case_id}/observable")
@app.post("/case/{case_id}/artifact")
@app.post("/api/case/{case_id}/observable")
@app.post("/api/case/{case_id}/artifact")
async def proxy_create_observable(case_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" in content_type:
        form = await request.form()
        body_json = form.get("_json", "{}")
        try:
            body = json.loads(body_json)
        except Exception:
            body = {}
    else:
        body = await request.json()
    return _forward_request("POST", f"/case/{case_id}/observable", request, auth, json_body=body)

# (Previous proxy_list_observables removed - consolidated at line 2202)

@app.patch("/case/{case_id}/observable/{obs_id}")
@app.patch("/api/case/{case_id}/observable/{obs_id}")
async def proxy_update_observable(case_id: str, obs_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("PATCH", f"/case/{case_id}/observable/{obs_id}", request, auth, json_body=body)

@app.delete("/case/{case_id}/observable/{obs_id}")
@app.delete("/api/case/{case_id}/observable/{obs_id}")
async def proxy_delete_observable(case_id: str, obs_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    return _forward_request("DELETE", f"/case/{case_id}/observable/{obs_id}", request, auth)

@app.get("/case/artifact/{obs_id}")
@app.get("/api/case/artifact/{obs_id}")
async def proxy_get_observable(obs_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return _forward_request("GET", f"/observable/{obs_id}", request, auth)

@app.get("/case/artifact/{obs_id}/similar")
@app.get("/api/case/artifact/{obs_id}/similar")
async def proxy_get_similar_observables(obs_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return _forward_request("GET", f"/observable/{obs_id}/similar", request, auth)

@app.patch("/case/artifact/{obs_id}")
@app.patch("/api/case/artifact/{obs_id}")
async def proxy_update_observable_by_artifact(obs_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("PATCH", f"/observable/{obs_id}", request, auth, json_body=body)

@app.delete("/case/artifact/{obs_id}")
@app.delete("/api/case/artifact/{obs_id}")
async def proxy_delete_observable_by_artifact(obs_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    return _forward_request("DELETE", f"/observable/{obs_id}", request, auth)

@app.get("/case/{case_id}/observable/{obs_id}/shares")
@app.get("/api/case/{case_id}/observable/{obs_id}/shares")
@app.get("/case/artifact/{obs_id}/shares")
@app.get("/api/case/artifact/{obs_id}/shares")
async def proxy_get_observable_shares(obs_id: str, request: Request, case_id: Optional[str] = None, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    url = f"/observable/{obs_id}/shares"
    if case_id:
        url = f"/case/{case_id}/observable/{obs_id}/shares"
    return _forward_request("GET", url, request, auth)

@app.post("/case/{case_id}/observable/{obs_id}/shares")
@app.post("/api/case/{case_id}/observable/{obs_id}/shares")
@app.post("/case/artifact/{obs_id}/shares")
@app.post("/api/case/artifact/{obs_id}/shares")
async def proxy_update_observable_shares(obs_id: str, request: Request, case_id: Optional[str] = None, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    url = f"/observable/{obs_id}/shares"
    if case_id:
        url = f"/case/{case_id}/observable/{obs_id}/shares"
    return _forward_request("POST", url, request, auth, json_body=body)

@app.post("/case/{case_id}/ttp")
@app.post("/api/case/{case_id}/ttp")
async def proxy_create_ttp(case_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("POST", f"/case/{case_id}/ttp", request, auth, json_body=body)

@app.get("/case/{case_id}/ttp")
@app.get("/api/case/{case_id}/ttp")
async def proxy_list_ttps(case_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return _forward_request("GET", f"/case/{case_id}/ttp", request, auth)

@app.delete("/case/{case_id}/ttp/{ttp_id}")
@app.delete("/api/case/{case_id}/ttp/{ttp_id}")
async def proxy_delete_ttp(case_id: str, ttp_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    return _forward_request("DELETE", f"/case/{case_id}/ttp/{ttp_id}", request, auth)

@app.get("/case/{case_id}/note")
@app.get("/api/case/{case_id}/note")
async def proxy_list_case_notes(case_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return _forward_request("GET", f"/case/{case_id}/note", request, auth)

@app.post("/case/{case_id}/note")
@app.post("/api/case/{case_id}/note")
async def proxy_add_case_note(case_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("POST", f"/case/{case_id}/note", request, auth, json_body=body)

# --- Task Proxy Routes ---
@app.post("/case/{case_id}/task")
@app.post("/api/case/{case_id}/task")
async def proxy_create_task(case_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("POST", f"/case/{case_id}/task", request, auth, json_body=body)

@app.get("/case/{case_id}/task")
@app.get("/api/case/{case_id}/task")
async def proxy_list_tasks(case_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return _forward_request("GET", f"/case/{case_id}/task", request, auth)

@app.patch("/case/{case_id}/task/{task_id}")
@app.patch("/api/case/{case_id}/task/{task_id}")
async def proxy_update_task(case_id: str, task_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("PATCH", f"/case/{case_id}/task/{task_id}", request, auth, json_body=body)

@app.delete("/case/{case_id}/task/{task_id}")
@app.delete("/api/case/{case_id}/task/{task_id}")
async def proxy_delete_task(case_id: str, task_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    return _forward_request("DELETE", f"/case/{case_id}/task/{task_id}", request, auth)

@app.get("/tasks/{task_id}")
@app.get("/api/tasks/{task_id}")
async def proxy_get_task(task_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return _forward_request("GET", f"/tasks/{task_id}", request, auth)

@app.get("/tasks/{task_id}/logs")
@app.get("/api/tasks/{task_id}/logs")
async def proxy_get_task_logs(task_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return _forward_request("GET", f"/tasks/{task_id}/logs", request, auth)

@app.post("/case/{case_id}/task/{task_id}/log")
@app.post("/api/case/{case_id}/task/{task_id}/log")
async def proxy_add_task_log(case_id: str, task_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("POST", f"/case/{case_id}/task/{task_id}/log", request, auth, json_body=body)

@app.delete("/case/task/log/{log_id}")
@app.delete("/api/case/task/log/{log_id}")
@app.delete("/case/{case_id}/task/{task_id}/log/{log_id}")
@app.delete("/api/case/{case_id}/task/{task_id}/log/{log_id}")
async def delete_task_log(log_id: str, request: Request, case_id: Optional[str] = None, task_id: Optional[str] = None, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    """Delete a task log entry by log_id."""
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM case_task_logs WHERE log_id = %s AND tenant_id = %s",
                (log_id, auth.tenant_id)
            )
            if cur.rowcount == 0:
                conn.rollback()
                raise HTTPException(status_code=404, detail="Task log not found")
        conn.commit()
        logger.info(f"Deleted task log {log_id} for tenant {auth.tenant_id}")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting task log {log_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete task log: {str(e)}")
    finally:
        conn.close()


@app.get("/case/task/{task_id}/shares")
@app.get("/api/case/task/{task_id}/shares")
@app.get("/task/{task_id}/shares")
@app.get("/api/task/{task_id}/shares")
async def proxy_get_task_shares_simple(task_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return _forward_request("GET", f"/case/task/{task_id}/shares", request, auth)

@app.get("/case/{case_id}/task/{task_id}/shares")
@app.get("/api/case/{case_id}/task/{task_id}/shares")
async def proxy_get_task_shares_full(case_id: str, task_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    return _forward_request("GET", f"/case/{case_id}/task/{task_id}/shares", request, auth)

@app.post("/case/task/{task_id}/shares")
@app.post("/api/case/task/{task_id}/shares")
@app.post("/task/{task_id}/shares")
@app.post("/api/task/{task_id}/shares")
async def proxy_update_task_shares_simple(task_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("POST", f"/case/task/{task_id}/shares", request, auth, json_body=body)

@app.post("/case/{case_id}/task/{task_id}/shares")
@app.post("/api/case/{case_id}/task/{task_id}/shares")
async def proxy_update_task_shares_full(case_id: str, task_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    return _forward_request("POST", f"/case/{case_id}/task/{task_id}/shares", request, auth, json_body=body)


# Legacy Case Rewrite Middleware removed to avoid interference with new singular route structure.



# ── Stub Endpoints for Unimplemented Legacy Features ─────────────────────────
@app.get("/flow")
async def legacy_flow(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    """Return empty array for activity flow/timeline — not yet implemented."""
    return []

@app.get("/customField")
@app.get("/api/customField")
async def legacy_custom_fields(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    """Return common Enterprise Custom Field Definitions."""
    return [
        {"name": "business-impact", "reference": "business-impact", "type": "string", "description": "Business Impact"},
        {"name": "affected-users", "reference": "affected-users", "type": "number", "description": "Number of affected internal users"},
        {"name": "is-ransomware", "reference": "is-ransomware", "type": "boolean", "description": "Is Ransomware involved?"},
        {"name": "department", "reference": "department", "type": "string", "description": "Affected Department"}
    ]

@app.post("/case/task/_search")
async def legacy_task_search(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    """Return empty array for task search — not yet implemented."""
    return []

@app.post("/case/_search")
async def legacy_case_search(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    """Return empty array for legacy case search."""
    return []

@app.post("/query")
@app.post("/api/v1/query")
@app.post("/api/v0/query")
@app.post("/v1/query")
@app.post("/v0/query")
async def legacy_v0_v1_query(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    """
    Catch-all for legacy /api/v0/query and /api/v1/query.
    Now intercepts Case Metrics rollups and queries Postgres.
    """
    body = await request.json()
    # The UI payload is typically { "query": [{ "_name": "countTask", "caseId": "..." }] }
    if isinstance(body, dict) and "query" in body and isinstance(body["query"], list):
        queries = body["query"]
        if len(queries) > 0:
            query_def = queries[0]
            action = query_def.get("_name")
            case_id = query_def.get("caseId")
            
            # --- getTask chain: supports getTask, getTask→logs, getTask→assignableUsers ---
            if action == "getTask":
                task_id = query_def.get("idOrName")
                logger.info(f"v1 query: getTask chain, task_id={task_id}, queries_count={len(queries)}")
                if not task_id:
                    return []
                    
                # Determine the second operation in the chain
                second_op = queries[1].get("_name") if len(queries) > 1 else None
                logger.info(f"v1 query: second_op={second_op}, tenant_id={auth.tenant_id}")
                
                conn = get_db_conn()
                try:
                    with conn.cursor() as cur:
                        if second_op == "logs":
                            # Return task logs
                            cur.execute(
                                "SELECT log_id, body, created_by, created_at FROM case_task_logs WHERE tenant_id = %s AND task_id = %s ORDER BY created_at DESC",
                                (auth.tenant_id, task_id)
                            )
                            return [{"_id": r[0], "id": r[0], "message": r[1], "createdBy": r[2], "createdAt": r[3], "startDate": r[3]} for r in cur.fetchall()]
                        elif second_op == "assignableUsers":
                            # Return list of users
                            cur.execute("SELECT DISTINCT user_id, display_name, login FROM users WHERE tenant_id = %s", (auth.tenant_id,))
                            rows = cur.fetchall()
                            if rows:
                                return [{"login": r[2] or r[0], "name": r[1] or r[0]} for r in rows]
                            return [{"login": auth.user_id, "name": auth.user_id}]
                        elif second_op == "actions":
                            # Return empty actions list (no Cortex actions by default)
                            return []
                        else:
                            # Return the task itself
                            cur.execute(
                                "SELECT task_id, case_id, title, status, created_by, assigned_to, created_at, updated_at, description, due_date, priority FROM case_tasks WHERE tenant_id = %s AND task_id = %s",
                                (auth.tenant_id, task_id)
                            )
                            r = cur.fetchone()
                            if not r:
                                return []
                            db_status = r[3].upper() if r[3] else ""
                            if db_status in ["OPEN", "WAITING"]:
                                status = "Waiting"
                            elif db_status in ["INPROGRESS", "IN PROGRESS"]:
                                status = "InProgress"
                            elif db_status == "COMPLETED":
                                status = "Completed"
                            else:
                                status = r[3] or "Waiting"
                            return [{
                                "_id": r[0], "id": r[0], "task_id": r[0], "case_id": r[1],
                                "title": r[2], "status": status,
                                "createdBy": r[4], "assignee": r[5], "owner": r[5],
                                "startDate": r[6], "created_at": r[6], "updated_at": r[7],
                                "description": r[8], "dueDate": r[9], "priority": r[10],
                                "group": "default", "flag": False,
                                "extraData": {"shareCount": 0, "actionRequired": False}
                            }]
                except Exception as e:
                    logger.error(f"getTask query failed: {e}")
                    return []
                finally:
                    conn.close()
            
            if action in ["countTask", "countCaseObservable", "countRelatedAlert"] and case_id:
                conn = get_db_conn()
                try:
                    with conn.cursor() as cur:
                        if action == "countTask":
                            # Exclude 'Cancel' status.
                            cur.execute("SELECT COUNT(*) FROM case_tasks WHERE tenant_id = %s AND case_id = %s AND status != 'Cancel'", (auth.tenant_id, case_id))
                            count = cur.fetchone()[0]
                            return [count]
                        elif action == "countCaseObservable":
                            # Note: OpenSearch is the primary artifact store. Let's do a fast distinct on pg artifacts for now.
                            cur.execute("SELECT COUNT(*) FROM artifacts WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, case_id))
                            count = cur.fetchone()[0]
                            return [count]
                        elif action == "countRelatedAlert":
                            cur.execute("SELECT COUNT(*) FROM case_alert_links WHERE tenant_id = %s AND case_id = %s", (auth.tenant_id, case_id))
                            count = cur.fetchone()[0]
                            return [count]
                except Exception as e:
                    logger.error(f"Metrics count failed: {e}")
                    return [0]
                finally:
                    conn.close()
            # Fallback: check _EMPTY_RESPONSES for legacy names
            for step in reversed(queries):
                name = step.get("_name", "")
                if name in _EMPTY_RESPONSES:
                    return _EMPTY_RESPONSES[name]

    return []


@app.patch("/api/v1/task/_bulk")
@app.patch("/v1/task/_bulk")
@app.patch("/api/task/_bulk")
@app.patch("/task/_bulk")
@app.patch("/api/v1/tasks/_bulk")
@app.patch("/v1/tasks/_bulk")
@app.patch("/api/tasks/_bulk")
@app.patch("/tasks/_bulk")
async def proxy_bulk_task_update(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    body = await request.json()
    logger.info(f"BULK TASK UPDATE: {body}")
    # Map legacy {ids: [...], status: '...'} to Engine bulk update
    engine_payload = {
        "ids": body.get("ids", []),
        "status": body.get("status"),
        "priority": body.get("priority")
    }
    
    if engine_payload["status"] == "Cancel":
        return _forward_request("POST", "/tasks/bulk-delete", request, auth, json_body={"ids": engine_payload["ids"]})
        
    return _forward_request("POST", "/tasks/bulk-update", request, auth, json_body=engine_payload)

@app.patch("/api/case/_bulk")
@app.patch("/case/_bulk")
@app.patch("/api/cases/_bulk")
@app.patch("/cases/_bulk")
async def proxy_bulk_case_task_update(request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    # This is often used for bulk task removal or status change from the case view
    body = await request.json()
    logger.info(f"BULK CASE TASK UPDATE: {body}")
    return await proxy_bulk_task_update(request, auth)


# ══════════════════════════════════════════════════════════════════════════════
# Incident State Machine APIs (Feature D)
# ══════════════════════════════════════════════════════════════════════════════

VALID_TRANSITIONS = {
    "VISIBLE":      ["ACKNOWLEDGED", "DISMISSED"],
    "ACKNOWLEDGED": ["RESOLVED", "DISMISSED"],
    "DISMISSED":    ["VISIBLE"],  # Re-activate
}

def _get_corr_pg():
    corr_host = os.getenv("CORRELATION_POSTGRES_HOST", os.getenv("POSTGRES_HOST", "postgres"))
    return psycopg2.connect(
        host=corr_host,
        database=os.getenv("POSTGRES_DB", "nv_vault"),
        user=os.getenv("POSTGRES_USER", "nv_user"),
        password=os.getenv("POSTGRES_PASSWORD", "nv_pass"),
        connect_timeout=5
    )

@app.get("/api/incidents")
async def list_incidents(
    state: Optional[str] = None,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    """List incidents with optional state filter."""
    try:
        conn = _get_corr_pg()
        with conn.cursor() as cur:
            if state:
                cur.execute(
                    "SELECT group_id, tenant_id, rule_name, alert_count, max_severity, "
                    "visibility_state, first_seen, last_seen, acknowledged_by, acknowledged_at, "
                    "dismissed_at, resolved_at, visible_at, sla_tta_ms, sla_ttr_ms "
                    "FROM correlation_groups WHERE visibility_state = %s ORDER BY last_seen DESC LIMIT 200",
                    (state.upper(),)
                )
            else:
                cur.execute(
                    "SELECT group_id, tenant_id, rule_name, alert_count, max_severity, "
                    "visibility_state, first_seen, last_seen, acknowledged_by, acknowledged_at, "
                    "dismissed_at, resolved_at, visible_at, sla_tta_ms, sla_ttr_ms "
                    "FROM correlation_groups ORDER BY last_seen DESC LIMIT 200"
                )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()
        return {"incidents": rows, "count": len(rows)}
    except Exception as e:
        raise HTTPException(500, f"Failed to list incidents: {e}")


def _transition_incident(group_id: str, target_state: str, user: str):
    """Perform validated state transition with SLA computation."""
    now_ms = int(time.time() * 1000)
    conn = _get_corr_pg()
    with conn.cursor() as cur:
        cur.execute("SELECT visibility_state, visible_at, acknowledged_at FROM correlation_groups WHERE group_id = %s", (group_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, f"Incident {group_id} not found")

        current_state, visible_at, ack_at = row
        allowed = VALID_TRANSITIONS.get(current_state, [])
        if target_state not in allowed:
            conn.close()
            raise HTTPException(400, f"Invalid transition: {current_state} → {target_state}")

        updates = {"visibility_state": target_state}
        sla_tta = None
        sla_ttr = None

        if target_state == "ACKNOWLEDGED":
            updates["acknowledged_by"] = user
            updates["acknowledged_at"] = now_ms
            # SLA: TTA = acknowledged_at - visible_at
            if visible_at:
                sla_tta = now_ms - visible_at
                updates["sla_tta_ms"] = sla_tta
        elif target_state == "DISMISSED":
            updates["dismissed_at"] = now_ms
        elif target_state == "RESOLVED":
            updates["resolved_at"] = now_ms
            # SLA: TTR = resolved_at - acknowledged_at
            if ack_at:
                sla_ttr = now_ms - ack_at
                updates["sla_ttr_ms"] = sla_ttr

        set_clause = ", ".join(f"{k} = %s" for k in updates.keys())
        cur.execute(
            f"UPDATE correlation_groups SET {set_clause} WHERE group_id = %s",
            list(updates.values()) + [group_id]
        )
    conn.commit()
    conn.close()
    return {
        "group_id": group_id, "state": target_state,
        "sla_tta_ms": sla_tta, "sla_ttr_ms": sla_ttr
    }


@app.patch("/api/incidents/{group_id}/acknowledge")
async def acknowledge_incident(group_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    return _transition_incident(group_id, "ACKNOWLEDGED", auth.user_id)

@app.patch("/api/incidents/{group_id}/dismiss")
async def dismiss_incident(group_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    return _transition_incident(group_id, "DISMISSED", auth.user_id)

@app.patch("/api/incidents/{group_id}/resolve")
async def resolve_incident(group_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    return _transition_incident(group_id, "RESOLVED", auth.user_id)


# ══════════════════════════════════════════════════════════════════════════════
# SLA Dashboard (TTA/TTR Metrics)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/sla/dashboard")
async def sla_dashboard(
    hours: int = 24,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    """SLA Dashboard — avg TTA, avg TTR, breach counts for a time window."""
    cutoff_ms = int(time.time() * 1000) - (hours * 3600 * 1000)
    try:
        conn = _get_corr_pg()
        with conn.cursor() as cur:
            # Average TTA (Time-to-Acknowledge)
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE sla_tta_ms IS NOT NULL) as tta_count,
                    AVG(sla_tta_ms) FILTER (WHERE sla_tta_ms IS NOT NULL) as avg_tta_ms,
                    MIN(sla_tta_ms) FILTER (WHERE sla_tta_ms IS NOT NULL) as min_tta_ms,
                    MAX(sla_tta_ms) FILTER (WHERE sla_tta_ms IS NOT NULL) as max_tta_ms,
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY sla_tta_ms) FILTER (WHERE sla_tta_ms IS NOT NULL) as p95_tta_ms,
                    COUNT(*) FILTER (WHERE sla_ttr_ms IS NOT NULL) as ttr_count,
                    AVG(sla_ttr_ms) FILTER (WHERE sla_ttr_ms IS NOT NULL) as avg_ttr_ms,
                    MIN(sla_ttr_ms) FILTER (WHERE sla_ttr_ms IS NOT NULL) as min_ttr_ms,
                    MAX(sla_ttr_ms) FILTER (WHERE sla_ttr_ms IS NOT NULL) as max_ttr_ms,
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY sla_ttr_ms) FILTER (WHERE sla_ttr_ms IS NOT NULL) as p95_ttr_ms,
                    COUNT(*) FILTER (WHERE visibility_state = 'VISIBLE' AND visible_at < %s) as tta_breaches,
                    COUNT(*) FILTER (WHERE visibility_state = 'ACKNOWLEDGED' AND acknowledged_at < %s) as ttr_breaches,
                    COUNT(*) as total_incidents
                FROM correlation_groups
                WHERE last_seen >= %s
            """, (cutoff_ms, cutoff_ms, cutoff_ms))
            row = cur.fetchone()
            cols = [d[0] for d in cur.description]
            result = dict(zip(cols, row)) if row else {}

            # State distribution
            cur.execute("""
                SELECT visibility_state, COUNT(*) as count
                FROM correlation_groups WHERE last_seen >= %s
                GROUP BY visibility_state
            """, (cutoff_ms,))
            state_dist = {r[0]: r[1] for r in cur.fetchall()}

        conn.close()

        # Convert ms to human-readable
        def ms_to_human(ms):
            if ms is None:
                return None
            secs = int(ms / 1000)
            if secs < 60:
                return f"{secs}s"
            elif secs < 3600:
                return f"{secs // 60}m {secs % 60}s"
            else:
                return f"{secs // 3600}h {(secs % 3600) // 60}m"

        return {
            "window_hours": hours,
            "tta": {
                "count": result.get("tta_count", 0),
                "avg_ms": round(result.get("avg_tta_ms") or 0),
                "avg_human": ms_to_human(result.get("avg_tta_ms")),
                "min_ms": result.get("min_tta_ms"),
                "max_ms": result.get("max_tta_ms"),
                "p95_ms": round(result.get("p95_tta_ms") or 0),
                "breaches": result.get("tta_breaches", 0),
            },
            "ttr": {
                "count": result.get("ttr_count", 0),
                "avg_ms": round(result.get("avg_ttr_ms") or 0),
                "avg_human": ms_to_human(result.get("avg_ttr_ms")),
                "min_ms": result.get("min_ttr_ms"),
                "max_ms": result.get("max_ttr_ms"),
                "p95_ms": round(result.get("p95_ttr_ms") or 0),
                "breaches": result.get("ttr_breaches", 0),
            },
            "state_distribution": state_dist,
            "total_incidents": result.get("total_incidents", 0),
        }
    except Exception as e:
        raise HTTPException(500, f"SLA dashboard failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# DLQ Query & Retry APIs (Fix C)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/dlq")
async def list_dlq_events(
    limit: int = 50,
    auth: AuthContext = Depends(require_permission(PERM_ALERT_READ))
):
    """List recent DLQ events from Kafka (alerts.ingest.dlq.v1)."""
    try:
        from kafka import KafkaConsumer as KC
        kafka_bs = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
        consumer = KC(
            "alerts.ingest.dlq.v1",
            bootstrap_servers=kafka_bs,
            group_id=f"nv-query-dlq-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="latest",
            enable_auto_commit=False,
            max_poll_records=limit,
            consumer_timeout_ms=3000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )
        events = []
        for msg in consumer:
            events.append(msg.value)
            if len(events) >= limit:
                break
        consumer.close()
        return {"dlq_events": events, "count": len(events)}
    except Exception as e:
        raise HTTPException(500, f"DLQ read failed: {e}")


@app.post("/api/dlq/{event_id}/retry")
async def retry_dlq_event(
    event_id: str,
    request: Request,
    auth: AuthContext = Depends(require_permission(PERM_ALERT_WRITE))
):
    """Re-publish a DLQ event back to the ingest topic for reprocessing."""
    try:
        body = await request.json()
        from kafka import KafkaProducer as KP
        kafka_bs = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
        producer = KP(
            bootstrap_servers=kafka_bs,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        retry_event = body.get("original_event", body)
        retry_event["retried_from_dlq"] = True
        retry_event["retry_timestamp"] = int(time.time() * 1000)
        producer.send("alerts.ingest.v1", retry_event)
        producer.flush()
        producer.close()
        return {"status": "retried", "event_id": event_id}
    except Exception as e:
        raise HTTPException(500, f"DLQ retry failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Workflow Management APIs (Feature B)
# ══════════════════════════════════════════════════════════════════════════════

class WorkflowCreateModel(BaseModel):
    workflow_id: str
    name: str
    description: str = ""
    tenant_id: str = "dev-tenant"
    trigger_config: Dict = {}
    steps: List[Dict] = []

def _get_wf_pg():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        database=os.getenv("POSTGRES_DB", "nv_vault"),
        user=os.getenv("POSTGRES_USER", "nv_user"),
        password=os.getenv("POSTGRES_PASSWORD", "nv_pass"),
        connect_timeout=5
    )

@app.post("/api/workflows")
async def create_workflow(wf: WorkflowCreateModel, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    try:
        conn = _get_wf_pg()
        now = int(time.time() * 1000)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO workflow_definitions (workflow_id, tenant_id, name, description, trigger_config, steps, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)
                ON CONFLICT (workflow_id) DO UPDATE SET
                    name = EXCLUDED.name, description = EXCLUDED.description,
                    trigger_config = EXCLUDED.trigger_config, steps = EXCLUDED.steps, updated_at = EXCLUDED.updated_at
            """, (wf.workflow_id, wf.tenant_id, wf.name, wf.description,
                  json.dumps(wf.trigger_config), json.dumps(wf.steps), now, now))
        conn.commit()
        conn.close()
        return {"status": "created", "workflow_id": wf.workflow_id}
    except Exception as e:
        raise HTTPException(500, f"Create workflow failed: {e}")


@app.get("/api/workflows")
async def list_workflows(auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    try:
        conn = _get_wf_pg()
        with conn.cursor() as cur:
            cur.execute("SELECT workflow_id, tenant_id, name, description, enabled, created_at FROM workflow_definitions ORDER BY created_at DESC")
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()
        return {"workflows": rows}
    except Exception as e:
        raise HTTPException(500, f"List workflows failed: {e}")


@app.get("/api/workflows/{workflow_id}")
async def get_workflow(workflow_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_READ))):
    try:
        conn = _get_wf_pg()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM workflow_definitions WHERE workflow_id = %s", (workflow_id,))
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, f"Workflow {workflow_id} not found")
        return dict(zip(cols, row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Get workflow failed: {e}")


@app.delete("/api/workflows/{workflow_id}")
async def delete_workflow(workflow_id: str, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    try:
        conn = _get_wf_pg()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM workflow_definitions WHERE workflow_id = %s RETURNING workflow_id", (workflow_id,))
            deleted = cur.fetchone()
        conn.commit()
        conn.close()
        if not deleted:
            raise HTTPException(404, f"Workflow {workflow_id} not found")
        return {"status": "deleted", "workflow_id": workflow_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Delete workflow failed: {e}")


@app.post("/api/workflows/{workflow_id}/trigger")
async def trigger_workflow(workflow_id: str, request: Request, auth: AuthContext = Depends(require_permission(PERM_CASE_WRITE))):
    """Manual trigger: publishes to workflow.trigger.v1 Kafka topic."""
    try:
        body = await request.json()
        from kafka import KafkaProducer as KP
        kafka_bs = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
        producer = KP(
            bootstrap_servers=kafka_bs,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        producer.send("workflow.trigger.v1", {
            "event_id": str(uuid.uuid4()),
            "tenant_id": body.get("tenant_id", "dev-tenant"),
            "payload": body.get("payload", {}),
            "workflow_id": workflow_id,
            "timestamp": int(time.time() * 1000),
        })
        producer.flush()
        producer.close()
        return {"status": "triggered", "workflow_id": workflow_id}
    except Exception as e:
        raise HTTPException(500, f"Trigger failed: {e}")


@app.get("/api/workflows/executions")
async def list_workflow_executions(
    workflow_id: Optional[str] = None,
    limit: int = 50,
    auth: AuthContext = Depends(require_permission(PERM_CASE_READ))
):
    try:
        conn = _get_wf_pg()
        with conn.cursor() as cur:
            if workflow_id:
                cur.execute(
                    "SELECT execution_id, workflow_id, tenant_id, status, total_steps, started_at, finished_at, error "
                    "FROM workflow_executions WHERE workflow_id = %s ORDER BY started_at DESC LIMIT %s",
                    (workflow_id, limit)
                )
            else:
                cur.execute(
                    "SELECT execution_id, workflow_id, tenant_id, status, total_steps, started_at, finished_at, error "
                    "FROM workflow_executions ORDER BY started_at DESC LIMIT %s",
                    (limit,)
                )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()
        return {"executions": rows, "count": len(rows)}
    except Exception as e:
        raise HTTPException(500, f"List executions failed: {e}")
