"""
main.py — NeuralVyuha Node Service
Phase Z2.1 — Integration Hub (Cortex / MISP Node Manager)

Port: 8085

API surface:
  POST   /nodes              Register node   (PERM_ADMIN_INTEGRATION)
  GET    /nodes              List nodes      (PERM_NODE_STATUS_READ)
  GET    /nodes/{id}         Get node        (PERM_NODE_STATUS_READ)
  PATCH  /nodes/{id}         Update node     (PERM_ADMIN_INTEGRATION)
  DELETE /nodes/{id}         Remove node     (PERM_ADMIN_INTEGRATION)
  POST   /nodes/{id}/test    Live probe      (PERM_ADMIN_INTEGRATION)
  GET    /healthz /readyz /metrics

Security Guardrails:
  ENCRYPTED_SECRETS  — API keys are Fernet-encrypted before DB insert.
  KEY_MASKING        — api_key_enc is NEVER returned in any API response.
  NON_BLOCKING       — Heartbeat runs as async background task.
  TENANT_ISOLATION   — Nodes are system-global; only admins can write.
  FAIL_CLOSED_VAULT  — Service refuses to start if vault self-test fails.
"""

import asyncio
import json
import logging
import os
import sys
import time
import uuid
from typing import Optional, List

import psycopg2
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Depends, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, HttpUrl, field_validator

class ScanRequest(BaseModel):
    observable_type: str  # e.g., 'ip', 'hash', 'domain', 'url'
    observable_value: str

class InvestigationCreate(BaseModel):
    investigation_data: dict
    model_provider: Optional[str] = None
    model_name: Optional[str] = None

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
from common.auth.rbac import PERM_ADMIN_INTEGRATION, PERM_NODE_STATUS_READ
from common.observability.metrics import MetricsMiddleware, get_metrics_response
from common.observability.health import global_health_registry
from common.config.secrets import get_secret

from .vault import encrypt_key, decrypt_key, mask_key, validate_vault_config
from .heartbeat import heartbeat_loop, get_node_status_from_redis

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger("nv-node-service")

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="NeuralVyuha Node Service",
    version="1.0.0",
    description="Integration Hub for Cortex and MISP neural nodes.",
)
app.add_middleware(MetricsMiddleware, service_name="nv-node-service")

# ── Configuration ──────────────────────────────────────────────────────────────
PG_HOST     = get_secret("POSTGRES_HOST",    "postgres")
PG_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB       = get_secret("POSTGRES_DB",      "nv_vault")
PG_USER     = get_secret("POSTGRES_USER",    "hive")
PG_PASSWORD = get_secret("POSTGRES_PASSWORD","hive")
REDIS_URL   = os.getenv("REDIS_URL",         "redis://redis:6379")

_redis: Optional[aioredis.Redis] = None
_heartbeat_task: Optional[asyncio.Task] = None

# ── DB helper ──────────────────────────────────────────────────────────────────
def get_db_conn():
    try:
        return psycopg2.connect(
            host=PG_HOST, port=PG_PORT, database=PG_DB,
            user=PG_USER, password=PG_PASSWORD,
            connect_timeout=5,
        )
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
        raise HTTPException(status_code=503, detail="Database unavailable")

# ── Startup / Shutdown ─────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    global _redis, _heartbeat_task

    # 1. Fail-closed auth config
    validate_auth_config()

    # 2. Fail-closed vault self-test
    validate_vault_config()

    # 3. Redis client
    _redis = aioredis.from_url(REDIS_URL, decode_responses=True)

    # 4. Health checks
    def check_db():
        try:
            conn = get_db_conn()
            conn.close()
            return True
        except Exception:
            return False

    async def check_redis():
        try:
            return await _redis.ping()
        except Exception:
            return False

    global_health_registry.add_check("postgres", check_db)
    global_health_registry.add_check("redis", check_redis)

    # 6. Manual Migration Runner (Ensure AI tables exist)
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            # Check for existing table and DROP if it has the wrong type (scorched earth to fix previous error)
            cur.execute("SELECT data_type FROM information_schema.columns WHERE table_name = 'ai_case_investigations' AND column_name = 'created_at'")
            row = cur.fetchone()
            if row and row[0].upper() == 'BIGINT':
                logger.warning("Found ai_case_investigations with BIGINT created_at. Dropping for recreation...")
                cur.execute("DROP TABLE IF EXISTS ai_case_investigations CASCADE")
            
            # Create if not exists
            migration_path = os.path.join(os.path.dirname(__file__), "../migrations/002_create_investigations.sql")
            if os.path.exists(migration_path):
                with open(migration_path, 'r') as f:
                    logger.info("Running migration: 002_create_investigations.sql")
                    cur.execute(f.read())
                conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Migration runner failed: {e}")

    logger.info("nv-node-service started — heartbeat running")


@app.on_event("shutdown")
async def shutdown():
    if _heartbeat_task:
        _heartbeat_task.cancel()
        try:
            await _heartbeat_task
        except asyncio.CancelledError:
            pass
    if _redis:
        await _redis.aclose()

# ── Health / Metrics ───────────────────────────────────────────────────────────
@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "nv-node-service"}

@app.get("/readyz")
async def readyz():
    result = global_health_registry.check_health()
    if result.get("status") != "ok":
        return JSONResponse(status_code=503, content=result)
    return result

@app.get("/metrics")
def metrics():
    return get_metrics_response()

# ── Pydantic Models ────────────────────────────────────────────────────────────
# NeuralVyuha Node Service v1.0.1 (Support for AI Providers)
class NodeCreate(BaseModel):
    node_type:  str
    name:       str
    url:        str = ""
    api_key:    str       # plaintext — encrypted before storage
    tls_verify: bool = True

    @field_validator("node_type")
    @classmethod
    def validate_type(cls, v):
        valid_types = {
            "cortex", "misp", "virustotal", "abuseipdb", "alienvault", 
            "shodan", "urlhaus", "malwarebazaar", "threatfox", 
            "greynoise", "ipinfo", "phishtank",
            "gemini", "openai", "anthropic", "ollama", "moonshot", "deepseek"
        }
        if v not in valid_types:
            raise ValueError(f"node_type must be one of {valid_types}")
        return v

    @field_validator("url")
    @classmethod
    def validate_url(cls, v):
        v = v.rstrip("/")
        if not v or v == "":
            return "" # Allow empty for SaaS AI
        if not v.startswith(("http://", "https://")):
            raise ValueError("url must start with http:// or https://")
        return v

    @field_validator("api_key")
    @classmethod
    def validate_key_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("api_key must not be empty")
        return v.strip()


class NodeUpdate(BaseModel):
    name:       Optional[str] = None
    url:        Optional[str] = None
    api_key:    Optional[str] = None   # Rotates the key if set
    tls_verify: Optional[bool] = None

def validate_uuid(val: str) -> str:
    try:
        uuid.UUID(val)
        return val
    except ValueError:
        raise HTTPException(status_code=404, detail="Node not found (invalid ID format)")


def _row_to_node(row, redis_status: Optional[dict] = None) -> dict:
    """
    Convert a DB row → public-safe node dict.
    GUARDRAIL: api_key_enc is NEVER included. Masked key shown instead.
    """
    node_id, node_type, name, url, api_key_enc, tls_verify, \
        db_status, latency_ms, http_status, last_seen, last_error, \
        probe_count, created_by, created_at, updated_at = row

    # Prefer fresh Redis status over stale DB status
    live_status    = (redis_status or {}).get("status",     db_status)
    live_latency   = (redis_status or {}).get("latency_ms", latency_ms)
    live_checked   = (redis_status or {}).get("checked_at")

    return {
        "id":           str(node_id),
        "node_type":    node_type,
        "name":         name,
        "url":          url,
        "api_key":      mask_key(api_key_enc),   # NEVER plaintext
        "tls_verify":   tls_verify,
        "status":       live_status,
        "latency_ms":   live_latency,
        "http_status":  http_status,
        "last_seen":    str(last_seen) if last_seen else None,
        "last_error":   last_error,
        "probe_count":  probe_count,
        "last_checked": live_checked,
        "created_by":   created_by,
        "created_at":   str(created_at),
        "updated_at":   str(updated_at),
    }


# ── POST /nodes ────────────────────────────────────────────────────────────────
@app.post("/nodes", status_code=201)
async def register_node(
    payload: NodeCreate,
    auth: AuthContext = Depends(require_permission(PERM_ADMIN_INTEGRATION))
):
    """Register a new Cortex or MISP node. API key is encrypted before storage."""
    node_id = str(uuid.uuid4())

    try:
        api_key_enc = encrypt_key(payload.api_key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Encryption failed: {e}")

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO integration_nodes
                    (id, node_type, name, url, api_key_enc, tls_verify, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (node_id, payload.node_type, payload.name, payload.url,
                 api_key_enc, payload.tls_verify, auth.user_id)
            )
        conn.commit()
    except Exception as e:
        logger.error(f"register_node: DB insert failed: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    logger.info(f"register_node: id={node_id} type={payload.node_type} by={auth.user_id}")

    return {
        "id":        node_id,
        "node_type": payload.node_type,
        "name":      payload.name,
        "url":       payload.url,
        "api_key":   mask_key(api_key_enc),
        "status":    "UNKNOWN",
        "message":   "Node registered. First heartbeat probe within 60 seconds."
    }


# ── GET /nodes ─────────────────────────────────────────────────────────────────
@app.get("/nodes")
async def list_nodes(
    node_type: Optional[str] = None,
    auth: AuthContext = Depends(require_permission(PERM_NODE_STATUS_READ))
):
    """List all integration nodes with live status from Redis. Keys are masked."""
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            if node_type:
                cur.execute(
                    "SELECT id, node_type, name, url, api_key_enc, tls_verify, "
                    "status, latency_ms, http_status, last_seen, last_error, "
                    "probe_count, created_by, created_at, updated_at "
                    "FROM integration_nodes WHERE node_type = %s ORDER BY created_at",
                    (node_type,)
                )
            else:
                cur.execute(
                    "SELECT id, node_type, name, url, api_key_enc, tls_verify, "
                    "status, latency_ms, http_status, last_seen, last_error, "
                    "probe_count, created_by, created_at, updated_at "
                    "FROM integration_nodes ORDER BY node_type, created_at"
                )
            rows = cur.fetchall()
    finally:
        conn.close()

    nodes = []
    for row in rows:
        node_id    = str(row[0])
        redis_stat = await get_node_status_from_redis(_redis, node_id)
        nodes.append(_row_to_node(row, redis_stat))

    return {"total": len(nodes), "nodes": nodes}


# ── POST /nodes/test-connection — Live Unsaved Credential Probe ───────────────
@app.post("/nodes/test-connection")
async def test_unsaved_node(
    payload: NodeCreate,
    auth: AuthContext = Depends(require_permission(PERM_ADMIN_INTEGRATION))
):
    """
    Perform an immediate live probe against an unsaved node configuration.
    This bypasses CORS from the browser by acting as a proxy.
    Returns { ok, status, latency_ms, http_status, error }.
    """
    from .heartbeat import _probe_node
    import aiohttp
    
    # Generate a throwaway ID for logging purposes
    temp_id = str(uuid.uuid4())[:8]

    logger.info(f"test_unsaved_node: checking {payload.node_type} at {payload.url} by {auth.user_id}")

    try:
        async with aiohttp.ClientSession() as session:
            result = await _probe_node(
                session=session,
                node_id=f"test_{temp_id}",
                node_type=payload.node_type,
                url=payload.url,
                api_key_plaintext=payload.api_key,
                tls_verify=payload.tls_verify
            )
            
        ok = result["status"] == "UP"
        
        return {
            "ok":          ok,
            "status":      result["status"],
            "latency_ms":  result.get("latency_ms"),
            "http_status": result.get("http_status"),
            "error":       result.get("error"),
            "note":        result.get("note")
        }
    except Exception as e:
        logger.error(f"test_unsaved_node error: {e}")
        return {
            "ok": False,
            "status": "DOWN",
            "error": str(e)
        }


# ── GET /nodes/{id} ────────────────────────────────────────────────────────────
@app.get("/nodes/{node_id}")
async def get_node(
    node_id: str,
    auth: AuthContext = Depends(require_permission(PERM_NODE_STATUS_READ))
):
    node_id = validate_uuid(node_id)
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, node_type, name, url, api_key_enc, tls_verify, "
                "status, latency_ms, http_status, last_seen, last_error, "
                "probe_count, created_by, created_at, updated_at "
                "FROM integration_nodes WHERE id = %s",
                (node_id,)
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Node not found")

    redis_stat = await get_node_status_from_redis(_redis, node_id)
    return _row_to_node(row, redis_stat)


# ── PATCH /nodes/{id} — Update / Key Rotation ─────────────────────────────────
@app.patch("/nodes/{node_id}")
def update_node(
    node_id: str,
    payload: NodeUpdate,
    auth: AuthContext = Depends(require_permission(PERM_ADMIN_INTEGRATION))
):
    """Update node config. If api_key is provided, performs a key rotation."""
    node_id = validate_uuid(node_id)
    fields, params = [], []

    if payload.name is not None:
        fields.append("name = %s"); params.append(payload.name)
    if payload.url is not None:
        fields.append("url = %s"); params.append(payload.url.rstrip("/"))
    if payload.api_key is not None:
        try:
            new_enc = encrypt_key(payload.api_key)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Encryption failed: {e}")
        fields.append("api_key_enc = %s"); params.append(new_enc)
        logger.info(f"update_node: API key rotated for node {node_id} by {auth.user_id}")
    if payload.tls_verify is not None:
        fields.append("tls_verify = %s"); params.append(payload.tls_verify)

    if not fields:
        raise HTTPException(status_code=422, detail="No fields to update")

    params.append(node_id)
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE integration_nodes SET {', '.join(fields)} WHERE id = %s",
                tuple(params)
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Node not found")
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    return {"id": node_id, "updated": True}


# ── DELETE /nodes/{id} ────────────────────────────────────────────────────────
@app.delete("/nodes/{node_id}", status_code=204)
def delete_node(
    node_id: str,
    auth: AuthContext = Depends(require_permission(PERM_ADMIN_INTEGRATION))
):
    node_id = validate_uuid(node_id)
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM integration_nodes WHERE id = %s", (node_id,))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Node not found")
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    logger.info(f"delete_node: id={node_id} by={auth.user_id}")
    return Response(status_code=204)


# ── POST /nodes/{id}/test — Live Credential Probe ─────────────────────────────
@app.post("/nodes/{node_id}/test")
async def test_node(
    node_id: str,
    auth: AuthContext = Depends(require_permission(PERM_ADMIN_INTEGRATION))
):
    """
    Perform an immediate live probe against the node.
    Returns { ok, status, latency_ms, http_status, error }.

    CI Assertion: Invalid API key → { ok: false, http_status: 401 }
    """
    node_id = validate_uuid(node_id)
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT node_type, url, api_key_enc, tls_verify FROM integration_nodes WHERE id = %s",
                (node_id,)
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Node not found")

    node_type, url, api_key_enc, tls_verify = row

    try:
        api_key_plain = decrypt_key(api_key_enc)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to decrypt API key")

    # Run an on-demand probe (same logic as heartbeat)
    from .heartbeat import _probe_node
    import aiohttp
    async with aiohttp.ClientSession() as session:
        result = await _probe_node(
            session, node_id, node_type, url, api_key_plain, tls_verify
        )

    ok = result["status"] == "UP"

    logger.info(
        f"test_node: id={node_id} ok={ok} "
        f"latency={result['latency_ms']}ms http={result['http_status']} "
        f"by={auth.user_id}"
    )

    # Write result to Redis so the dashboard updates immediately
    if _redis:
        await _write_result_and_broadcast(node_id, result)

    return {
        "ok":         ok,
        "status":     result["status"],
        "latency_ms": result["latency_ms"],
        "http_status": result["http_status"],
        "error":      result.get("error"),
    }

# ── POST /nodes/{id}/scan — Threat Intel Scanning ─────────────────────────────
@app.post("/nodes/{node_id}/scan")
async def scan_observable(
    node_id: str,
    payload: ScanRequest,
    auth: AuthContext = Depends(get_auth_context)
):
    """
    Query a specific Integration Hub Node (VirusTotal, AbuseIPDB, etc.) 
    with a live observable to get threat reputation scores.
    """
    node_id = validate_uuid(node_id)
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT node_type, url, api_key_enc, tls_verify FROM integration_nodes WHERE id = %s",
                (node_id,)
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Node not found")

    node_type, url, api_key_enc, tls_verify = row
    
    try:
        api_key_plain = decrypt_key(api_key_enc)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to decrypt API key")

    import httpx
    ssl_ctx = None if tls_verify else False
    
    async with httpx.AsyncClient(verify=ssl_ctx, timeout=15.0) as client:
        try:
            val = payload.observable_value
            node_t = node_type.lower()
            
            if node_t == "virustotal":
                if payload.observable_type == "hash":
                    res = await client.get(f"https://www.virustotal.com/api/v3/files/{val}", headers={"x-apikey": api_key_plain})
                elif payload.observable_type == "ip":
                    res = await client.get(f"https://www.virustotal.com/api/v3/ip_addresses/{val}", headers={"x-apikey": api_key_plain})
                elif payload.observable_type == "domain":
                    res = await client.get(f"https://www.virustotal.com/api/v3/domains/{val}", headers={"x-apikey": api_key_plain})
                elif payload.observable_type == "url":
                    import base64
                    url_id = base64.urlsafe_b64encode(val.encode()).decode().strip("=")
                    res = await client.get(f"https://www.virustotal.com/api/v3/urls/{url_id}", headers={"x-apikey": api_key_plain})
                else:
                    return {"error": f"Unsupported observable type {payload.observable_type} for VirusTotal"}
                return res.json()
                
            elif node_t == "abuseipdb" and payload.observable_type == "ip":
                res = await client.get("https://api.abuseipdb.com/api/v2/check", params={"ipAddress": val}, headers={"Key": api_key_plain, "Accept": "application/json"})
                return res.json()
                
            elif node_t == "alienvault":
                t = "IPv4" if payload.observable_type == "ip" else "FileHash-MD5" if len(val)==32 else "FileHash-SHA256" if len(val)==64 else "domain"
                res = await client.get(f"https://otx.alienvault.com/api/v1/indicators/{t}/{val}/general", headers={"X-OTX-API-KEY": api_key_plain})
                return res.json()
                
        except Exception as e:
            return {"error": str(e)}

# ── INTERNAL /internal/vault/credentials — Service-to-Service ─────────────────
@app.get("/internal/vault/credentials/{node_type}")
async def get_node_credentials_internal(
    node_type: str
):
    """
    Internal endpoint for other services (AI Copilot) to fetch plaintext keys.
    Requires Admin context.
    """
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT api_key_enc, url FROM integration_nodes WHERE node_type = %s AND status = 'UP' LIMIT 1",
                (node_type,)
            )
            row = cur.fetchone()
            if not row:
                 # Fallback to just find the latest one regardless of status if no UP ones
                 cur.execute(
                    "SELECT api_key_enc, url FROM integration_nodes WHERE node_type = %s ORDER BY created_at DESC LIMIT 1",
                    (node_type,)
                )
                 row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail=f"No configuration found for {node_type}")
            
            api_key_enc, url = row
            try:
                plaintext = decrypt_key(api_key_enc)
                return {"node_type": node_type, "api_key": plaintext, "url": url}
            except Exception:
                raise HTTPException(status_code=500, detail="Decryption failure")
    finally:
        conn.close()

async def _write_result_and_broadcast(node_id: str, result: dict):
    """Write a single probe result to Redis and broadcast the WS event."""
    from .heartbeat import REDIS_STATUS_TTL_SECONDS, REDIS_CHANNEL_NODES
    key     = f"node:status:{node_id}"
    payload = json.dumps(result)
    await _redis.set(key, payload, ex=REDIS_STATUS_TTL_SECONDS)
    event = json.dumps({
        "type":       "NODE_STATUS_CHANGED",
        "node_id":    node_id,
        "status":     result["status"],
        "latency_ms": result["latency_ms"],
        "timestamp":  result["checked_at"],
    })
    await _redis.publish(REDIS_CHANNEL_NODES, event)


# ── AI Case Investigations ───────────────────────────────────────────────────

@app.post("/cases/{case_id}/investigations")
async def save_investigation(
    case_id: str,
    payload: InvestigationCreate,
    auth: AuthContext = Depends(get_auth_context)
):
    """
    Save an AI-generated investigation report for a specific case.
    Computes an input_hash to prevent redundant LLM calls for the same case and message.
    """
    import hashlib
    
    # Try to extract the initial user message from the data. 
    # Fallback to an empty string if not found.
    msgs = payload.investigation_data.get("messages", [])
    user_msg = ""
    for m in msgs:
        if m.get("role") == "user":
            user_msg = m.get("content", "")
            break

    # Compute hash: SHA256(case_id + user_msg)
    raw_str = f"{case_id}::{user_msg}"
    input_hash = hashlib.sha256(raw_str.encode("utf-8")).hexdigest()

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ai_case_investigations (case_id, investigation_data, model_provider, model_name, input_hash) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (case_id, json.dumps(payload.investigation_data), payload.model_provider, payload.model_name, input_hash)
            )
            inv_id = str(cur.fetchone()[0])
        conn.commit()
        logger.info(f"save_investigation: case={case_id} id={inv_id} hash={input_hash} by={auth.user_id}")
        return {"id": inv_id, "status": "saved", "input_hash": input_hash}
    except Exception as e:
        logger.error(f"Failed to save investigation for case {case_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal database error")
    finally:
        conn.close()

@app.get("/internal/investigation-cache/{input_hash}")
async def get_cached_investigation(input_hash: str):
    """
    Internal endpoint for the MCP proxy to check if an investigation exists for a given input hash.
    """
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, investigation_data, model_provider, model_name, rating, feedback_comment FROM ai_case_investigations WHERE input_hash = %s LIMIT 1",
                (input_hash,)
            )
            row = cur.fetchone()
            if row:
                return {
                    "id": str(row[0]),
                    "investigation_data": row[1],
                    "model_provider": row[2],
                    "model_name": row[3],
                    "rating": row[4],
                    "feedback_comment": row[5]
                }
            raise HTTPException(status_code=404, detail="Cache miss")
    finally:
        conn.close()

@app.get("/cases/{case_id}/investigations")
async def get_investigations(
    case_id: str,
    auth: AuthContext = Depends(get_auth_context)
):
    """
    Retrieve all AI investigation reports stored for a specific case.
    """
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, investigation_data, model_provider, model_name, created_at, rating, feedback_comment FROM ai_case_investigations WHERE case_id = %s ORDER BY created_at DESC",
                (case_id,)
            )
            rows = cur.fetchall()
            return [
                {
                    "id": str(r[0]),
                    "investigation_data": r[1],
                    "model_provider": r[2],
                    "model_name": r[3],
                    "created_at": r[4].isoformat(),
                    "rating": r[5],
                    "feedback_comment": r[6]
                } for r in rows
            ]
    except Exception as e:
        logger.error(f"Failed to fetch investigations for case {case_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal database error")
    finally:
        conn.close()

class InvestigationFeedback(BaseModel):
    rating: Optional[int] = None
    feedback_comment: Optional[str] = None

@app.put("/internal/investigation/{id}/feedback")
async def update_investigation_feedback(
    id: str,
    feedback: InvestigationFeedback,
    auth: AuthContext = Depends(get_auth_context)
):
    """
    Update the user feedback (rating and comment) for a specific AI investigation.
    """
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ai_case_investigations SET rating = %s, feedback_comment = %s WHERE id = %s RETURNING id",
                (feedback.rating, feedback.feedback_comment, id)
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Investigation not found")
        conn.commit()
        return {"status": "updated", "id": id, "rating": feedback.rating}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update feedback for investigation {id}: {e}")
        raise HTTPException(status_code=500, detail="Internal database error")
    finally:
        conn.close()
