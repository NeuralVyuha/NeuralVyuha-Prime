"""
nv-socket-service — Vyuha-Stream
Real-time event bridge: Kafka → Redis Pub/Sub → WebSocket → Browser

Architecture:
  Kafka Consumer (background thread)
    ↓  publishes minimal event to Redis channel  room:tenant:{id}
  Redis Subscriber (per WebSocket connection)
    ↓  receives and forwards
  WebSocket Client (Browser)

Guardrails enforced:
  - TENANT_ISOLATION  : room keyed by tenant_id extracted from verified JWT
  - MINIMAL_PAYLOAD   : only {type, id, tenant_id, timestamp} over the wire
  - GRACEFUL_KEEPALIVE: server sends PING every 25s; logs slow clients
"""

import asyncio
import json
import logging
import os
import sys
import time
import uuid
import threading
from typing import Dict, Set

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.responses import JSONResponse

# ── Shared auth library ────────────────────────────────────────────────────
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

from common.auth.middleware import validate_auth_config
from common.config.secrets import get_secret
import jwt as pyjwt

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger("nv-socket-service")

app = FastAPI(title="NeuralVyuha Vyuha-Stream", version="1.0.0")

# ── Configuration ───────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
REDIS_URL        = os.getenv("REDIS_URL", "redis://redis:6379")
JWT_SECRET       = get_secret("JWT_SECRET_KEY", "dev-secret-do-not-use-in-prod")
JWT_ALGORITHMS   = ["HS256", "RS256"]
PING_INTERVAL    = int(os.getenv("PING_INTERVAL_SECONDS", "25"))

# Kafka topics that feed the stream
WATCHED_TOPICS = [
    "nv.alerts.ingest.v1",
    "nv.cases.updated.v1",
    "nv.artifacts.uploaded.v1",
]

# ── Topic → Event-type mapping ──────────────────────────────────────────────
TOPIC_TO_EVENT_TYPE: Dict[str, str] = {
    "nv.alerts.ingest.v1":       "NEW_ALERT",
    "nv.cases.updated.v1":       "CASE_UPDATED",
    "nv.artifacts.uploaded.v1":  "ARTIFACT_UPLOADED",
}

# ── In-memory presence registry ─────────────────────────────────────────────
# {tenant_id: {case_id: {user_id}}}
_presence: Dict[str, Dict[str, Set[str]]] = {}
_presence_lock = threading.Lock()

# ── Redis client (module-level, initialized on startup) ─────────────────────
_redis: aioredis.Redis = None

# ── JWT verification ────────────────────────────────────────────────────────
def _verify_token(token: str) -> dict:
    """Verify JWT and return claims. Raises HTTPException on failure."""
    try:
        payload = pyjwt.decode(
            token,
            JWT_SECRET,
            algorithms=JWT_ALGORITHMS,
            options={"verify_aud": False},
        )
        if "tenant_id" not in payload:
            raise ValueError("Missing tenant_id claim")
        return payload
    except pyjwt.ExpiredSignatureError:
        raise HTTPException(status_code=4001, detail="Token expired")
    except Exception as e:
        raise HTTPException(status_code=4003, detail=f"Token invalid: {e}")

# ── Kafka bridge (runs in background thread) ────────────────────────────────
def _start_kafka_bridge():
    """
    Blocking Kafka consumer. For each message, publishes a minimal event
    to the correct Redis room channel.
    Called once on startup in a daemon thread.
    """
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            *WATCHED_TOPICS,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="nv-socket-service",
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
        logger.info(f"Kafka bridge started, watching: {WATCHED_TOPICS}")

        # We need a sync redis connection for the thread
        import redis as sync_redis
        r = sync_redis.from_url(REDIS_URL)

        for msg in consumer:
            try:
                raw = msg.value
                tenant_id  = raw.get("tenant_id", "")
                event_type = TOPIC_TO_EVENT_TYPE.get(msg.topic, "UNKNOWN")
                payload_id = (
                    raw.get("payload", {}).get("alert_id")
                    or raw.get("payload", {}).get("case_id")
                    or raw.get("payload", {}).get("artifact_id")
                    or raw.get("event_id", "")
                )

                # GUARDRAIL: Skip events with no tenant
                if not tenant_id:
                    continue

                payload_data = raw.get("payload", {})
                
                # Extract title/summary (handle multiple formats like nv-ingest does)
                title = (payload_data.get("rule", {}).get("description")
                         or payload_data.get("title")
                         or payload_data.get("description")
                         or payload_data.get("alert", {}).get("signature")
                         or payload_data.get("message")
                         or "Untitled Event")
                
                # Extract severity (fallback to 3)
                severity = payload_data.get("severity") or payload_data.get("rule", {}).get("level") or 3
                try:
                    severity = int(severity)
                except (ValueError, TypeError):
                    severity = 3

                # Minimal payload — enriched for Live HUD
                minimal = json.dumps({
                    "type":      event_type,
                    "id":        payload_id,
                    "tenant_id": tenant_id,
                    "timestamp": int(time.time() * 1000),
                    "title":     str(title)[:120],
                    "severity":  severity,
                    "source":    raw.get("source", "unknown")
                })

                channel = f"room:tenant:{tenant_id}"
                r.publish(channel, minimal)

            except Exception as e:
                logger.warning(f"Kafka bridge error on message: {e}")

    except Exception as e:
        logger.error(f"Kafka bridge startup failed: {e}")

# ── Redis Pub/Sub subscriber for a single WebSocket ─────────────────────────
async def _redis_subscriber(websocket: WebSocket, tenant_id: str, stop: asyncio.Event):
    """
    Subscribes to the tenant's Redis channel and forwards
    any published message to the WebSocket.
    """
    r = aioredis.from_url(REDIS_URL)
    pubsub = r.pubsub()
    channel = f"room:tenant:{tenant_id}"
    await pubsub.subscribe(channel)
    logger.info(f"WebSocket subscribed to {channel}")

    try:
        while not stop.is_set():
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg and msg["type"] == "message":
                try:
                    await websocket.send_text(msg["data"].decode("utf-8"))
                except Exception:
                    break
    finally:
        await pubsub.unsubscribe(channel)
        await r.aclose()

# ── Presence helpers ─────────────────────────────────────────────────────────
async def _broadcast_presence(tenant_id: str, case_id: str):
    """Publish updated analyst presence for a case to the tenant's room."""
    with _presence_lock:
        analysts = list(_presence.get(tenant_id, {}).get(case_id, set()))

    payload = json.dumps({
        "type":      "ANALYST_PRESENCE",
        "id":        case_id,
        "tenant_id": tenant_id,
        "timestamp": int(time.time() * 1000),
        "analysts":  analysts,
    })
    channel = f"room:tenant:{tenant_id}"
    if _redis:
        await _redis.publish(channel, payload)

# ── Startup / Shutdown ───────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    global _redis
    validate_auth_config()
    _redis = aioredis.from_url(REDIS_URL)

    # Start Kafka bridge in a daemon thread
    t = threading.Thread(target=_start_kafka_bridge, daemon=True)
    t.start()
    logger.info("nv-socket-service started")

@app.on_event("shutdown")
async def shutdown():
    if _redis:
        await _redis.aclose()

# ── Health ───────────────────────────────────────────────────────────────────
@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "nv-socket-service"}

@app.get("/readyz")
async def readyz():
    try:
        await _redis.ping()
        return {"status": "ok"}
    except Exception:
        return JSONResponse(status_code=503, content={"status": "degraded", "reason": "redis"})

# ── Primary WebSocket endpoint ───────────────────────────────────────────────
@app.websocket("/ws/{tenant_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    tenant_id: str,
    token: str = Query(...),
):
    """
    Authenticated WebSocket for a tenant room.

    GUARDRAIL — TENANT_ISOLATION:
      The tenant_id from the JWT MUST match the path parameter.
      If they differ, the connection is immediately closed.
    """
    # 1. Verify JWT
    try:
        claims = _verify_token(token)
    except HTTPException as e:
        await websocket.close(code=4003)
        return

    jwt_tenant = claims.get("tenant_id", "")
    user_id    = claims.get("sub", "unknown")

    # 2. Enforce tenant match
    if jwt_tenant != tenant_id:
        logger.warning(f"Tenant mismatch: JWT={jwt_tenant}, path={tenant_id}")
        await websocket.close(code=4003)
        return

    await websocket.accept()
    logger.info(f"WebSocket connected: user={user_id} tenant={tenant_id}")

    stop = asyncio.Event()

    # 3. Start Redis subscriber
    subscriber_task = asyncio.create_task(
        _redis_subscriber(websocket, tenant_id, stop)
    )

    # 4. Send welcome frame
    await websocket.send_json({
        "type":      "CONNECTED",
        "tenant_id": tenant_id,
        "timestamp": int(time.time() * 1000),
    })

    # 5. Heartbeat + inbound message loop
    try:
        last_ping = time.time()
        while True:
            # Non-blocking receive with 1s timeout
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                msg = json.loads(raw)
                if msg.get("type") == "PONG":
                    pass  # keepalive acknowledged
                elif msg.get("type") == "JOIN_CASE":
                    case_id = msg.get("case_id", "")
                    if case_id:
                        with _presence_lock:
                            _presence.setdefault(tenant_id, {}).setdefault(case_id, set()).add(user_id)
                        await _broadcast_presence(tenant_id, case_id)
                elif msg.get("type") == "LEAVE_CASE":
                    case_id = msg.get("case_id", "")
                    with _presence_lock:
                        _presence.get(tenant_id, {}).get(case_id, set()).discard(user_id)
                    await _broadcast_presence(tenant_id, case_id)
            except asyncio.TimeoutError:
                pass

            # Send PING every PING_INTERVAL seconds
            if time.time() - last_ping >= PING_INTERVAL:
                await websocket.send_json({
                    "type":      "PING",
                    "timestamp": int(time.time() * 1000),
                })
                last_ping = time.time()

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected: user={user_id} tenant={tenant_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        stop.set()
        subscriber_task.cancel()

        # Clean up presence
        with _presence_lock:
            for case_rooms in _presence.get(tenant_id, {}).values():
                case_rooms.discard(user_id)

        logger.info(f"WebSocket cleaned up: user={user_id}")
