"""
heartbeat.py — NeuralVyuha Node Service Async Heartbeat Engine
Phase Z2.1

NON_BLOCKING_HEALTH guardrail:
  All probes are async (aiohttp) with a hard 10s timeout per node.
  A single slow MISP server can never block or lag other probes or the API.

Architecture:
  ┌─ asyncio event loop (in-process) ──────────────────────────────────┐
  │  heartbeat_loop()  →  every 60s                                    │
  │     ↓ gathers all probes concurrently (asyncio.gather)             │
  │     ↓ per-node: _probe_cortex() or _probe_misp()                   │
  │     ↓ writes result to Redis: node:status:{node_id}  (TTL=120s)    │
  │     ↓ publishes NODE_STATUS_CHANGED to redis channel               │
  └────────────────────────────────────────────────────────────────────┘

Redis Schema:
  Key:   node:status:{node_id}
  Value: JSON { status, latency_ms, http_status, checked_at, error }
  TTL:   120 seconds (circuit breaker: if service crashes, status expires)

WebSocket Event:
  { type: "NODE_STATUS_CHANGED", node_id, status, latency_ms, timestamp }
  Published to Redis channel: room:system:nodes
"""

import asyncio
import json
import logging
import time
from typing import Optional

import aiohttp

logger = logging.getLogger("nv-node-service.heartbeat")

# ── Constants ──────────────────────────────────────────────────────────────────
HEARTBEAT_INTERVAL_SECONDS = 60
PROBE_TIMEOUT_SECONDS      = 10
REDIS_STATUS_TTL_SECONDS   = 120   # Circuit breaker TTL
REDIS_CHANNEL_NODES        = "room:system:nodes"

# Node type → probe path
PROBE_PATHS = {
    "cortex":        "/api/status",
    "misp":          "/servers/getPyMISPVersion.json",
    "virustotal":    "/api/v3/users/current",
    "abuseipdb":     "/api/v2/check?ipAddress=8.8.8.8",
    "alienvault":    "/api/v1/user/me",
    "shodan":        "/api-info",
    "urlhaus":       "/v1/payloads/recent/",
    "malwarebazaar": "/api/v1/",
    "threatfox":     "/api/v1/",
    "greynoise":     "/v2/meta/ping",
    "ipinfo":        "/8.8.8.8",
    "phishtank":     "/",
    "gemini":        "/v1beta/models",
    "openai":        "/v1/models",
    "anthropic":     "/v1/models",
    "moonshot":      "/v1/models",
    "deepseek":      "/v1/models"
}

# Status thresholds
LATENCY_DEGRADED_MS = 2000   # > 2s = DEGRADED
LATENCY_DOWN_MS     = 10000  # >= 10s (timeout) = DOWN


# ── Probe result dataclass ─────────────────────────────────────────────────────
def _build_result(
    status: str,
    latency_ms: Optional[int] = None,
    http_status: Optional[int] = None,
    error: Optional[str] = None,
) -> dict:
    return {
        "status":      status,
        "latency_ms":  latency_ms,
        "http_status": http_status,
        "checked_at":  int(time.time() * 1000),
        "error":       error[:256] if error else None,  # Truncate long errors
    }


# ── Per-node probe logic ───────────────────────────────────────────────────────
async def _probe_node(
    session: aiohttp.ClientSession,
    node_id: str,
    node_type: str,
    url: str,
    api_key_plaintext: str,
    tls_verify: bool,
) -> dict:
    """
    Probe a node using authentic API verification schemas.
    Returns a result dict: { status, latency_ms, http_status, checked_at, error }
    """
    probe_path = PROBE_PATHS.get(node_type, "/")
    base = url.rstrip("/")
    
    # Avoid duplicate /v1/ or /api/v1/ if already in base URL
    if probe_path.startswith("/v1") and "/v1" in base:
        p_path = probe_path.replace("/v1", "", 1)
        probe_url = f"{base}{p_path}"
    elif probe_path.startswith("/api/v1") and "/api/v1" in base:
        p_path = probe_path.replace("/api/v1", "", 1)
        probe_url = f"{base}{p_path}"
    else:
        probe_url  = f"{base}{probe_path}"
    
    headers = {}
    params = {}
    data = None
    req_method = "GET"

    # Vendor-specific API logic
    if node_type == "cortex":
        headers = {"Authorization": f"Bearer {api_key_plaintext}"} if api_key_plaintext.startswith("eyJ") else {"Authorization": api_key_plaintext}
    elif node_type == "misp":
        headers = {"Authorization": api_key_plaintext, "Accept": "application/json"}
    elif node_type == "virustotal":
        headers = {"x-apikey": api_key_plaintext}
    elif node_type == "abuseipdb":
        headers = {"Key": api_key_plaintext, "Accept": "application/json"}
    elif node_type == "alienvault":
        headers = {"X-OTX-API-KEY": api_key_plaintext}
    elif node_type == "shodan":
        params = {"key": api_key_plaintext}
    elif node_type == "greynoise":
        headers = {"key": api_key_plaintext}
    elif node_type == "ipinfo":
        params = {"token": api_key_plaintext}
    elif node_type == "malwarebazaar":
        headers = {"API-KEY": api_key_plaintext}
        req_method = "POST"
        data = {"query": "get_info"}
    elif node_type == "threatfox":
        headers = {"API-KEY": api_key_plaintext, "Content-Type": "application/json"}
        req_method = "POST"
        data = '{"query": "get_iocs", "days": 1}'
    elif node_type == "gemini":
        if not base: base = "https://generativelanguage.googleapis.com"
        probe_url = f"{base.rstrip('/')}{probe_path}"
        params = {"key": api_key_plaintext}
    elif node_type == "openai":
        if not base: base = "https://api.openai.com"
        probe_url = f"{base.rstrip('/')}{probe_path}"
        headers = {"Authorization": f"Bearer {api_key_plaintext}"}
    elif node_type == "anthropic":
        if not base: base = "https://api.anthropic.com"
        probe_url = f"{base.rstrip('/')}{probe_path}"
        headers = {
            "x-api-key": api_key_plaintext,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json"
        }
    elif node_type == "ollama":
        if not base: base = "http://host.docker.internal:11434"
        probe_url = f"{base.rstrip('/')}/api/tags"
    elif node_type == "moonshot":
        if not base: base = "https://api.moonshot.cn"
        probe_url = f"{base.rstrip('/')}{probe_path}"
        headers = {"Authorization": f"Bearer {api_key_plaintext}"}
    elif node_type == "deepseek":
        if not base: base = "https://api.deepseek.com"
        probe_url = f"{base.rstrip('/')}{probe_path}"
        headers = {"Authorization": f"Bearer {api_key_plaintext}"}

    t_start = time.monotonic()

    try:
        ssl_ctx = None if tls_verify else False
        kwargs = {
            "headers": headers,
            "ssl": ssl_ctx,
            "timeout": aiohttp.ClientTimeout(total=PROBE_TIMEOUT_SECONDS),
            "allow_redirects": True
        }
        if params: kwargs["params"] = params
        if data: kwargs["data"] = data

        async with session.request(req_method, probe_url, **kwargs) as resp:
            latency_ms  = int((time.monotonic() - t_start) * 1000)
            http_status = resp.status

            if node_type == "misp" and http_status == 200:
                try:
                    body = await resp.json(content_type=None)
                    if "version" not in body:
                        return _build_result("DEGRADED", latency_ms, http_status, "MISP response missing 'version' key")
                except Exception:
                    return _build_result("DEGRADED", latency_ms, http_status, "MISP response not valid JSON")

            if http_status in (401, 403):
                msg = "Authentication failed (401)" if http_status == 401 else "Authorisation denied (403)"
                return _build_result("DOWN", latency_ms, http_status, msg)
            if http_status >= 500:
                return _build_result("DEGRADED", latency_ms, http_status, f"Server error ({http_status})")
            if http_status != 200:
                return _build_result("DEGRADED", latency_ms, http_status, f"Unexpected status {http_status}")

            if latency_ms >= LATENCY_DEGRADED_MS:
                return _build_result("DEGRADED", latency_ms, http_status, f"High latency: {latency_ms}ms")

            return _build_result("UP", latency_ms, http_status)

    except asyncio.TimeoutError:
        elapsed_ms = int((time.monotonic() - t_start) * 1000)
        return _build_result("DOWN", elapsed_ms, None,
                             f"Probe timed out after {PROBE_TIMEOUT_SECONDS}s")

    except OSError as e:
        # Catches: aiohttp.ClientConnectorError (subclass of OSError),
        # ConnectionRefusedError, ConnectionResetError, etc.
        elapsed_ms = int((time.monotonic() - t_start) * 1000)
        return _build_result("DOWN", elapsed_ms, None, f"Connection error: {e}")

    except Exception as e:
        elapsed_ms = int((time.monotonic() - t_start) * 1000)
        logger.warning(f"heartbeat: unexpected probe error for node {node_id}: {e}")
        return _build_result("DOWN", elapsed_ms, None, str(e))


# ── Redis write helper ─────────────────────────────────────────────────────────
async def _write_result_to_redis(redis_client, node_id: str, result: dict) -> None:
    """
    Write probe result to Redis with TTL=120s (circuit breaker).
    Publish NODE_STATUS_CHANGED to the system nodes channel.
    """
    key     = f"node:status:{node_id}"
    payload = json.dumps(result)

    await redis_client.set(key, payload, ex=REDIS_STATUS_TTL_SECONDS)

    # Publish to WebSocket fan-out channel
    event = json.dumps({
        "type":       "NODE_STATUS_CHANGED",
        "node_id":    node_id,
        "status":     result["status"],
        "latency_ms": result["latency_ms"],
        "timestamp":  result["checked_at"],
    })
    await redis_client.publish(REDIS_CHANNEL_NODES, event)


# ── DB fetch: get all active nodes for probing ─────────────────────────────────
def _fetch_active_nodes(db_conn, vault_decrypt_fn) -> list:
    """
    Returns a list of node dicts with decrypted API keys.
    Decryption happens here, in-process — plaintext never stored.
    """
    nodes = []
    try:
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT id, node_type, name, url, api_key_enc, tls_verify "
                "FROM integration_nodes ORDER BY created_at"
            )
            for row in cur.fetchall():
                node_id, node_type, name, url, api_key_enc, tls_verify = row
                try:
                    api_key_plain = vault_decrypt_fn(api_key_enc)
                except Exception as e:
                    logger.warning(f"heartbeat: failed to decrypt key for node {node_id}: {e}")
                    api_key_plain = ""
                nodes.append({
                    "id":         str(node_id),
                    "node_type":  node_type,
                    "name":       name,
                    "url":        url,
                    "api_key":    api_key_plain,
                    "tls_verify": tls_verify,
                })
    except Exception as e:
        logger.error(f"heartbeat: failed to fetch nodes from DB: {e}")
    return nodes


# ── Update DB with latest probe result ────────────────────────────────────────
def _update_node_status_db(db_conn, node_id: str, result: dict) -> None:
    try:
        with db_conn.cursor() as cur:
            cur.execute(
                """
                UPDATE integration_nodes
                SET status      = %s,
                    latency_ms  = %s,
                    http_status = %s,
                    last_seen   = NOW(),
                    last_error  = %s,
                    probe_count = probe_count + 1
                WHERE id = %s
                """,
                (
                    result["status"],
                    result["latency_ms"],
                    result["http_status"],
                    result.get("error"),
                    node_id,
                )
            )
        db_conn.commit()
    except Exception as e:
        logger.error(f"heartbeat: DB update failed for node {node_id}: {e}")
        try:
            db_conn.rollback()
        except Exception:
            pass


# ── Main heartbeat coroutine ───────────────────────────────────────────────────
async def heartbeat_loop(get_db_conn_fn, redis_client, vault_decrypt_fn):
    """
    Main async heartbeat loop.
    Runs every HEARTBEAT_INTERVAL_SECONDS (60s).

    NON_BLOCKING_HEALTH: All node probes are gathered concurrently.
    A single slow node never blocks others.

    Args:
        get_db_conn_fn:   Callable → psycopg2 connection (sync).
        redis_client:     Async Redis client (redis.asyncio).
        vault_decrypt_fn: Callable to decrypt API key ciphertext.
    """
    logger.info("heartbeat: loop started — interval=%ds timeout=%ds",
                HEARTBEAT_INTERVAL_SECONDS, PROBE_TIMEOUT_SECONDS)

    connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)

    while True:
        cycle_start = time.monotonic()
        logger.info("heartbeat: ── cycle start ──")

        conn = None
        try:
            conn = get_db_conn_fn()
            nodes = _fetch_active_nodes(conn, vault_decrypt_fn)
            logger.info(f"heartbeat: probing {len(nodes)} node(s)")

            if nodes:
                async with aiohttp.ClientSession(connector=connector,
                                                  connector_owner=False) as session:
                    tasks = [
                        _probe_node(
                            session,
                            n["id"], n["node_type"], n["url"],
                            n["api_key"], n["tls_verify"]
                        )
                        for n in nodes
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                # Write results back
                for node, result in zip(nodes, results):
                    if isinstance(result, Exception):
                        result = _build_result("DOWN", None, None, str(result))

                    logger.info(
                        f"heartbeat: node={node['name']} type={node['node_type']} "
                        f"status={result['status']} latency={result['latency_ms']}ms"
                    )

                    # Non-blocking: run DB update sync in executor
                    await asyncio.get_event_loop().run_in_executor(
                        None, _update_node_status_db, conn, node["id"], result
                    )
                    await _write_result_to_redis(redis_client, node["id"], result)

        except Exception as e:
            logger.error(f"heartbeat: cycle error: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        cycle_ms = int((time.monotonic() - cycle_start) * 1000)
        logger.info(f"heartbeat: ── cycle done in {cycle_ms}ms ──")

        # Sleep for the remainder of the interval
        sleep_secs = max(1, HEARTBEAT_INTERVAL_SECONDS - (cycle_ms / 1000))
        await asyncio.sleep(sleep_secs)


async def get_node_status_from_redis(redis_client, node_id: str) -> Optional[dict]:
    """
    Instant cache read for a single node's latest status.
    Returns None if the TTL has expired (circuit breaker triggered).
    """
    try:
        raw = await redis_client.get(f"node:status:{node_id}")
        if raw:
            return json.loads(raw)
    except Exception as e:
        logger.warning(f"heartbeat: Redis read failed for {node_id}: {e}")
    return None
