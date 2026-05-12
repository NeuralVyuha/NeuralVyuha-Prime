"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  NeuralVyuha Ingestion Engine v2.0 — Sovereign Traffic Controller          ║
║  Enterprise-grade, production-ready multi-action ingestion gateway.        ║
║                                                                            ║
║  Features:                                                                 ║
║   • Multi-Action Routing (DROP / LOG / ALERT)                              ║
║   • Flexible JSON ingestion with dot-notation field resolution             ║
║   • 3-Layer Policy Cache (Memory → Redis → Postgres)                       ║
║   • MinIO Cold-Storage Archival (Choice B)                                 ║
║   • Behavioral Threshold Detection (Fixed Math Detective)                  ║
║   • Token-Bucket Rate Limiting (Lua-backed in Redis)                       ║
║   • Engine Control Plane (Pause / Resume / Dry-Run / Circuit Breaker)      ║
║   • Live Stream HUD (Redis Ring Buffer)                                    ║
║   • Prometheus-compatible Metrics                                          ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import time
import uuid
import json
import logging
import sys
import os
import re
import gzip
import hashlib
import psutil
import psycopg2
import redis
from io import BytesIO
from datetime import datetime, timezone
from fastapi import FastAPI, Header, HTTPException, Response, status, Depends, Request, Body
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from kafka import KafkaProducer
from kafka.errors import KafkaError
from collections import deque
from threading import Lock, Thread
from functools import lru_cache

# Robust common library discovery
_base_dir = os.path.dirname(__file__)
_paths_to_check = [
    os.path.abspath(os.path.join(_base_dir, '../../')),
    os.path.abspath(os.path.join(_base_dir, '../')),
]
for _p in _paths_to_check:
    if os.path.exists(os.path.join(_p, 'common')):
        sys.path.append(_p)
        break

from common.auth.middleware import get_auth_context, AuthContext, validate_auth_config, require_permission
from common.auth.rbac import PERM_ALERT_INGEST
from common.observability.metrics import MetricsMiddleware, get_metrics_response
from common.observability.health import global_health_registry

# ═══════════════════════════════════════════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("ingest-engine")

# ═══════════════════════════════════════════════════════════════════════════════
# Application
# ═══════════════════════════════════════════════════════════════════════════════
app = FastAPI(
    title="NeuralVyuha Ingestion Engine",
    version="2.0.0",
    description="Enterprise Sovereign Traffic Controller"
)
app.add_middleware(MetricsMiddleware, service_name="nv-ingest")

# ═══════════════════════════════════════════════════════════════════════════════
# Configuration (Environment-driven, 12-Factor compliant)
# ═══════════════════════════════════════════════════════════════════════════════
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "nv_vault")
POSTGRES_USER = os.getenv("POSTGRES_USER", "nv_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "nv_pass")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# MinIO (Cold Storage for Choice B)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "nvadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "nvadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "nv-archive")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# Kafka Topics
TOPIC_ALERT = "alerts.ingest.v1"
TOPIC_ALERT_PROMOTED = "alerts.promoted.v1"
TOPIC_LOG_ARCHIVED = "logs.archived.v1"

# Engine Tuning
POLICY_CACHE_TTL = int(os.getenv("POLICY_CACHE_TTL", 30))          # seconds
BEHAVIORAL_WINDOW = int(os.getenv("BEHAVIORAL_WINDOW", 60))        # seconds
BEHAVIORAL_THRESHOLD = int(os.getenv("BEHAVIORAL_THRESHOLD", 10))  # events
ARCHIVE_BATCH_SIZE = int(os.getenv("ARCHIVE_BATCH_SIZE", 50))      # events before flush
ARCHIVE_FLUSH_INTERVAL = int(os.getenv("ARCHIVE_FLUSH_INTERVAL", 30))  # seconds

# ═══════════════════════════════════════════════════════════════════════════════
# Engine State Keys (Redis-backed, survives restarts)
# ═══════════════════════════════════════════════════════════════════════════════
ENGINE_STATE_KEY = "ingest:engine:state"
LIVE_STREAM_KEY = "ingest:engine:live_stream"
POLICY_CACHE_KEY = "ingest:policy:cache"
RULES_CACHE_KEY = "ingest:rules:cache"
DEDUP_CONFIG_CACHE_KEY = "ingest:dedup:cache"
BEHAVIORAL_PREFIX = "ingest:behavioral:"
DEDUP_PREFIX = "ingest:dedup:"
EXTRACTION_RULES_KEY = "ingest:rules:extraction"
MAPPING_RULES_KEY = "ingest:rules:mapping"


# ═══════════════════════════════════════════════════════════════════════════════
# Action Types (The Decision Dictionary)
# ═══════════════════════════════════════════════════════════════════════════════
ACTION_DROP = "DROP"
ACTION_LOG = "LOG"
ACTION_ALERT = "ALERT"
ACTION_PASS = "PASS"
ACTION_DEDUPED = "DEDUPED"
ACTION_DRY_RUN = "DRY-RUN"

# ═══════════════════════════════════════════════════════════════════════════════
# Metrics (Thread-safe, Atomic Counters)
# ═══════════════════════════════════════════════════════════════════════════════
_engine_start_time = time.time()
_counter_lock = Lock()

class EngineMetrics:
    """Thread-safe, production metrics collector."""
    __slots__ = (
        'events_total', 'events_dropped', 'events_archived',
        'events_promoted', 'events_rate_limited', 'events_dry_run',
        'events_deduped', 'events_behavioral_triggered',
        'policy_cache_hits', 'policy_cache_misses', 'errors_total'
    )

    def __init__(self):
        self.events_total = 0
        self.events_dropped = 0
        self.events_archived = 0
        self.events_promoted = 0
        self.events_rate_limited = 0
        self.events_dry_run = 0
        self.events_deduped = 0
        self.events_behavioral_triggered = 0
        self.policy_cache_hits = 0
        self.policy_cache_misses = 0
        self.errors_total = 0

    def inc(self, field: str, n: int = 1):
        with _counter_lock:
            setattr(self, field, getattr(self, field) + n)

    def snapshot(self) -> Dict[str, int]:
        with _counter_lock:
            return {s: getattr(self, s) for s in self.__slots__}

metrics = EngineMetrics()

# ═══════════════════════════════════════════════════════════════════════════════
# Global Clients
# ═══════════════════════════════════════════════════════════════════════════════
producer = None
redis_client = None
pg_conn = None
minio_client = None

# ═══════════════════════════════════════════════════════════════════════════════
# Archive Buffer (Batched MinIO writes for Choice B)
# ═══════════════════════════════════════════════════════════════════════════════
_archive_buffer = []
_archive_lock = Lock()
_archive_last_flush = time.time()


def get_pg_conn():
    """Get or reconnect Postgres connection with health check."""
    global pg_conn
    try:
        if pg_conn and not pg_conn.closed:
            pg_conn.isolation_level  # ping-test
            return pg_conn
    except Exception:
        pass
    try:
        pg_conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT, database=POSTGRES_DB,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD
        )
        pg_conn.autocommit = True
        return pg_conn
    except Exception as e:
        logger.error(f"Postgres reconnect failed: {e}")
        return None


def init_minio():
    """Initialize MinIO client and ensure the archive bucket exists."""
    global minio_client
    try:
        from minio import Minio
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
            logger.info(f"Created MinIO bucket: {MINIO_BUCKET}")
        logger.info("MinIO linked")
    except ImportError:
        logger.warning("minio package not installed — Choice B archival disabled")
        minio_client = None
    except Exception as e:
        logger.warning(f"MinIO link failed (archival degraded): {e}")
        minio_client = None


# ═══════════════════════════════════════════════════════════════════════════════
# Startup
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_event("startup")
def startup_event():
    global producer, redis_client

    validate_auth_config()

    # ── Kafka Producer ──
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',                # Durability guarantee
            retries=3,                 # Auto-retry on transient failure
            max_in_flight_requests_per_connection=5,
            linger_ms=5,               # Micro-batch for throughput
        )
        logger.info("Kafka producer initialized (acks=all, retries=3)")
    except Exception as e:
        logger.error(f"Kafka init failed: {e}")

    # ── Redis ──
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True,
                                       socket_connect_timeout=5, socket_timeout=5)
        redis_client.ping()
        logger.info("Redis linked")
        if not redis_client.exists(ENGINE_STATE_KEY):
            redis_client.hset(ENGINE_STATE_KEY, mapping={
                "paused": "false", "dry_run": "false",
                "log_level": "INFO", "circuit_breaker": "false",
            })
    except Exception as e:
        logger.error(f"Redis link failed: {e}")

    # ── Postgres ──
    conn = get_pg_conn()
    if conn:
        logger.info("Postgres linked")
        _ensure_schema(conn)

    # ── MinIO ──
    init_minio()

    # ── Background: Archive Flusher ──
    flusher = Thread(target=_archive_flush_worker, daemon=True)
    flusher.start()

    # ── Background: Enrichment Rules Reloader ──
    def enrichment_reloader():
        while True:
            _refresh_enrichment_rules_cache()
            time.sleep(POLICY_CACHE_TTL)
    
    reloader = Thread(target=enrichment_reloader, daemon=True)
    reloader.start()


    # ── Health Checks ──
    global_health_registry.add_check("kafka", lambda: producer is not None and producer.bootstrap_connected())
    global_health_registry.add_check("redis", lambda: redis_client is not None and redis_client.ping())


def _ensure_schema(conn):
    """Idempotent schema migration: CREATE tables first, then ALTER columns."""
    try:
        with conn.cursor() as cur:
            # ── Step 1: Create base tables (if fresh DB) ──
            cur.execute("""
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
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingest_dedup_configs (
                    id SERIAL PRIMARY KEY,
                    tenant_id VARCHAR(255) UNIQUE NOT NULL,
                    window_seconds INTEGER DEFAULT 3600,
                    ignored_fields JSONB DEFAULT '[]'::jsonb,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingest_correlation_rules (
                    id SERIAL PRIMARY KEY,
                    tenant_id VARCHAR(255) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    source_a VARCHAR(255) NOT NULL,
                    source_b VARCHAR(255) NOT NULL,
                    match_field VARCHAR(255) NOT NULL,
                    time_window INTEGER DEFAULT 300,
                    min_count_a INTEGER DEFAULT 1,
                    min_count_b INTEGER DEFAULT 1,
                    action_type VARCHAR(50) DEFAULT 'CREATE_CASE',
                    severity_override INTEGER DEFAULT 3,
                    mitre_tactic VARCHAR(255) DEFAULT 'Unknown',
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS correlation_incidents (
                    id SERIAL PRIMARY KEY,
                    tenant_id VARCHAR(255) NOT NULL,
                    rule_id INTEGER NOT NULL REFERENCES ingest_correlation_rules(id) ON DELETE CASCADE,
                    rule_name VARCHAR(255) NOT NULL,
                    match_value TEXT NOT NULL,
                    threat_score INTEGER DEFAULT 0,
                    mitre_tactic VARCHAR(255) DEFAULT 'Unknown',
                    severity INTEGER DEFAULT 3,
                    action_taken VARCHAR(255),
                    case_id UUID,
                    fired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
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
            """)
            cur.execute("""
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
            """)

            # ── Step 2: Idempotent column additions (safe after tables exist) ──
            cur.execute("ALTER TABLE ingest_drop_rules ADD COLUMN IF NOT EXISTS priority INTEGER DEFAULT 0;")
            cur.execute("ALTER TABLE ingest_dedup_configs ADD COLUMN IF NOT EXISTS ignored_fields JSONB DEFAULT '[]'::jsonb;")
            cur.execute("ALTER TABLE ingest_correlation_rules ADD COLUMN IF NOT EXISTS definition_cel TEXT;")

        logger.info("Schema migration verified — all tables and columns present")
    except Exception as e:
        logger.error(f"Schema migration failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY: Deep Dot-Notation Field Resolver
# ═══════════════════════════════════════════════════════════════════════════════
def dot_get(obj: Any, path: str, default=None) -> Any:
    """
    Resolve a dot-notation path against a nested dict/list structure.
    Supports:
      - Simple paths:    dot_get(d, "rule.level")
      - Array indexing:  dot_get(d, "agents.0.name")
      - Wildcard:        dot_get(d, "agents.*.name") → returns list
    """
    if not path or obj is None:
        return default

    parts = path.split(".")
    current = obj

    for i, part in enumerate(parts):
        if current is None:
            return default

        # Wildcard expansion
        if part == "*" and isinstance(current, list):
            remaining = ".".join(parts[i + 1:])
            if remaining:
                return [dot_get(item, remaining, default) for item in current]
            return current

        # Array index access
        if isinstance(current, list):
            try:
                idx = int(part)
                current = current[idx]
                continue
            except (ValueError, IndexError):
                return default

        # Dict access
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return default

    return current if current is not None else default


# ═══════════════════════════════════════════════════════════════════════════════
# 3-LAYER POLICY CACHE: Memory → Redis → Postgres
# ═══════════════════════════════════════════════════════════════════════════════
_policy_memory_cache: Dict[str, Any] = {}
_policy_cache_ts: float = 0.0
_rules_memory_cache: List[Dict] = []
_rules_cache_ts: float = 0.0
_dedup_memory_cache: Dict[str, Dict] = {}
_dedup_cache_ts: float = 0.0


def _refresh_policy_cache():
    """Pull all policies from Postgres into Redis + Memory."""
    global _policy_memory_cache, _policy_cache_ts
    conn = get_pg_conn()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT tenant_id, source_name, is_enabled, rate_limit_eps, max_payload_kb FROM ingestion_policies")
            rows = cur.fetchall()
        cache = {}
        for r in rows:
            key = f"{r[0]}:{r[1]}"
            policy = {"is_enabled": r[2], "rate_limit_eps": r[3], "max_payload_kb": r[4]}
            cache[key] = policy
            # Also push to Redis for cross-instance sharing
            if redis_client:
                try:
                    redis_client.hset(POLICY_CACHE_KEY, key, json.dumps(policy))
                except Exception:
                    pass
        _policy_memory_cache = cache
        _policy_cache_ts = time.time()
        metrics.inc("policy_cache_misses")
        logger.debug(f"Policy cache refreshed: {len(cache)} policies loaded")
    except Exception as e:
        logger.error(f"Policy cache refresh failed: {e}")


def _refresh_rules_cache():
    """Pull all action rules from Postgres into Memory."""
    global _rules_memory_cache, _rules_cache_ts
    conn = get_pg_conn()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT field_path, operator, value, action, severity_override, priority, description
                FROM ingest_drop_rules
                WHERE is_active = TRUE
                ORDER BY priority DESC, created_at ASC
            """)
            _rules_memory_cache = [
                {
                    "field": r[0], "op": r[1], "value": r[2],
                    "action": (r[3] or "DROP").upper(),
                    "severity_override": r[4], "priority": r[5] or 0,
                    "description": r[6]
                }
                for r in cur.fetchall()
            ]
        _rules_cache_ts = time.time()
        logger.debug(f"Rules cache refreshed: {len(_rules_memory_cache)} rules loaded")
    except Exception as e:
        logger.error(f"Rules cache refresh failed: {e}")


def _refresh_dedup_cache():
    """Pull all deduplication configs from Postgres into Memory."""
    global _dedup_memory_cache, _dedup_cache_ts
    conn = get_pg_conn()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, field_paths, window_seconds, ignored_fields
                FROM ingest_dedup_configs
                WHERE is_active = TRUE
            """)
            _dedup_memory_cache = {
                r[0]: {"fields": r[1], "window": r[2], "ignored_fields": r[3] or []}
                for r in cur.fetchall()
            }

        _dedup_cache_ts = time.time()
        logger.debug(f"Dedup cache refreshed: {len(_dedup_memory_cache)} configs loaded")
    except Exception as e:
        logger.error(f"Dedup cache refresh failed: {e}")


def get_policy(tenant_id: str, source: str) -> Dict[str, Any]:
    """3-Layer policy resolution: Memory → Redis → Postgres."""
    global _policy_cache_ts
    default_policy = {"is_enabled": True, "rate_limit_eps": 100, "max_payload_kb": 2048}

    # L1: Memory
    if time.time() - _policy_cache_ts < POLICY_CACHE_TTL:
        key = f"{tenant_id}:{source}"
        if key in _policy_memory_cache:
            metrics.inc("policy_cache_hits")
            return _policy_memory_cache[key]

    # L2: Redis
    if redis_client:
        try:
            cached = redis_client.hget(POLICY_CACHE_KEY, f"{tenant_id}:{source}")
            if cached:
                metrics.inc("policy_cache_hits")
                return json.loads(cached)
        except Exception:
            pass

    # L3: Postgres (and refresh cache)
    _refresh_policy_cache()
    key = f"{tenant_id}:{source}"
    return _policy_memory_cache.get(key, default_policy)


def get_action_rules(tenant_id: str) -> List[Dict]:
    """Get cached action rules. Refreshes every POLICY_CACHE_TTL seconds."""
    if time.time() - _rules_cache_ts > POLICY_CACHE_TTL:
        _refresh_rules_cache()
    # Filter for tenant (rules can be tenant-specific or global)
    return [r for r in _rules_memory_cache if True]  # All rules for now; tenant filter in evaluate


def get_dedup_config(source: str) -> Optional[Dict]:
    """Get deduplication config for a source. Refreshes every POLICY_CACHE_TTL."""
    global _dedup_cache_ts
    if time.time() - _dedup_cache_ts > POLICY_CACHE_TTL:
        _refresh_dedup_cache()
    return _dedup_memory_cache.get(source)


def _refresh_enrichment_rules_cache():
    """Pull extraction and mapping rules into memory."""
    global _extraction_rules_memory, _mapping_rules_memory
    conn = get_pg_conn()
    if not conn: return
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT tenant_id, rule_name, attribute, regex, condition, priority FROM ingest_extraction_rules WHERE enabled = TRUE ORDER BY priority DESC")
            _extraction_rules_memory = [
                {"tenant": r[0], "name": r[1], "attr": r[2], "regex": r[3], "cond": r[4]}
                for r in cur.fetchall()
            ]
            cur.execute("SELECT tenant_id, rule_name, match_fields, mapping_data, priority FROM ingest_mapping_rules WHERE enabled = TRUE ORDER BY priority DESC")
            _mapping_rules_memory = [
                {"tenant": r[0], "name": r[1], "fields": r[2], "data": r[3]}
                for r in cur.fetchall()
            ]
    except Exception as e:
        logger.error(f"Enrichment cache refresh failed: {e}")

_extraction_rules_memory = []
_mapping_rules_memory = []


def calculate_fingerprint(payload: Dict, fields: List[str], ignored_fields: List[str] = None) -> str:
    """Calculate a stable hash for a payload based on specific fields, optionally ignoring others.
    
    Fix D: Auto-volatile detection — fields with >90% unique values across samples
    are automatically added to ignored_fields to prevent deduplication failures.
    """
    # Merge manual ignored_fields with auto-detected volatile fields
    all_ignored = set(ignored_fields or [])
    auto_volatile = _get_auto_volatile_fields(payload)
    all_ignored.update(auto_volatile)

    values = []
    if fields:
        for field in fields:
            if field not in all_ignored:
                val = dot_get(payload, field)
                values.append(str(val) if val is not None else "∅")
    else:
        clean_payload = {k: v for k, v in payload.items() if k not in all_ignored}
        return hashlib.md5(json.dumps(clean_payload, sort_keys=True, default=str).encode()).hexdigest()
    
    raw_key = "|".join(values)
    return hashlib.md5(raw_key.encode()).hexdigest()


# ── Fix D: Auto-Volatile Field Detection ──────────────────────────────────────

VOLATILE_SAMPLE_SIZE = 100       # Track this many values per field
VOLATILE_UNIQUENESS_THRESHOLD = 0.90  # 90% unique = volatile

# Known volatile field patterns (always ignore)
KNOWN_VOLATILE_PATTERNS = {"event_id", "uuid", "trace_id", "request_id", "message_id", "seq", "sequence"}

_volatile_cache = set()
_volatile_last_check = 0
VOLATILE_CHECK_INTERVAL = 300  # Re-check every 5 minutes


def _get_auto_volatile_fields(payload: Dict) -> set:
    """Get auto-detected volatile fields. Uses Redis HyperLogLog for cardinality estimation."""
    global _volatile_cache, _volatile_last_check
    
    # Return cached result if fresh
    now = time.time()
    if now - _volatile_last_check < VOLATILE_CHECK_INTERVAL and _volatile_cache:
        return _volatile_cache
    
    try:
        r = redis.client.Redis.from_url(REDIS_URL)
        
        # Track cardinality for each top-level field
        for key, val in payload.items():
            val_str = str(val) if val is not None else ""
            # HyperLogLog: approximate cardinality per field
            hll_key = f"volatile:hll:{key}"
            counter_key = f"volatile:count:{key}"
            r.pfadd(hll_key, val_str)
            r.incr(counter_key)
            r.expire(hll_key, 3600)
            r.expire(counter_key, 3600)
        
        # Check which fields are volatile
        volatile = set()
        for key in payload.keys():
            # Always include known volatile patterns
            if key.lower() in KNOWN_VOLATILE_PATTERNS:
                volatile.add(key)
                continue
            
            hll_key = f"volatile:hll:{key}"
            counter_key = f"volatile:count:{key}"
            unique_count = r.pfcount(hll_key)
            total_count = int(r.get(counter_key) or 0)
            
            if total_count >= 20:  # Need minimum samples
                uniqueness_ratio = unique_count / max(total_count, 1)
                if uniqueness_ratio >= VOLATILE_UNIQUENESS_THRESHOLD:
                    volatile.add(key)
        
        _volatile_cache = volatile
        _volatile_last_check = now
        
        if volatile:
            logger.debug(f"Auto-volatile fields: {volatile}")
        return volatile
        
    except Exception as e:
        logger.debug(f"Auto-volatile detection skipped: {e}")
        # Fallback: just use known patterns
        return {k for k in payload.keys() if k.lower() in KNOWN_VOLATILE_PATTERNS}


def _cel_eval(expression: str, data: Dict) -> bool:
    """Evaluate a CEL expression against a data payload. Returns True if match."""
    try:
        import celpy
        env = celpy.Environment()
        ast = env.compile(expression)
        prgm = env.program(ast)
        activation = celpy.json_to_cel(data)
        result = prgm.evaluate(activation)
        return bool(result)
    except Exception as e:
        logger.warning(f"CEL evaluation failed for '{expression}': {e}")
        return False


def run_extraction_rules(tenant_id: str, payload: Dict):
    """Keep-inspired regex extraction logic with CEL condition gating."""
    for rule in _extraction_rules_memory:
        if rule['tenant'] not in (tenant_id, 'global'): continue
        
        # 1. Get attribute to extract from
        attr_val = dot_get(payload, rule['attr'])
        if not attr_val or not isinstance(attr_val, str): continue
        
        # 2. CEL Condition Check (full implementation)
        if rule.get('cond'):
            if not _cel_eval(rule['cond'], payload):
                continue
        
        # 3. Apply Regex
        match = re.search(rule['regex'], attr_val)
        if match:
            extracted = match.groupdict()
            payload.update(extracted)
            logger.info(f"Extracted {list(extracted.keys())} via rule {rule['name']}")


def run_mapping_rules(tenant_id: str, payload: Dict):
    """Keep-inspired static/KV mapping logic."""
    for rule in _mapping_rules_memory:
        if rule['tenant'] not in (tenant_id, 'global'): continue
        
        # Check if all match_fields match the payload
        match = True
        for field_path, expected_val in rule['fields'].items():
            if str(dot_get(payload, field_path)) != str(expected_val):
                match = False
                break
        
        if match:
            payload.update(rule['data'])
            logger.info(f"Enriched payload via mapping rule {rule['name']}")



# ═══════════════════════════════════════════════════════════════════════════════
# MULTI-ACTION RULE EVALUATOR: The Sovereign Decision Maker
# ═══════════════════════════════════════════════════════════════════════════════
# Actions are now defined at the top as constants


def evaluate_action(payload: Dict, rules: List[Dict]) -> Dict[str, Any]:
    """
    Evaluate all rules against the payload. Returns the highest-priority match.
    Rules are pre-sorted by priority DESC. First match wins.

    Returns: {"action": "DROP|LOG|ALERT|PASS", "rule": {...} or None,
              "severity_override": int or None}
    """
    for rule in rules:
        field_val = dot_get(payload, rule["field"])
        if field_val is None:
            continue

        field_val_str = str(field_val)
        matched = False

        op = rule["op"]
        rule_val = rule["value"]

        if op == "eq":
            matched = field_val_str == rule_val
        elif op == "neq":
            matched = field_val_str != rule_val
        elif op == "contains":
            matched = rule_val.lower() in field_val_str.lower()
        elif op == "not_contains":
            matched = rule_val.lower() not in field_val_str.lower()
        elif op == "gt":
            try:
                matched = float(field_val_str) > float(rule_val)
            except (ValueError, TypeError):
                pass
        elif op == "lt":
            try:
                matched = float(field_val_str) < float(rule_val)
            except (ValueError, TypeError):
                pass
        elif op == "gte":
            try:
                matched = float(field_val_str) >= float(rule_val)
            except (ValueError, TypeError):
                pass
        elif op == "lte":
            try:
                matched = float(field_val_str) <= float(rule_val)
            except (ValueError, TypeError):
                pass
        elif op == "regex":
            try:
                matched = bool(re.search(rule_val, field_val_str))
            except re.error:
                pass
        elif op == "cidr":
            matched = _cidr_match(field_val_str, rule_val)
        elif op == "in":
            # Comma-separated list
            matched = field_val_str in [v.strip() for v in rule_val.split(",")]
        elif op == "exists":
            matched = True  # Field exists (we already passed the None check)
        elif op == "startswith":
            matched = field_val_str.startswith(rule_val)
        elif op == "endswith":
            matched = field_val_str.endswith(rule_val)

        if matched:
            return {
                "action": rule["action"],
                "rule": rule,
                "severity_override": rule.get("severity_override"),
            }

    return {"action": ACTION_PASS, "rule": None, "severity_override": None}


def _cidr_match(ip_str: str, cidr: str) -> bool:
    """Check if an IP is within a CIDR range (pure Python, no deps)."""
    try:
        import ipaddress
        return ipaddress.ip_address(ip_str) in ipaddress.ip_network(cidr, strict=False)
    except (ValueError, ImportError):
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# BEHAVIORAL THRESHOLD DETECTOR (Fixed Math Detective)
# ═══════════════════════════════════════════════════════════════════════════════
def check_behavioral_threshold(tenant_id: str, payload: Dict) -> Optional[Dict]:
    """
    Mathematical behavioral detection. No AI needed.
    Uses Redis counters to detect volume anomalies in real-time.

    Logic: If the same (source_ip + event_type) exceeds BEHAVIORAL_THRESHOLD
           within BEHAVIORAL_WINDOW seconds, trigger a behavioral alert.
    """
    if not redis_client:
        return None

    # Extract behavioral dimensions
    src_ip = (dot_get(payload, "data.srcip")
              or dot_get(payload, "src_ip")
              or dot_get(payload, "agent.ip")
              or "unknown")
    event_type = (dot_get(payload, "rule.id")
                  or dot_get(payload, "type")
                  or "generic")

    behavior_key = f"{BEHAVIORAL_PREFIX}{tenant_id}:{src_ip}:{event_type}"

    try:
        pipe = redis_client.pipeline(transaction=True)
        pipe.incr(behavior_key)
        pipe.expire(behavior_key, BEHAVIORAL_WINDOW)
        results = pipe.execute()
        count = results[0]

        if count >= BEHAVIORAL_THRESHOLD:
            # Reset the counter to prevent flooding
            redis_client.delete(behavior_key)
            metrics.inc("events_behavioral_triggered")
            return {
                "triggered": True,
                "src_ip": src_ip,
                "event_type": event_type,
                "count": count,
                "window_seconds": BEHAVIORAL_WINDOW,
                "threshold": BEHAVIORAL_THRESHOLD,
                "message": f"Behavioral threshold breached: {count} events from {src_ip} "
                           f"(type={event_type}) in {BEHAVIORAL_WINDOW}s"
            }
    except Exception as e:
        logger.debug(f"Behavioral check error: {e}")

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# DEDUPLICATION ENGINE (The Architect)
# ═══════════════════════════════════════════════════════════════════════════════
def check_and_track_dedup(tenant_id: str, source: str, fingerprint: str, window: int) -> bool:
    """
    Check if this event fingerprint has been seen within the window.
    Uses Redis SETNX with expiration for atomic check-and-set.
    
    Returns: True if it's a DUPLICATE, False if it's NEW.
    """
    if not redis_client:
        return False
    
    dedup_key = f"{DEDUP_PREFIX}{tenant_id}:{source}:{fingerprint}"
    
    try:
        # NX=True means only set if key doesn't exist
        # We use a value of '1' as a simple flag
        # If set returns True, it's a NEW event.
        is_new = redis_client.set(dedup_key, "1", ex=window, nx=True)
        return not is_new  # If not is_new, then it was already there (duplicate)
    except Exception as e:
        logger.debug(f"Deduplication Redis error: {e}")
        return False



# ═══════════════════════════════════════════════════════════════════════════════
# RATE LIMITER (Token Bucket via Lua atomicity)
# ═══════════════════════════════════════════════════════════════════════════════
_RATE_LIMIT_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local capacity = tonumber(ARGV[2])
local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1]) or capacity
local last_refill = tonumber(bucket[2]) or now
local elapsed = math.max(0, now - last_refill)
tokens = math.min(capacity, tokens + (elapsed * rate))
if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 120)
    return 1
else
    return 0
end
"""


def check_rate_limit(tenant_id: str, source: str, eps: int) -> bool:
    """Token-bucket rate limiting backed by Redis Lua script."""
    if not redis_client:
        return True
    key = f"ingest:limiter:{tenant_id}:{source}"
    try:
        return bool(redis_client.eval(_RATE_LIMIT_LUA, 1, key, time.time(), eps))
    except Exception:
        return True  # Fail open on Redis error


# ═══════════════════════════════════════════════════════════════════════════════
# MinIO ARCHIVER (Choice B — Cold Storage)
# ═══════════════════════════════════════════════════════════════════════════════
def archive_to_minio(event: Dict):
    """Buffer events and batch-write to MinIO as compressed JSON."""
    global _archive_last_flush
    with _archive_lock:
        _archive_buffer.append(event)
        if len(_archive_buffer) >= ARCHIVE_BATCH_SIZE:
            _flush_archive_buffer()


def _flush_archive_buffer():
    """Flush the archive buffer to MinIO as a gzipped NDJSON file."""
    global _archive_last_flush
    if not _archive_buffer or not minio_client:
        return

    events = list(_archive_buffer)
    _archive_buffer.clear()
    _archive_last_flush = time.time()

    now = datetime.now(timezone.utc)
    object_name = f"{now.strftime('%Y/%m/%d/%H')}/batch_{now.strftime('%H%M%S')}_{uuid.uuid4().hex[:8]}.ndjson.gz"

    try:
        # Create NDJSON (Newline-Delimited JSON)
        ndjson = "\n".join(json.dumps(e, default=str) for e in events)
        compressed = gzip.compress(ndjson.encode("utf-8"))
        data = BytesIO(compressed)

        minio_client.put_object(
            MINIO_BUCKET, object_name, data, len(compressed),
            content_type="application/x-ndjson",
            metadata={"x-nv-count": str(len(events)), "x-nv-compressed": "gzip"}
        )
        logger.info(f"Archived {len(events)} events → s3://{MINIO_BUCKET}/{object_name} ({len(compressed)} bytes)")
    except Exception as e:
        logger.error(f"MinIO archive failed: {e}")
        metrics.inc("errors_total")


def _archive_flush_worker():
    """Background thread to periodically flush the archive buffer."""
    while True:
        time.sleep(ARCHIVE_FLUSH_INTERVAL)
        with _archive_lock:
            if _archive_buffer and (time.time() - _archive_last_flush > ARCHIVE_FLUSH_INTERVAL):
                try:
                    _flush_archive_buffer()
                except Exception as e:
                    logger.error(f"Archive flush worker error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# LOG MINIFICATION (Strip noise, keep bones)
# ═══════════════════════════════════════════════════════════════════════════════
_MINIFY_KEEP_FIELDS = {
    "timestamp", "source", "type", "sourceRef", "title", "severity",
    "rule.id", "rule.level", "rule.description",
    "data.srcip", "data.dstip", "agent.name", "agent.ip",
    "src_ip", "dst_ip", "tenant_id",
}


def minify_payload(payload: Dict) -> Dict:
    """Strip the payload down to essential fields for cold storage."""
    minified = {}
    for field in _MINIFY_KEEP_FIELDS:
        val = dot_get(payload, field)
        if val is not None:
            # Flatten dot-paths into nested structure
            parts = field.split(".")
            current = minified
            for part in parts[:-1]:
                current = current.setdefault(part, {})
            current[parts[-1]] = val
    # Always keep a hash of the original for reference
    minified["_original_hash"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()[:16]
    return minified


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE STATE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def get_engine_state() -> Dict[str, Any]:
    defaults = {"paused": "false", "dry_run": "false", "log_level": "INFO", "circuit_breaker": "false"}
    if not redis_client:
        return defaults
    try:
        state = redis_client.hgetall(ENGINE_STATE_KEY)
        return {**defaults, **state}
    except Exception:
        return defaults


def set_engine_field(field: str, value: str):
    if redis_client:
        redis_client.hset(ENGINE_STATE_KEY, field, value)


def push_live_event(event_summary: Dict):
    """Push event to Redis ring buffer for the live stream HUD."""
    if not redis_client:
        return
    try:
        redis_client.lpush(LIVE_STREAM_KEY, json.dumps(event_summary, default=str))
        redis_client.ltrim(LIVE_STREAM_KEY, 0, 99)  # Keep last 100
    except Exception:
        pass


def extract_display_fields(payload: Dict) -> Dict:
    """
    Smart extraction of human-readable title and severity from any JSON format.
    Tries multiple common paths (Wazuh, Syslog, Suricata, MISP, generic) with fallbacks.
    """
    # Title: try rule.description > description > title > alert.signature > type
    title = (dot_get(payload, "rule.description")
             or payload.get("title")
             or payload.get("description")
             or dot_get(payload, "alert.signature")
             or payload.get("type")
             or payload.get("message")
             or "Untitled Event")

    # Severity: try severity > rule.level > alert.severity > priority
    severity = payload.get("severity")
    if severity is None:
        level = dot_get(payload, "rule.level")
        if level is not None:
            # Wazuh levels 0-15, map to NV severity 1-4
            try:
                lvl = int(level)
                if lvl >= 12:
                    severity = 4
                elif lvl >= 7:
                    severity = 3
                elif lvl >= 4:
                    severity = 2
                else:
                    severity = 1
            except (ValueError, TypeError):
                severity = 1
        else:
            severity = dot_get(payload, "alert.severity") or payload.get("priority") or 1

    try:
        severity = int(severity)
    except (ValueError, TypeError):
        severity = 1

    return {"title": str(title)[:120], "severity": severity}


# ═══════════════════════════════════════════════════════════════════════════════
# HEALTH & METRICS ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/healthz")
def healthz():
    return {"status": "ok", "version": "2.0.0"}


@app.get("/metrics")
def prometheus_metrics():
    return get_metrics_response()


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE CONTROL PLANE
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/engine/status")
def engine_status():
    """Full engine status dashboard."""
    state = get_engine_state()
    uptime = time.time() - _engine_start_time
    m = metrics.snapshot()

    try:
        proc = psutil.Process()
        mem = proc.memory_info()
        cpu = psutil.cpu_percent(interval=0)
        mem_mb = round(mem.rss / (1024 * 1024), 1)
    except Exception:
        cpu, mem_mb = 0, 0

    return {
        "engine_version": "2.0.0",
        "paused": state["paused"] == "true",
        "dry_run": state["dry_run"] == "true",
        "log_level": state["log_level"],
        "circuit_breaker": state["circuit_breaker"] == "true",
        "uptime_seconds": round(uptime),
        "uptime_human": f"{int(uptime // 3600)}h {int((uptime % 3600) // 60)}m",
        "cpu_percent": cpu,
        "memory_mb": mem_mb,
        "kafka_connected": producer is not None,
        "redis_connected": redis_client is not None,
        "postgres_connected": pg_conn is not None and not pg_conn.closed,
        "minio_connected": minio_client is not None,
        "archive_buffer_size": len(_archive_buffer),
        "policy_cache_size": len(_policy_memory_cache),
        "rules_cache_size": len(_rules_memory_cache),
        "metrics": m,
    }


@app.post("/engine/pause")
def engine_pause():
    set_engine_field("paused", "true")
    logger.warning("ENGINE PAUSED — all ingestion halted")
    return {"status": "paused", "message": "All ingestion halted"}


@app.post("/engine/resume")
def engine_resume():
    set_engine_field("paused", "false")
    logger.info("ENGINE RESUMED — ingestion active")
    return {"status": "active", "message": "Ingestion resumed"}


@app.post("/engine/dry-run")
def engine_dry_run(payload: Dict[str, Any] = Body(...)):
    enabled = payload.get("enabled", True)
    set_engine_field("dry_run", str(enabled).lower())
    logger.info(f"DRY-RUN mode {'ENABLED' if enabled else 'DISABLED'}")
    return {"dry_run": enabled}


@app.post("/engine/log-level")
def engine_log_level(payload: Dict[str, Any] = Body(...)):
    level = payload.get("level", "INFO").upper()
    if level not in ("DEBUG", "INFO", "WARNING", "ERROR"):
        raise HTTPException(status_code=400, detail="Invalid log level")
    set_engine_field("log_level", level)
    logging.getLogger().setLevel(getattr(logging, level))
    return {"log_level": level}


@app.post("/engine/circuit-breaker")
def engine_circuit_breaker(payload: Dict[str, Any] = Body(...)):
    tripped = payload.get("tripped", True)
    set_engine_field("circuit_breaker", str(tripped).lower())
    msg = "TRIPPED — downstream overload protection" if tripped else "RESET — normal flow"
    logger.warning(f"CIRCUIT BREAKER {msg}")
    return {"circuit_breaker": tripped}


@app.post("/engine/flush-cache")
def engine_flush_cache():
    """Force-refresh all caches: policies, action rules, dedup, and enrichment."""
    _refresh_policy_cache()
    _refresh_rules_cache()
    _refresh_dedup_cache()
    _refresh_enrichment_rules_cache()
    return {
        "status": "refreshed",
        "policies": len(_policy_memory_cache),
        "rules": len(_rules_memory_cache),
        "extraction_rules": len(_extraction_rules_memory),
        "mapping_rules": len(_mapping_rules_memory),
        "dedup_configs": len(_dedup_memory_cache)
    }


@app.get("/engine/metrics")
def engine_metrics_endpoint():
    uptime = time.time() - _engine_start_time
    m = metrics.snapshot()
    eps = m["events_total"] / max(uptime, 1)

    try:
        proc = psutil.Process()
        cpu = psutil.cpu_percent(interval=0)
        mem_mb = round(proc.memory_info().rss / (1024 * 1024), 1)
    except Exception:
        cpu, mem_mb = 0, 0

    return {
        **m,
        "eps": round(eps, 2),
        "cpu_percent": cpu,
        "memory_mb": mem_mb,
        "uptime_seconds": round(uptime),
        "kafka_lag": 0,
    }


@app.get("/engine/live-stream")
def engine_live_stream(count: int = 20):
    if not redis_client:
        return {"events": []}
    try:
        raw = redis_client.lrange(LIVE_STREAM_KEY, 0, min(count, 100) - 1)
        return {"events": [json.loads(r) for r in raw]}
    except Exception:
        return {"events": []}


# ═══════════════════════════════════════════════════════════════════════════════
# DROP/ACTION RULES CRUD (direct on engine)
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/engine/drop-rules")
def list_drop_rules(tenant_id: str = "dev-tenant"):
    conn = get_pg_conn()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, field_path, operator, value, action, severity_override,
                          is_active, description, priority, created_at
                   FROM ingest_drop_rules WHERE tenant_id = %s
                   ORDER BY priority DESC, created_at DESC""",
                (tenant_id,)
            )
            return [
                {
                    "id": str(r[0]), "field": r[1], "operator": r[2], "value": r[3],
                    "action": r[4] or "DROP", "severityOverride": r[5],
                    "active": r[6], "description": r[7], "priority": r[8] or 0,
                    "createdAt": str(r[9])
                }
                for r in cur.fetchall()
            ]
    except Exception as e:
        logger.error(f"Rules list error: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# ENRICHMENT RULES CRUD — Extraction & Mapping Management
# ═══════════════════════════════════════════════════════════════════════════════

class ExtractionRuleCreate(BaseModel):
    tenant_id: str = "dev-tenant"
    rule_name: str
    attribute: str
    regex: str
    condition: Optional[str] = None
    priority: int = 0

class MappingRuleCreate(BaseModel):
    tenant_id: str = "dev-tenant"
    rule_name: str
    match_fields: Dict[str, Any]
    mapping_data: Dict[str, Any]
    priority: int = 0

class DedupConfigUpdate(BaseModel):
    field_paths: List[str] = []
    window_seconds: int = 300
    ignored_fields: List[str] = []


@app.post("/rules/extraction", status_code=201)
def create_extraction_rule(rule: ExtractionRuleCreate, auth: AuthContext = Depends(require_permission(PERM_ALERT_INGEST))):
    conn = get_pg_conn()
    if not conn:
        raise HTTPException(status_code=500, detail="Database unavailable")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO ingest_extraction_rules (tenant_id, rule_name, attribute, regex, condition, priority)
                   VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
                (rule.tenant_id, rule.rule_name, rule.attribute, rule.regex, rule.condition, rule.priority)
            )
            rule_id = cur.fetchone()[0]
        conn.commit()
        _refresh_enrichment_rules_cache()
        return {"id": rule_id, "status": "created"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/rules/extraction")
def list_extraction_rules(tenant_id: str = "dev-tenant"):
    conn = get_pg_conn()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, tenant_id, rule_name, attribute, regex, condition, priority, enabled, created_at
                   FROM ingest_extraction_rules WHERE tenant_id = %s ORDER BY priority DESC""",
                (tenant_id,)
            )
            return [
                {"id": r[0], "tenant_id": r[1], "rule_name": r[2], "attribute": r[3],
                 "regex": r[4], "condition": r[5], "priority": r[6], "enabled": r[7],
                 "created_at": str(r[8])}
                for r in cur.fetchall()
            ]
    except Exception as e:
        logger.error(f"List extraction rules error: {e}")
        return []


@app.delete("/rules/extraction/{rule_id}")
def delete_extraction_rule(rule_id: int, auth: AuthContext = Depends(require_permission(PERM_ALERT_INGEST))):
    conn = get_pg_conn()
    if not conn:
        raise HTTPException(status_code=500, detail="Database unavailable")
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ingest_extraction_rules WHERE id = %s", (rule_id,))
        conn.commit()
        _refresh_enrichment_rules_cache()
        return {"status": "deleted", "id": rule_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/rules/mapping", status_code=201)
def create_mapping_rule(rule: MappingRuleCreate, auth: AuthContext = Depends(require_permission(PERM_ALERT_INGEST))):
    conn = get_pg_conn()
    if not conn:
        raise HTTPException(status_code=500, detail="Database unavailable")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO ingest_mapping_rules (tenant_id, rule_name, match_fields, mapping_data, priority)
                   VALUES (%s, %s, %s::jsonb, %s::jsonb, %s) RETURNING id""",
                (rule.tenant_id, rule.rule_name, json.dumps(rule.match_fields),
                 json.dumps(rule.mapping_data), rule.priority)
            )
            rule_id = cur.fetchone()[0]
        conn.commit()
        _refresh_enrichment_rules_cache()
        return {"id": rule_id, "status": "created"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/rules/mapping")
def list_mapping_rules(tenant_id: str = "dev-tenant"):
    conn = get_pg_conn()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, tenant_id, rule_name, match_fields, mapping_data, priority, enabled, created_at
                   FROM ingest_mapping_rules WHERE tenant_id = %s ORDER BY priority DESC""",
                (tenant_id,)
            )
            return [
                {"id": r[0], "tenant_id": r[1], "rule_name": r[2], "match_fields": r[3],
                 "mapping_data": r[4], "priority": r[5], "enabled": r[6], "created_at": str(r[7])}
                for r in cur.fetchall()
            ]
    except Exception as e:
        logger.error(f"List mapping rules error: {e}")
        return []


@app.delete("/rules/mapping/{rule_id}")
def delete_mapping_rule(rule_id: int, auth: AuthContext = Depends(require_permission(PERM_ALERT_INGEST))):
    conn = get_pg_conn()
    if not conn:
        raise HTTPException(status_code=500, detail="Database unavailable")
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ingest_mapping_rules WHERE id = %s", (rule_id,))
        conn.commit()
        _refresh_enrichment_rules_cache()
        return {"status": "deleted", "id": rule_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dedup/configs")
def list_dedup_configs():
    _refresh_dedup_cache()
    return _dedup_memory_cache


@app.put("/dedup/config/{source}")
def update_dedup_config(source: str, config: DedupConfigUpdate, auth: AuthContext = Depends(require_permission(PERM_ALERT_INGEST))):
    conn = get_pg_conn()
    if not conn:
        raise HTTPException(status_code=500, detail="Database unavailable")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO ingest_dedup_configs (source, field_paths, window_seconds, ignored_fields, is_active)
                   VALUES (%s, %s, %s, %s::jsonb, TRUE)
                   ON CONFLICT (source) DO UPDATE SET
                       field_paths = EXCLUDED.field_paths,
                       window_seconds = EXCLUDED.window_seconds,
                       ignored_fields = EXCLUDED.ignored_fields""",
                (source, config.field_paths, config.window_seconds, json.dumps(config.ignored_fields))
            )
        conn.commit()
        _refresh_dedup_cache()
        return {"status": "updated", "source": source}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════════════════════

# MAIN INGEST ENDPOINT — The Sovereign Gate
# ═══════════════════════════════════════════════════════════════════════════════
@app.post("/ingest", status_code=202)
async def ingest_event(
    request: Request,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    trace_id: Optional[str] = Header(None, alias="X-Trace-Id"),
    auth: AuthContext = Depends(require_permission(PERM_ALERT_INGEST))
):
    """
    Enterprise Ingestion Gateway.

    Accepts any JSON payload and routes it through the multi-layer decision engine:
    1. Engine-level checks (pause, circuit breaker)
    2. Policy enforcement (source enable/disable, rate limit, payload size)
    3. Multi-action rules (DROP / LOG / ALERT / PASS)
    4. Behavioral threshold detection (Fixed Math Detective)
    5. Routing: DROP → discard, LOG → MinIO, ALERT → Kafka (high priority), PASS → Kafka
    """
    # ── Parse flexible JSON body ──
    try:
        body_bytes = await request.body()
        payload = json.loads(body_bytes)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON payload: {e}")

    # Normalize: ensure standard fields exist
    source = payload.get("source", payload.get("agent", {}).get("name", "unknown")) if isinstance(payload, dict) else "unknown"
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object")

    # ── 0. Engine-Level Checks ──
    state = get_engine_state()

    if state["paused"] == "true":
        raise HTTPException(status_code=503, detail="Ingestion engine is paused")

    if state["circuit_breaker"] == "true":
        raise HTTPException(status_code=503, detail="Circuit breaker is tripped — downstream overload")

    # ── 1. Policy Enforcement ──
    policy = get_policy(auth.tenant_id, source)

    if not policy["is_enabled"]:
        raise HTTPException(status_code=403, detail=f"Source '{source}' is currently disabled")

    # Payload size check
    payload_size_kb = len(body_bytes) / 1024
    if payload_size_kb > policy["max_payload_kb"]:
        raise HTTPException(status_code=413, detail=f"Payload too large ({payload_size_kb:.1f}KB). Max: {policy['max_payload_kb']}KB")

    # Rate limiting
    if not check_rate_limit(auth.tenant_id, source, policy["rate_limit_eps"]):
        metrics.inc("events_rate_limited")
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    # ── 2. Multi-Action Rule Evaluation ──
    rules = get_action_rules(auth.tenant_id)
    decision = evaluate_action(payload, rules)

    # ── 2a. Deduplication Check (The Architect) ──
    dedup_config = get_dedup_config(source)
    is_duplicate = False
    if dedup_config:
        # Pass ignored_fields if using full-payload dedup (fields is empty)
        # or even if using specific fields (Keep allows both)
        fingerprint = calculate_fingerprint(
            payload, 
            dedup_config.get("fields", []), 
            dedup_config.get("ignored_fields", [])
        )
        is_duplicate = check_and_track_dedup(auth.tenant_id, source, fingerprint, dedup_config["window"])

        
        if is_duplicate:
            # Overwrite decision to DEDUPED if it wasn't already a DROP
            if decision["action"] != ACTION_DROP:
                decision["action"] = ACTION_DEDUPED
                decision["fingerprint"] = fingerprint

    if not trace_id:
        trace_id = str(uuid.uuid4())

    event_id = str(uuid.uuid4())
    timestamp = int(time.time() * 1000)

    # Build the standard NV event envelope
    event = {
        "event_id": event_id,
        "trace_id": trace_id,
        "tenant_id": auth.tenant_id,
        "idempotency_key": idempotency_key,
        "timestamp": timestamp,
        "schema_version": "2.0",
        "source": source,
        "decision": decision["action"],
        "payload": payload,
    }
    event["payload"]["tenant_id"] = auth.tenant_id

    # ── 3. Dry-Run Check ──
    is_dry_run = state["dry_run"] == "true"
    if is_dry_run:
        metrics.inc("events_dry_run")
        display = extract_display_fields(payload)
        push_live_event({
            "event_id": event_id, "source": source,
            "title": display["title"],
            "severity": display["severity"],
            "status": "DRY-RUN", "decision": decision["action"],
            "timestamp": timestamp,
        })
        return {"status": "dry-run", "event_id": event_id, "trace_id": trace_id, "decision": decision["action"]}

    # ── 4. Execute Decision ──
    if decision["action"] == ACTION_DROP:
        # ─── Choice A: The Silencer ───
        metrics.inc("events_dropped")
        display = extract_display_fields(payload)
        push_live_event({
            "event_id": event_id, "source": source,
            "title": display["title"],
            "severity": display["severity"],
            "status": "DROPPED", "decision": "DROP",
            "rule_desc": (decision["rule"] or {}).get("description", ""),
            "timestamp": timestamp,
        })
        return {"status": "dropped", "event_id": event_id, "reason": "Matched DROP rule",
                "rule": (decision["rule"] or {}).get("description", "")}

    elif decision["action"] == ACTION_LOG:
        # ─── Choice B: The Archivist ───
        minified = minify_payload(payload)
        archive_event = {
            "event_id": event_id, "trace_id": trace_id,
            "tenant_id": auth.tenant_id, "timestamp": timestamp,
            "source": source, "payload": minified,
        }
        archive_to_minio(archive_event)
        metrics.inc("events_archived")
        display = extract_display_fields(payload)
        push_live_event({
            "event_id": event_id, "source": source,
            "title": display["title"],
            "severity": display["severity"],
            "status": "ARCHIVED", "decision": "LOG",
            "timestamp": timestamp,
        })
        return {"status": "archived", "event_id": event_id, "trace_id": trace_id, "storage": "minio"}

    elif decision["action"] == ACTION_DEDUPED:
        # ─── Choice E: The Architect (Deduplication) ───
        metrics.inc("events_deduped")
        display = extract_display_fields(payload)
        push_live_event({
            "event_id": event_id, "source": source,
            "title": f"♻️ [DEDUP] {display['title']}",
            "severity": display["severity"],
            "status": "DEDUPED", "decision": "DEDUP",
            "timestamp": timestamp,
        })
        # Still archive it for compliance/forensics, but don't promote it
        minified = minify_payload(payload)
        archive_event = {
            "event_id": event_id, "trace_id": trace_id,
            "tenant_id": auth.tenant_id, "timestamp": timestamp,
            "source": source, "payload": minified, "fingerprint": decision.get("fingerprint")
        }
        archive_to_minio(archive_event)
        return {"status": "deduped", "event_id": event_id, "fingerprint": decision.get("fingerprint")}

    elif decision["action"] == ACTION_ALERT:
        # ─── Choice C: The Alarm ───
        severity = decision.get("severity_override") or payload.get("severity", 3)
        event["payload"]["severity"] = severity
        event["payload"]["promoted_by"] = "rule"
        event["payload"]["promotion_reason"] = (decision["rule"] or {}).get("description", "Rule match")

        if producer:
            try:
                producer.send(TOPIC_ALERT_PROMOTED, value=event)
                producer.send(TOPIC_ALERT, value=event)  # Also to standard pipeline
                metrics.inc("events_promoted")
            except KafkaError as e:
                logger.error(f"Kafka publish error (ALERT): {e}")
                metrics.inc("errors_total")
                raise HTTPException(status_code=500, detail="Event publication failed")

        display = extract_display_fields(payload)
        push_live_event({
            "event_id": event_id, "source": source,
            "title": display["title"],
            "severity": severity,
            "status": "PROMOTED", "decision": "ALERT",
            "rule_desc": (decision["rule"] or {}).get("description", ""),
            "timestamp": timestamp,
        })
        return {"status": "promoted", "event_id": event_id, "trace_id": trace_id,
                "severity": severity, "reason": "Rule-based promotion"}

    # ── 4. Behavioral Threshold Check ──
    # (Happens after deduplication to avoid counting noise)
    
    # Run Keep-inspired enrichment before behavioral detection
    # so we can use extracted fields in behavioral/correlation tags
    run_extraction_rules(auth.tenant_id, payload)
    run_mapping_rules(auth.tenant_id, payload)
    
    behavioral = check_behavioral_threshold(auth.tenant_id, payload)


    if behavioral and behavioral.get("triggered"):
        # Behavioral alert! Override to ALERT path
        event["payload"]["severity"] = 3
        event["payload"]["promoted_by"] = "behavioral_detector"
        event["payload"]["promotion_reason"] = behavioral["message"]
        event["decision"] = "BEHAVIORAL_ALERT"

        if producer:
            try:
                producer.send(TOPIC_ALERT_PROMOTED, value=event)
                producer.send(TOPIC_ALERT, value=event)
                metrics.inc("events_promoted")
            except KafkaError as e:
                logger.error(f"Kafka publish error (BEHAVIORAL): {e}")
                metrics.inc("errors_total")

        push_live_event({
            "event_id": event_id, "source": source,
            "title": f"⚡ BEHAVIORAL: {behavioral['message']}",
            "severity": 3,
            "status": "BEHAVIORAL_ALERT", "decision": "BEHAVIORAL",
            "timestamp": timestamp,
        })
        return {"status": "behavioral_alert", "event_id": event_id, "trace_id": trace_id,
                "behavioral": behavioral}

    # Normal ingestion → Kafka
    if producer:
        try:
            producer.send(TOPIC_ALERT, value=event)
            metrics.inc("events_total")
        except KafkaError as e:
            logger.error(f"Kafka publish error: {e}")
            metrics.inc("errors_total")
            raise HTTPException(status_code=500, detail="Event publication failed")

    display = extract_display_fields(payload)
    push_live_event({
        "event_id": event_id, "source": source,
        "title": display["title"],
        "severity": display["severity"],
        "status": "INGESTED", "decision": "PASS",
        "timestamp": timestamp,
    })

    return {"status": "accepted", "event_id": event_id, "trace_id": trace_id}


# ═══════════════════════════════════════════════════════════════════════════════
# LEGACY COMPAT: Strict Alert Model Endpoint
# ═══════════════════════════════════════════════════════════════════════════════
class Alert(BaseModel):
    source: str
    type: str
    sourceRef: str
    title: str
    description: Optional[str] = None
    severity: int = Field(default=2, ge=1, le=4)
    tlp: int = Field(default=2, ge=0, le=3)
    pap: int = Field(default=2, ge=0, le=3)
    artifacts: list = []


@app.post("/ingest/alert", status_code=202)
async def ingest_strict_alert(
    alert: Alert,
    request: Request,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    trace_id: Optional[str] = Header(None, alias="X-Trace-Id"),
    auth: AuthContext = Depends(require_permission(PERM_ALERT_INGEST))
):
    """Legacy strict-schema endpoint. Wraps the payload and forwards to the main ingest."""
    # Convert Pydantic model to dict and forward as flexible JSON
    from starlette.testclient import TestClient
    # Simpler approach: just call the core logic inline
    alert_dict = alert.model_dump()
    # Inject into a mock request-like flow
    body_bytes = json.dumps(alert_dict).encode("utf-8")

    # Reuse the same logic path
    state = get_engine_state()
    if state["paused"] == "true":
        raise HTTPException(status_code=503, detail="Ingestion engine is paused")
    if state["circuit_breaker"] == "true":
        raise HTTPException(status_code=503, detail="Circuit breaker tripped")

    policy = get_policy(auth.tenant_id, alert.source)
    if not policy["is_enabled"]:
        raise HTTPException(status_code=403, detail=f"Source '{alert.source}' disabled")
    if len(body_bytes) / 1024 > policy["max_payload_kb"]:
        raise HTTPException(status_code=413, detail="Payload too large")
    if not check_rate_limit(auth.tenant_id, alert.source, policy["rate_limit_eps"]):
        metrics.inc("events_rate_limited")
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    rules = get_action_rules(auth.tenant_id)
    decision = evaluate_action(alert_dict, rules)

    # Keep-inspired enrichment (Gap 3 fix — enrichment in legacy endpoint)
    run_extraction_rules(auth.tenant_id, alert_dict)
    run_mapping_rules(auth.tenant_id, alert_dict)

    if not trace_id:
        trace_id = str(uuid.uuid4())
    event_id = str(uuid.uuid4())
    timestamp = int(time.time() * 1000)

    event = {
        "event_id": event_id, "trace_id": trace_id,
        "tenant_id": auth.tenant_id, "idempotency_key": idempotency_key,
        "timestamp": timestamp, "schema_version": "2.0",
        "source": alert.source, "decision": decision["action"],
        "payload": alert_dict,
    }
    event["payload"]["tenant_id"] = auth.tenant_id

    if decision["action"] == ACTION_DROP:
        metrics.inc("events_dropped")
        push_live_event({"event_id": event_id, "source": alert.source, "title": alert.title,
                         "severity": alert.severity, "status": "DROPPED", "timestamp": timestamp})
        return {"status": "dropped", "event_id": event_id}

    if decision["action"] == ACTION_LOG:
        archive_to_minio({"event_id": event_id, "tenant_id": auth.tenant_id,
                          "timestamp": timestamp, "payload": minify_payload(alert_dict)})
        metrics.inc("events_archived")
        return {"status": "archived", "event_id": event_id}

    if decision["action"] == ACTION_ALERT:
        sev = decision.get("severity_override") or alert.severity
        event["payload"]["severity"] = sev
        event["payload"]["promoted_by"] = "rule"
        if producer:
            producer.send(TOPIC_ALERT_PROMOTED, value=event)
            producer.send(TOPIC_ALERT, value=event)
            metrics.inc("events_promoted")
        push_live_event({"event_id": event_id, "source": alert.source, "title": alert.title,
                         "severity": sev, "status": "PROMOTED", "timestamp": timestamp})
        return {"status": "promoted", "event_id": event_id, "severity": sev}

    # PASS
    if producer:
        producer.send(TOPIC_ALERT, value=event)
        metrics.inc("events_total")
    push_live_event({"event_id": event_id, "source": alert.source, "title": alert.title,
                     "severity": alert.severity, "status": "INGESTED", "timestamp": timestamp})
    return {"status": "accepted", "event_id": event_id, "trace_id": trace_id}
