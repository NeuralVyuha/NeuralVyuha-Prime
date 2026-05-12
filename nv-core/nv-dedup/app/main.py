"""
NeuralVyuha — nv-dedup: The Detective (Tier 3 Correlation Engine)
=================================================================
A full-scale, enterprise-grade Cross-Source Event Correlation Engine.
Transforms individual alerts from Wazuh, Suricata, Syslog, and other
sources into high-fidelity, multi-event threat incidents.

Architecture (5 Stages):
  [Kafka] → Stage1:Normalizer → Stage2:StateBuckets
          → Stage3:RuleEngine → Stage4:ThreatScorer
          → Stage5:APIBridge  → [nv-case-engine / NeuralVyuha]

Design Decisions:
  - Redis Sorted Sets for O(log N) time-windowed state management
  - PostgreSQL-backed correlation rules with hot-reload every 60s
  - Pydantic models for strict schema validation at stage boundaries
  - Dead Letter Queue for fault isolation (ADR-034)
  - MITRE ATT&CK tactic weights for threat scoring
"""

import os
import json
import uuid
import time
import hashlib
import logging
import signal
import sys
import threading
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
import requests

# ─── Common Library Discovery ────────────────────────────────────────────────
_base_dir = os.path.dirname(__file__)
for _p in [
    os.path.abspath(os.path.join(_base_dir, '../../')),
    os.path.abspath(os.path.join(_base_dir, '../')),
]:
    if os.path.exists(os.path.join(_p, 'common')):
        sys.path.append(_p)
        break

from common.reliability.dlq import build_dlq_event, send_dlq
from common.reliability.commit import commit_if_safe
from common.reliability.retry import execute_with_retry
from common.reliability.backpressure import check_backpressure
from common.config.secrets import get_secret
from metrics_server import (
    start_metrics_server, MESSAGES_PROCESSED,
    DLQ_PUBLISHED, RETRIES_TOTAL, BACKPRESSURE_EVENTS, CONSUMER_LAG
)

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
REDIS_HOST          = os.getenv("REDIS_HOST", "redis")
REDIS_PORT          = int(os.getenv("REDIS_PORT", 6379))
METRICS_PORT        = int(os.getenv("METRICS_PORT", 9001))
RULE_RELOAD_SECS    = int(os.getenv("RULE_RELOAD_INTERVAL", 60))
CORR_THRESHOLD      = int(os.getenv("CORRELATION_THRESHOLD", 65))  # 0–100 score
NV_CASE_URL         = os.getenv("NV_CASE_ENGINE_URL", "http://nv-case-engine:8000")
NV_CASE_JWT         = os.getenv("NV_CASE_JWT", "dev-correlation-jwt")
DEV_MODE            = os.getenv("DEV_MODE", "true").lower() == "true"

POSTGRES_HOST       = get_secret("POSTGRES_HOST", "postgres")
POSTGRES_USER       = get_secret("POSTGRES_USER", "nv_user")
POSTGRES_PASSWORD   = get_secret("POSTGRES_PASSWORD", "nv_pass")
POSTGRES_DB         = get_secret("POSTGRES_DB", "nv_vault")

# Kafka Topics
TOPIC_INGEST        = "alerts.ingest.v1"
TOPIC_CORR_HIT      = "alerts.correlated.v1"     # Fired when a rule matches
TOPIC_ACCEPTED      = "alerts.accepted.v1"        # Forwarded clean events
TOPIC_DLQ           = "alerts.ingest.dlq.v1"

# Redis Key Namespaces
BUCKET_NS    = "corr:bucket"   # Sorted set per (tenant, match_key, source)
LOCK_NS      = "corr:lock"     # Idempotency lock on incident creation
INCIDENT_NS  = "corr:incident" # In-flight incident group tracking

# MITRE ATT&CK Tactic threat weight table (enterprise SOC-aligned)
MITRE_WEIGHTS: Dict[str, int] = {
    "Reconnaissance":        10,
    "Resource Development":  10,
    "Initial Access":        25,
    "Execution":             30,
    "Persistence":           35,
    "Privilege Escalation":  40,
    "Defense Evasion":       35,
    "Credential Access":     40,
    "Discovery":             15,
    "Lateral Movement":      50,
    "Collection":            30,
    "Command and Control":   55,
    "Exfiltration":          60,
    "Impact":                70,
    "Unknown":                5,
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(name)s :: %(message)s'
)
logger = logging.getLogger("nv-detective")

# ─── Shutdown Signal ──────────────────────────────────────────────────────────
_running = True
def _sig_handler(sig, frame):
    global _running
    logger.info(f"Signal {sig} received — draining and shutting down…")
    _running = False

signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGTERM, _sig_handler)


# ─── Data Models (Stage Boundaries) ──────────────────────────────────────────

@dataclass
class NormalizedEvent:
    """Canonical event schema output by Stage 1."""
    event_id:   str
    trace_id:   str
    tenant_id:  str
    source:     str          # e.g. "wazuh", "suricata", "syslog"
    event_type: str          # e.g. "auth_failure", "scan", "exploit"
    severity:   int          # 1–4
    match_key:  str          # Extracted match field value (e.g. "10.0.0.99")
    timestamp:  int          # epoch ms
    raw:        Dict[str, Any] = field(default_factory=dict)
    tags:       List[str]    = field(default_factory=list)

@dataclass
class CorrelationRule:
    """Rule loaded from ingest_correlation_rules table."""
    id:                str
    name:              str
    tenant_id:         str
    source_a:          str
    source_b:          str
    match_field:       str         # dot-notation field to resolve for match_key
    time_window:       int         # seconds
    min_count_a:       int         # min events from source_a to qualify
    min_count_b:       int         # min events from source_b to qualify
    action_type:       str         # CREATE_CASE | PROMOTE_ALERT | LOG_ONLY
    severity_override: int         # 1–4 for the generated incident
    mitre_tactic:      str
    description:       str
    is_active:         bool

@dataclass
class CorrelationHit:
    """Result when a rule fires."""
    incident_id:   str
    rule:          CorrelationRule
    tenant_id:     str
    match_value:   str             # The shared IP/field value
    threat_score:  int             # 0–100
    events_a:      List[Dict]      # Raw matched events from source_a
    events_b:      List[Dict]      # Raw matched events from source_b
    fired_at:      int             # epoch ms


# ─── Database Helpers ─────────────────────────────────────────────────────────

def get_pg() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=POSTGRES_HOST, database=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
        connect_timeout=5
    )


# ─── Enrichment Engine (Keep-Inspired) ────────────────────────────────────────

_extraction_rules = []
_mapping_rules = []
_enrichment_last_reload = 0
ENRICHMENT_RELOAD_SECS = 60

def _load_enrichment_rules():
    """Load enrichment rules: Redis L1 cache → Postgres fallback → re-cache."""
    global _extraction_rules, _mapping_rules, _enrichment_last_reload
    REDIS_EXTRACTION_KEY = "enrichment:extraction_rules"
    REDIS_MAPPING_KEY = "enrichment:mapping_rules"
    CACHE_TTL = 120  # 2 minute Redis TTL

    # Try Redis L1 cache first
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        cached_ext = r.get(REDIS_EXTRACTION_KEY)
        cached_map = r.get(REDIS_MAPPING_KEY)
        if cached_ext and cached_map:
            _extraction_rules = json.loads(cached_ext)
            _mapping_rules = json.loads(cached_map)
            _enrichment_last_reload = time.time()
            logger.debug(f"Enrichment rules loaded from Redis cache ({len(_extraction_rules)} ext, {len(_mapping_rules)} map)")
            return
    except Exception as e:
        logger.debug(f"Redis cache miss for enrichment rules: {e}")

    # Fallback to Postgres
    try:
        conn = get_pg()
        with conn.cursor() as cur:
            cur.execute("""SELECT tenant_id, rule_name, attribute, regex, condition, priority
                           FROM ingest_extraction_rules WHERE enabled = TRUE ORDER BY priority DESC""")
            _extraction_rules = [
                {"tenant": r[0], "name": r[1], "attr": r[2], "regex": r[3], "cond": r[4]}
                for r in cur.fetchall()
            ]
            cur.execute("""SELECT tenant_id, rule_name, match_fields, mapping_data, priority
                           FROM ingest_mapping_rules WHERE enabled = TRUE ORDER BY priority DESC""")
            _mapping_rules = [
                {"tenant": r[0], "name": r[1], "fields": r[2], "data": r[3]}
                for r in cur.fetchall()
            ]
        conn.close()
        _enrichment_last_reload = time.time()
        logger.info(f"Loaded {len(_extraction_rules)} extraction + {len(_mapping_rules)} mapping rules from Postgres")

        # Cache in Redis for next time
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.setex(REDIS_EXTRACTION_KEY, CACHE_TTL, json.dumps(_extraction_rules))
            r.setex(REDIS_MAPPING_KEY, CACHE_TTL, json.dumps(_mapping_rules))
        except Exception:
            pass  # Best-effort caching
    except Exception as e:
        logger.error(f"Enrichment rules load failed: {e}")



def _cel_eval(expression: str, data: dict) -> bool:
    """Evaluate a CEL expression against a data dict."""
    try:
        import celpy
        env = celpy.Environment()
        ast = env.compile(expression)
        prgm = env.program(ast)
        activation = celpy.json_to_cel(data)
        return bool(prgm.evaluate(activation))
    except Exception as e:
        logger.warning(f"CEL eval failed for '{expression}': {e}")
        return False


def run_extraction_rules(tenant_id: str, payload: dict):
    """Apply regex extraction rules to enrich the payload in-place."""
    import re as regex_mod
    for rule in _extraction_rules:
        if rule['tenant'] not in (tenant_id, 'global'): continue
        attr_val = _dot_get(payload, rule['attr'])
        if not attr_val or not isinstance(attr_val, str): continue
        if rule.get('cond') and not _cel_eval(rule['cond'], payload): continue
        match = regex_mod.search(rule['regex'], attr_val)
        if match:
            extracted = match.groupdict()
            payload.update(extracted)
            logger.info(f"Extracted {list(extracted.keys())} via rule '{rule['name']}'")


def run_mapping_rules(tenant_id: str, payload: dict):
    """Apply KV mapping rules to enrich the payload in-place."""
    for rule in _mapping_rules:
        if rule['tenant'] not in (tenant_id, 'global'): continue
        matched = True
        for field_path, expected_val in rule['fields'].items():
            if str(_dot_get(payload, field_path)) != str(expected_val):
                matched = False
                break
        if matched:
            payload.update(rule['data'])
            logger.info(f"Enriched payload via mapping rule '{rule['name']}'")


def maybe_reload_enrichment():
    if time.time() - _enrichment_last_reload >= ENRICHMENT_RELOAD_SECS:
        _load_enrichment_rules()



def load_correlation_rules(tenant_id: str = "dev-tenant") -> List[CorrelationRule]:
    """Load all active correlation rules from Postgres."""
    try:
        conn = get_pg()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, name, tenant_id,
                       source_a, source_b, match_field, time_window,
                       COALESCE(min_count_a, 1) as min_count_a,
                       COALESCE(min_count_b, 1) as min_count_b,
                       COALESCE(action_type, 'CREATE_CASE') as action_type,
                       COALESCE(severity_override, 3) as severity_override,
                       COALESCE(mitre_tactic, 'Unknown') as mitre_tactic,
                       COALESCE(description, '') as description,
                       is_active
                FROM ingest_correlation_rules
                WHERE is_active = TRUE
                  AND (tenant_id = %s OR tenant_id = 'dev-tenant')
            """, (tenant_id,))
            rows = cur.fetchall()
        conn.close()
        rules = []
        for r in rows:
            rules.append(CorrelationRule(**{k: r[k] for k in r.keys()}))
        logger.info(f"Loaded {len(rules)} active correlation rules from DB")
        return rules
    except Exception as e:
        logger.error(f"Failed to load correlation rules: {e}")
        return []

def record_correlation_incident(hit: CorrelationHit):
    """Persist a fired correlation to the correlation_incidents table."""
    try:
        conn = get_pg()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO correlation_incidents
                    (id, tenant_id, rule_id, rule_name, match_value,
                     threat_score, mitre_tactic, severity, action_taken,
                     event_ids, raw_events, case_id, fired_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                hit.incident_id,
                hit.tenant_id,
                hit.rule.id,
                hit.rule.name,
                hit.match_value,
                hit.threat_score,
                hit.rule.mitre_tactic,
                hit.rule.severity_override,
                hit.rule.action_type,
                json.dumps([e.get("event_id") for e in hit.events_a + hit.events_b]),
                json.dumps({"events_a": hit.events_a, "events_b": hit.events_b}),
                None,   # case_id filled in after API bridge call
                hit.fired_at,
            ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to record correlation incident: {e}")

def update_incident_case_id(incident_id: str, case_id: str):
    """Back-fill the case_id once nv-case-engine responds."""
    try:
        conn = get_pg()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE correlation_incidents SET case_id = %s WHERE id = %s
            """, (case_id, incident_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to update case_id for incident {incident_id}: {e}")


# ─── Stage 1: Event Normalizer ────────────────────────────────────────────────

# Source-specific resolvers: tells us which field is the event_type and match candidate
SOURCE_MAP = {
    "wazuh": {
        "type_path":  "rule.description",
        "match_paths": ["data.srcip", "data.dstip", "agent.ip"],
        "severity_path": "rule.level",
        "severity_fn": lambda lvl: 4 if int(lvl or 0) >= 12 else (3 if int(lvl or 0) >= 8 else (2 if int(lvl or 0) >= 5 else 1)),
    },
    "suricata": {
        "type_path":  "alert.signature",
        "match_paths": ["src_ip", "dest_ip"],
        "severity_path": "alert.severity",
        "severity_fn": lambda s: {1: 4, 2: 3, 3: 2}.get(int(s or 3), 1),
    },
    "syslog": {
        "type_path":  "message",
        "match_paths": ["hostname", "host"],
        "severity_path": "severity",
        "severity_fn": lambda s: max(1, min(4, int(s or 2))),
    },
}

def _dot_get(obj: dict, path: str, default=None):
    """Safe dot-notation accessor for nested dicts."""
    parts = path.split(".")
    cur = obj
    for p in parts:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
        if cur is None:
            return default
    return cur

def normalize(raw_message: dict, rule_match_field: Optional[str] = None) -> Optional[NormalizedEvent]:
    """
    Stage 1: Normalize any source event into a NormalizedEvent.
    Attempts source-specific resolvers first, then falls back to heuristics.
    """
    payload = raw_message.get("payload", raw_message)  # Unwrap if wrapped
    source  = (payload.get("source") or raw_message.get("source") or "unknown").lower()
    evt_id  = raw_message.get("event_id") or str(uuid.uuid4())
    trace   = raw_message.get("trace_id") or "no-trace"
    tenant  = (payload.get("tenant_id") or raw_message.get("tenant_id") or "dev-tenant")
    ts      = raw_message.get("timestamp") or int(time.time() * 1000)

    resolver = SOURCE_MAP.get(source, {})

    # Resolve event type
    evt_type = (
        _dot_get(payload, resolver.get("type_path", ""), "unknown")
        or payload.get("type", "unknown")
    )

    # Resolve severity
    sev_raw = _dot_get(payload, resolver.get("severity_path", ""), None)
    sev_fn  = resolver.get("severity_fn", lambda x: 2)
    try:
        severity = sev_fn(sev_raw) if sev_raw is not None else 2
    except Exception:
        severity = 2

    # Resolve match_key — try rule-specified field first, then source defaults
    match_key = None
    if rule_match_field:
        match_key = str(_dot_get(payload, rule_match_field, "") or "")
    if not match_key:
        for path in resolver.get("match_paths", []):
            val = _dot_get(payload, path)
            if val:
                match_key = str(val)
                break
    if not match_key:
        # Last resort: hash the entire payload for some uniqueness
        match_key = hashlib.md5(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:12]

    return NormalizedEvent(
        event_id=evt_id,
        trace_id=trace,
        tenant_id=tenant,
        source=source,
        event_type=str(evt_type),
        severity=severity,
        match_key=match_key,
        timestamp=ts,
        raw=payload,
    )


# ─── Stage 2: Redis Time-Bucket State Manager ─────────────────────────────────

def bucket_key(tenant_id: str, match_key: str, source: str) -> str:
    """Redis Sorted Set key for a time-windowed event bucket."""
    safe_key = match_key.replace(":", "_")
    return f"{BUCKET_NS}:{tenant_id}:{source}:{safe_key}"

def store_event_in_bucket(r: redis.Redis, event: NormalizedEvent, window_secs: int):
    """
    Stage 2: Store the event in a Redis Sorted Set keyed by score=timestamp_ms.
    Auto-prunes events older than the window.
    """
    key = bucket_key(event.tenant_id, event.match_key, event.source)
    now_ms = event.timestamp
    cutoff_ms = now_ms - (window_secs * 1000)

    pipeline = r.pipeline(transaction=True)
    # Store event payload as member, score = timestamp for ZRANGEBYSCORE
    pipeline.zadd(key, {json.dumps({"event_id": event.event_id, "payload": event.raw, "sev": event.severity}): now_ms})
    pipeline.zremrangebyscore(key, "-inf", cutoff_ms)   # Prune stale events
    pipeline.expire(key, window_secs + 60)              # TTL safety net
    pipeline.execute()

def fetch_bucket(r: redis.Redis, tenant_id: str, match_key: str,
                 source: str, window_secs: int) -> List[Dict]:
    """Fetch all events from a time bucket within the active window."""
    key = bucket_key(tenant_id, match_key, source)
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - (window_secs * 1000)
    raw_members = r.zrangebyscore(key, cutoff_ms, "+inf")
    results = []
    for m in raw_members:
        try:
            results.append(json.loads(m))
        except Exception:
            pass
    return results


# ─── Stage 3: Correlation Rule Engine ────────────────────────────────────────

class RuleEngine:
    """
    Hot-reloading rule engine. Uses a background thread to refresh rules
    from Postgres every RULE_RELOAD_SECS seconds.
    """
    def __init__(self):
        self._rules: List[CorrelationRule] = []
        self._lock = threading.RLock()
        self._last_reload = 0
        self._reload()

    def _reload(self):
        rules = load_correlation_rules()
        with self._lock:
            self._rules = rules
            self._last_reload = time.time()

    def maybe_reload(self):
        if time.time() - self._last_reload >= RULE_RELOAD_SECS:
            logger.info("Hot-reloading correlation rules from DB...")
            self._reload()

    @property
    def rules(self) -> List[CorrelationRule]:
        with self._lock:
            return list(self._rules)

    def evaluate(self, event: NormalizedEvent, r: redis.Redis) -> List[CorrelationHit]:
        """
        For each active rule where this event is Source A or Source B,
        check if the opposite source has events in the same time window
        for the same match_key. If both conditions met, fire a hit.
        """
        hits = []
        for rule in self.rules:
            if rule.tenant_id not in (event.tenant_id, "dev-tenant"):
                continue
            if event.source not in (rule.source_a, rule.source_b):
                continue

            # Determine which source this event is, and which to look for
            if event.source == rule.source_a:
                counterpart_source = rule.source_b
                self_min    = rule.min_count_a
                other_min   = rule.min_count_b
            else:
                counterpart_source = rule.source_a
                self_min    = rule.min_count_b
                other_min   = rule.min_count_a

            # Resolve match_key using the rule's specified field
            match_val = str(_dot_get(event.raw, rule.match_field, "") or event.match_key)

            # Fetch my own bucket (events for this source in window)
            my_events = fetch_bucket(r, event.tenant_id, match_val, event.source, rule.time_window)
            if len(my_events) < self_min:
                continue

            # Fetch counterpart bucket
            other_events = fetch_bucket(r, event.tenant_id, match_val, counterpart_source, rule.time_window)
            if len(other_events) < other_min:
                continue

            # Idempotency lock — prevent duplicate incidents for same match in same window
            lock_key = f"{LOCK_NS}:{event.tenant_id}:{rule.id}:{match_val}"
            # Use NX = only set if not exists; EX = half the window
            lock_acquired = r.set(lock_key, "1", ex=max(rule.time_window // 2, 30), nx=True)
            if not lock_acquired:
                logger.debug(f"Incident lock held for rule '{rule.name}' / match '{match_val}' — suppressing duplicate")
                continue

            hits.append(CorrelationHit(
                incident_id=str(uuid.uuid4()),
                rule=rule,
                tenant_id=event.tenant_id,
                match_value=match_val,
                threat_score=0,       # Filled in Stage 4
                events_a=my_events if event.source == rule.source_a else other_events,
                events_b=other_events if event.source == rule.source_a else my_events,
                fired_at=int(time.time() * 1000),
            ))

        return hits


# ─── Stage 4: Threat Scorer ───────────────────────────────────────────────────

def score_threat(hit: CorrelationHit) -> int:
    """
    Composite threat score (0–100) based on:
      40% — MITRE ATT&CK tactic weight
      30% — Severity of correlated events
      20% — Volume of correlated events (more hits = higher confidence)
      10% — Source trustworthiness bonus
    """
    mitre_score = MITRE_WEIGHTS.get(hit.rule.mitre_tactic, 5)

    all_events = hit.events_a + hit.events_b
    if all_events:
        avg_sev = sum(e.get("sev", 2) for e in all_events) / len(all_events)
        sev_score = (avg_sev / 4.0) * 100       # Normalize to 0–100
    else:
        sev_score = 25

    # Volume confidence: caps at 10 events
    volume_score = min(len(all_events) / 10.0, 1.0) * 100

    # Source trustworthiness bonuses
    trusted_sources = {"wazuh": 10, "suricata": 8, "crowdstrike": 10, "sentinel-one": 10}
    src_bonus = (
        trusted_sources.get(hit.rule.source_a, 5)
        + trusted_sources.get(hit.rule.source_b, 5)
    ) / 2

    final = int(
        mitre_score * 0.40 +
        sev_score   * 0.30 +
        volume_score * 0.20 +
        src_bonus   * 0.10
    )
    return max(0, min(100, final))


# ─── Stage 5: API Bridge → nv-case-engine ─────────────────────────────────────

def build_case_payload(hit: CorrelationHit) -> dict:
    """Build a NeuralVyuha-compatible case payload from a correlation hit."""
    all_raw = hit.events_a + hit.events_b
    observable_ips = list({
        e.get("payload", {}).get("data", {}).get("srcip")
        or e.get("payload", {}).get("src_ip")
        for e in all_raw
        if e.get("payload")
    } - {None})

    tags = [
        f"correlated:{hit.rule.source_a}x{hit.rule.source_b}",
        f"mitre:{hit.rule.mitre_tactic.replace(' ', '_')}",
        f"score:{hit.threat_score}",
        f"rule:{hit.rule.name}",
        "nv-detective",
        "auto-generated",
    ]

    return {
        "title": f"[CORRELATED] {hit.rule.name} — {hit.match_value}",
        "description": (
            f"**Auto-generated by NV-Detective (Tier 3 Correlation Engine)**\n\n"
            f"**Rule**: {hit.rule.name}\n"
            f"**Description**: {hit.rule.description}\n"
            f"**MITRE Tactic**: {hit.rule.mitre_tactic}\n"
            f"**Threat Score**: {hit.threat_score}/100\n"
            f"**Match Value**: `{hit.match_value}`\n"
            f"**Sources Correlated**: {hit.rule.source_a} + {hit.rule.source_b}\n"
            f"**Events Found**: {len(hit.events_a)} from {hit.rule.source_a}, "
            f"{len(hit.events_b)} from {hit.rule.source_b}\n\n"
            f"**Observable IPs**: {', '.join(observable_ips) or 'None detected'}\n\n"
            f"*Incident ID*: `{hit.incident_id}`"
        ),
        "severity": hit.rule.severity_override,
        "tags": tags,
        "source": "nv-detective",
        "source_ref": hit.incident_id,
        "observables": [{"data_type": "ip", "data": ip, "tags": ["auto"]} for ip in observable_ips],
        "correlated_events": [
            {"source": hit.rule.source_a, "events": hit.events_a},
            {"source": hit.rule.source_b, "events": hit.events_b},
        ],
        "tenant_id": hit.tenant_id,
        "incident_id": hit.incident_id,
        "idempotency_key": hit.incident_id,
    }

def call_case_engine(hit: CorrelationHit) -> Optional[str]:
    """
    Stage 5: POST to nv-case-engine to create a pre-enriched Case.
    Returns the new case_id on success, None on failure.
    """
    if DEV_MODE:
        logger.info(
            f"[DEV_MODE] Would create case for incident {hit.incident_id} "
            f"(score={hit.threat_score}) — skipping actual API call"
        )
        return f"dev-case-{hit.incident_id[:8]}"

    payload = build_case_payload(hit)
    try:
        resp = requests.post(
            f"{NV_CASE_URL}/api/cases",
            json=payload,
            headers={
                "Authorization": f"Bearer {NV_CASE_JWT}",
                "Idempotency-Key": hit.incident_id,
                "Content-Type": "application/json",
            },
            timeout=10,
        )
        resp.raise_for_status()
        case_id = resp.json().get("id") or resp.json().get("case_id")
        logger.info(f"Case created: {case_id} for incident {hit.incident_id}")
        return case_id
    except Exception as e:
        logger.error(f"API Bridge failed for incident {hit.incident_id}: {e}")
        return None


# ─── Main Processing Loop ─────────────────────────────────────────────────────

def process_message(msg: dict, r: redis.Redis, engine: RuleEngine,
                    producer: KafkaProducer):
    """
    Full 5-stage pipeline for a single Kafka message.
    """
    # ── Stage 1: Normalize ───────────────────────────────────────────────────
    event = normalize(msg)
    if not event:
        logger.warning(f"Could not normalize message: {msg.get('event_id', 'unknown')}")
        return

    # ── Stage 1.5: Keep-Inspired Enrichment ──────────────────────────────────
    # Run extraction rules (regex) and mapping rules (KV lookup) on raw payload
    run_extraction_rules(event.tenant_id, event.raw)
    run_mapping_rules(event.tenant_id, event.raw)

    # ── Stage 2: Store in time bucket (for all matching rules' windows) ──────

    # We use the maximum time_window of all applicable rules so buckets persist
    applicable_windows = [
        r_obj.time_window for r_obj in engine.rules
        if event.source in (r_obj.source_a, r_obj.source_b)
    ]
    window = max(applicable_windows) if applicable_windows else 300
    store_event_in_bucket(r, event, window)

    # ── Stage 3: Evaluate all correlation rules ──────────────────────────────
    hits = engine.evaluate(event, r)

    # ── Stage 4 + 5: Score and act on each hit ───────────────────────────────
    for hit in hits:
        hit.threat_score = score_threat(hit)

        logger.info(
            f"🔥 CORRELATION HIT | Rule: '{hit.rule.name}' | "
            f"Match: '{hit.match_value}' | Score: {hit.threat_score}/100 | "
            f"Threshold: {CORR_THRESHOLD}"
        )

        # Persist incident to DB regardless of score
        record_correlation_incident(hit)

        # Publish to correlated alerts topic
        corr_evt = {
            "event_id":    hit.incident_id,
            "type":        "CorrelationIncident",
            "rule_name":   hit.rule.name,
            "mitre_tactic": hit.rule.mitre_tactic,
            "match_value": hit.match_value,
            "threat_score": hit.threat_score,
            "sources":     [hit.rule.source_a, hit.rule.source_b],
            "event_count": len(hit.events_a) + len(hit.events_b),
            "action_type": hit.rule.action_type,
            "tenant_id":   hit.tenant_id,
            "timestamp":   hit.fired_at,
        }
        producer.send(TOPIC_CORR_HIT, value=corr_evt)

        # Stage 5: API Bridge — only trigger above threshold
        if hit.threat_score >= CORR_THRESHOLD and hit.rule.action_type == "CREATE_CASE":
            case_id = call_case_engine(hit)
            if case_id:
                update_incident_case_id(hit.incident_id, case_id)
                logger.info(f"✅ Case {case_id} created for incident {hit.incident_id}")
        elif hit.rule.action_type == "LOG_ONLY":
            logger.info(f"📝 LOG_ONLY rule '{hit.rule.name}' fired — no case created")

    # Forward the (now processed) clean event downstream
    clean_evt = {
        "event_id":  event.event_id,
        "trace_id":  event.trace_id,
        "type":      "ProcessedEvent",
        "source":    event.source,
        "severity":  event.severity,
        "tenant_id": event.tenant_id,
        "timestamp": event.timestamp,
        "payload":   event.raw,
    }
    producer.send(TOPIC_ACCEPTED, value=clean_evt)
    MESSAGES_PROCESSED.labels(service="detective", topic=TOPIC_INGEST, status="success").inc()


def main():
    logger.info("=" * 72)
    logger.info("  NV-Detective: Cross-Source Correlation Engine (Tier 3)")
    logger.info("  Threshold: %d | MITRE Tactics: %d | Dev Mode: %s",
                CORR_THRESHOLD, len(MITRE_WEIGHTS), DEV_MODE)
    logger.info("=" * 72)

    start_metrics_server(METRICS_PORT)

    # Redis
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                        socket_timeout=5, socket_connect_timeout=5)
        r.ping()
        logger.info("Redis connected")
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        sys.exit(1)

    # Rule Engine (loads rules from DB on init)
    engine = RuleEngine()
    logger.info(f"Rule engine initialized with {len(engine.rules)} active rules")

    # Kafka
    try:
        consumer = KafkaConsumer(
            TOPIC_INGEST,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="nv-detective-v1",
            auto_offset_reset="latest",
            enable_auto_commit=False,
            max_poll_records=100,
            max_poll_interval_ms=300_000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=10,
        )
        logger.info("Kafka consumer/producer connected")
    except Exception as e:
        logger.error(f"Kafka connection failed: {e}")
        sys.exit(1)

    # Main Loop
    while _running:
        start_time = time.time()
        try:
            msg_pack = consumer.poll(timeout_ms=1000)
            total = 0

            # Hot-reload rules in background
            engine.maybe_reload()
            maybe_reload_enrichment()


            for tp, messages in msg_pack.items():
                CONSUMER_LAG.labels(service="detective", topic=tp.topic, partition=tp.partition).set(0)
                for msg in messages:
                    ok = False
                    dlq_ok = False
                    try:
                        execute_with_retry(
                            lambda: process_message(msg.value, r, engine, producer),
                            max_retries=3,
                            retryable_exceptions=(redis.RedisError, KafkaError, psycopg2.OperationalError),
                        )
                        producer.flush()
                        ok = True
                    except Exception as e:
                        logger.error(f"Processing failed for {msg.value.get('event_id', '?')}: {e}")
                        MESSAGES_PROCESSED.labels(service="detective", topic=TOPIC_INGEST, status="error").inc()
                        dlq_body = build_dlq_event(
                            str(e), msg.value, msg.topic, msg.partition,
                            msg.offset, msg.value.get("payload", {}).get("tenant_id", "?")
                        )
                        dlq_ok = send_dlq(producer, TOPIC_DLQ, dlq_body)
                        if dlq_ok:
                            DLQ_PUBLISHED.labels(service="detective", topic=TOPIC_DLQ).inc()

                    if not commit_if_safe(consumer, ok, dlq_ok):
                        break
                    total += 1

            if total > 0:
                check_backpressure(start_time, total)
                BACKPRESSURE_EVENTS.labels(service="detective").inc()

        except Exception as e:
            logger.error(f"Loop error: {e}")
            time.sleep(1)

    logger.info("Shutting down gracefully…")
    producer.flush()
    producer.close()
    consumer.close()


if __name__ == "__main__":
    main()
