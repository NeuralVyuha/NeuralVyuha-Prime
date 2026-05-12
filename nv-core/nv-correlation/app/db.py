import psycopg2
import logging
import os
import json
import redis
from typing import Dict, Tuple, Optional, List

logger = logging.getLogger("correlation-db")

class Database:
    def __init__(self):
        self.conn = None
        self.host = os.getenv("POSTGRES_HOST", "postgres")
        self.port = int(os.getenv("POSTGRES_PORT", 5432))
        self.db = os.getenv("POSTGRES_DB", "nv_vault")
        self.user = os.getenv("POSTGRES_USER", "nv_user")
        self.password = os.getenv("POSTGRES_PASSWORD", "nv_pass")
        self.redis_host = os.getenv("REDIS_HOST", "redis")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        self._connect()
        self._ensure_schema()

    def _connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.host, port=self.port, database=self.db, user=self.user, password=self.password
            )
            self.conn.autocommit = True
            logger.info("Connected to Postgres")
        except Exception as e:
            logger.error(f"Postgres connection failed: {e}")
            raise

    def _ensure_schema(self):
        """Idempotent schema: CREATE tables first, then ALTER columns."""
        try:
            with self.conn.cursor() as cur:
                # ── Step 1: Create base tables (fresh DB support) ──
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS correlation_groups (
                        tenant_id VARCHAR(64) NOT NULL,
                        group_id VARCHAR(64) NOT NULL,
                        correlation_key TEXT NOT NULL,
                        rule_id VARCHAR(64) NOT NULL,
                        rule_name TEXT,
                        confidence VARCHAR(16) NOT NULL DEFAULT 'MEDIUM',
                        status VARCHAR(16) NOT NULL DEFAULT 'OPEN',
                        first_seen BIGINT NOT NULL,
                        last_seen BIGINT NOT NULL,
                        alert_count INT NOT NULL DEFAULT 1,
                        max_severity INT NOT NULL DEFAULT 1,
                        created_at BIGINT NOT NULL,
                        updated_at BIGINT NOT NULL,
                        PRIMARY KEY (tenant_id, group_id)
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS correlation_group_alert_links (
                        tenant_id VARCHAR(64) NOT NULL,
                        group_id VARCHAR(64) NOT NULL,
                        original_event_id VARCHAR(64) NOT NULL,
                        linked_at BIGINT NOT NULL,
                        link_reason TEXT,
                        PRIMARY KEY (tenant_id, group_id, original_event_id)
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS correlation_rules (
                        rule_id VARCHAR(64) PRIMARY KEY,
                        rule_name TEXT NOT NULL,
                        enabled BOOLEAN NOT NULL DEFAULT TRUE,
                        confidence VARCHAR(16) NOT NULL DEFAULT 'MEDIUM',
                        window_minutes INT NOT NULL DEFAULT 15,
                        correlation_key_template TEXT NOT NULL,
                        required_fields TEXT[] NOT NULL,
                        created_at BIGINT NOT NULL,
                        updated_at BIGINT NOT NULL
                    );
                """)
                # ── Seed initial rules ──
                cur.execute("""
                    INSERT INTO correlation_rules (rule_id, rule_name, enabled, confidence, window_minutes, correlation_key_template, required_fields, created_at, updated_at)
                    VALUES
                    ('R1_HOST_RULE', 'Same host + same rule_id', TRUE, 'HIGH', 15, '{tenant_id}|{host}|{rule_id}', ARRAY['tenant_id', 'host', 'rule_id'], 1700000000000, 1700000000000),
                    ('R2_USER_AUTH', 'Auth anomalies by user', TRUE, 'HIGH', 10, '{tenant_id}|{user}|{auth_type}', ARRAY['tenant_id', 'user', 'auth_type'], 1700000000000, 1700000000000),
                    ('R3_OBSERVABLE_HASH', 'Same observable hash', TRUE, 'MEDIUM', 30, '{tenant_id}|{observable_hash}|{observable_type}', ARRAY['tenant_id', 'observable_hash', 'observable_type'], 1700000000000, 1700000000000)
                    ON CONFLICT (rule_id) DO NOTHING;
                """)

                # ── Step 2: Idempotent column additions ──
                cur.execute("ALTER TABLE correlation_rules ADD COLUMN IF NOT EXISTS definition_cel TEXT;")
                cur.execute("ALTER TABLE correlation_rules ADD COLUMN IF NOT EXISTS name_template TEXT;")
                cur.execute("ALTER TABLE correlation_rules ADD COLUMN IF NOT EXISTS visibility_threshold INT DEFAULT 3;")
                # State Machine + SLA columns
                cur.execute("ALTER TABLE correlation_groups ADD COLUMN IF NOT EXISTS visibility_state TEXT DEFAULT 'HIDDEN';")
                cur.execute("ALTER TABLE correlation_groups ADD COLUMN IF NOT EXISTS visibility_threshold INT DEFAULT 3;")
                cur.execute("ALTER TABLE correlation_groups ADD COLUMN IF NOT EXISTS acknowledged_by TEXT;")
                cur.execute("ALTER TABLE correlation_groups ADD COLUMN IF NOT EXISTS acknowledged_at BIGINT;")
                cur.execute("ALTER TABLE correlation_groups ADD COLUMN IF NOT EXISTS dismissed_at BIGINT;")
                cur.execute("ALTER TABLE correlation_groups ADD COLUMN IF NOT EXISTS resolved_at BIGINT;")
                cur.execute("ALTER TABLE correlation_groups ADD COLUMN IF NOT EXISTS visible_at BIGINT;")
                cur.execute("ALTER TABLE correlation_groups ADD COLUMN IF NOT EXISTS sla_tta_ms BIGINT;")
                cur.execute("ALTER TABLE correlation_groups ADD COLUMN IF NOT EXISTS sla_ttr_ms BIGINT;")

            logger.info("Correlation schema verified — all tables and columns present")
        except Exception as e:
            logger.error(f"Correlation schema migration FAILED: {e}")


    def process_correlation(self, group: Dict, link: Dict) -> Tuple[bool, bool, int, int]:
        """
        Atomically ensures group exists, inserts link, and updates group stats if link is new.
        Returns (is_new_group, is_new_link, new_count, new_severity).
        """
        try:
            with self.conn.cursor() as cur:
                # 1. Ensure Group Exists (Idempotent Insert)
                insert_group_query = """
                INSERT INTO correlation_groups (
                    tenant_id, group_id, correlation_key, rule_id, rule_name, confidence, status,
                    first_seen, last_seen, alert_count, max_severity, created_at, updated_at
                ) VALUES (
                    %(tenant_id)s, %(group_id)s, %(correlation_key)s, %(rule_id)s, %(rule_name)s, %(confidence)s, %(status)s,
                    %(first_seen)s, %(last_seen)s, 1, %(max_severity)s, %(first_seen)s, %(last_seen)s
                )
                ON CONFLICT (tenant_id, group_id) DO NOTHING
                RETURNING TRUE;
                """
                cur.execute(insert_group_query, group)
                is_new_group = bool(cur.fetchone())

                # 2. Insert Link
                insert_link_query = """
                INSERT INTO correlation_group_alert_links (
                    tenant_id, group_id, original_event_id, linked_at, link_reason
                ) VALUES (
                    %(tenant_id)s, %(group_id)s, %(original_event_id)s, %(linked_at)s, %(link_reason)s
                )
                ON CONFLICT (tenant_id, group_id, original_event_id) DO NOTHING
                RETURNING TRUE;
                """
                cur.execute(insert_link_query, link)
                is_new_link = bool(cur.fetchone())

                new_count = 1
                new_severity = group['max_severity']

                # 3. If Link is New AND Group was NOT new (it existed), we must update stats
                if is_new_link and not is_new_group:
                    update_group_query = """
                    UPDATE correlation_groups
                    SET
                        alert_count = alert_count + 1,
                        last_seen = GREATEST(last_seen, %(last_seen)s),
                        max_severity = GREATEST(max_severity, %(max_severity)s),
                        updated_at = GREATEST(updated_at, %(last_seen)s)
                    WHERE tenant_id = %(tenant_id)s AND group_id = %(group_id)s
                    RETURNING alert_count, max_severity;
                    """
                    cur.execute(update_group_query, group)
                    row = cur.fetchone()
                    if row:
                        new_count, new_severity = row

                # Feature D: Auto-transition HIDDEN → VISIBLE
                threshold = group.get('visibility_threshold', 3)
                if new_count >= threshold:
                    cur.execute("""
                        UPDATE correlation_groups
                        SET visibility_state = 'VISIBLE',
                            visible_at = %(last_seen)s
                        WHERE tenant_id = %(tenant_id)s AND group_id = %(group_id)s
                          AND visibility_state = 'HIDDEN'
                    """, group)

                # Also re-activate DISMISSED groups if new alerts arrive
                if is_new_link:
                    cur.execute("""
                        UPDATE correlation_groups
                        SET visibility_state = 'VISIBLE', dismissed_at = NULL
                        WHERE tenant_id = %(tenant_id)s AND group_id = %(group_id)s
                          AND visibility_state = 'DISMISSED'
                    """, group)

                return is_new_group, is_new_link, new_count, new_severity

        except Exception as e:
            logger.error(f"Process correlation failed: {e}")
            raise

    def fetch_rules(self) -> List[Dict]:
        """
        Fetch all active rules: Redis L1 cache → Postgres fallback → re-cache.
        """
        REDIS_RULES_KEY = "correlation:active_rules"
        CACHE_TTL = 120

        # Try Redis L1 cache first
        try:
            r = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)
            cached = r.get(REDIS_RULES_KEY)
            if cached:
                rules = json.loads(cached)
                logger.debug(f"Correlation rules loaded from Redis cache ({len(rules)} rules)")
                return rules
        except Exception:
            pass  # Redis miss or unavailable

        # Fallback to Postgres
        query = "SELECT rule_id, rule_name, enabled, confidence, window_minutes, correlation_key_template, required_fields, definition_cel, name_template, COALESCE(visibility_threshold, 3) as visibility_threshold FROM correlation_rules WHERE enabled = TRUE"
        try:
            if self.conn.closed:
                self._connect()

            with self.conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                rules = []
                for r in rows:
                    rules.append({
                        "rule_id": r[0],
                        "rule_name": r[1],
                        "enabled": r[2],
                        "confidence": r[3],
                        "window_minutes": r[4],
                        "correlation_key_template": r[5],
                        "required_fields": r[6],
                        "definition_cel": r[7],
                        "name_template": r[8],
                        "visibility_threshold": r[9]
                    })

                # Cache in Redis for next time
                try:
                    rc = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)
                    rc.setex(REDIS_RULES_KEY, CACHE_TTL, json.dumps(rules))
                except Exception:
                    pass

                logger.info(f"Loaded {len(rules)} correlation rules from Postgres")
                return rules
        except Exception as e:
            logger.error(f"Fetch rules failed: {e}")
            return []

    def close(self):
        if self.conn:
            self.conn.close()
