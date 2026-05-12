"""
NV-Workflow Service v3 — Full-Scale DAG Workflow Engine.

Key capabilities:
  - DAG-based step execution with branching (if/else), parallel, foreach
  - Redis sorted-set timer wheel for deferred wait scheduling
  - Postgres checkpoint persistence for crash recovery
  - REST API for workflow CRUD, trigger, and execution history
  - FastAPI management plane + Kafka consumer data plane
"""
import os
import sys
import json
import time
import signal
import logging
import uuid
import threading
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaConsumer, KafkaProducer
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import uvicorn

# Path setup
_base_dir = os.path.dirname(__file__)
_paths_to_check = [
    os.path.abspath(os.path.join(_base_dir, '../../')),
    os.path.abspath(os.path.join(_base_dir, '../')),
]
for _p in _paths_to_check:
    if os.path.exists(os.path.join(_p, 'common')):
        sys.path.append(_p)
        break

from runner import WorkflowRunner, ensure_checkpoint_schema, scheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger("nv-workflow")

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "nv_vault")
POSTGRES_USER = os.getenv("POSTGRES_USER", "nv_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "nv_pass")

TRIGGER_TOPIC = "workflow.trigger.v1"
EXECUTION_LOG_TOPIC = "workflow.execution.v1"
RESUME_POLL_INTERVAL = int(os.getenv("RESUME_POLL_INTERVAL", "3"))
API_PORT = int(os.getenv("WORKFLOW_API_PORT", "8000"))

running = True

def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ── FastAPI Management Plane ──────────────────────────────────────────────────

app = FastAPI(title="NeuralVyuha Workflow Engine v3", version="3.0.0")

# ── Database ──────────────────────────────────────────────────────────────────

def get_pg():
    return psycopg2.connect(
        host=POSTGRES_HOST, database=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
        connect_timeout=5
    )


def ensure_schema():
    """Create workflow tables + checkpoint table."""
    try:
        conn = get_pg()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS workflow_definitions (
                    id SERIAL PRIMARY KEY,
                    workflow_id TEXT UNIQUE NOT NULL,
                    tenant_id TEXT DEFAULT 'dev-tenant',
                    name TEXT NOT NULL,
                    description TEXT DEFAULT '',
                    trigger_config JSONB DEFAULT '{}',
                    steps JSONB DEFAULT '[]',
                    enabled BOOLEAN DEFAULT TRUE,
                    created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
                    updated_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS workflow_executions (
                    id SERIAL PRIMARY KEY,
                    execution_id TEXT UNIQUE NOT NULL,
                    workflow_id TEXT NOT NULL,
                    tenant_id TEXT DEFAULT 'dev-tenant',
                    status TEXT DEFAULT 'RUNNING',
                    current_step_id TEXT,
                    total_steps INT DEFAULT 0,
                    started_at BIGINT,
                    finished_at BIGINT,
                    trigger_event JSONB DEFAULT '{}',
                    step_results JSONB DEFAULT '[]',
                    error TEXT,
                    created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT
                );
            """)
            # Migration for v3
            cur.execute("""
                ALTER TABLE workflow_executions
                ADD COLUMN IF NOT EXISTS current_step_id TEXT,
                ADD COLUMN IF NOT EXISTS step_results JSONB DEFAULT '[]';
            """)
        conn.commit()
        conn.close()
        logger.info("Workflow schema ensured")
    except Exception as e:
        logger.error(f"Schema creation failed: {e}")

    # Also ensure checkpoint table
    ensure_checkpoint_schema()


def load_workflows(tenant_id: str = None):
    try:
        conn = get_pg()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if tenant_id:
                cur.execute(
                    "SELECT * FROM workflow_definitions WHERE enabled = TRUE AND (tenant_id = %s OR tenant_id = 'dev-tenant')",
                    (tenant_id,)
                )
            else:
                cur.execute("SELECT * FROM workflow_definitions WHERE enabled = TRUE")
            rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Failed to load workflows: {e}")
        return []


def load_workflow_by_id(workflow_id: str):
    """Load a single workflow definition by ID."""
    try:
        conn = get_pg()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM workflow_definitions WHERE workflow_id = %s", (workflow_id,))
            row = cur.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception as e:
        logger.error(f"Failed to load workflow {workflow_id}: {e}")
        return None


def record_execution(execution_result: dict):
    try:
        conn = get_pg()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO workflow_executions
                    (execution_id, workflow_id, tenant_id, status, total_steps,
                     started_at, finished_at, step_results, error)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (execution_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    finished_at = EXCLUDED.finished_at,
                    step_results = EXCLUDED.step_results,
                    error = EXCLUDED.error
            """, (
                execution_result.get("execution_id"),
                execution_result.get("workflow_id"),
                execution_result.get("tenant_id", "dev-tenant"),
                execution_result.get("status"),
                execution_result.get("total_steps"),
                execution_result.get("started_at", int(time.time() * 1000)),
                execution_result.get("started_at", int(time.time() * 1000)) + execution_result.get("duration_ms", 0),
                json.dumps(execution_result.get("step_results", [])),
                execution_result.get("error"),
            ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to record execution: {e}")


# ── CEL Guard Evaluation ─────────────────────────────────────────────────────

def evaluate_cel_guard(expression: str, event_data: dict) -> bool:
    if not expression:
        return True
    try:
        import celpy
        env = celpy.Environment()
        ast = env.compile(expression)
        prgm = env.program(ast)
        activation = celpy.json_to_cel(event_data)
        return bool(prgm.evaluate(activation))
    except Exception as e:
        logger.warning(f"CEL guard eval failed: {e}")
        return False


# ── Execute a single workflow ─────────────────────────────────────────────────

def execute_workflow(wf: dict, payload: dict, tenant_id: str, producer=None):
    """Execute a workflow definition against a payload."""
    definition = {
        "workflow_id": wf["workflow_id"],
        "name": wf["name"],
        "steps": wf.get("steps", []),
    }

    logger.info(f"Triggering workflow '{wf['name']}' ({wf['workflow_id']})")

    runner = WorkflowRunner(definition, payload, tenant_id)
    result = runner.execute()
    result["tenant_id"] = tenant_id

    # Record final results (WAITING is handled by checkpoint)
    if result["status"] != "WAITING":
        record_execution(result)

    # Publish execution event
    if producer:
        try:
            producer.send(EXECUTION_LOG_TOPIC, {
                "event_id": str(uuid.uuid4()),
                "type": "WorkflowExecuted",
                "tenant_id": tenant_id,
                "timestamp": int(time.time() * 1000),
                "payload": {
                    "execution_id": result["execution_id"],
                    "workflow_id": result["workflow_id"],
                    "status": result["status"],
                    "steps_executed": result["steps_executed"],
                }
            })
            producer.flush()
        except Exception as e:
            logger.warning(f"Failed to publish execution event: {e}")

    status_msg = result["status"]
    if status_msg == "WAITING":
        wait_remaining = (result.get('wait_until', 0) - int(time.time() * 1000)) // 1000
        status_msg += f" (resumes in {wait_remaining}s)"
    logger.info(f"Workflow '{wf['name']}': {status_msg}")

    return result


# ── Deferred Wait Resume Thread ──────────────────────────────────────────────

def resume_waiting_workflows(producer):
    """Background thread: polls Redis/Postgres for WAITING workflows ready to resume."""
    logger.info("Deferred-resume thread started (Redis + Postgres fallback)")
    while running:
        try:
            waiting = WorkflowRunner.get_waiting_checkpoints()
            for cp in waiting:
                exec_id = cp.get("execution_id")
                wf_id = cp.get("workflow_id")
                tenant_id = cp.get("tenant_id", "dev-tenant")

                logger.info(f"Resuming WAITING workflow {exec_id} (wf: {wf_id})")

                wf_def = load_workflow_by_id(wf_id)
                if not wf_def:
                    logger.warning(f"Cannot resume {exec_id}: workflow {wf_id} not found")
                    continue

                definition = {
                    "workflow_id": wf_def["workflow_id"],
                    "name": wf_def["name"],
                    "steps": wf_def.get("steps", []),
                }

                runner = WorkflowRunner.resume(exec_id, definition)
                if not runner:
                    logger.warning(f"Cannot resume {exec_id}: checkpoint not found")
                    continue

                result = runner.execute()
                result["tenant_id"] = tenant_id
                record_execution(result)

                # Publish execution event
                try:
                    producer.send(EXECUTION_LOG_TOPIC, {
                        "event_id": str(uuid.uuid4()),
                        "type": "WorkflowResumed",
                        "tenant_id": tenant_id,
                        "timestamp": int(time.time() * 1000),
                        "payload": {
                            "execution_id": result["execution_id"],
                            "workflow_id": result["workflow_id"],
                            "status": result["status"],
                            "steps_executed": result["steps_executed"],
                        }
                    })
                    producer.flush()
                except Exception:
                    pass

                logger.info(f"Resumed workflow '{wf_def['name']}': {result['status']}")

        except Exception as e:
            logger.error(f"Resume thread error: {e}")

        time.sleep(RESUME_POLL_INTERVAL)
    logger.info("Deferred-resume thread stopped")


# ── Kafka Consumer Thread ─────────────────────────────────────────────────────

_kafka_producer = None

def kafka_consumer_thread():
    """Background thread: consumes Kafka trigger events and dispatches workflows."""
    global _kafka_producer
    try:
        consumer = KafkaConsumer(
            TRIGGER_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="nv-workflow-v3",
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        _kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        logger.error(f"Kafka connection failed: {e}")
        return

    # Start the deferred-resume background thread
    resume_thread = threading.Thread(
        target=resume_waiting_workflows, args=(_kafka_producer,), daemon=True
    )
    resume_thread.start()

    logger.info(f"Kafka consumer listening on topic: {TRIGGER_TOPIC}")

    while running:
        try:
            msg_pack = consumer.poll(timeout_ms=2000)

            for tp, messages in msg_pack.items():
                for message in messages:
                    event = message.value
                    tenant_id = event.get("tenant_id", "dev-tenant")
                    payload = event.get("payload", {})

                    workflows = load_workflows(tenant_id)

                    for wf in workflows:
                        trigger_config = wf.get("trigger_config", {})
                        cel_guard = trigger_config.get("cel_condition")

                        if cel_guard and not evaluate_cel_guard(cel_guard, payload):
                            continue

                        execute_workflow(wf, payload, tenant_id, _kafka_producer)

        except Exception as e:
            logger.error(f"Error in workflow loop: {e}")
            time.sleep(2)

    consumer.close()
    _kafka_producer.close()
    logger.info("Kafka consumer stopped")


# ── REST API Endpoints ────────────────────────────────────────────────────────

class WorkflowCreate(BaseModel):
    workflow_id: str
    name: str
    tenant_id: str = "dev-tenant"
    description: str = ""
    trigger_config: Dict[str, Any] = {}
    steps: List[Dict[str, Any]] = []
    enabled: bool = True

class WorkflowTrigger(BaseModel):
    workflow_id: str
    tenant_id: str = "dev-tenant"
    payload: Dict[str, Any] = {}


@app.on_event("startup")
def startup():
    ensure_schema()
    seed_example_workflows()
    # Start Kafka consumer in background thread
    t = threading.Thread(target=kafka_consumer_thread, daemon=True)
    t.start()
    logger.info("NV-Workflow Engine v3 started (DAG + Redis scheduler)")


# ── Cascading Health Checks ───────────────────────────────────────────────────

def _check_postgres():
    try:
        conn = get_pg()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        return True
    except Exception:
        return False

def _check_redis():
    try:
        import redis as redis_lib
        r = redis_lib.Redis(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            socket_timeout=3
        )
        return r.ping()
    except Exception:
        return False

def _check_kafka():
    try:
        from kafka import KafkaConsumer
        c = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=3000,
            api_version_auto_timeout_ms=3000
        )
        topics = c.topics()
        c.close()
        return len(topics) > 0
    except Exception:
        return False


@app.get("/health")
def health():
    """Cascading health check — verifies all downstream dependencies."""
    import time as t
    checks = {}
    overall = "ok"

    for name, fn in [("postgres", _check_postgres), ("redis", _check_redis), ("kafka", _check_kafka)]:
        start = t.time()
        try:
            ok = fn()
            ms = round((t.time() - start) * 1000, 1)
            checks[name] = {"status": "ok" if ok else "fail", "response_ms": ms}
            if not ok:
                overall = "degraded"
        except Exception as e:
            ms = round((t.time() - start) * 1000, 1)
            checks[name] = {"status": "error", "error": str(e), "response_ms": ms}
            overall = "degraded"

    return {"status": overall, "version": "3.0.0", "engine": "DAG", "checks": checks}


@app.get("/healthz")
def healthz():
    """Lightweight liveness probe (Kubernetes)."""
    return {"status": "ok"}


@app.get("/workflows")
def list_workflows(tenant_id: str = None):
    return load_workflows(tenant_id)


@app.get("/workflows/{workflow_id}")
def get_workflow(workflow_id: str):
    wf = load_workflow_by_id(workflow_id)
    if not wf:
        raise HTTPException(404, "Workflow not found")
    return wf


@app.post("/workflows", status_code=201)
def create_workflow(body: WorkflowCreate):
    try:
        conn = get_pg()
        now = int(time.time() * 1000)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO workflow_definitions
                    (workflow_id, tenant_id, name, description, trigger_config, steps, enabled, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s)
                ON CONFLICT (workflow_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    trigger_config = EXCLUDED.trigger_config,
                    steps = EXCLUDED.steps,
                    enabled = EXCLUDED.enabled,
                    updated_at = EXCLUDED.updated_at
            """, (
                body.workflow_id, body.tenant_id, body.name, body.description,
                json.dumps(body.trigger_config), json.dumps(body.steps),
                body.enabled, now, now
            ))
        conn.commit()
        conn.close()
        return {"workflow_id": body.workflow_id, "status": "created"}
    except Exception as e:
        raise HTTPException(500, f"Failed to create workflow: {e}")


@app.delete("/workflows/{workflow_id}")
def delete_workflow(workflow_id: str):
    try:
        conn = get_pg()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM workflow_definitions WHERE workflow_id = %s", (workflow_id,))
        conn.commit()
        conn.close()
        return {"deleted": workflow_id}
    except Exception as e:
        raise HTTPException(500, f"Failed to delete: {e}")


@app.post("/workflows/trigger")
def trigger_workflow(body: WorkflowTrigger):
    """Manually trigger a workflow via the REST API."""
    wf = load_workflow_by_id(body.workflow_id)
    if not wf:
        raise HTTPException(404, "Workflow not found")

    result = execute_workflow(wf, body.payload, body.tenant_id, _kafka_producer)
    return result


@app.get("/executions")
def list_executions(workflow_id: str = None, status: str = None, limit: int = 20):
    try:
        conn = get_pg()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = "SELECT * FROM workflow_executions WHERE 1=1"
            params = []
            if workflow_id:
                query += " AND workflow_id = %s"
                params.append(workflow_id)
            if status:
                query += " AND status = %s"
                params.append(status)
            query += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(500, f"Failed to list executions: {e}")


@app.get("/executions/{execution_id}")
def get_execution(execution_id: str):
    try:
        conn = get_pg()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM workflow_executions WHERE execution_id = %s", (execution_id,))
            row = cur.fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, "Execution not found")
        return dict(row)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Failed to get execution: {e}")


# ── Seed Example DAG Workflows ───────────────────────────────────────────────

def seed_example_workflows():
    """Seed the database with example DAG workflows."""
    examples = [
        {
            "workflow_id": "brute_force_response",
            "name": "Brute Force Auto-Response",
            "description": "Full DAG: Enrich → Branch on severity → Notify or Log → Wait → Create Case",
            "trigger_config": {
                "cel_condition": "has(event.category) && event.category == 'authentication_failure'"
            },
            "steps": [
                {
                    "id": "enrich",
                    "type": "enrich",
                    "config": {
                        "fields": {
                            "threat_level": "HIGH",
                            "enriched_source": "{{source_ip}}",
                            "enriched_host": "{{host}}"
                        }
                    },
                    "next": "check_severity"
                },
                {
                    "id": "check_severity",
                    "type": "if_else",
                    "config": {
                        "expression": "threat_level == 'HIGH'"
                    },
                    "on_true": "notify_soc",
                    "on_false": "log_only"
                },
                {
                    "id": "notify_soc",
                    "type": "notify",
                    "config": {
                        "message": "🚨 HIGH THREAT: Brute force from {{enriched_source}} on {{enriched_host}}"
                    },
                    "next": "wait_for_ack"
                },
                {
                    "id": "log_only",
                    "type": "set_variable",
                    "config": {
                        "key": "action_taken",
                        "value": "logged_only"
                    },
                    "next": "done"
                },
                {
                    "id": "wait_for_ack",
                    "type": "wait",
                    "config": {"seconds": 60},
                    "next": "create_case"
                },
                {
                    "id": "create_case",
                    "type": "create_case",
                    "config": {
                        "title": "Auto: Brute Force from {{enriched_source}}",
                        "severity": 3,
                        "description": "Automated case from brute force detection on {{enriched_host}}"
                    },
                    "retry": {"max": 3, "backoff": "exponential", "initial_delay_ms": 1000},
                    "next": "done"
                },
                {
                    "id": "done",
                    "type": "set_variable",
                    "config": {"key": "workflow_complete", "value": "true"}
                }
            ]
        },
        {
            "workflow_id": "parallel_enrichment",
            "name": "Parallel Enrichment Pipeline",
            "description": "Enrich → Parallel (IP lookup + Hash check) → Create case",
            "trigger_config": {},
            "steps": [
                {
                    "id": "initial_enrich",
                    "type": "enrich",
                    "config": {
                        "fields": {"pipeline": "parallel_enrichment"}
                    },
                    "next": "parallel_lookups"
                },
                {
                    "id": "parallel_lookups",
                    "type": "parallel",
                    "config": {
                        "steps": [
                            {
                                "id": "ip_geo",
                                "type": "set_variable",
                                "config": {"key": "geo_country", "value": "US"}
                            },
                            {
                                "id": "hash_check",
                                "type": "set_variable",
                                "config": {"key": "hash_verdict", "value": "clean"}
                            },
                            {
                                "id": "reputation",
                                "type": "set_variable",
                                "config": {"key": "reputation_score", "value": "85"}
                            }
                        ]
                    },
                    "next": "summary"
                },
                {
                    "id": "summary",
                    "type": "transform",
                    "config": {
                        "mappings": {
                            "enrichment_summary": "Country={{geo_country}}, Hash={{hash_verdict}}, Rep={{reputation_score}}"
                        }
                    }
                }
            ]
        }
    ]

    try:
        conn = get_pg()
        now = int(time.time() * 1000)
        with conn.cursor() as cur:
            for wf in examples:
                cur.execute("""
                    INSERT INTO workflow_definitions
                        (workflow_id, tenant_id, name, description, trigger_config, steps, enabled, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s)
                    ON CONFLICT (workflow_id) DO NOTHING
                """, (
                    wf["workflow_id"], "dev-tenant", wf["name"], wf["description"],
                    json.dumps(wf["trigger_config"]), json.dumps(wf["steps"]),
                    True, now, now
                ))
        conn.commit()
        conn.close()
        logger.info(f"Seeded {len(examples)} example DAG workflows")
    except Exception as e:
        logger.warning(f"Seed workflows: {e}")


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=API_PORT)
