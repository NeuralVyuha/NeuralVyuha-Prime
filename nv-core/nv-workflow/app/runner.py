"""
Workflow Runner v3 — Full-Scale DAG Workflow Engine.

Architecture:
  - Steps are connected via `next`, `on_true`, `on_false` pointers (DAG)
  - Supports: if_else branching, parallel execution, foreach loops, sub-workflows
  - Each step has an optional retry policy with exponential backoff
  - Wait steps are non-blocking (Redis sorted-set scheduler)
  - Full context checkpointing to Postgres for crash recovery
"""
import time
import logging
import json
import requests
import uuid
import psycopg2
import os
import threading
import re
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("workflow-runner")

# ── Checkpoint DB ─────────────────────────────────────────────────────────────

def _get_pg():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        database=os.getenv("POSTGRES_DB", "nv_vault"),
        user=os.getenv("POSTGRES_USER", "nv_user"),
        password=os.getenv("POSTGRES_PASSWORD", "nv_pass"),
        connect_timeout=5
    )


def ensure_checkpoint_schema():
    """Create checkpoint table if it doesn't exist."""
    try:
        conn = _get_pg()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS workflow_checkpoints (
                    execution_id TEXT PRIMARY KEY,
                    workflow_id TEXT NOT NULL,
                    tenant_id TEXT DEFAULT 'dev-tenant',
                    current_step_id TEXT,
                    total_steps INT DEFAULT 0,
                    status TEXT DEFAULT 'RUNNING',
                    context JSONB DEFAULT '{}',
                    step_results JSONB DEFAULT '[]',
                    wait_until BIGINT,
                    error TEXT,
                    created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
                    updated_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT
                );
            """)
            # Migration: add missing columns if upgrading from older workflow versions
            cur.execute("""
                ALTER TABLE workflow_checkpoints
                ADD COLUMN IF NOT EXISTS current_step_id TEXT,
                ADD COLUMN IF NOT EXISTS step_results JSONB DEFAULT '[]';
            """)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Checkpoint schema check: {e}")


# ── Redis Scheduler ───────────────────────────────────────────────────────────

class RedisScheduler:
    """Redis sorted-set based timer wheel for deferred workflow resumption."""

    SCHEDULE_KEY = "nv:workflow:schedule"

    def __init__(self):
        self._redis = None

    def _get_redis(self):
        if self._redis is None:
            try:
                import redis as redis_lib
                host = os.getenv("REDIS_HOST", "redis")
                port = int(os.getenv("REDIS_PORT", "6379"))
                self._redis = redis_lib.Redis(host=host, port=port, decode_responses=True)
                self._redis.ping()
                logger.info(f"Redis scheduler connected to {host}:{port}")
            except Exception as e:
                logger.warning(f"Redis unavailable, falling back to Postgres polling: {e}")
                self._redis = None
        return self._redis

    def schedule_resume(self, execution_id: str, resume_at_ms: int, metadata: dict = None):
        """Schedule a workflow to resume at a future time."""
        r = self._get_redis()
        if r:
            value = json.dumps({"execution_id": execution_id, **(metadata or {})})
            r.zadd(self.SCHEDULE_KEY, {value: resume_at_ms})
            logger.info(f"Scheduled resume for {execution_id} at {resume_at_ms}")
        # Postgres checkpoint is always written as a fallback

    def pop_ready(self) -> List[dict]:
        """Pop all scheduled items whose time has arrived."""
        r = self._get_redis()
        if not r:
            return []
        now = int(time.time() * 1000)
        try:
            items = r.zrangebyscore(self.SCHEDULE_KEY, 0, now)
            results = []
            for item in items:
                r.zrem(self.SCHEDULE_KEY, item)
                results.append(json.loads(item))
            return results
        except Exception as e:
            logger.warning(f"Redis pop_ready failed: {e}")
            return []


# Global scheduler instance
scheduler = RedisScheduler()


# ── Template Renderer ─────────────────────────────────────────────────────────

def render_template(template: str, context: dict) -> str:
    """Render {{variable}} templates from context."""
    if not isinstance(template, str):
        return template
    def replacer(match):
        key = match.group(1).strip()
        # Support nested keys like event.host
        val = context
        for part in key.split("."):
            if isinstance(val, dict):
                val = val.get(part, match.group(0))
            else:
                return match.group(0)
        return str(val)
    return re.sub(r'\{\{(.+?)\}\}', replacer, template)


# ── Retry Policy ──────────────────────────────────────────────────────────────

class RetryPolicy:
    """Configurable retry with exponential backoff."""

    def __init__(self, config: dict = None):
        config = config or {}
        self.max_retries = config.get("max", 0)
        self.backoff = config.get("backoff", "fixed")  # "fixed" | "exponential"
        self.initial_delay_ms = config.get("initial_delay_ms", 1000)

    def execute_with_retry(self, func, step_name: str):
        """Execute func with retry policy. Returns (result, attempts)."""
        last_error = None
        for attempt in range(1 + self.max_retries):
            try:
                result = func()
                return result, attempt + 1
            except Exception as e:
                last_error = e
                if attempt < self.max_retries:
                    delay_ms = self.initial_delay_ms
                    if self.backoff == "exponential":
                        delay_ms = self.initial_delay_ms * (2 ** attempt)
                    delay_s = min(delay_ms / 1000.0, 30.0)  # Cap at 30s
                    logger.warning(
                        f"Step '{step_name}' failed (attempt {attempt + 1}/{1 + self.max_retries}), "
                        f"retrying in {delay_s}s: {e}"
                    )
                    time.sleep(delay_s)
        raise last_error


# ── DAG Workflow Runner ───────────────────────────────────────────────────────

class WorkflowRunner:
    """
    Full-scale DAG-based workflow runner.

    Steps are defined with:
      - id: unique step identifier
      - type: step handler type
      - config: step-specific configuration
      - next: default next step id (None = end)
      - on_true / on_false: conditional branching (for if_else)
      - retry: { max, backoff, initial_delay_ms }
      - on_failure: "continue" | "abort" (default: continue)
    """

    def __init__(self, definition: Dict, trigger_event: Dict, tenant_id: str,
                 execution_id: str = None, start_step_id: str = None,
                 prior_context: Dict = None, prior_results: List = None):
        self.definition = definition
        self.steps_list = definition.get("steps", [])
        self.steps_map = self._build_steps_map(self.steps_list)
        self.trigger_event = trigger_event
        self.tenant_id = tenant_id
        self.context = prior_context or {**trigger_event}
        self.execution_id = execution_id or str(uuid.uuid4())
        self.step_results = prior_results or []
        self.start_step_id = start_step_id
        self.status = "RUNNING"
        self.error = None
        self._should_stop = False
        self.wait_until = None

        # Service URLs
        self.case_engine_url = os.getenv("NV_CASE_ENGINE_URL", "http://nv-case-engine:8000")
        self.ingest_url = os.getenv("NV_INGEST_URL", "http://nv-ingest:8000")

    @staticmethod
    def _build_steps_map(steps_list: List[Dict]) -> Dict[str, Dict]:
        """Build a lookup map from step id to step definition."""
        smap = {}
        for i, step in enumerate(steps_list):
            step_id = step.get("id", f"step_{i}")
            step["id"] = step_id  # Ensure id is always set
            smap[step_id] = step
        return smap

    def _get_first_step_id(self) -> Optional[str]:
        """Get the first step id in the workflow."""
        if not self.steps_list:
            return None
        return self.steps_list[0].get("id", "step_0")

    def execute(self) -> Dict[str, Any]:
        """Execute the DAG starting from start_step_id or the first step."""
        started_at = int(time.time() * 1000)

        current_id = self.start_step_id or self._get_first_step_id()

        visited = set()  # Cycle detection
        max_steps = 500  # Safety limit
        steps_executed = 0

        while current_id and not self._should_stop and steps_executed < max_steps:
            if current_id in visited:
                logger.warning(f"[{self.execution_id}] Cycle detected at step '{current_id}', stopping")
                break
            visited.add(current_id)

            step = self.steps_map.get(current_id)
            if not step:
                logger.error(f"[{self.execution_id}] Step '{current_id}' not found in definition")
                self.status = "FAILED"
                self.error = f"Step '{current_id}' not found"
                break

            step_type = step.get("type", "unknown")
            config = step.get("config", {})
            retry_config = step.get("retry", {})
            on_failure = step.get("on_failure", "continue")

            logger.info(f"[{self.execution_id}] Executing: {current_id} ({step_type})")

            try:
                retry_policy = RetryPolicy(retry_config)
                result, attempts = retry_policy.execute_with_retry(
                    lambda: self._execute_step(step_type, config, step),
                    current_id
                )

                self.step_results.append({
                    "step": current_id, "type": step_type,
                    "status": "SUCCESS", "result": result,
                    "attempts": attempts
                })

                # Merge result into context
                if isinstance(result, dict):
                    self.context.update(result)

                # NON-BLOCKING WAIT: return WAITING status
                if step_type == "wait" and self.status == "WAITING":
                    next_step = step.get("next")
                    self._checkpoint(current_step_id=next_step)
                    scheduler.schedule_resume(self.execution_id, self.wait_until, {
                        "workflow_id": self.definition.get("workflow_id"),
                        "tenant_id": self.tenant_id,
                    })
                    return self._build_result(started_at)

                # Checkpoint after each successful step
                next_step = self._resolve_next(step, result)
                self._checkpoint(current_step_id=next_step)

                current_id = next_step
                steps_executed += 1

            except Exception as e:
                logger.error(f"[{self.execution_id}] Step '{current_id}' failed: {e}")
                self.step_results.append({
                    "step": current_id, "type": step_type,
                    "status": "FAILED", "error": str(e)
                })

                if on_failure == "abort":
                    self.status = "FAILED"
                    self.error = f"Step '{current_id}' failed: {e}"
                    self._checkpoint(current_step_id=current_id)
                    break
                else:
                    # Continue to next step
                    current_id = step.get("next")
                    steps_executed += 1

        if self.status == "RUNNING":
            self.status = "COMPLETED"
            self._delete_checkpoint()

        return self._build_result(started_at)

    def _resolve_next(self, step: Dict, result: Any) -> Optional[str]:
        """Resolve the next step based on step type and result."""
        step_type = step.get("type")

        # if_else branching
        if step_type == "if_else":
            condition_result = result.get("result", True) if isinstance(result, dict) else bool(result)
            if condition_result:
                return step.get("on_true", step.get("next"))
            else:
                return step.get("on_false", step.get("next"))

        # Default: follow `next` pointer
        return step.get("next")

    def _build_result(self, started_at: int) -> Dict:
        finished_at = int(time.time() * 1000)
        return {
            "execution_id": self.execution_id,
            "workflow_id": self.definition.get("workflow_id", "unknown"),
            "status": self.status,
            "steps_executed": len(self.step_results),
            "total_steps": len(self.steps_list),
            "duration_ms": finished_at - started_at,
            "step_results": self.step_results,
            "error": self.error,
            "wait_until": self.wait_until,
            "started_at": started_at,
        }

    # ── Checkpointing ─────────────────────────────────────────────────────────

    def _checkpoint(self, current_step_id: str = None):
        """Persist execution state to Postgres for crash recovery."""
        try:
            conn = _get_pg()
            now = int(time.time() * 1000)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO workflow_checkpoints
                        (execution_id, workflow_id, tenant_id, current_step_id, total_steps,
                         status, context, step_results, wait_until, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)
                    ON CONFLICT (execution_id) DO UPDATE SET
                        current_step_id = EXCLUDED.current_step_id,
                        status = EXCLUDED.status,
                        context = EXCLUDED.context,
                        step_results = EXCLUDED.step_results,
                        wait_until = EXCLUDED.wait_until,
                        updated_at = EXCLUDED.updated_at
                """, (
                    self.execution_id, self.definition.get("workflow_id"),
                    self.tenant_id, current_step_id, len(self.steps_list),
                    self.status, json.dumps(self.context),
                    json.dumps(self.step_results), self.wait_until, now
                ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning(f"Checkpoint save failed: {e}")

    def _delete_checkpoint(self):
        """Remove checkpoint after successful completion."""
        try:
            conn = _get_pg()
            with conn.cursor() as cur:
                cur.execute("DELETE FROM workflow_checkpoints WHERE execution_id = %s",
                           (self.execution_id,))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning(f"Checkpoint cleanup failed: {e}")

    @classmethod
    def resume(cls, execution_id: str, definition: Dict) -> Optional['WorkflowRunner']:
        """Load a checkpointed execution and build a runner to resume it."""
        try:
            conn = _get_pg()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT workflow_id, tenant_id, current_step_id, context, step_results
                    FROM workflow_checkpoints
                    WHERE execution_id = %s AND status IN ('RUNNING', 'WAITING')
                """, (execution_id,))
                row = cur.fetchone()
            conn.close()
            if not row:
                return None

            wf_id, tenant_id, current_step_id, context, step_results = row
            if isinstance(context, str):
                context = json.loads(context)
            if isinstance(step_results, str):
                step_results = json.loads(step_results)

            return cls(
                definition=definition,
                trigger_event={},
                tenant_id=tenant_id,
                execution_id=execution_id,
                start_step_id=current_step_id,
                prior_context=context,
                prior_results=step_results,
            )
        except Exception as e:
            logger.error(f"Resume failed for {execution_id}: {e}")
            return None

    @staticmethod
    def get_waiting_checkpoints() -> List[Dict]:
        """Fetch all WAITING checkpoints that are past their wait_until time."""
        # First try Redis scheduler
        redis_items = scheduler.pop_ready()
        if redis_items:
            return redis_items

        # Fallback to Postgres polling
        try:
            conn = _get_pg()
            now = int(time.time() * 1000)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT execution_id, workflow_id, tenant_id, current_step_id, context, step_results
                    FROM workflow_checkpoints
                    WHERE status = 'WAITING' AND wait_until <= %s
                """, (now,))
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            conn.close()
            return rows
        except Exception as e:
            logger.warning(f"Failed to fetch waiting checkpoints: {e}")
            return []

    # ── Step Handlers ─────────────────────────────────────────────────────────

    def _execute_step(self, step_type: str, config: Dict, step_def: Dict = None) -> Any:
        handlers = {
            "enrich": self._step_enrich,
            "notify": self._step_notify,
            "wait": self._step_wait,
            "condition": self._step_condition,
            "if_else": self._step_if_else,
            "create_case": self._step_create_case,
            "http": self._step_http,
            "set_variable": self._step_set_variable,
            "transform": self._step_transform,
            "parallel": lambda c: self._step_parallel(c, step_def),
            "foreach": lambda c: self._step_foreach(c, step_def),
            "call_workflow": self._step_call_workflow,
        }
        handler = handlers.get(step_type)
        if not handler:
            raise ValueError(f"Unknown step type: {step_type}")
        return handler(config)

    # ── Core Step Types ───────────────────────────────────────────────────────

    def _step_enrich(self, config: Dict) -> Dict:
        """Add/transform context fields using template variables."""
        fields = config.get("fields", {})
        enriched = {}
        for key, template in fields.items():
            enriched[key] = render_template(template, self.context)
        self.context.update(enriched)
        return {"enriched_fields": list(enriched.keys())}

    def _step_notify(self, config: Dict) -> Dict:
        """Send webhook/log notification."""
        url = config.get("url")
        message = render_template(config.get("message", "Workflow notification"), self.context)
        channel = config.get("channel", "#alerts")

        if url:
            try:
                resp = requests.post(url, json={
                    "text": message,
                    "channel": channel,
                    "workflow_id": self.definition.get("workflow_id"),
                    "execution_id": self.execution_id,
                }, timeout=10)
                return {"notified": True, "status_code": resp.status_code}
            except Exception as e:
                logger.warning(f"Notify webhook failed: {e}")
                return {"notified": False, "error": str(e)}
        else:
            logger.info(f"NOTIFICATION: {message}")
            return {"notified": True, "method": "log", "message": message}

    def _step_wait(self, config: Dict) -> Dict:
        """NON-BLOCKING WAIT: sets WAITING status with Redis-backed scheduling."""
        seconds = config.get("seconds", 10)
        max_wait = 3600  # 1 hour max
        wait_time = min(seconds, max_wait)
        self.wait_until = int(time.time() * 1000) + (wait_time * 1000)
        self.status = "WAITING"
        logger.info(f"Workflow WAITING for {wait_time}s (resumes at {self.wait_until})")
        return {"waited_seconds": wait_time, "wait_until": self.wait_until}

    def _step_condition(self, config: Dict) -> Dict:
        """Legacy condition step — stops remaining steps if False."""
        expression = config.get("expression", "true")
        result = self._eval_cel(expression)
        if not result:
            self.status = "COMPLETED"
            logger.info(f"Condition '{expression}' = False — ending workflow")
            self._should_stop = True
        return {"condition": expression, "result": result}

    # ── DAG Step Types ────────────────────────────────────────────────────────

    def _step_if_else(self, config: Dict) -> Dict:
        """Evaluate a CEL expression and return the result for DAG branching.

        The actual branching (on_true/on_false routing) is handled by _resolve_next().
        """
        expression = config.get("expression", "true")
        result = self._eval_cel(expression)
        branch = "on_true" if result else "on_false"
        logger.info(f"Branch: '{expression}' → {branch}")
        return {"condition": expression, "result": result, "branch": branch}

    def _step_parallel(self, config: Dict, step_def: Dict = None) -> Dict:
        """Execute a list of sub-steps concurrently using ThreadPoolExecutor."""
        sub_steps = config.get("steps", [])
        max_workers = config.get("max_workers", min(len(sub_steps), 8))
        results = []

        def run_sub_step(sub):
            sub_type = sub.get("type", "unknown")
            sub_config = sub.get("config", {})
            sub_name = sub.get("id", sub_type)
            try:
                result = self._execute_step(sub_type, sub_config)
                return {"step": sub_name, "status": "SUCCESS", "result": result}
            except Exception as e:
                return {"step": sub_name, "status": "FAILED", "error": str(e)}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(run_sub_step, s): s for s in sub_steps}
            for future in as_completed(futures):
                results.append(future.result())

        # Merge all successful results into context
        for r in results:
            if r["status"] == "SUCCESS" and isinstance(r.get("result"), dict):
                self.context.update(r["result"])

        failed = [r for r in results if r["status"] == "FAILED"]
        return {
            "parallel_results": results,
            "total": len(sub_steps),
            "succeeded": len(sub_steps) - len(failed),
            "failed": len(failed),
        }

    def _step_foreach(self, config: Dict, step_def: Dict = None) -> Dict:
        """Iterate over a context list and execute a sub-step for each item."""
        list_key = config.get("list", "items")
        item_key = config.get("item_var", "item")
        sub_step = config.get("step", {})
        sub_type = sub_step.get("type", "unknown")
        sub_config = sub_step.get("config", {})

        items = self.context.get(list_key, [])
        if not isinstance(items, list):
            items = [items]

        iteration_results = []
        for idx, item in enumerate(items):
            self.context[item_key] = item
            self.context["_index"] = idx
            try:
                result = self._execute_step(sub_type, sub_config)
                iteration_results.append({"index": idx, "status": "SUCCESS", "result": result})
            except Exception as e:
                iteration_results.append({"index": idx, "status": "FAILED", "error": str(e)})

        return {
            "iterations": len(items),
            "results": iteration_results,
            "succeeded": sum(1 for r in iteration_results if r["status"] == "SUCCESS"),
        }

    def _step_call_workflow(self, config: Dict) -> Dict:
        """Execute another workflow definition as a sub-workflow."""
        workflow_id = config.get("workflow_id")
        if not workflow_id:
            raise ValueError("call_workflow requires a workflow_id")

        # Load sub-workflow from Postgres
        try:
            conn = _get_pg()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT workflow_id, name, steps FROM workflow_definitions WHERE workflow_id = %s",
                    (workflow_id,)
                )
                row = cur.fetchone()
            conn.close()

            if not row:
                raise ValueError(f"Sub-workflow '{workflow_id}' not found")

            sub_def = {
                "workflow_id": row[0],
                "name": row[1],
                "steps": row[2] if isinstance(row[2], list) else json.loads(row[2]),
            }

            # Pass a subset of context as the trigger event
            pass_context = config.get("pass_context", True)
            trigger = dict(self.context) if pass_context else {}

            sub_runner = WorkflowRunner(sub_def, trigger, self.tenant_id)
            result = sub_runner.execute()

            # Merge sub-workflow context back
            if config.get("merge_context", True):
                self.context.update(sub_runner.context)

            return {
                "sub_workflow_id": workflow_id,
                "sub_execution_id": result["execution_id"],
                "sub_status": result["status"],
                "sub_steps_executed": result["steps_executed"],
            }
        except Exception as e:
            raise RuntimeError(f"Sub-workflow '{workflow_id}' failed: {e}")

    def _step_set_variable(self, config: Dict) -> Dict:
        """Set a context variable."""
        key = config.get("key")
        value = config.get("value")
        if key:
            rendered_value = render_template(str(value), self.context) if isinstance(value, str) else value
            self.context[key] = rendered_value
        return {"set": key, "value": self.context.get(key)}

    def _step_transform(self, config: Dict) -> Dict:
        """Render multiple template strings into context variables."""
        mappings = config.get("mappings", {})
        transformed = {}
        for key, template in mappings.items():
            transformed[key] = render_template(template, self.context)
        self.context.update(transformed)
        return {"transformed_fields": list(transformed.keys())}

    def _step_create_case(self, config: Dict) -> Dict:
        """Create a case in nv-case-engine."""
        title = render_template(config.get("title", "Workflow-Generated Case"), self.context)
        severity = config.get("severity", 2)
        description = render_template(config.get("description", ""), self.context)

        import jwt as pyjwt
        secret = os.getenv("JWT_SECRET_KEY", "prod-secret-change-me")
        token = pyjwt.encode({
            "sub": "workflow-engine", "tenant_id": self.tenant_id,
            "roles": ["SYSTEM_ADMIN"], "exp": int(time.time()) + 300
        }, secret, algorithm="HS256")

        try:
            resp = requests.post(
                f"{self.case_engine_url}/cases",
                json={"title": title, "severity": severity, "description": description},
                headers={"Authorization": f"Bearer {token}", "Idempotency-Key": str(uuid.uuid4())},
                timeout=10
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            raise RuntimeError(f"Case creation failed: {e}")

    def _step_http(self, config: Dict) -> Dict:
        """Generic HTTP call."""
        method = config.get("method", "GET").upper()
        url = render_template(config.get("url", ""), self.context)
        body = config.get("body")
        headers = config.get("headers", {})

        # Render templates in body
        if isinstance(body, dict):
            body = {k: render_template(str(v), self.context) if isinstance(v, str) else v
                    for k, v in body.items()}

        try:
            resp = requests.request(method, url, json=body, headers=headers, timeout=15)
            return {"status_code": resp.status_code, "body": resp.text[:500]}
        except Exception as e:
            raise RuntimeError(f"HTTP step failed: {e}")

    # ── Utility ───────────────────────────────────────────────────────────────

    def _eval_cel(self, expression: str) -> bool:
        """Evaluate a CEL expression against the current context."""
        if not expression or expression.strip().lower() == "true":
            return True
        try:
            import celpy
            env = celpy.Environment()
            ast = env.compile(expression)
            prgm = env.program(ast)
            activation = celpy.json_to_cel(self.context)
            return bool(prgm.evaluate(activation))
        except Exception as e:
            logger.warning(f"CEL eval failed: {e}, defaulting to True")
            return True
