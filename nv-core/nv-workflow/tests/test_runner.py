"""
Tests for Workflow Runner v2 — Non-blocking, checkpoint-persistent.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))
from runner import WorkflowRunner


class TestWorkflowRunner:
    def _make_runner(self, steps, trigger=None):
        definition = {
            "workflow_id": "test-wf-1",
            "name": "Test Workflow",
            "steps": steps,
        }
        return WorkflowRunner(definition, trigger or {}, "dev-tenant")

    def test_enrich_step(self):
        runner = self._make_runner([
            {"name": "add_label", "type": "enrich", "config": {"fields": {"label": "critical", "env": "production"}}}
        ])
        result = runner.execute()
        assert result["status"] == "COMPLETED"
        assert result["steps_executed"] == 1
        assert runner.context.get("label") == "critical"

    def test_wait_step_returns_waiting(self):
        """v2: wait step should return WAITING status instead of blocking."""
        runner = self._make_runner([
            {"name": "short_wait", "type": "wait", "config": {"seconds": 10}},
            {"name": "after", "type": "enrich", "config": {"fields": {"reached": "yes"}}}
        ])
        result = runner.execute()
        assert result["status"] == "WAITING", f"Expected WAITING, got: {result['status']}"
        assert result["wait_until"] is not None
        assert result["steps_executed"] == 1  # Only the wait step executed
        assert runner.context.get("reached") is None  # After step not reached

    def test_notify_stub(self):
        runner = self._make_runner([
            {"name": "log_notify", "type": "notify", "config": {"message": "Hello from test"}}
        ])
        result = runner.execute()
        assert result["status"] == "COMPLETED"
        assert result["step_results"][0]["status"] == "SUCCESS"

    def test_condition_true(self):
        runner = self._make_runner([
            {"name": "check", "type": "condition", "config": {"expression": "true"}},
            {"name": "after", "type": "enrich", "config": {"fields": {"reached": "yes"}}}
        ])
        result = runner.execute()
        assert result["status"] == "COMPLETED"
        assert result["steps_executed"] == 2
        assert runner.context.get("reached") == "yes"

    def test_condition_false_skips_remaining(self):
        runner = self._make_runner([
            {"name": "check", "type": "condition", "config": {"expression": "false"}},
            {"name": "should_skip", "type": "enrich", "config": {"fields": {"reached": "yes"}}}
        ])
        result = runner.execute()
        assert result["status"] == "COMPLETED"
        assert runner.context.get("reached") is None

    def test_multi_step_pipeline(self):
        runner = self._make_runner([
            {"name": "enrich1", "type": "enrich", "config": {"fields": {"source": "wazuh"}}},
            {"name": "log", "type": "notify", "config": {"message": "Source: {{source}}"}},
        ])
        result = runner.execute()
        assert result["status"] == "COMPLETED"
        assert result["steps_executed"] == 2

    def test_failure_continue(self):
        runner = self._make_runner([
            {"name": "bad", "type": "http", "config": {"url": "http://localhost:99999/x"}, "on_failure": "continue"},
            {"name": "good", "type": "enrich", "config": {"fields": {"ok": "yes"}}}
        ])
        result = runner.execute()
        assert result["status"] == "COMPLETED"
        assert result["steps_executed"] == 2

    def test_failure_abort(self):
        runner = self._make_runner([
            {"name": "bad", "type": "http", "config": {"url": "http://localhost:99999/x"}, "on_failure": "abort"},
            {"name": "skip", "type": "enrich", "config": {"fields": {"ok": "yes"}}}
        ])
        result = runner.execute()
        assert result["status"] == "FAILED"
        assert result["steps_executed"] == 1

    def test_resume_constructor(self):
        """Test that runner can be constructed with prior state for resumption."""
        definition = {"workflow_id": "resume-test", "name": "Resume", "steps": [
            {"name": "s1", "type": "enrich", "config": {"fields": {"a": "1"}}},
            {"name": "s2", "type": "enrich", "config": {"fields": {"b": "2"}}},
            {"name": "s3", "type": "enrich", "config": {"fields": {"c": "3"}}},
        ]}
        # Simulate resuming from step 2 with prior context
        runner = WorkflowRunner(
            definition=definition,
            trigger_event={},
            tenant_id="dev",
            execution_id="test-resume-id",
            start_step=2,
            prior_context={"a": "1", "b": "2"},
            prior_results=[{"step": "s1"}, {"step": "s2"}]
        )
        result = runner.execute()
        assert result["status"] == "COMPLETED"
        assert result["steps_executed"] == 3  # 2 prior + 1 new
        assert runner.context.get("c") == "3"


class TestEnrichTemplates:
    def test_template_substitution(self):
        runner = WorkflowRunner(
            {"workflow_id": "t1", "name": "test", "steps": [
                {"name": "enrich", "type": "enrich", "config": {"fields": {"msg": "Alert from {{source}} sev {{level}}"}}}
            ]},
            {"source": "wazuh", "level": "HIGH"},
            "dev-tenant"
        )
        result = runner.execute()
        assert runner.context["msg"] == "Alert from wazuh sev HIGH"


if __name__ == "__main__":
    passed = 0
    failed = 0
    for cls in [TestWorkflowRunner, TestEnrichTemplates]:
        inst = cls()
        for method in dir(inst):
            if method.startswith("test_"):
                try:
                    getattr(inst, method)()
                    print(f"  \u2705 {cls.__name__}.{method}")
                    passed += 1
                except (AssertionError, Exception) as e:
                    print(f"  \u274c {cls.__name__}.{method}: {e}")
                    failed += 1
    print(f"\n{'='*50}")
    print(f"Workflow Runner v2: {passed} passed, {failed} failed")
    print(f"{'='*50}")
    exit(1 if failed else 0)
