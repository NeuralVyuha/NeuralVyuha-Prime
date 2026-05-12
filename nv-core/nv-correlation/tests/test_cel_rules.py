"""
Tests for CEL-based correlation rules and Mustache templating in nv-correlation.
Covers: CEL matching, dynamic naming via chevron, and fallback to field-based matching.
"""
import sys
import os
import hashlib

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))

from rules import Rule


# ═══════════════════════════════════════════════════════════════════════════════
# Test Data Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def make_rule(overrides=None):
    base = {
        "rule_id": "test-rule-1",
        "rule_name": "Test Rule",
        "confidence": 80,
        "window_minutes": 15,
        "correlation_key_template": "key_{source}",
        "required_fields": ["source"],
        "definition_cel": None,
        "name_template": None,
    }
    if overrides:
        base.update(overrides)
    return Rule(base)


# ═══════════════════════════════════════════════════════════════════════════════
# Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestFieldBasedMatching:
    def test_basic_field_match(self):
        rule = make_rule({"required_fields": ["source", "severity"]})
        payload = {"source": "wazuh", "severity": 4}
        matches = rule.match("dev-tenant", payload, 1000000)
        assert len(matches) == 1, f"Expected 1 match, got {len(matches)}"
        assert matches[0]["rule_id"] == "test-rule-1"

    def test_missing_field_rejects(self):
        rule = make_rule({"required_fields": ["source", "nonexistent_field"],
                          "definition_cel": None})
        payload = {"source": "wazuh", "severity": 4}
        matches = rule.match("dev-tenant", payload, 1000000)
        assert len(matches) == 0, "Missing required field should produce no matches"


class TestCELMatching:
    def test_cel_match_true(self):
        rule = make_rule({
            "definition_cel": 'severity > 3',
            "required_fields": []
        })
        payload = {"source": "wazuh", "severity": 4}
        matches = rule.match("dev-tenant", payload, 1000000)
        assert len(matches) == 1, f"CEL should match severity=4 > 3"

    def test_cel_match_false(self):
        rule = make_rule({
            "definition_cel": 'severity > 3',
            "required_fields": []
        })
        payload = {"source": "wazuh", "severity": 2}
        matches = rule.match("dev-tenant", payload, 1000000)
        assert len(matches) == 0, "CEL should NOT match severity=2 > 3"

    def test_cel_complex_expression(self):
        rule = make_rule({
            "definition_cel": 'source == "wazuh" && severity >= 3',
            "required_fields": []
        })
        payload = {"source": "wazuh", "severity": 3}
        matches = rule.match("dev-tenant", payload, 1000000)
        assert len(matches) == 1


class TestMustacheTemplating:
    def test_mustache_template(self):
        rule = make_rule({
            "name_template": "Alert from {{source}} sev {{severity}}",
            "required_fields": ["source"]
        })
        payload = {"source": "wazuh", "severity": 4}
        matches = rule.match("dev-tenant", payload, 1000000)
        assert len(matches) == 1
        assert "wazuh" in matches[0]["correlation_key"]
        assert "4" in matches[0]["correlation_key"]

    def test_fallback_to_format(self):
        rule = make_rule({
            "correlation_key_template": "key_{source}",
            "name_template": None,
            "required_fields": ["source"]
        })
        payload = {"source": "suricata"}
        matches = rule.match("dev-tenant", payload, 1000000)
        assert len(matches) == 1
        assert matches[0]["correlation_key"] == "key_suricata"


class TestDeterministicGroupID:
    def test_same_inputs_same_group(self):
        rule = make_rule({"required_fields": ["source"]})
        payload = {"source": "wazuh"}
        m1 = rule.match("dev-tenant", payload, 1000000)
        m2 = rule.match("dev-tenant", payload, 1000000)
        assert m1[0]["group_id"] == m2[0]["group_id"]

    def test_different_window_different_group(self):
        rule = make_rule({"required_fields": ["source"], "window_minutes": 1})
        payload = {"source": "wazuh"}
        m1 = rule.match("dev-tenant", payload, 60000)   # window_idx = 1
        m2 = rule.match("dev-tenant", payload, 120000)  # window_idx = 2
        assert m1[0]["group_id"] != m2[0]["group_id"]


# ═══════════════════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    passed = 0
    failed = 0

    for cls_name in [TestFieldBasedMatching, TestCELMatching, TestMustacheTemplating, TestDeterministicGroupID]:
        inst = cls_name()
        for method in dir(inst):
            if method.startswith("test_"):
                try:
                    getattr(inst, method)()
                    print(f"  \u2705 {cls_name.__name__}.{method}")
                    passed += 1
                except AssertionError as e:
                    print(f"  \u274c {cls_name.__name__}.{method}: {e}")
                    failed += 1
                except Exception as e:
                    print(f"  \u274c {cls_name.__name__}.{method}: UNEXPECTED {e}")
                    failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed")
    print(f"{'='*50}")
    exit(1 if failed else 0)
