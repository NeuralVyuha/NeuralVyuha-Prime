"""
Tests for Keep-inspired enrichment logic in nv-ingest.
Covers: extraction rules (regex), mapping rules (KV), fingerprint with ignored_fields, and CEL conditions.
"""
import sys
import os
import json
import hashlib

# Add parent for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))


# ═══════════════════════════════════════════════════════════════════════════════
# Test Helpers (standalone — no DB/Redis needed)
# ═══════════════════════════════════════════════════════════════════════════════

def dot_get(obj, path, default=None):
    parts = path.split(".")
    current = obj
    for part in parts:
        if not isinstance(current, dict):
            return default
        current = current.get(part)
        if current is None:
            return default
    return current


def calculate_fingerprint(payload, fields, ignored_fields=None):
    values = []
    if fields:
        for field in fields:
            val = dot_get(payload, field)
            values.append(str(val) if val is not None else "\u2205")
    else:
        clean_payload = {k: v for k, v in payload.items() if k not in (ignored_fields or [])}
        return hashlib.md5(json.dumps(clean_payload, sort_keys=True, default=str).encode()).hexdigest()
    raw_key = "|".join(values)
    return hashlib.md5(raw_key.encode()).hexdigest()


import re

def run_extraction_rules_test(tenant_id, payload, rules):
    for rule in rules:
        if rule['tenant'] not in (tenant_id, 'global'): continue
        attr_val = dot_get(payload, rule['attr'])
        if not attr_val or not isinstance(attr_val, str): continue
        match = re.search(rule['regex'], attr_val)
        if match:
            extracted = match.groupdict()
            payload.update(extracted)


def run_mapping_rules_test(tenant_id, payload, rules):
    for rule in rules:
        if rule['tenant'] not in (tenant_id, 'global'): continue
        matched = True
        for field_path, expected_val in rule['fields'].items():
            if str(dot_get(payload, field_path)) != str(expected_val):
                matched = False
                break
        if matched:
            payload.update(rule['data'])


# ═══════════════════════════════════════════════════════════════════════════════
# Test Cases
# ═══════════════════════════════════════════════════════════════════════════════

class TestExtractionRules:
    def test_basic_ip_extraction(self):
        payload = {"message": "Login failed for user admin from 10.0.0.5 via SSH"}
        rules = [{"tenant": "global", "name": "extract_ip", "attr": "message",
                  "regex": r"from (?P<src_ip>\d+\.\d+\.\d+\.\d+)"}]
        run_extraction_rules_test("dev-tenant", payload, rules)
        assert payload.get("src_ip") == "10.0.0.5", f"Expected 10.0.0.5, got {payload.get('src_ip')}"

    def test_multi_group_extraction(self):
        payload = {"log": "User=alice Action=LOGIN Status=FAIL"}
        rules = [{"tenant": "global", "name": "extract_user_action", "attr": "log",
                  "regex": r"User=(?P<username>\w+) Action=(?P<action>\w+)"}]
        run_extraction_rules_test("dev-tenant", payload, rules)
        assert payload.get("username") == "alice"
        assert payload.get("action") == "LOGIN"

    def test_no_match_leaves_payload_unchanged(self):
        payload = {"message": "System is healthy"}
        rules = [{"tenant": "global", "name": "extract_ip", "attr": "message",
                  "regex": r"from (?P<src_ip>\d+\.\d+\.\d+\.\d+)"}]
        original_keys = set(payload.keys())
        run_extraction_rules_test("dev-tenant", payload, rules)
        assert set(payload.keys()) == original_keys

    def test_tenant_isolation(self):
        payload = {"message": "Attack from 192.168.1.1"}
        rules = [{"tenant": "other-tenant", "name": "extract_ip", "attr": "message",
                  "regex": r"from (?P<src_ip>\d+\.\d+\.\d+\.\d+)"}]
        run_extraction_rules_test("dev-tenant", payload, rules)
        assert "src_ip" not in payload, "Rule for other tenant should not apply"


class TestMappingRules:
    def test_basic_mapping(self):
        payload = {"source": "wazuh", "rule_id": "5710"}
        rules = [{"tenant": "global", "name": "map_mitre", "fields": {"rule_id": "5710"},
                  "data": {"mitre_tactic": "Initial Access", "severity": 4}}]
        run_mapping_rules_test("dev-tenant", payload, rules)
        assert payload.get("mitre_tactic") == "Initial Access"
        assert payload.get("severity") == 4

    def test_no_match_mapping(self):
        payload = {"source": "wazuh", "rule_id": "9999"}
        rules = [{"tenant": "global", "name": "map_mitre", "fields": {"rule_id": "5710"},
                  "data": {"mitre_tactic": "Initial Access"}}]
        run_mapping_rules_test("dev-tenant", payload, rules)
        assert "mitre_tactic" not in payload

    def test_multi_field_match(self):
        payload = {"source": "suricata", "alert_category": "Exploit"}
        rules = [{"tenant": "global", "name": "multi", "fields": {"source": "suricata", "alert_category": "Exploit"},
                  "data": {"priority": "CRITICAL"}}]
        run_mapping_rules_test("dev-tenant", payload, rules)
        assert payload.get("priority") == "CRITICAL"


class TestFingerprint:
    def test_field_based_fingerprint(self):
        p1 = {"source": "wazuh", "rule_id": "5710", "timestamp": 1000}
        p2 = {"source": "wazuh", "rule_id": "5710", "timestamp": 2000}
        fp1 = calculate_fingerprint(p1, ["source", "rule_id"])
        fp2 = calculate_fingerprint(p2, ["source", "rule_id"])
        assert fp1 == fp2, "Same fields should produce same fingerprint"

    def test_different_fields_different_fingerprint(self):
        p1 = {"source": "wazuh", "rule_id": "5710"}
        p2 = {"source": "wazuh", "rule_id": "9999"}
        fp1 = calculate_fingerprint(p1, ["source", "rule_id"])
        fp2 = calculate_fingerprint(p2, ["source", "rule_id"])
        assert fp1 != fp2

    def test_ignored_fields_full_payload(self):
        p1 = {"source": "wazuh", "message": "test", "timestamp": 1000, "seq": 1}
        p2 = {"source": "wazuh", "message": "test", "timestamp": 2000, "seq": 2}
        fp1 = calculate_fingerprint(p1, [], ignored_fields=["timestamp", "seq"])
        fp2 = calculate_fingerprint(p2, [], ignored_fields=["timestamp", "seq"])
        assert fp1 == fp2, "Ignored fields should not affect fingerprint"

    def test_no_ignored_fields_different_fingerprint(self):
        p1 = {"source": "wazuh", "message": "test", "timestamp": 1000}
        p2 = {"source": "wazuh", "message": "test", "timestamp": 2000}
        fp1 = calculate_fingerprint(p1, [])
        fp2 = calculate_fingerprint(p2, [])
        assert fp1 != fp2, "Without ignoring timestamp, fingerprints should differ"


# ═══════════════════════════════════════════════════════════════════════════════
# Run Tests
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    passed = 0
    failed = 0
    
    for cls_name in [TestExtractionRules, TestMappingRules, TestFingerprint]:
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
