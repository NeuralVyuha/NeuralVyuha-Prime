import unittest
import sys
import os

# Add app directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../app')))

from rules import Rule, RuleEngine

class TestRules(unittest.TestCase):
    def test_rule_match(self):
        rule_data = {
            "rule_id": "R1",
            "rule_name": "Test Rule",
            "confidence": "HIGH",
            "window_minutes": 15,
            "correlation_key_template": "{host}|{rule_id}",
            "required_fields": ["host", "rule_id"]
        }
        rule = Rule(rule_data)

        payload = {"host": "h1", "rule_id": "r1"}
        matches = rule.match("tenant1", payload, 1000)

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]['correlation_key'], "h1|r1")
        self.assertTrue(matches[0]['group_id']) # Should be set

    def test_rule_match_list(self):
        rule_data = {
            "rule_id": "R1",
            "rule_name": "Test Rule",
            "confidence": "HIGH",
            "window_minutes": 15,
            "correlation_key_template": "{host}",
            "required_fields": ["host"]
        }
        rule = Rule(rule_data)

        payload = {"host": ["h1", "h2"]}
        matches = rule.match("tenant1", payload, 1000)

        self.assertEqual(len(matches), 2)
        keys = sorted([m['correlation_key'] for m in matches])
        self.assertEqual(keys, ["h1", "h2"])

if __name__ == '__main__':
    unittest.main()
