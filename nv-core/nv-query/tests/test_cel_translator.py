"""
Tests for CEL-to-OpenSearch Translator.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))
from cel_translator import cel_to_opensearch


class TestSimpleComparisons:
    def test_equals_string(self):
        result = cel_to_opensearch('source == "wazuh"')
        assert result == {"term": {"source.keyword": "wazuh"}}, f"Got: {result}"

    def test_equals_number(self):
        result = cel_to_opensearch("severity == 4")
        assert result == {"term": {"severity": 4}}, f"Got: {result}"

    def test_not_equals(self):
        result = cel_to_opensearch('source != "test"')
        assert "must_not" in str(result), f"Got: {result}"

    def test_greater_than(self):
        result = cel_to_opensearch("severity > 3")
        assert result == {"range": {"severity": {"gt": 3}}}, f"Got: {result}"

    def test_less_than_equal(self):
        result = cel_to_opensearch("severity <= 2")
        assert result == {"range": {"severity": {"lte": 2}}}, f"Got: {result}"

    def test_gte(self):
        result = cel_to_opensearch("severity >= 3")
        assert result == {"range": {"severity": {"gte": 3}}}, f"Got: {result}"


class TestLogicalOperators:
    def test_and(self):
        result = cel_to_opensearch('severity > 3 && source == "wazuh"')
        assert "must" in result.get("bool", {}), f"Got: {result}"
        assert len(result["bool"]["must"]) == 2

    def test_or(self):
        result = cel_to_opensearch('source == "wazuh" || source == "suricata"')
        assert "should" in result.get("bool", {}), f"Got: {result}"
        assert len(result["bool"]["should"]) == 2

    def test_not(self):
        result = cel_to_opensearch('!(severity > 3)')
        assert "must_not" in result.get("bool", {}), f"Got: {result}"


class TestStringFunctions:
    def test_contains(self):
        result = cel_to_opensearch('message.contains("login failed")')
        assert result == {"match_phrase": {"message": "login failed"}}, f"Got: {result}"

    def test_starts_with(self):
        result = cel_to_opensearch('hostname.startsWith("prod-")')
        assert result == {"prefix": {"hostname.keyword": "prod-"}}, f"Got: {result}"


class TestInOperator:
    def test_in_list(self):
        result = cel_to_opensearch('severity in [3, 4, 5]')
        assert result == {"terms": {"severity": [3, 4, 5]}}, f"Got: {result}"


class TestEdgeCases:
    def test_empty_expression(self):
        result = cel_to_opensearch("")
        assert result == {"match_all": {}}, f"Got: {result}"

    def test_none_expression(self):
        result = cel_to_opensearch(None)
        assert result == {"match_all": {}}, f"Got: {result}"

    def test_complex_nested(self):
        result = cel_to_opensearch('severity > 3 && (source == "wazuh" || source == "suricata")')
        assert "must" in result.get("bool", {}), f"Got: {result}"


if __name__ == "__main__":
    passed = 0
    failed = 0
    for cls in [TestSimpleComparisons, TestLogicalOperators, TestStringFunctions, TestInOperator, TestEdgeCases]:
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
    print(f"CEL Translator: {passed} passed, {failed} failed")
    print(f"{'='*50}")
    exit(1 if failed else 0)
