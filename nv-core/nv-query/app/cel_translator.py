"""
CEL-to-OpenSearch Query Translator
===================================
Translates Common Expression Language (CEL) filter expressions into
OpenSearch/Elasticsearch DSL queries for server-side filtering.

Supports: ==, !=, >, <, >=, <=, &&, ||, !, contains, startsWith, in
"""
import logging
import re
from typing import Dict, Any, Optional, List

logger = logging.getLogger("cel-translator")


def cel_to_opensearch(cel_expr: str) -> Dict[str, Any]:
    """
    Convert a CEL expression string into an OpenSearch bool query dict.
    
    Examples:
        'severity > 3'                    -> {"range": {"severity": {"gt": 3}}}
        'source == "wazuh"'               -> {"term": {"source.keyword": "wazuh"}}
        'severity > 3 && source == "wazuh"' -> {"bool": {"must": [...]}}
        'message.contains("login")'       -> {"match_phrase": {"message": "login"}}
    """
    if not cel_expr or not cel_expr.strip():
        return {"match_all": {}}
    
    cel_expr = cel_expr.strip()
    
    try:
        return _parse_expression(cel_expr)
    except Exception as e:
        logger.warning(f"CEL translation failed for '{cel_expr}': {e}")
        # Fallback: treat as a full-text query string
        return {"query_string": {"query": cel_expr, "default_field": "*", "lenient": True}}


def _parse_expression(expr: str) -> Dict:
    """Recursively parse a CEL expression into OpenSearch DSL."""
    expr = expr.strip()
    
    # Remove outer parentheses if they wrap the entire expression
    if expr.startswith("(") and _find_matching_paren(expr, 0) == len(expr) - 1:
        expr = expr[1:-1].strip()
    
    # Handle OR (||) — split at top-level ||
    or_parts = _split_at_operator(expr, "||")
    if len(or_parts) > 1:
        return {"bool": {"should": [_parse_expression(p) for p in or_parts], "minimum_should_match": 1}}
    
    # Handle AND (&&) — split at top-level &&
    and_parts = _split_at_operator(expr, "&&")
    if len(and_parts) > 1:
        return {"bool": {"must": [_parse_expression(p) for p in and_parts]}}
    
    # Handle NOT (!)
    if expr.startswith("!"):
        inner = expr[1:].strip()
        if inner.startswith("("):
            end = _find_matching_paren(inner, 0)
            inner = inner[1:end].strip()
        return {"bool": {"must_not": [_parse_expression(inner)]}}
    
    # Handle .contains("value")
    contains_match = re.match(r'^(\w[\w.]*)\s*\.\s*contains\s*\(\s*"([^"]*)"\s*\)$', expr)
    if contains_match:
        field, value = contains_match.group(1), contains_match.group(2)
        return {"match_phrase": {field: value}}
    
    # Handle .startsWith("value")
    starts_match = re.match(r'^(\w[\w.]*)\s*\.\s*startsWith\s*\(\s*"([^"]*)"\s*\)$', expr)
    if starts_match:
        field, value = starts_match.group(1), starts_match.group(2)
        return {"prefix": {_keyword_field(field): value}}
    
    # Handle "field in [val1, val2, ...]"
    in_match = re.match(r'^(\w[\w.]*)\s+in\s+\[([^\]]*)\]$', expr)
    if in_match:
        field = in_match.group(1)
        raw_values = in_match.group(2)
        values = [_parse_value(v.strip()) for v in raw_values.split(",")]
        return {"terms": {_keyword_field(field): values}}
    
    # Handle comparison operators: ==, !=, >=, <=, >, <
    for op in [">=", "<=", "!=", "==", ">", "<"]:
        parts = _split_comparison(expr, op)
        if parts:
            field, value = parts
            return _comparison_to_query(field, op, value)
    
    # Fallback: treat as query string
    return {"query_string": {"query": expr, "default_field": "*", "lenient": True}}


def _comparison_to_query(field: str, op: str, raw_value: str) -> Dict:
    """Convert a single comparison into an OpenSearch query clause."""
    value = _parse_value(raw_value)
    
    if op == "==":
        if value is None:
            return {"bool": {"must_not": [{"exists": {"field": field}}]}}
        if isinstance(value, str):
            return {"term": {_keyword_field(field): value}}
        return {"term": {field: value}}
    
    if op == "!=":
        if value is None:
            return {"exists": {"field": field}}
        if isinstance(value, str):
            return {"bool": {"must_not": [{"term": {_keyword_field(field): value}}]}}
        return {"bool": {"must_not": [{"term": {field: value}}]}}
    
    range_map = {">": "gt", "<": "lt", ">=": "gte", "<=": "lte"}
    return {"range": {field: {range_map[op]: value}}}


def _keyword_field(field: str) -> str:
    """Append .keyword for text fields to enable exact matching."""
    # Don't double-add .keyword
    if field.endswith(".keyword"):
        return field
    # Known numeric/boolean fields that don't need .keyword
    numeric_fields = {"severity", "timestamp", "alert_count", "max_severity", "threat_score", "confidence"}
    if field in numeric_fields:
        return field
    return f"{field}.keyword"


def _parse_value(raw: str) -> Any:
    """Parse a literal value from CEL into Python type."""
    raw = raw.strip()
    if raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1]
    if raw.startswith("'") and raw.endswith("'"):
        return raw[1:-1]
    if raw.lower() == "true":
        return True
    if raw.lower() == "false":
        return False
    if raw.lower() == "null":
        return None
    try:
        if "." in raw:
            return float(raw)
        return int(raw)
    except ValueError:
        return raw


def _split_at_operator(expr: str, op: str) -> List[str]:
    """Split expression at top-level occurrences of a logical operator (&&, ||)."""
    parts = []
    depth = 0
    current = []
    i = 0
    while i < len(expr):
        ch = expr[i]
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif depth == 0 and expr[i:i+len(op)] == op:
            parts.append("".join(current).strip())
            current = []
            i += len(op)
            continue
        current.append(ch)
        i += 1
    parts.append("".join(current).strip())
    return [p for p in parts if p]


def _split_comparison(expr: str, op: str) -> Optional[tuple]:
    """Try to split an expression at a comparison operator, respecting strings."""
    # Find the operator outside of quotes
    in_str = False
    str_char = None
    i = 0
    while i < len(expr):
        ch = expr[i]
        if ch in ('"', "'") and not in_str:
            in_str = True
            str_char = ch
        elif ch == str_char and in_str:
            in_str = False
        elif not in_str and expr[i:i+len(op)] == op:
            # Make sure it's not part of a longer operator
            if op in (">", "<"):
                if i + 1 < len(expr) and expr[i+1] == "=":
                    i += 1
                    continue
            if op == "=" and i > 0 and expr[i-1] in ("!", ">", "<"):
                i += 1
                continue
            field = expr[:i].strip()
            value = expr[i+len(op):].strip()
            if field and value:
                return (field, value)
        i += 1
    return None


def _find_matching_paren(expr: str, start: int) -> int:
    """Find the index of the matching closing parenthesis."""
    depth = 0
    for i in range(start, len(expr)):
        if expr[i] == '(':
            depth += 1
        elif expr[i] == ')':
            depth -= 1
            if depth == 0:
                return i
    return len(expr) - 1
