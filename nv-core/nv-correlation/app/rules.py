import yaml
import hashlib
import logging
from typing import List, Dict, Any, Optional
import os
import itertools
import celpy
import chevron

logger = logging.getLogger("rules-engine")

class Rule:
    def __init__(self, data: Dict):
        self.rule_id = data['rule_id']
        self.rule_name = data['rule_name']
        self.confidence = data['confidence']
        self.window_minutes = int(data.get('window_minutes', 15))
        self.template = data['correlation_key_template']
        self.required_fields = data.get('required_fields', [])
        self.definition_cel = data.get('definition_cel')
        self.name_template = data.get('name_template')

    def _dot_get(self, obj: Any, path: str, default=None) -> Any:
        if not path or obj is None: return default
        parts = path.split(".")
        current = obj
        for part in parts:
            if not isinstance(current, dict): return default
            current = current.get(part)
            if current is None: return default
        return current

    def match(self, tenant_id: str, payload: Dict, timestamp: int) -> List[Dict]:
        matches = []
        
        try:
            # Prepare template context
            template_ctx = {**payload, "tenant_id": tenant_id}
            
            # Use CEL for matching if definition_cel is provided
            if self.definition_cel:
                env = celpy.Environment()
                ast = env.compile(self.definition_cel)
                prgm = env.program(ast)
                activation = celpy.json_to_cel(payload)
                if not prgm.evaluate(activation):
                    return []
            else:
                # Fallback to field-based matching if no CEL
                for field in self.required_fields:
                    val = self._dot_get(payload, field)
                    if val is None:
                        logger.debug(f"Rule {self.rule_id} rejected: missing field {field}")
                        return []
            
            # Use Dynamic Templating (Chevron/Mustache)
            if self.name_template:
                key = chevron.render(self.name_template, template_ctx)
            else:
                # Fallback to standard python format
                # We need safe templates for .format if fields have dots
                safe_template = self.template
                format_ctx = {**template_ctx}
                for f in self.required_fields:
                    if "." in f:
                        flattened_key = f.replace('.', '_')
                        safe_template = safe_template.replace(f"{{{f}}}", f"{{{flattened_key}}}")
                        format_ctx[flattened_key] = self._dot_get(payload, f)
                
                key = safe_template.format(**format_ctx)

            # 4. Deterministic Group ID
            window_ms = self.window_minutes * 60 * 1000
            window_idx = int(timestamp / window_ms) if window_ms > 0 else 0

            raw_id = f"{tenant_id}:{self.rule_id}:{key}:{window_idx}"
            group_id = hashlib.sha256(raw_id.encode('utf-8')).hexdigest()

            matches.append({
                "tenant_id": tenant_id,
                "rule_id": self.rule_id,
                "rule_name": self.rule_name,
                "confidence": self.confidence,
                "correlation_key": key,
                "group_id": group_id,
                "window_idx": window_idx,
                "first_seen": timestamp,
                "last_seen": timestamp,
                "status": "OPEN",
                "alert_count": 1,
                "max_severity": payload.get("severity", 1)
            })

        except Exception as e:
            logger.error(f"Rule match error for {self.rule_id}: {e}")
            return []

        return matches

class RuleEngine:
    def __init__(self, db_instance=None):
        self.rules = []
        self.db = db_instance
        if self.db:
            self.reload_rules()
        else:
            self._load_file(os.getenv("RULES_FILE", "rules.yaml"))

    def _load_file(self, path: str):
        if not os.path.exists(path):
            logger.warning(f"Rules file not found: {path}")
            return
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
            if data and 'rules' in data:
                for r in data['rules']:
                    self.rules.append(Rule(r))
        logger.info(f"Loaded {len(self.rules)} rules from file")

    def reload_rules(self):
        if not self.db:
            return

        db_rules = self.db.fetch_rules()
        if not db_rules:
            logger.warning("No rules fetched from DB (or error). Keeping existing rules.")
            return

        new_rules = []
        for r in db_rules:
            new_rules.append(Rule(r))

        self.rules = new_rules
        rule_ids = [r.rule_id for r in self.rules]
        logger.info(f"Reloaded {len(self.rules)} rules from DB: {rule_ids}")

    def evaluate(self, tenant_id: str, payload: Dict, timestamp: int) -> List[Dict]:
        all_matches = []
        for rule in self.rules:
            matches = rule.match(tenant_id, payload, timestamp)
            all_matches.extend(matches)
        return all_matches
