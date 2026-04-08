"""
Rule Engine — Sprint 76
Externalizes conversion rules from hardcoded Python into versioned,
shareable YAML/JSON rulesets that enterprises can customize.

Rules are loaded from the `rules/` directory and merged with defaults.
Enterprise overrides can be specified via `migration.yaml` → `custom_rules_dir`.

Usage:
    from rule_engine import RuleEngine
    engine = RuleEngine()
    engine.load_rules()
    result = engine.apply_sql_rules(sql_text, db_type="oracle")
"""

import json
import logging
import os
import re
from pathlib import Path

logger = logging.getLogger(__name__)

WORKSPACE = Path(__file__).resolve().parent
DEFAULT_RULES_DIR = WORKSPACE / "rules"

# ─────────────────────────────────────────────
#  Rule Schema
# ─────────────────────────────────────────────

# Each rule has:
#   name:       Human-readable name
#   category:   sql | transform | naming
#   db_type:    oracle | sqlserver | teradata | db2 | mysql | postgresql | all
#   match:      Regex pattern to match
#   action:     replace | flag | skip
#   output:     Replacement template (for action=replace)
#   priority:   1-100 (higher = applied first)
#   enabled:    true/false

RULE_SCHEMA = {
    "required": ["name", "match", "action", "output"],
    "optional": ["category", "db_type", "priority", "enabled", "description"],
    "defaults": {
        "category": "sql",
        "db_type": "all",
        "priority": 50,
        "enabled": True,
        "description": "",
    },
}


def validate_rule(rule):
    """Validate a rule dict against the schema. Returns (is_valid, errors)."""
    errors = []
    for field in RULE_SCHEMA["required"]:
        if field not in rule:
            errors.append(f"Missing required field: {field}")

    if "match" in rule:
        try:
            re.compile(rule["match"])
        except re.error as e:
            errors.append(f"Invalid regex pattern: {e}")

    if "priority" in rule:
        if not isinstance(rule["priority"], int) or not (1 <= rule["priority"] <= 100):
            errors.append("Priority must be an integer from 1 to 100")

    if "action" in rule and rule["action"] not in ("replace", "flag", "skip"):
        errors.append(f"Invalid action: {rule['action']} (must be replace, flag, or skip)")

    return len(errors) == 0, errors


class RuleEngine:
    """Load and apply externalized conversion rules."""

    def __init__(self, rules_dir=None, custom_rules_dir=None):
        self.rules_dir = Path(rules_dir) if rules_dir else DEFAULT_RULES_DIR
        self.custom_rules_dir = Path(custom_rules_dir) if custom_rules_dir else None
        self._sql_rules = {}       # db_type → [(compiled_pat, repl, priority, name)]
        self._transform_rules = {} # tx_type → rule_dict
        self._loaded = False

    def load_rules(self):
        """Load rules from default + custom directories."""
        self._sql_rules.clear()
        self._transform_rules.clear()

        # Load defaults
        self._load_rules_from_dir(self.rules_dir)

        # Merge enterprise overrides
        if self.custom_rules_dir and self.custom_rules_dir.exists():
            self._load_rules_from_dir(self.custom_rules_dir)
            logger.info("Loaded custom rules from %s", self.custom_rules_dir)

        self._loaded = True
        return self

    def _load_rules_from_dir(self, rules_dir):
        """Load all rule files from a directory."""
        rules_dir = Path(rules_dir)
        if not rules_dir.exists():
            return

        # Load YAML files
        for rule_file in sorted(rules_dir.glob("*.yaml")) + sorted(rules_dir.glob("*.yml")):
            self._load_yaml_rules(rule_file)

        # Load JSON files
        for rule_file in sorted(rules_dir.glob("*.json")):
            self._load_json_rules(rule_file)

    def _load_yaml_rules(self, filepath):
        """Load rules from a YAML file."""
        try:
            import yaml
            with open(filepath, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except ImportError:
            # Fallback: basic YAML parsing for simple rule files
            data = self._parse_simple_yaml(filepath)
        except Exception as e:
            logger.warning("Failed to load rules from %s: %s", filepath, e)
            return

        self._process_rules(data, str(filepath))

    def _load_json_rules(self, filepath):
        """Load rules from a JSON file."""
        try:
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.warning("Failed to load rules from %s: %s", filepath, e)
            return

        self._process_rules(data, str(filepath))

    def _parse_simple_yaml(self, filepath):
        """Basic YAML-like parser for simple rule files (no PyYAML dependency)."""
        rules = []
        current_rule = {}
        with open(filepath, encoding="utf-8") as f:
            for line in f:
                line = line.rstrip()
                if line.startswith("  - name:") or line.startswith("- name:"):
                    if current_rule:
                        rules.append(current_rule)
                    current_rule = {"name": line.split(":", 1)[1].strip().strip('"').strip("'")}
                elif line.strip().startswith("match:"):
                    current_rule["match"] = line.split(":", 1)[1].strip().strip('"').strip("'")
                elif line.strip().startswith("action:"):
                    current_rule["action"] = line.split(":", 1)[1].strip().strip('"').strip("'")
                elif line.strip().startswith("output:"):
                    current_rule["output"] = line.split(":", 1)[1].strip().strip('"').strip("'")
                elif line.strip().startswith("db_type:"):
                    current_rule["db_type"] = line.split(":", 1)[1].strip().strip('"').strip("'")
                elif line.strip().startswith("priority:"):
                    try:
                        current_rule["priority"] = int(line.split(":", 1)[1].strip())
                    except ValueError:
                        pass
                elif line.strip().startswith("category:"):
                    current_rule["category"] = line.split(":", 1)[1].strip().strip('"').strip("'")
                elif line.strip().startswith("enabled:"):
                    val = line.split(":", 1)[1].strip().lower()
                    current_rule["enabled"] = val in ("true", "yes", "1")
        if current_rule:
            rules.append(current_rule)
        return {"rules": rules}

    def _process_rules(self, data, source_file):
        """Process loaded rule data and add to registries."""
        rules_list = data.get("rules", [])
        if isinstance(data, list):
            rules_list = data

        for rule in rules_list:
            # Apply defaults
            for key, default in RULE_SCHEMA["defaults"].items():
                rule.setdefault(key, default)

            if not rule.get("enabled", True):
                continue

            is_valid, errors = validate_rule(rule)
            if not is_valid:
                logger.warning("Invalid rule '%s' in %s: %s", rule.get("name", "?"), source_file, errors)
                continue

            category = rule.get("category", "sql")
            if category == "sql":
                self._add_sql_rule(rule)
            elif category == "transform":
                self._add_transform_rule(rule)

    def _add_sql_rule(self, rule):
        """Add a SQL conversion rule."""
        db_type = rule.get("db_type", "all")
        priority = rule.get("priority", 50)
        try:
            compiled = re.compile(rule["match"], re.IGNORECASE)
        except re.error:
            return

        entry = (compiled, rule["output"], priority, rule["name"])

        if db_type == "all":
            for dt in ("oracle", "sqlserver", "teradata", "db2", "mysql", "postgresql"):
                self._sql_rules.setdefault(dt, []).append(entry)
        else:
            self._sql_rules.setdefault(db_type, []).append(entry)

        # Sort by priority (descending — higher priority first)
        for dt in self._sql_rules:
            self._sql_rules[dt].sort(key=lambda x: x[2], reverse=True)

    def _add_transform_rule(self, rule):
        """Add a transformation conversion rule."""
        tx_type = rule.get("match", "").upper()
        self._transform_rules[tx_type] = rule

    def apply_sql_rules(self, sql_text, db_type="oracle"):
        """Apply all loaded SQL rules for the given DB type.

        Returns the converted SQL text.
        """
        if not self._loaded:
            self.load_rules()

        result = sql_text
        rules = self._sql_rules.get(db_type, [])

        for pattern, replacement, priority, name in rules:
            result = pattern.sub(replacement, result)

        return result

    def get_transform_rule(self, tx_type):
        """Get the rule for a transformation type, or None."""
        if not self._loaded:
            self.load_rules()
        return self._transform_rules.get(tx_type.upper())

    def get_sql_rule_count(self, db_type=None):
        """Return the number of loaded SQL rules."""
        if db_type:
            return len(self._sql_rules.get(db_type, []))
        return sum(len(rules) for rules in self._sql_rules.values())

    def get_transform_rule_count(self):
        """Return the number of loaded transform rules."""
        return len(self._transform_rules)

    def export_rules(self, output_path=None):
        """Export all loaded rules to a JSON file for inspection."""
        export = {
            "sql_rules": {},
            "transform_rules": {},
        }
        for db_type, rules in self._sql_rules.items():
            export["sql_rules"][db_type] = [
                {"name": name, "pattern": pat.pattern, "replacement": repl, "priority": priority}
                for pat, repl, priority, name in rules
            ]
        for tx_type, rule in self._transform_rules.items():
            export["transform_rules"][tx_type] = rule

        if output_path:
            Path(output_path).write_text(json.dumps(export, indent=2), encoding="utf-8")

        return export
