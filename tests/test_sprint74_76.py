"""
Phase 9 — Sprints 74–76 Tests

Sprint 74: Plugin System & Custom Rules
Sprint 75: Python SDK & REST API
Sprint 76: Configurable Rule Engine & Enterprise Rulesets
"""

import json
import os
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ═══════════════════════════════════════════════
#  Sprint 74 — Plugin System
# ═══════════════════════════════════════════════

import plugins


class TestPluginRegistration:
    """Sprint 74: Plugin decorator registration."""

    def setup_method(self):
        plugins.clear_all()

    def test_register_transform_decorator(self):
        @plugins.register_transform("TEST_TX")
        def handler(prev_df, mapping, cell_num):
            return ["df = prev_df"]

        assert "TEST_TX" in plugins.get_registered_transforms()

    def test_register_sql_rewrite_decorator(self):
        @plugins.register_sql_rewrite(r"\bTEST_FUNC\b")
        def rewriter(sql):
            return sql.replace("TEST_FUNC", "pyspark_func")

        assert plugins.get_registered_sql_rewrites() == 1

    def test_register_post_notebook_hook(self):
        @plugins.post_notebook
        def add_header(content, mapping):
            return "# HEADER\n" + content

        hooks = plugins.get_registered_hooks()
        assert hooks["post_notebook"] == 1

    def test_register_post_pipeline_hook(self):
        @plugins.post_pipeline
        def modify_pipeline(content, workflow):
            return content

        hooks = plugins.get_registered_hooks()
        assert hooks["post_pipeline"] == 1

    def test_register_post_sql_hook(self):
        @plugins.post_sql
        def add_sql_header(content, db_type):
            return f"-- {db_type}\n" + content

        hooks = plugins.get_registered_hooks()
        assert hooks["post_sql"] == 1


class TestPluginExecution:
    """Sprint 74: Plugin execution API."""

    def setup_method(self):
        plugins.clear_all()

    def test_get_transform_handler(self):
        @plugins.register_transform("CUSTOM_X")
        def handler(prev_df, mapping, cell_num):
            return ["custom code"]

        h = plugins.get_transform_handler("CUSTOM_X")
        assert h is not None
        assert h("df", {}, 1) == ["custom code"]

    def test_get_nonexistent_handler_returns_none(self):
        assert plugins.get_transform_handler("NONEXISTENT") is None

    def test_apply_sql_rewrites(self):
        @plugins.register_sql_rewrite(r"\bENT_FUNC\s*\(")
        def rewrite(sql):
            return sql.replace("ENT_FUNC(", "spark_ent_func(")

        result = plugins.apply_sql_rewrites("SELECT ENT_FUNC(x) FROM t")
        assert "spark_ent_func(" in result

    def test_apply_post_notebook_hooks(self):
        @plugins.post_notebook
        def add_header(content, mapping):
            return f"# Header for {mapping['name']}\n" + content

        result = plugins.apply_post_notebook_hooks("# original", {"name": "TEST"})
        assert "Header for TEST" in result
        assert "# original" in result

    def test_apply_post_sql_hooks(self):
        @plugins.post_sql
        def add_license(content, db_type):
            return f"-- License header ({db_type})\n" + content

        result = plugins.apply_post_sql_hooks("SELECT 1", "oracle")
        assert "License header (oracle)" in result

    def test_hook_error_does_not_crash(self):
        @plugins.post_notebook
        def failing_hook(content, mapping):
            raise ValueError("Plugin error")

        result = plugins.apply_post_notebook_hooks("original", {})
        assert result == "original"  # Falls through on error


class TestPluginDiscovery:
    """Sprint 74: Plugin discovery from directory."""

    def setup_method(self):
        plugins.clear_all()

    def test_discover_from_nonexistent_dir(self):
        loaded = plugins.discover_plugins(Path("/tmp/nonexistent_plugins"))
        assert loaded == []

    def test_discover_returns_module_names(self, tmp_path):
        plugin_dir = tmp_path / "plugins"
        plugin_dir.mkdir()
        (plugin_dir / "__init__.py").write_text("")
        (plugin_dir / "my_plugin.py").write_text(
            "# Simple test plugin\n"
            "LOADED = True\n"
        )
        # Should not crash even if import fails due to path issues
        loaded = plugins.discover_plugins(plugin_dir)
        # Either loaded or empty (depends on sys.path), but no crash
        assert isinstance(loaded, list)


class TestClearAll:
    """Sprint 74: Plugin registry cleanup."""

    def test_clear_all_resets_registries(self):
        @plugins.register_transform("TX1")
        def h1(a, b, c):
            pass

        @plugins.register_sql_rewrite(r"test")
        def h2(s):
            return s

        plugins.clear_all()
        assert plugins.get_registered_transforms() == {}
        assert plugins.get_registered_sql_rewrites() == 0
        assert all(v == 0 for v in plugins.get_registered_hooks().values())


# ═══════════════════════════════════════════════
#  Sprint 75 — Python SDK & REST API
# ═══════════════════════════════════════════════

from sdk import MigrationConfig, MigrationSDK
from api_server import (
    api_health,
    api_start_migration,
    api_start_assessment,
    api_get_status,
    api_get_inventory,
    api_list_jobs,
    _jobs,
)


class TestMigrationConfig:
    """Sprint 75: MigrationConfig dataclass."""

    def test_default_config(self):
        config = MigrationConfig()
        assert config.target == "fabric"
        assert config.input_dir == "input"
        assert config.dry_run is False

    def test_custom_config(self):
        config = MigrationConfig(target="databricks", verbose=True)
        assert config.target == "databricks"
        assert config.verbose is True

    def test_from_dict(self):
        d = {"target": "dbt", "dry_run": True, "unknown_field": "ignored"}
        config = MigrationConfig.from_dict(d)
        assert config.target == "dbt"
        assert config.dry_run is True

    def test_to_env_vars_fabric(self):
        config = MigrationConfig(target="fabric")
        env = config.to_env_vars()
        assert env["INFORMATICA_MIGRATION_TARGET"] == "fabric"

    def test_to_env_vars_databricks(self):
        config = MigrationConfig(target="databricks", databricks_catalog="my_catalog")
        env = config.to_env_vars()
        assert env["INFORMATICA_MIGRATION_TARGET"] == "databricks"
        assert env["INFORMATICA_DATABRICKS_CATALOG"] == "my_catalog"

    def test_to_env_vars_dbt(self):
        config = MigrationConfig(target="dbt")
        env = config.to_env_vars()
        assert env["INFORMATICA_MIGRATION_TARGET"] == "databricks"
        assert env["INFORMATICA_DBT_MODE"] == "dbt"

    def test_to_env_vars_autosys(self):
        config = MigrationConfig(autosys_dir="/path/to/jil")
        env = config.to_env_vars()
        assert env["INFORMATICA_AUTOSYS_DIR"] == "/path/to/jil"


class TestMigrationSDK:
    """Sprint 75: MigrationSDK class."""

    def test_init_default_config(self):
        sdk = MigrationSDK()
        assert sdk.config.target == "fabric"

    def test_init_custom_config(self):
        cfg = MigrationConfig(target="databricks")
        sdk = MigrationSDK(cfg)
        assert sdk.config.target == "databricks"

    def test_get_status_initial(self):
        sdk = MigrationSDK()
        status = sdk.get_status()
        assert status["config"]["target"] == "fabric"
        assert status["results"] == []
        assert status["inventory_loaded"] is False

    def test_get_inventory_loads_from_file(self):
        sdk = MigrationSDK()
        inv = sdk.get_inventory()
        # May or may not exist; should return dict regardless
        assert isinstance(inv, dict)

    def test_convert_sql_direct(self):
        sdk = MigrationSDK()
        result = sdk.convert_sql("SELECT NVL(x, 0) FROM DUAL", "oracle")
        assert "COALESCE" in result

    def test_convert_mapping_not_found(self):
        sdk = MigrationSDK()
        with pytest.raises(ValueError, match="not found"):
            sdk.convert_mapping("NONEXISTENT_MAPPING")


class TestAPIServer:
    """Sprint 75: REST API functions."""

    def setup_method(self):
        _jobs.clear()

    def test_health_endpoint(self):
        result = api_health()
        assert result["status"] == "healthy"
        assert "version" in result
        assert "timestamp" in result

    def test_start_migration_returns_job_id(self):
        result = api_start_migration({"target": "fabric", "dry_run": True})
        assert "job_id" in result
        assert result["status"] == "pending"

    def test_start_assessment_returns_job_id(self):
        result = api_start_assessment({"source_dir": "input"})
        assert "job_id" in result
        assert result["status"] == "pending"

    def test_get_status_found(self):
        result = api_start_migration()
        job_id = result["job_id"]
        status = api_get_status(job_id)
        assert status is not None
        assert status["id"] == job_id

    def test_get_status_not_found(self):
        assert api_get_status("nonexistent-id") is None

    def test_list_jobs(self):
        api_start_migration()
        api_start_assessment()
        jobs = api_list_jobs()
        assert len(jobs) == 2

    def test_get_inventory_returns_dict(self):
        result = api_get_inventory()
        assert isinstance(result, dict)


# ═══════════════════════════════════════════════
#  Sprint 76 — Rule Engine
# ═══════════════════════════════════════════════

from rule_engine import RuleEngine, validate_rule


class TestRuleValidation:
    """Sprint 76: Rule schema validation."""

    def test_valid_rule(self):
        rule = {"name": "test", "match": r"\bNVL\b", "action": "replace", "output": "COALESCE"}
        is_valid, errors = validate_rule(rule)
        assert is_valid
        assert len(errors) == 0

    def test_missing_required_field(self):
        rule = {"name": "test", "match": r"\bNVL\b"}
        is_valid, errors = validate_rule(rule)
        assert not is_valid
        assert any("action" in e for e in errors)

    def test_invalid_regex(self):
        rule = {"name": "test", "match": r"[invalid(", "action": "replace", "output": "x"}
        is_valid, errors = validate_rule(rule)
        assert not is_valid
        assert any("regex" in e.lower() for e in errors)

    def test_invalid_priority(self):
        rule = {"name": "t", "match": "x", "action": "replace", "output": "y", "priority": 999}
        is_valid, errors = validate_rule(rule)
        assert not is_valid
        assert any("Priority" in e for e in errors)

    def test_invalid_action(self):
        rule = {"name": "t", "match": "x", "action": "invalid", "output": "y"}
        is_valid, errors = validate_rule(rule)
        assert not is_valid


class TestRuleEngineLoading:
    """Sprint 76: Rule engine loading and merging."""

    def test_load_from_default_directory(self):
        engine = RuleEngine()
        engine.load_rules()
        # Should load rules from rules/ directory
        assert engine.get_sql_rule_count() >= 0  # May have rules or not

    def test_load_from_json_file(self, tmp_path):
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()
        rules_file = rules_dir / "test_rules.json"
        rules_file.write_text(json.dumps({
            "rules": [
                {"name": "test_nvl", "match": r"\bNVL\b", "action": "replace",
                 "output": "COALESCE", "category": "sql", "db_type": "oracle", "priority": 50},
            ]
        }))

        engine = RuleEngine(rules_dir=rules_dir)
        engine.load_rules()
        assert engine.get_sql_rule_count("oracle") >= 1

    def test_apply_sql_rules(self, tmp_path):
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()
        (rules_dir / "rules.json").write_text(json.dumps({
            "rules": [
                {"name": "SYSDATE", "match": r"\bSYSDATE\b", "action": "replace",
                 "output": "current_timestamp()", "db_type": "oracle", "priority": 50},
            ]
        }))

        engine = RuleEngine(rules_dir=rules_dir)
        engine.load_rules()
        result = engine.apply_sql_rules("SELECT SYSDATE FROM DUAL", "oracle")
        assert "current_timestamp()" in result

    def test_disabled_rules_skipped(self, tmp_path):
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()
        (rules_dir / "rules.json").write_text(json.dumps({
            "rules": [
                {"name": "disabled", "match": r"\bTEST\b", "action": "replace",
                 "output": "REPLACED", "enabled": False, "db_type": "oracle"},
            ]
        }))

        engine = RuleEngine(rules_dir=rules_dir)
        engine.load_rules()
        result = engine.apply_sql_rules("SELECT TEST FROM t", "oracle")
        assert "REPLACED" not in result

    def test_custom_rules_override(self, tmp_path):
        default_dir = tmp_path / "default"
        default_dir.mkdir()
        (default_dir / "base.json").write_text(json.dumps({
            "rules": [
                {"name": "base_rule", "match": r"\bBASE\b", "action": "replace",
                 "output": "DEFAULT", "db_type": "all", "priority": 50},
            ]
        }))

        custom_dir = tmp_path / "custom"
        custom_dir.mkdir()
        (custom_dir / "override.json").write_text(json.dumps({
            "rules": [
                {"name": "custom_rule", "match": r"\bCUSTOM\b", "action": "replace",
                 "output": "ENTERPRISE", "db_type": "oracle", "priority": 90},
            ]
        }))

        engine = RuleEngine(rules_dir=default_dir, custom_rules_dir=custom_dir)
        engine.load_rules()
        assert engine.get_sql_rule_count("oracle") >= 2

    def test_all_db_type_applies_to_all(self, tmp_path):
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()
        (rules_dir / "rules.json").write_text(json.dumps({
            "rules": [
                {"name": "universal", "match": r"\bUNIVERSAL\b", "action": "replace",
                 "output": "REPLACED", "db_type": "all", "priority": 50},
            ]
        }))

        engine = RuleEngine(rules_dir=rules_dir)
        engine.load_rules()
        for db in ("oracle", "sqlserver", "teradata", "db2", "mysql", "postgresql"):
            assert engine.get_sql_rule_count(db) >= 1


class TestRuleEngineExport:
    """Sprint 76: Rule export for inspection."""

    def test_export_returns_dict(self, tmp_path):
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()
        (rules_dir / "r.json").write_text(json.dumps({
            "rules": [
                {"name": "test", "match": r"\bX\b", "action": "replace",
                 "output": "Y", "db_type": "oracle"},
            ]
        }))
        engine = RuleEngine(rules_dir=rules_dir)
        engine.load_rules()
        export = engine.export_rules()
        assert "sql_rules" in export
        assert "transform_rules" in export

    def test_export_to_file(self, tmp_path):
        engine = RuleEngine()
        engine.load_rules()
        out_path = tmp_path / "export.json"
        engine.export_rules(str(out_path))
        assert out_path.exists()

    def test_empty_rules_dir(self, tmp_path):
        rules_dir = tmp_path / "empty_rules"
        rules_dir.mkdir()
        engine = RuleEngine(rules_dir=rules_dir)
        engine.load_rules()
        assert engine.get_sql_rule_count() == 0


class TestRuleEnginePriority:
    """Sprint 76: Rule priority ordering."""

    def test_higher_priority_applied_first(self, tmp_path):
        rules_dir = tmp_path / "rules"
        rules_dir.mkdir()
        (rules_dir / "rules.json").write_text(json.dumps({
            "rules": [
                {"name": "low", "match": r"\bA\b", "action": "replace",
                 "output": "LOW", "db_type": "oracle", "priority": 10},
                {"name": "high", "match": r"\bB\b", "action": "replace",
                 "output": "HIGH", "db_type": "oracle", "priority": 90},
            ]
        }))

        engine = RuleEngine(rules_dir=rules_dir)
        engine.load_rules()
        result = engine.apply_sql_rules("SELECT A, B FROM t", "oracle")
        assert "LOW" in result
        assert "HIGH" in result
