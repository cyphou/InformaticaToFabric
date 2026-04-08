"""
Phase 7 — Sprints 68–70 Tests

Sprint 68: DevOps & CI/CD (env configs, deployment pipelines, pre-deployment validation,
           promotion, Databricks Asset Bundles)
Sprint 69: Platform-Native Features (Lakehouse vs Warehouse advisor, T-SQL DDL,
           SQL Warehouse DDL, OneLake shortcuts, Delta Sharing, Mirroring)
Sprint 70: Observability (Fabric CU cost estimator, Azure Monitor metrics, webhook alerting)
"""

import json
import os
import sys
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# ═══════════════════════════════════════════════
#  Sprint 68 — DevOps & CI/CD
# ═══════════════════════════════════════════════

from deploy_to_fabric import (
    generate_env_configs,
    generate_deployment_pipeline_json,
    validate_pre_deployment,
    promote_deployment,
    OUTPUT_DIR as FABRIC_OUTPUT_DIR,
)
from deploy_to_databricks import (
    generate_dab_bundle,
    OUTPUT_DIR as DB_OUTPUT_DIR,
)


class TestGenerateEnvConfigs:
    """Sprint 68: Environment-specific config generation."""

    def test_generates_three_envs(self, tmp_path):
        configs = generate_env_configs(base_config_path=tmp_path / "nonexistent.yaml")
        assert set(configs.keys()) == {"dev", "test", "prod"}

    def test_env_configs_have_environment_key(self, tmp_path):
        configs = generate_env_configs(base_config_path=tmp_path / "x.yaml")
        for env_name, cfg in configs.items():
            assert cfg["environment"] == env_name

    def test_dev_uses_debug_logging(self, tmp_path):
        configs = generate_env_configs(base_config_path=tmp_path / "x.yaml")
        assert configs["dev"]["logging"]["level"] == "DEBUG"

    def test_prod_uses_warning_logging(self, tmp_path):
        configs = generate_env_configs(base_config_path=tmp_path / "x.yaml")
        assert configs["prod"]["logging"]["level"] == "WARNING"

    def test_workspace_id_is_placeholder(self, tmp_path):
        configs = generate_env_configs(base_config_path=tmp_path / "x.yaml")
        assert "DEV_WORKSPACE_ID" in configs["dev"]["fabric"]["workspace_id"]
        assert "PROD_WORKSPACE_ID" in configs["prod"]["fabric"]["workspace_id"]

    def test_connection_string_placeholder(self, tmp_path):
        configs = generate_env_configs(base_config_path=tmp_path / "x.yaml")
        assert "DEV_CONNECTION_STRING" in configs["dev"]["sources"]["connection_string"]

    def test_env_files_written_to_disk(self, tmp_path):
        configs = generate_env_configs(base_config_path=tmp_path / "x.yaml")
        env_dir = FABRIC_OUTPUT_DIR / "environments"
        for name in ("dev", "test", "prod"):
            assert (env_dir / f"{name}.yaml").exists()


class TestGenerateDeploymentPipeline:
    """Sprint 68: Fabric Deployment Pipeline JSON generation."""

    def test_returns_dict(self):
        result = generate_deployment_pipeline_json()
        assert isinstance(result, dict)

    def test_has_three_stages(self):
        result = generate_deployment_pipeline_json()
        assert len(result["stages"]) == 3

    def test_stage_names(self):
        result = generate_deployment_pipeline_json()
        names = [s["displayName"] for s in result["stages"]]
        assert names == ["Development", "Test", "Production"]

    def test_stage_order(self):
        result = generate_deployment_pipeline_json()
        orders = [s["order"] for s in result["stages"]]
        assert orders == [0, 1, 2]

    def test_has_rules(self):
        result = generate_deployment_pipeline_json()
        assert len(result["rules"]) >= 2

    def test_notebook_rule_exists(self):
        result = generate_deployment_pipeline_json()
        types = [r["artifactType"] for r in result["rules"]]
        assert "Notebook" in types

    def test_pipeline_rule_exists(self):
        result = generate_deployment_pipeline_json()
        types = [r["artifactType"] for r in result["rules"]]
        assert "DataPipeline" in types

    def test_writes_to_output(self):
        generate_deployment_pipeline_json()
        out_path = FABRIC_OUTPUT_DIR / "deployment_pipeline.json"
        assert out_path.exists()
        data = json.loads(out_path.read_text(encoding="utf-8"))
        assert "stages" in data

    def test_schema_field(self):
        result = generate_deployment_pipeline_json()
        assert "$schema" in result


class TestValidatePreDeployment:
    """Sprint 68: Pre-deployment validation checks."""

    def test_returns_dict(self):
        result = validate_pre_deployment()
        assert isinstance(result, dict)
        assert "valid" in result
        assert "checks" in result
        assert "errors" in result

    def test_checks_list_not_empty(self):
        result = validate_pre_deployment()
        assert len(result["checks"]) >= 3

    def test_inventory_check_present(self):
        result = validate_pre_deployment()
        check_names = [c["check"] for c in result["checks"]]
        assert "inventory_exists" in check_names

    def test_notebooks_check_present(self):
        result = validate_pre_deployment()
        check_names = [c["check"] for c in result["checks"]]
        assert "notebooks_exist" in check_names

    def test_pipelines_check_present(self):
        result = validate_pre_deployment()
        check_names = [c["check"] for c in result["checks"]]
        assert "pipelines_exist" in check_names

    def test_credential_leak_check(self):
        result = validate_pre_deployment()
        check_names = [c["check"] for c in result["checks"]]
        assert "no_credential_leaks" in check_names


class TestPromoteDeployment:
    """Sprint 68: Promotion between environments."""

    def test_dry_run_returns_dict(self):
        result = promote_deployment("dev", "test", dry_run=True)
        assert isinstance(result, dict)
        assert result["source"] == "dev"
        assert result["target"] == "test"
        assert result["dry_run"] is True

    def test_has_timestamp(self):
        result = promote_deployment("dev", "test", dry_run=True)
        assert "timestamp" in result

    def test_has_actions(self):
        result = promote_deployment("dev", "test", dry_run=True)
        assert "actions" in result
        assert isinstance(result["actions"], list)

    def test_writes_promotion_log(self):
        promote_deployment("dev", "prod", dry_run=True)
        promo_path = FABRIC_OUTPUT_DIR / "promotion_dev_to_prod.json"
        assert promo_path.exists()

    def test_promotion_log_structure(self):
        promote_deployment("test", "prod", dry_run=True)
        promo_path = FABRIC_OUTPUT_DIR / "promotion_test_to_prod.json"
        data = json.loads(promo_path.read_text(encoding="utf-8"))
        assert data["source"] == "test"
        assert data["target"] == "prod"


class TestGenerateDABBundle:
    """Sprint 68: Databricks Asset Bundle generation."""

    def test_returns_result_dict(self):
        result = generate_dab_bundle()
        assert isinstance(result, dict)
        assert "bundle_dir" in result
        assert "notebooks" in result
        assert "workflows" in result
        assert "targets" in result

    def test_creates_bundle_dir(self):
        generate_dab_bundle()
        bundle_dir = DB_OUTPUT_DIR / "databricks_bundle"
        assert bundle_dir.exists()

    def test_creates_databricks_yml(self):
        generate_dab_bundle()
        bundle_dir = DB_OUTPUT_DIR / "databricks_bundle"
        assert (bundle_dir / "databricks.yml").exists()

    def test_databricks_yml_has_bundle_name(self):
        generate_dab_bundle()
        bundle_dir = DB_OUTPUT_DIR / "databricks_bundle"
        content = (bundle_dir / "databricks.yml").read_text(encoding="utf-8")
        assert "informatica_migration" in content

    def test_creates_src_dir(self):
        generate_dab_bundle()
        bundle_dir = DB_OUTPUT_DIR / "databricks_bundle"
        assert (bundle_dir / "src").exists()

    def test_creates_resources_dir(self):
        generate_dab_bundle()
        bundle_dir = DB_OUTPUT_DIR / "databricks_bundle"
        assert (bundle_dir / "resources").exists()

    def test_target_envs_in_yml(self):
        result = generate_dab_bundle()
        assert set(result["targets"]) == {"dev", "staging", "prod"}

    def test_databricks_yml_has_targets(self):
        generate_dab_bundle()
        bundle_dir = DB_OUTPUT_DIR / "databricks_bundle"
        content = (bundle_dir / "databricks.yml").read_text(encoding="utf-8")
        assert "dev:" in content
        assert "staging:" in content
        assert "prod:" in content

    def test_bundle_summary_json(self):
        generate_dab_bundle()
        bundle_dir = DB_OUTPUT_DIR / "databricks_bundle"
        assert (bundle_dir / "bundle_summary.json").exists()
        data = json.loads((bundle_dir / "bundle_summary.json").read_text(encoding="utf-8"))
        assert "notebooks" in data

    def test_custom_catalog(self):
        generate_dab_bundle(catalog="analytics")
        bundle_dir = DB_OUTPUT_DIR / "databricks_bundle"
        content = (bundle_dir / "databricks.yml").read_text(encoding="utf-8")
        assert "analytics" in content

    def test_custom_profile(self):
        generate_dab_bundle(profile="MY_PROFILE")
        bundle_dir = DB_OUTPUT_DIR / "databricks_bundle"
        content = (bundle_dir / "databricks.yml").read_text(encoding="utf-8")
        assert "MY_PROFILE" in content


# ═══════════════════════════════════════════════
#  Sprint 69 — Platform-Native Features
# ═══════════════════════════════════════════════

from run_schema_generator import (
    recommend_storage_target,
    generate_warehouse_ddl,
    _map_type_to_tsql,
    generate_sql_warehouse_ddl,
    generate_onelake_shortcuts,
    generate_delta_sharing_config,
    generate_mirroring_config,
    extract_target_schemas,
    map_type_to_delta,
)


def _make_mapping(**overrides):
    """Helper to build a mapping dict for Sprint 69 tests."""
    mapping = {
        "name": "M_TEST",
        "sources": ["Oracle.SALES.CUSTOMERS"],
        "targets": ["DIM_CUSTOMER"],
        "transformations": ["SQ", "EXP"],
        "has_sql_override": False,
        "complexity": "Simple",
        "sql_overrides": [],
        "lookup_conditions": [],
        "parameters": [],
        "target_load_order": [],
        "has_mapplet": False,
    }
    mapping.update(overrides)
    return mapping


def _make_table(**overrides):
    """Helper to build a table dict for DDL tests."""
    table = {
        "name": "DIM_CUSTOMER",
        "tier": "silver",
        "mapping": "M_LOAD_CUSTOMERS",
        "columns": [
            {"name": "customer_id", "type": "INT", "source": "SQ.CUSTOMER_ID"},
            {"name": "customer_name", "type": "STRING", "source": "SQ.NAME"},
            {"name": "balance", "type": "DECIMAL(19,4)", "source": "EXP.CALC_BALANCE"},
            {"name": "created_date", "type": "TIMESTAMP", "source": "SQ.CREATED_DT"},
        ],
        "partition_key": None,
    }
    table.update(overrides)
    return table


class TestRecommendStorageTarget:
    """Sprint 69: Lakehouse vs Warehouse recommendation engine."""

    def test_simple_sql_maps_to_warehouse(self):
        mapping = _make_mapping(
            has_sql_override=True,
            has_stored_proc=True,
            transformations=["SQ", "AGG"],
            complexity="Simple",
        )
        result = recommend_storage_target(mapping)
        assert result["recommendation"] == "warehouse"

    def test_complex_transforms_maps_to_lakehouse(self):
        mapping = _make_mapping(
            transformations=["SQ", "JNR", "LKP", "NRM", "SP"],
            complexity="Complex",
        )
        result = recommend_storage_target(mapping)
        assert result["recommendation"] == "lakehouse"

    def test_returns_scores(self):
        mapping = _make_mapping()
        result = recommend_storage_target(mapping)
        assert "lakehouse_score" in result
        assert "warehouse_score" in result

    def test_includes_mapping_name(self):
        mapping = _make_mapping(name="M_SALES_DAILY")
        result = recommend_storage_target(mapping)
        assert result["mapping"] == "M_SALES_DAILY"

    def test_includes_reason(self):
        mapping = _make_mapping()
        result = recommend_storage_target(mapping)
        assert "reason" in result
        assert len(result["reason"]) > 10

    def test_multi_source_prefers_lakehouse(self):
        mapping = _make_mapping(
            sources=["DB1.X", "DB2.Y", "DB3.Z"],
            transformations=["SQ", "JNR"],
            complexity="Medium",
        )
        result = recommend_storage_target(mapping)
        assert result["recommendation"] == "lakehouse"

    def test_simple_agg_only_prefers_warehouse(self):
        mapping = _make_mapping(
            transformations=["SQ", "AGG", "FIL"],
            has_sql_override=True,
            complexity="Simple",
        )
        result = recommend_storage_target(mapping)
        assert result["recommendation"] == "warehouse"


class TestMapTypeToTsql:
    """Sprint 69: Source type → T-SQL type mapping."""

    def test_string_to_nvarchar(self):
        assert _map_type_to_tsql("STRING") == "NVARCHAR(4000)"

    def test_int(self):
        assert _map_type_to_tsql("INT") == "INT"

    def test_bigint(self):
        assert _map_type_to_tsql("BIGINT") == "BIGINT"

    def test_timestamp_to_datetime2(self):
        assert _map_type_to_tsql("TIMESTAMP") == "DATETIME2"

    def test_date(self):
        assert _map_type_to_tsql("DATE") == "DATE"

    def test_boolean_to_bit(self):
        assert _map_type_to_tsql("BOOLEAN") == "BIT"

    def test_decimal(self):
        assert _map_type_to_tsql("DECIMAL(19,4)") == "DECIMAL(19,4)"

    def test_float(self):
        assert _map_type_to_tsql("FLOAT") == "FLOAT"

    def test_unknown_defaults_to_nvarchar(self):
        assert _map_type_to_tsql("OBSCURE_TYPE") == "NVARCHAR(4000)"

    def test_case_insensitive(self):
        assert _map_type_to_tsql("string") == "NVARCHAR(4000)"
        assert _map_type_to_tsql("timestamp") == "DATETIME2"


class TestGenerateWarehouseDDL:
    """Sprint 69: Fabric Warehouse T-SQL DDL generation."""

    def test_returns_string(self):
        ddl = generate_warehouse_ddl(_make_table())
        assert isinstance(ddl, str)

    def test_contains_create_table(self):
        ddl = generate_warehouse_ddl(_make_table())
        assert "CREATE TABLE" in ddl

    def test_uses_bracket_notation(self):
        ddl = generate_warehouse_ddl(_make_table())
        assert "[silver].[dim_customer]" in ddl

    def test_contains_if_not_exists(self):
        ddl = generate_warehouse_ddl(_make_table())
        assert "IF NOT EXISTS" in ddl

    def test_contains_etl_columns(self):
        ddl = generate_warehouse_ddl(_make_table())
        assert "_etl_load_timestamp" in ddl
        assert "_etl_source_mapping" in ddl

    def test_uses_tsql_types(self):
        ddl = generate_warehouse_ddl(_make_table())
        assert "NVARCHAR" in ddl or "INT" in ddl
        assert "DATETIME2" in ddl

    def test_includes_mapping_comment(self):
        ddl = generate_warehouse_ddl(_make_table())
        assert "M_LOAD_CUSTOMERS" in ddl


class TestGenerateSqlWarehouseDDL:
    """Sprint 69: Databricks SQL Warehouse DDL with CLUSTER BY."""

    def test_returns_string(self):
        ddl = generate_sql_warehouse_ddl(_make_table())
        assert isinstance(ddl, str)

    def test_uses_three_level_namespace(self):
        ddl = generate_sql_warehouse_ddl(_make_table(), catalog="analytics")
        assert "analytics.silver.dim_customer" in ddl

    def test_contains_using_delta(self):
        ddl = generate_sql_warehouse_ddl(_make_table())
        assert "USING DELTA" in ddl

    def test_cluster_by_id_columns(self):
        ddl = generate_sql_warehouse_ddl(_make_table())
        assert "CLUSTER BY" in ddl
        assert "customer_id" in ddl

    def test_zorder_for_date_columns(self):
        ddl = generate_sql_warehouse_ddl(_make_table())
        assert "ZORDER" in ddl
        assert "created_date" in ddl

    def test_etl_meta_columns(self):
        ddl = generate_sql_warehouse_ddl(_make_table())
        assert "_etl_load_timestamp" in ddl
        assert "_etl_source_mapping" in ddl

    def test_migration_comment(self):
        ddl = generate_sql_warehouse_ddl(_make_table())
        assert "COMMENT" in ddl
        assert "M_LOAD_CUSTOMERS" in ddl

    def test_partition_key_included(self):
        table = _make_table(partition_key="created_date")
        ddl = generate_sql_warehouse_ddl(table)
        assert "PARTITIONED BY" in ddl

    def test_no_cluster_by_without_id_cols(self):
        table = _make_table(columns=[
            {"name": "value", "type": "STRING", "source": "SQ.VAL"},
            {"name": "amount", "type": "FLOAT", "source": "SQ.AMT"},
        ])
        ddl = generate_sql_warehouse_ddl(table)
        assert "CLUSTER BY" not in ddl


class TestGenerateOneLakeShortcuts:
    """Sprint 69: OneLake shortcut generation from DB links."""

    def test_empty_inventory_returns_empty(self):
        result = generate_onelake_shortcuts({"mappings": []})
        assert result == []

    def test_detects_dblink_pattern(self):
        inv = {"mappings": [{
            "name": "M_X",
            "sources": ["SCHEMA.TABLE@DBLINK_NAME"],
        }]}
        result = generate_onelake_shortcuts(inv)
        assert len(result) >= 1
        shortcut = result[0]
        assert shortcut["type"] == "onelake_shortcut"
        assert shortcut["source_database_link"] == "DBLINK_NAME"

    def test_detects_cross_db_reference(self):
        inv = {"mappings": [{
            "name": "M_Y",
            "sources": ["Oracle.SALES.CUSTOMERS"],
        }]}
        result = generate_onelake_shortcuts(inv)
        # Cross-DB reference detected
        cross_db = [s for s in result if s.get("source_system") == "Oracle"]
        assert len(cross_db) >= 1

    def test_shortcut_has_target_path(self):
        inv = {"mappings": [{
            "name": "M_Z",
            "sources": ["DB.SCHEMA.TABLE@LINK"],
        }]}
        result = generate_onelake_shortcuts(inv)
        for shortcut in result:
            assert "target_path" in shortcut
            assert shortcut["target_path"].startswith("Tables/")

    def test_mapping_name_preserved(self):
        inv = {"mappings": [{
            "name": "M_ORDERS",
            "sources": ["SRC@LINK1"],
        }]}
        result = generate_onelake_shortcuts(inv)
        assert any(s["mapping"] == "M_ORDERS" for s in result)


class TestGenerateDeltaSharingConfig:
    """Sprint 69: Delta Sharing provider/share/recipient config."""

    def _inv_with_gold(self):
        return {"mappings": [{
            "name": "M_LOAD_CUSTOMERS",
            "sources": ["Oracle.SALES.CUSTOMERS"],
            "targets": ["DIM_CUSTOMER"],
            "transformations": ["SQ", "AGG"],
            "has_sql_override": False,
            "complexity": "Simple",
            "sql_overrides": [],
            "lookup_conditions": [],
            "parameters": [],
            "target_load_order": [],
            "has_mapplet": False,
        }]}

    def test_returns_dict(self):
        result = generate_delta_sharing_config(self._inv_with_gold())
        assert isinstance(result, dict)

    def test_has_provider(self):
        result = generate_delta_sharing_config(self._inv_with_gold())
        assert "provider" in result
        assert result["provider"]["name"] == "migration_provider"

    def test_has_shares(self):
        result = generate_delta_sharing_config(self._inv_with_gold())
        assert "shares" in result
        assert len(result["shares"]) >= 1

    def test_has_recipients(self):
        result = generate_delta_sharing_config(self._inv_with_gold())
        assert "recipients" in result

    def test_has_grant_sql(self):
        result = generate_delta_sharing_config(self._inv_with_gold())
        assert "grant_sql" in result
        assert any("CREATE SHARE" in s for s in result["grant_sql"])

    def test_custom_provider_name(self):
        result = generate_delta_sharing_config(self._inv_with_gold(), provider_name="my_provider")
        assert result["provider"]["name"] == "my_provider"


class TestGenerateMirroringConfig:
    """Sprint 69: Fabric Mirroring configuration recommendations."""

    def test_returns_dict(self):
        result = generate_mirroring_config({"mappings": []})
        assert isinstance(result, dict)

    def test_detects_oracle_as_supported(self):
        inv = {"mappings": [{
            "name": "M_X",
            "sources": ["Oracle.SALES.CUSTOMERS"],
        }]}
        result = generate_mirroring_config(inv)
        candidates = result["mirroring_candidates"]
        oracle_candidates = [c for c in candidates if "Oracle" in c["source_database"]]
        if oracle_candidates:
            assert oracle_candidates[0]["supported"] is True

    def test_detects_unsupported_db(self):
        inv = {"mappings": [{
            "name": "M_X",
            "sources": ["SybaseIQ.DBO.TABLE1"],
        }]}
        result = generate_mirroring_config(inv)
        candidates = result["mirroring_candidates"]
        for c in candidates:
            if "Sybase" in c["source_database"]:
                assert c["supported"] is False

    def test_total_source_tables(self):
        inv = {"mappings": [{
            "name": "M_X",
            "sources": ["Oracle.S1.T1", "Oracle.S1.T2"],
        }]}
        result = generate_mirroring_config(inv)
        assert result["total_source_tables"] >= 2

    def test_recommendation_text(self):
        inv = {"mappings": [{
            "name": "M_X",
            "sources": ["PostgreSQL.PUBLIC.ORDERS"],
        }]}
        result = generate_mirroring_config(inv)
        candidates = result["mirroring_candidates"]
        pg = [c for c in candidates if "PostgreSQL" in c["source_database"]]
        if pg:
            assert "Mirroring" in pg[0]["recommendation"]


# ═══════════════════════════════════════════════
#  Sprint 70 — Observability
# ═══════════════════════════════════════════════

from deploy_to_fabric import (
    estimate_fabric_cu_cost,
    emit_azure_monitor_metrics,
    send_webhook_alert,
    CU_COST_PER_HOUR,
)


class TestEstimateFabricCUCost:
    """Sprint 70: Fabric Capacity Unit cost estimation."""

    def _write_inventory(self, tmp_path, mappings):
        inv_path = tmp_path / "inventory.json"
        inv_path.write_text(json.dumps({"mappings": mappings}), encoding="utf-8")
        return inv_path

    def test_missing_inventory(self, tmp_path):
        result = estimate_fabric_cu_cost(tmp_path / "missing.json")
        assert "error" in result

    def test_single_simple_mapping(self, tmp_path):
        inv = self._write_inventory(tmp_path, [{
            "name": "M_SIMPLE",
            "complexity": "Simple",
            "transformations": ["SQ", "EXP"],
        }])
        result = estimate_fabric_cu_cost(inv)
        assert result["total_mappings"] == 1
        assert result["estimated_monthly_cu_hours"] == 15.0  # 0.5 * 30

    def test_complex_mapping_higher_cost(self, tmp_path):
        inv = self._write_inventory(tmp_path, [{
            "name": "M_COMPLEX",
            "complexity": "Complex",
            "transformations": ["SQ", "JNR", "AGG", "LKP"],
        }])
        result = estimate_fabric_cu_cost(inv)
        # Complex base: 4.0, JNR: 1.3, AGG: 1.2, LKP: 1.1 → 4.0 * 1.3 * 1.2 * 1.1 = 6.86
        assert result["estimated_monthly_cu_hours"] > 100

    def test_stored_proc_multiplier(self, tmp_path):
        inv_sp = self._write_inventory(tmp_path, [{
            "name": "M_SP",
            "complexity": "Simple",
            "transformations": ["SQ", "SP"],
        }])
        result_sp = estimate_fabric_cu_cost(inv_sp)

        inv_no_sp = self._write_inventory(tmp_path, [{
            "name": "M_NOSP",
            "complexity": "Simple",
            "transformations": ["SQ"],
        }])
        result_no_sp = estimate_fabric_cu_cost(inv_no_sp)

        assert result_sp["estimated_monthly_cu_hours"] > result_no_sp["estimated_monthly_cu_hours"]

    def test_cost_uses_cu_rate(self, tmp_path):
        inv = self._write_inventory(tmp_path, [{
            "name": "M_X",
            "complexity": "Simple",
            "transformations": [],
        }])
        result = estimate_fabric_cu_cost(inv)
        expected_cost = round(result["estimated_monthly_cu_hours"] * CU_COST_PER_HOUR, 2)
        assert result["estimated_monthly_cost_usd"] == expected_cost

    def test_details_per_mapping(self, tmp_path):
        inv = self._write_inventory(tmp_path, [
            {"name": "M_A", "complexity": "Simple", "transformations": []},
            {"name": "M_B", "complexity": "Complex", "transformations": ["JNR"]},
        ])
        result = estimate_fabric_cu_cost(inv)
        assert len(result["details"]) == 2
        assert result["details"][0]["mapping"] == "M_A"
        assert result["details"][1]["mapping"] == "M_B"

    def test_result_has_notes(self, tmp_path):
        inv = self._write_inventory(tmp_path, [
            {"name": "M_X", "complexity": "Simple", "transformations": []},
        ])
        result = estimate_fabric_cu_cost(inv)
        assert len(result["notes"]) >= 2

    def test_sql_override_multiplier(self, tmp_path):
        inv = self._write_inventory(tmp_path, [{
            "name": "M_SQL",
            "complexity": "Simple",
            "transformations": [],
            "has_sql_override": True,
        }])
        result = estimate_fabric_cu_cost(inv)
        # 0.5 * 1.2 = 0.6 per run → 18.0 monthly
        assert result["estimated_monthly_cu_hours"] == 18.0

    def test_sku_baseline_in_result(self, tmp_path):
        inv = self._write_inventory(tmp_path, [
            {"name": "M_X", "complexity": "Simple", "transformations": []},
        ])
        result = estimate_fabric_cu_cost(inv)
        assert result["sku_baseline"] == "F2"
        assert result["target"] == "fabric"


class TestEmitAzureMonitorMetrics:
    """Sprint 70: Azure Monitor metrics emission."""

    def test_deployment_results(self):
        results = [
            {"status": "created", "artifact": "NB_X"},
            {"status": "dry-run", "artifact": "NB_Y"},
            {"status": "error", "artifact": "NB_Z"},
        ]
        metrics = emit_azure_monitor_metrics(results, workspace_id="ws-123")
        assert metrics["workspace_id"] == "ws-123"
        custom = {m["name"]: m["value"] for m in metrics["custom_metrics"]}
        assert custom["migration/artifacts_deployed"] == 2
        assert custom["migration/deployment_errors"] == 1

    def test_inventory_results(self):
        results = {
            "mappings": [
                {"name": "M_A", "conversion_score": 90},
                {"name": "M_B", "conversion_score": 80},
                {"name": "M_C"},  # no score
            ]
        }
        metrics = emit_azure_monitor_metrics(results)
        custom = {m["name"]: m["value"] for m in metrics["custom_metrics"]}
        assert custom["migration/mappings_total"] == 3
        assert custom["migration/conversion_score_avg"] == 85.0

    def test_writes_metrics_file(self):
        emit_azure_monitor_metrics([], workspace_id="test")
        metrics_path = FABRIC_OUTPUT_DIR / "azure_monitor_metrics.json"
        assert metrics_path.exists()

    def test_dry_run_local_only(self):
        metrics = emit_azure_monitor_metrics([], dry_run=True)
        assert metrics["emit_status"] == "local-only"

    def test_no_endpoint_status(self):
        with patch.dict(os.environ, {}, clear=False):
            # Remove AZURE_MONITOR_ENDPOINT if set
            os.environ.pop("AZURE_MONITOR_ENDPOINT", None)
            metrics = emit_azure_monitor_metrics([], dry_run=False)
            assert metrics["emit_status"] in ("local-only", "no-endpoint")

    def test_timestamp_present(self):
        metrics = emit_azure_monitor_metrics([])
        assert "timestamp" in metrics


class TestSendWebhookAlert:
    """Sprint 70: Teams/Slack webhook alerting."""

    def test_no_webhook_returns_status(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MIGRATION_WEBHOOK_URL", None)
            result = send_webhook_alert("test", webhook_url=None)
            assert result["status"] == "no-webhook-configured"

    def test_teams_format_detection(self):
        """Teams webhook URL should generate MessageCard payload."""
        with patch("deploy_to_fabric.requests") as mock_req:
            mock_resp = type("MockResp", (), {"status_code": 200})()
            mock_req.post.return_value = mock_resp
            result = send_webhook_alert(
                "Deploy complete",
                webhook_url="https://company.webhook.office.com/webhookb2/abc",
                alert_type="success",
            )
            assert result["status"] == "sent"
            call_kwargs = mock_req.post.call_args
            payload = call_kwargs[1]["json"] if "json" in call_kwargs[1] else call_kwargs.kwargs["json"]
            assert payload["@type"] == "MessageCard"

    def test_slack_format_detection(self):
        """Slack webhook URL should generate attachment payload."""
        with patch("deploy_to_fabric.requests") as mock_req:
            mock_resp = type("MockResp", (), {"status_code": 200})()
            mock_req.post.return_value = mock_resp
            result = send_webhook_alert(
                "Deploy complete",
                webhook_url="https://hooks.slack.com/services/T00/B00/xxx",
                alert_type="success",
            )
            assert result["status"] == "sent"
            call_kwargs = mock_req.post.call_args
            payload = call_kwargs[1]["json"] if "json" in call_kwargs[1] else call_kwargs.kwargs["json"]
            assert "text" in payload

    def test_generic_webhook(self):
        """Non-Teams/Slack URL should send generic JSON."""
        with patch("deploy_to_fabric.requests") as mock_req:
            mock_resp = type("MockResp", (), {"status_code": 200})()
            mock_req.post.return_value = mock_resp
            result = send_webhook_alert(
                "Hello",
                webhook_url="https://my-custom-hook.example.com/notify",
                alert_type="warning",
                details={"count": 5},
            )
            assert result["status"] == "sent"

    def test_alert_types(self):
        """All alert types should produce valid payloads."""
        for alert_type in ("info", "warning", "error", "success"):
            result = send_webhook_alert(f"test {alert_type}", alert_type=alert_type)
            # No webhook configured, so just make sure it didn't crash
            assert "status" in result

    def test_details_passed_to_payload(self):
        with patch("deploy_to_fabric.requests") as mock_req:
            mock_resp = type("MockResp", (), {"status_code": 200})()
            mock_req.post.return_value = mock_resp
            send_webhook_alert(
                "Deploy",
                webhook_url="https://company.webhook.office.com/xx",
                details={"env": "prod", "count": 10},
            )
            call_kwargs = mock_req.post.call_args
            payload = call_kwargs[1]["json"] if "json" in call_kwargs[1] else call_kwargs.kwargs["json"]
            # Teams MessageCard puts details in sections.facts
            facts = payload.get("sections", [{}])[0].get("facts", [])
            fact_names = [f["name"] for f in facts]
            assert "env" in fact_names
            assert "count" in fact_names

    def test_requests_not_installed(self):
        with patch("deploy_to_fabric.requests", None):
            result = send_webhook_alert(
                "test",
                webhook_url="https://hooks.slack.com/x",
            )
            assert result["status"] == "requests-not-installed"
