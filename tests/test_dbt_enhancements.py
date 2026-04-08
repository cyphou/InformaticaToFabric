"""
Tests for DBT enhancements (Sprint 67 — Phase 5).

Covers:
1. DECODE → CASE expansion (nested, with/without default)
2. SCD2 snapshot candidate detection (_is_scd2_candidate)
3. Mixed workflow wiring (run_pipeline_migration.py integration)
4. Enriched intermediate CTEs (field_lineage, lookup_conditions)
5. Router → separate dbt models per target
6. Standalone deploy_dbt_project.py existence
7. Helper functions: _extract_instance_info, _fields_through_instance
"""

import json
import os
import re
import shutil
import sys
from pathlib import Path
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ═══════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════

def _make_mapping(name="M_TEST", complexity="Simple", transforms=None,
                  sources=None, targets=None, sql_overrides=None,
                  lookup_conditions=None, field_lineage=None):
    """Construct a minimal mapping dict for tests."""
    return {
        "name": name,
        "sources": sources or ["Oracle.SALES.CUSTOMERS"],
        "targets": targets or ["DIM_CUSTOMER"],
        "transformations": transforms or ["SQ", "EXP", "FIL"],
        "has_sql_override": bool(sql_overrides),
        "has_stored_proc": False,
        "complexity": complexity,
        "sql_overrides": sql_overrides or [],
        "lookup_conditions": lookup_conditions or [],
        "field_lineage": field_lineage or [],
        "parameters": [],
        "target_load_order": [],
        "conversion_score": 0,
        "manual_effort_hours": 0,
    }


# ═══════════════════════════════════════════════
#  1. DECODE → CASE expansion
# ═══════════════════════════════════════════════

class TestDecodeExpansion:
    """Test _expand_decode() with various patterns."""

    def test_simple_decode(self):
        from run_dbt_migration import _expand_decode
        sql = "SELECT _DECODE_(status, 'A', 'Active', 'I', 'Inactive', 'Unknown') FROM t"
        result = _expand_decode(sql)
        assert "CASE status" in result
        assert "WHEN 'A' THEN 'Active'" in result
        assert "WHEN 'I' THEN 'Inactive'" in result
        assert "ELSE 'Unknown'" in result
        assert "END" in result
        assert "_DECODE_" not in result

    def test_decode_no_default(self):
        from run_dbt_migration import _expand_decode
        sql = "_DECODE_(x, 1, 'one', 2, 'two')"
        result = _expand_decode(sql)
        assert "CASE x" in result
        assert "WHEN 1 THEN 'one'" in result
        assert "WHEN 2 THEN 'two'" in result
        assert "ELSE" not in result

    def test_decode_with_nested_parens(self):
        from run_dbt_migration import _expand_decode
        sql = "_DECODE_(UPPER(col), 'A', fn(1,2), 'B', 'beta')"
        result = _expand_decode(sql)
        assert "CASE UPPER(col)" in result
        assert "WHEN 'A' THEN fn(1,2)" in result
        assert "WHEN 'B' THEN 'beta'" in result

    def test_no_decode_passthrough(self):
        from run_dbt_migration import _expand_decode
        sql = "SELECT col FROM table WHERE x = 1"
        result = _expand_decode(sql)
        assert result == sql

    def test_decode_too_few_args(self):
        from run_dbt_migration import _expand_decode
        sql = "_DECODE_(a, b)"
        result = _expand_decode(sql)
        assert "DECODE(a, b)" in result  # passthrough

    def test_multiple_decodes(self):
        from run_dbt_migration import _expand_decode
        sql = "_DECODE_(a, 1, 'x') + _DECODE_(b, 2, 'y')"
        result = _expand_decode(sql)
        assert result.count("CASE") == 2
        assert result.count("END") == 2

    def test_split_decode_args(self):
        from run_dbt_migration import _split_decode_args
        args = _split_decode_args("a, fn(1,2), 'x', b")
        assert len(args) == 4
        assert args[1].strip() == "fn(1,2)"


# ═══════════════════════════════════════════════
#  2. SCD2 snapshot candidate detection
# ═══════════════════════════════════════════════

class TestSCD2CandidateDetection:
    """Test _is_scd2_candidate() logic."""

    def test_upd_transform_triggers_scd2(self):
        from run_dbt_migration import _is_scd2_candidate
        mapping = _make_mapping(transforms=["SQ", "EXP", "UPD"])
        assert _is_scd2_candidate(mapping) is True

    def test_target_name_history(self):
        from run_dbt_migration import _is_scd2_candidate
        mapping = _make_mapping(targets=["tgt_gold_inventory_history"])
        assert _is_scd2_candidate(mapping) is True

    def test_target_name_snapshot(self):
        from run_dbt_migration import _is_scd2_candidate
        mapping = _make_mapping(targets=["customer_snapshot"])
        assert _is_scd2_candidate(mapping) is True

    def test_target_name_scd(self):
        from run_dbt_migration import _is_scd2_candidate
        mapping = _make_mapping(targets=["dim_customer_scd2"])
        assert _is_scd2_candidate(mapping) is True

    def test_target_name_archive(self):
        from run_dbt_migration import _is_scd2_candidate
        mapping = _make_mapping(targets=["order_archive"])
        assert _is_scd2_candidate(mapping) is True

    def test_mapping_name_scd(self):
        from run_dbt_migration import _is_scd2_candidate
        mapping = _make_mapping(name="M_SCD_LOAD_DIM")
        assert _is_scd2_candidate(mapping) is True

    def test_mapping_name_history(self):
        from run_dbt_migration import _is_scd2_candidate
        mapping = _make_mapping(name="M_LOAD_HISTORY_TABLE")
        assert _is_scd2_candidate(mapping) is True

    def test_negative_case(self):
        from run_dbt_migration import _is_scd2_candidate
        mapping = _make_mapping(name="M_LOAD_CUSTOMERS", targets=["DIM_CUSTOMER"])
        assert _is_scd2_candidate(mapping) is False

    def test_negative_simple_mapping(self):
        from run_dbt_migration import _is_scd2_candidate
        mapping = _make_mapping(
            name="M_LOAD_ORDERS",
            targets=["FCT_ORDERS"],
            transforms=["SQ", "EXP", "FIL"],
        )
        assert _is_scd2_candidate(mapping) is False


# ═══════════════════════════════════════════════
#  3. Field lineage helper functions
# ═══════════════════════════════════════════════

class TestFieldLineageHelpers:
    """Test _extract_instance_info and _fields_through_instance."""

    def test_extract_instance_info_basic(self):
        from run_dbt_migration import _extract_instance_info
        lineage = [
            {
                "source_field": "FIRST_NAME",
                "target_field": "FULL_NAME",
                "transformations": [
                    {"instance": "EXP_DERIVE_FIELDS", "type": "Expression"},
                ],
            },
            {
                "source_field": "STATUS",
                "target_field": "STATUS_LABEL",
                "transformations": [
                    {"instance": "EXP_DERIVE_FIELDS", "type": "Expression"},
                    {"instance": "FIL_ACTIVE", "type": "Filter"},
                ],
            },
        ]
        info = _extract_instance_info(lineage)
        assert "EXP_DERIVE_FIELDS" in info
        assert info["EXP_DERIVE_FIELDS"]["type"] == "Expression"
        assert "FULL_NAME" in info["EXP_DERIVE_FIELDS"]["fields"]
        assert "STATUS_LABEL" in info["EXP_DERIVE_FIELDS"]["fields"]
        assert "FIL_ACTIVE" in info
        assert info["FIL_ACTIVE"]["type"] == "Filter"

    def test_extract_instance_info_empty(self):
        from run_dbt_migration import _extract_instance_info
        assert _extract_instance_info([]) == {}

    def test_fields_through_instance(self):
        from run_dbt_migration import _fields_through_instance
        lineage = [
            {
                "source_field": "CUSTOMER_ID",
                "target_field": "CUSTOMER_KEY",
                "transformations": [
                    {"instance": "EXP_DERIVE", "type": "Expression"},
                ],
            },
            {
                "source_field": "NAME",
                "target_field": "FULL_NAME",
                "transformations": [
                    {"instance": "EXP_DERIVE", "type": "Expression"},
                ],
            },
            {
                "source_field": "STATUS",
                "target_field": "IS_ACTIVE",
                "transformations": [
                    {"instance": "FIL_ACTIVE", "type": "Filter"},
                ],
            },
        ]
        fields = _fields_through_instance(lineage, "EXP_DERIVE")
        assert "CUSTOMER_KEY" in fields
        assert "FULL_NAME" in fields
        assert "IS_ACTIVE" not in fields

    def test_fields_through_instance_case_insensitive(self):
        from run_dbt_migration import _fields_through_instance
        lineage = [
            {
                "source_field": "A",
                "target_field": "B",
                "transformations": [
                    {"instance": "Exp_Derive", "type": "Expression"},
                ],
            },
        ]
        fields = _fields_through_instance(lineage, "exp_derive")
        assert "B" in fields

    def test_fields_through_instance_no_match(self):
        from run_dbt_migration import _fields_through_instance
        lineage = [
            {
                "source_field": "X",
                "target_field": "Y",
                "transformations": [
                    {"instance": "EXP_OTHER", "type": "Expression"},
                ],
            },
        ]
        fields = _fields_through_instance(lineage, "EXP_DERIVE")
        assert fields == []


# ═══════════════════════════════════════════════
#  4. Enriched intermediate CTEs
# ═══════════════════════════════════════════════

class TestEnrichedIntermediateCTEs:
    """Test that intermediate models use field_lineage data."""

    def test_exp_cte_references_instance_name(self):
        from run_dbt_migration import generate_intermediate_model
        mapping = _make_mapping(
            transforms=["SQ", "EXP"],
            field_lineage=[
                {
                    "source_field": "FIRST_NAME",
                    "target_field": "FULL_NAME",
                    "transformations": [
                        {"instance": "EXP_DERIVE_FIELDS", "type": "Expression"},
                    ],
                },
            ],
        )
        result = generate_intermediate_model(mapping)
        assert "EXP_DERIVE_FIELDS" in result
        assert "FULL_NAME" in result

    def test_fil_cte_references_instance_name(self):
        from run_dbt_migration import generate_intermediate_model
        mapping = _make_mapping(
            transforms=["SQ", "FIL"],
            field_lineage=[
                {
                    "source_field": "STATUS",
                    "target_field": "IS_ACTIVE",
                    "transformations": [
                        {"instance": "FIL_ACTIVE_CUSTOMERS", "type": "Filter"},
                    ],
                },
            ],
        )
        result = generate_intermediate_model(mapping)
        assert "FIL_ACTIVE_CUSTOMERS" in result

    def test_lkp_cte_uses_lookup_condition(self):
        from run_dbt_migration import generate_intermediate_model
        mapping = _make_mapping(
            transforms=["SQ", "LKP"],
            lookup_conditions=[
                {"lookup": "LKP_REGION", "condition": "src.REGION_ID = LKP_REGION.ID"},
            ],
            field_lineage=[
                {
                    "source_field": "REGION_ID",
                    "target_field": "REGION_NAME",
                    "transformations": [
                        {"instance": "LKP_REGION", "type": "Lookup Procedure"},
                    ],
                },
            ],
        )
        result = generate_intermediate_model(mapping)
        assert "LKP_REGION" in result
        # Should have the actual join condition, not a generic placeholder
        assert "REGION_ID" in result or "lkp_region" in result

    def test_jnr_cte_uses_second_source(self):
        from run_dbt_migration import generate_intermediate_model
        mapping = _make_mapping(
            transforms=["SQ", "JNR"],
            sources=["CUSTOMERS", "ORDERS"],
            field_lineage=[
                {
                    "source_field": "CUST_ID",
                    "target_field": "CUSTOMER_ID",
                    "transformations": [
                        {"instance": "JNR_CUST_ORDERS", "type": "Joiner"},
                    ],
                },
            ],
        )
        result = generate_intermediate_model(mapping)
        assert "JNR_CUST_ORDERS" in result
        assert "stg_orders" in result.lower()

    def test_agg_cte_references_instance(self):
        from run_dbt_migration import generate_intermediate_model
        mapping = _make_mapping(
            transforms=["SQ", "AGG"],
            field_lineage=[
                {
                    "source_field": "AMOUNT",
                    "target_field": "TOTAL_AMOUNT",
                    "transformations": [
                        {"instance": "AGG_TOTALS", "type": "Aggregator"},
                    ],
                },
            ],
        )
        result = generate_intermediate_model(mapping)
        assert "AGG_TOTALS" in result
        assert "TOTAL_AMOUNT" in result

    def test_intermediate_no_lineage_still_generates(self):
        """Even without field_lineage, CTEs should still be generated."""
        from run_dbt_migration import generate_intermediate_model
        mapping = _make_mapping(transforms=["SQ", "EXP", "FIL", "LKP"])
        result = generate_intermediate_model(mapping)
        assert "expressions_" in result
        assert "filtered_" in result
        assert "with_lookup_" in result

    def test_target_fields_extracted(self):
        from run_dbt_migration import _extract_instance_info
        lineage = [
            {
                "source_field": "A",
                "target_field": "OUT_A",
                "transformations": [{"instance": "EXP_1", "type": "Expression"}],
            },
            {
                "source_field": "B",
                "target_field": "OUT_B",
                "transformations": [{"instance": "EXP_1", "type": "Expression"}],
            },
        ]
        info = _extract_instance_info(lineage)
        assert "OUT_A" in info["EXP_1"]["fields"]
        assert "OUT_B" in info["EXP_1"]["fields"]


# ═══════════════════════════════════════════════
#  5. Router → separate dbt models
# ═══════════════════════════════════════════════

class TestRouterSeparateModels:
    """Test Router auto-split into separate per-target models."""

    def test_router_group_model_generated(self):
        from run_dbt_migration import _generate_router_group_model
        mapping = _make_mapping(
            name="M_SPLIT_DATA",
            transforms=["SQ", "EXP", "RTR"],
            targets=["TGT_HIGH_VALUE", "TGT_LOW_VALUE"],
        )
        result = _generate_router_group_model(mapping, "TGT_HIGH_VALUE", 1)
        assert "Router group 1" in result
        assert "TGT_HIGH_VALUE" in result
        assert "int_m_split_data" in result
        assert "{{ config(" in result

    def test_router_group_model_references_parent(self):
        from run_dbt_migration import _generate_router_group_model
        mapping = _make_mapping(name="M_ROUTE_TEST", transforms=["SQ", "RTR"],
                                targets=["A", "B"])
        result = _generate_router_group_model(mapping, "A", 1)
        assert "ref('int_m_route_test')" in result

    def test_router_generates_per_target_in_write(self):
        """Verify write_dbt_project creates router group files."""
        from run_dbt_migration import write_dbt_project, OUTPUT_DIR
        mapping = _make_mapping(
            name="M_RTR_DEMO",
            transforms=["SQ", "EXP", "RTR"],
            targets=["TGT_ALPHA", "TGT_BETA"],
        )
        try:
            write_dbt_project([mapping], [mapping])
            int_dir = OUTPUT_DIR / "models" / "intermediate"
            group1 = int_dir / "int_m_rtr_demo_group_1.sql"
            group2 = int_dir / "int_m_rtr_demo_group_2.sql"
            assert group1.exists(), f"Missing {group1}"
            assert group2.exists(), f"Missing {group2}"
            content1 = group1.read_text()
            assert "TGT_ALPHA" in content1 or "tgt_alpha" in content1
            content2 = group2.read_text()
            assert "TGT_BETA" in content2 or "tgt_beta" in content2
        finally:
            if OUTPUT_DIR.exists():
                shutil.rmtree(OUTPUT_DIR, ignore_errors=True)

    def test_no_router_no_extra_files(self):
        """Mappings without RTR should not generate group files."""
        from run_dbt_migration import write_dbt_project, OUTPUT_DIR
        mapping = _make_mapping(
            name="M_PLAIN",
            transforms=["SQ", "EXP", "FIL"],
            targets=["DIM_CUSTOMER"],
        )
        try:
            write_dbt_project([mapping], [mapping])
            int_dir = OUTPUT_DIR / "models" / "intermediate"
            group_files = list(int_dir.glob("*_group_*.sql"))
            assert len(group_files) == 0
        finally:
            if OUTPUT_DIR.exists():
                shutil.rmtree(OUTPUT_DIR, ignore_errors=True)


# ═══════════════════════════════════════════════
#  6. Standalone deploy_dbt_project.py
# ═══════════════════════════════════════════════

class TestDeployDBTScript:
    """Test standalone deploy_dbt_project.py exists and is valid."""

    def test_deploy_script_exists(self):
        deploy_path = PROJECT_ROOT / "deploy_dbt_project.py"
        assert deploy_path.exists(), "deploy_dbt_project.py should exist at project root"

    def test_deploy_script_has_main(self):
        deploy_path = PROJECT_ROOT / "deploy_dbt_project.py"
        content = deploy_path.read_text()
        assert 'if __name__ == "__main__"' in content
        assert "deploy_to_repos" in content

    def test_deploy_script_has_argparse(self):
        deploy_path = PROJECT_ROOT / "deploy_dbt_project.py"
        content = deploy_path.read_text()
        assert "--workspace-url" in content
        assert "--token" in content
        assert "--git-url" in content

    def test_deploy_script_importable(self):
        """The script should be importable without error (mocking requests)."""
        with mock.patch.dict("sys.modules", {"requests": mock.MagicMock()}):
            import importlib
            spec = importlib.util.spec_from_file_location(
                "deploy_dbt_project",
                str(PROJECT_ROOT / "deploy_dbt_project.py"),
            )
            mod = importlib.util.module_from_spec(spec)
            # Should not raise
            spec.loader.exec_module(mod)
            assert hasattr(mod, "deploy_to_repos")


# ═══════════════════════════════════════════════
#  7. Mixed workflow wiring
# ═══════════════════════════════════════════════

class TestMixedWorkflowWiring:
    """Test that run_pipeline_migration detects dbt mode."""

    def test_pipeline_main_imports_dbt_functions(self):
        """Verify run_pipeline_migration.py references classify_mappings and
        generate_mixed_workflow when in dbt mode."""
        pipeline_path = PROJECT_ROOT / "run_pipeline_migration.py"
        content = pipeline_path.read_text()
        assert "classify_mappings" in content
        assert "generate_mixed_workflow" in content

    def test_generate_mixed_workflow_exists(self):
        from run_dbt_migration import generate_mixed_workflow
        assert callable(generate_mixed_workflow)

    def test_classify_mappings_exists(self):
        from run_dbt_migration import classify_mappings
        assert callable(classify_mappings)

    def test_classify_mappings_returns_dbt_and_pyspark(self):
        from run_dbt_migration import classify_mappings
        mappings = [
            _make_mapping(name="M_SIMPLE", complexity="Simple",
                          transforms=["SQ", "EXP", "FIL"]),
            _make_mapping(name="M_COMPLEX", complexity="Complex",
                          transforms=["SQ", "EXP", "LKP", "JNR", "AGG", "UPD", "SP"]),
        ]
        with mock.patch.dict(os.environ, {"INFORMATICA_DBT_MODE": "auto"}):
            dbt_list, pyspark_list = classify_mappings(mappings)
        # Simple should go to dbt
        dbt_names = [m["name"] for m in dbt_list]
        assert "M_SIMPLE" in dbt_names


# ═══════════════════════════════════════════════
#  8. Integration: enriched write_dbt_project
# ═══════════════════════════════════════════════

class TestEnrichedWriteDBTProject:
    """Integration test: write_dbt_project with full field_lineage."""

    def test_full_project_with_lineage(self):
        from run_dbt_migration import write_dbt_project, OUTPUT_DIR
        mapping = _make_mapping(
            name="M_CUSTOMER_ENRICHED",
            transforms=["SQ", "EXP", "FIL", "LKP"],
            sources=["CUSTOMERS"],
            targets=["DIM_CUSTOMER"],
            lookup_conditions=[
                {"lookup": "LKP_REGION", "condition": "src.REGION_ID = lkp.ID"},
            ],
            field_lineage=[
                {
                    "source_field": "FIRST_NAME",
                    "target_field": "FULL_NAME",
                    "transformations": [
                        {"instance": "EXP_DERIVE_FIELDS", "type": "Expression"},
                    ],
                },
                {
                    "source_field": "STATUS",
                    "target_field": "IS_ACTIVE",
                    "transformations": [
                        {"instance": "FIL_ACTIVE", "type": "Filter"},
                    ],
                },
                {
                    "source_field": "REGION_ID",
                    "target_field": "REGION_NAME",
                    "transformations": [
                        {"instance": "LKP_REGION", "type": "Lookup Procedure"},
                    ],
                },
            ],
        )
        try:
            summary = write_dbt_project([mapping], [mapping])
            assert summary["total_dbt_mappings"] == 1

            # Check intermediate model has enriched content
            int_file = OUTPUT_DIR / "models" / "intermediate" / "int_m_customer_enriched.sql"
            assert int_file.exists()
            content = int_file.read_text()
            assert "EXP_DERIVE_FIELDS" in content
            assert "FULL_NAME" in content
            assert "FIL_ACTIVE" in content
            assert "LKP_REGION" in content or "lkp_region" in content
        finally:
            if OUTPUT_DIR.exists():
                shutil.rmtree(OUTPUT_DIR, ignore_errors=True)

    def test_snapshot_for_scd2_candidate(self):
        """Mapping with 'history' target should generate snapshot."""
        from run_dbt_migration import write_dbt_project, OUTPUT_DIR
        mapping = _make_mapping(
            name="M_INVENTORY_SNAP",
            transforms=["SQ", "EXP"],
            targets=["tgt_gold_inventory_history"],
        )
        try:
            write_dbt_project([mapping], [mapping])
            snp_file = OUTPUT_DIR / "snapshots" / "snp_m_inventory_snap.sql"
            assert snp_file.exists(), "Snapshot should be generated for SCD2 candidate"
        finally:
            if OUTPUT_DIR.exists():
                shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
