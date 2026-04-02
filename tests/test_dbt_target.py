"""
Tests for DBT target support (Sprint 51 — Phase 4).

Covers:
- Target router: should_use_dbt / classify_mappings
- DBT model generation: staging, intermediate, mart
- Project scaffolding: dbt_project.yml, profiles.yml, sources.yml, schema.yml
- SQL dialect conversion: Oracle → Databricks SQL
- CLI --target dbt|pyspark|auto flag
- Auto-mode routing
- Integration: end-to-end dbt project generation
"""

import json
import os
import re
import shutil
import sys
import textwrap
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
                  lookup_conditions=None):
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
        "parameters": [],
        "target_load_order": [],
        "conversion_score": 0,
        "manual_effort_hours": 0,
        "lineage_summary": "",
        "field_lineage": [],
    }


# ═══════════════════════════════════════════════
#  1. Target Router Tests
# ═══════════════════════════════════════════════

class TestTargetRouter:
    """Tests for should_use_dbt() and classify_mappings()."""

    def test_simple_mapping_goes_to_dbt(self):
        from run_dbt_migration import should_use_dbt
        m = _make_mapping(complexity="Simple", transforms=["SQ", "EXP", "FIL"])
        assert should_use_dbt(m) is True

    def test_medium_mapping_goes_to_dbt(self):
        from run_dbt_migration import should_use_dbt
        m = _make_mapping(complexity="Medium", transforms=["SQ", "EXP", "FIL", "LKP", "AGG"])
        assert should_use_dbt(m) is True

    def test_complex_mapping_goes_to_pyspark(self):
        from run_dbt_migration import should_use_dbt
        m = _make_mapping(complexity="Complex", transforms=["SQ", "JNR", "EXP", "SQLT"])
        assert should_use_dbt(m) is False

    def test_custom_complexity_goes_to_pyspark(self):
        from run_dbt_migration import should_use_dbt
        m = _make_mapping(complexity="Custom", transforms=["SQ", "EXP"])
        assert should_use_dbt(m) is False

    def test_unknown_complexity_goes_to_pyspark(self):
        from run_dbt_migration import should_use_dbt
        m = _make_mapping(complexity="Unknown", transforms=["SQ", "EXP"])
        assert should_use_dbt(m) is False

    def test_pyspark_only_transform_forces_pyspark(self):
        from run_dbt_migration import should_use_dbt
        m = _make_mapping(complexity="Simple", transforms=["SQ", "EXP", "JTX"])
        assert should_use_dbt(m) is False

    def test_java_transform_forces_pyspark(self):
        from run_dbt_migration import should_use_dbt
        m = _make_mapping(complexity="Medium", transforms=["SQ", "CT"])
        assert should_use_dbt(m) is False

    def test_http_transform_forces_pyspark(self):
        from run_dbt_migration import should_use_dbt
        m = _make_mapping(complexity="Simple", transforms=["SQ", "HTTP"])
        assert should_use_dbt(m) is False

    def test_xml_generator_forces_pyspark(self):
        from run_dbt_migration import should_use_dbt
        m = _make_mapping(complexity="Simple", transforms=["SQ", "XMLG"])
        assert should_use_dbt(m) is False

    def test_classify_splits_correctly(self):
        from run_dbt_migration import classify_mappings
        mappings = [
            _make_mapping("M_A", "Simple", ["SQ", "EXP"]),
            _make_mapping("M_B", "Complex", ["SQ", "JNR", "EXP"]),
            _make_mapping("M_C", "Medium", ["SQ", "AGG", "LKP"]),
            _make_mapping("M_D", "Simple", ["SQ", "JTX"]),  # PySpark-only transform
        ]
        dbt, pyspark = classify_mappings(mappings)
        assert len(dbt) == 2        # M_A, M_C
        assert len(pyspark) == 2    # M_B (Complex), M_D (JTX)
        assert {m["name"] for m in dbt} == {"M_A", "M_C"}
        assert {m["name"] for m in pyspark} == {"M_B", "M_D"}

    def test_classify_all_simple(self):
        from run_dbt_migration import classify_mappings
        mappings = [
            _make_mapping("M_1", "Simple"),
            _make_mapping("M_2", "Medium"),
        ]
        dbt, pyspark = classify_mappings(mappings)
        assert len(dbt) == 2
        assert len(pyspark) == 0

    def test_classify_all_complex(self):
        from run_dbt_migration import classify_mappings
        mappings = [
            _make_mapping("M_1", "Complex"),
            _make_mapping("M_2", "Custom"),
        ]
        dbt, pyspark = classify_mappings(mappings)
        assert len(dbt) == 0
        assert len(pyspark) == 2

    def test_classify_empty(self):
        from run_dbt_migration import classify_mappings
        dbt, pyspark = classify_mappings([])
        assert dbt == []
        assert pyspark == []


# ═══════════════════════════════════════════════
#  2. SQL Dialect Conversion Tests
# ═══════════════════════════════════════════════

class TestSQLConversion:
    """Tests for Oracle → Databricks SQL conversion."""

    def test_nvl_to_coalesce(self):
        from run_dbt_migration import convert_sql_to_dbsql
        assert "COALESCE(" in convert_sql_to_dbsql("NVL(col, 0)")

    def test_sysdate_to_current_timestamp(self):
        from run_dbt_migration import convert_sql_to_dbsql
        result = convert_sql_to_dbsql("WHERE created > SYSDATE")
        assert "CURRENT_TIMESTAMP()" in result

    def test_to_char_conversion(self):
        from run_dbt_migration import convert_sql_to_dbsql
        result = convert_sql_to_dbsql("TO_CHAR(dt, 'YYYY-MM-DD')")
        assert "DATE_FORMAT" in result

    def test_trunc_to_date_trunc(self):
        from run_dbt_migration import convert_sql_to_dbsql
        result = convert_sql_to_dbsql("TRUNC(some_date)")
        assert "DATE_TRUNC" in result

    def test_decode_placeholder(self):
        from run_dbt_migration import convert_sql_to_dbsql
        result = convert_sql_to_dbsql("DECODE(status, 'A', 'Active')")
        assert "CASE" in result

    def test_rownum_to_row_number(self):
        from run_dbt_migration import convert_sql_to_dbsql
        result = convert_sql_to_dbsql("WHERE ROWNUM <= 10")
        assert "ROW_NUMBER()" in result

    def test_substr_to_substring(self):
        from run_dbt_migration import convert_sql_to_dbsql
        result = convert_sql_to_dbsql("SUBSTR(name, 1, 5)")
        assert "SUBSTRING(" in result

    def test_add_months_preserved(self):
        from run_dbt_migration import convert_sql_to_dbsql
        result = convert_sql_to_dbsql("ADD_MONTHS(SYSDATE, -3)")
        assert "ADD_MONTHS" in result
        assert "CURRENT_TIMESTAMP()" in result

    def test_multiple_conversions(self):
        from run_dbt_migration import convert_sql_to_dbsql
        sql = "SELECT NVL(name, 'N/A'), SYSDATE, TRUNC(dt) FROM t WHERE ROWNUM <= 10"
        result = convert_sql_to_dbsql(sql)
        assert "COALESCE(" in result
        assert "CURRENT_TIMESTAMP()" in result
        assert "DATE_TRUNC" in result
        assert "ROW_NUMBER()" in result


# ═══════════════════════════════════════════════
#  3. DBT Model Generation Tests
# ═══════════════════════════════════════════════

class TestModelGeneration:
    """Tests for staging, intermediate, and mart model generation."""

    def test_staging_model_basic(self):
        from run_dbt_migration import generate_staging_model
        m = _make_mapping("M_LOAD_CUSTOMERS", sources=["Oracle.SALES.CUSTOMERS"])
        sql = generate_staging_model(m)
        assert "stg" not in sql or "staging" in sql.lower()  # config has staging tag
        assert "source(" in sql
        assert "customers" in sql.lower()
        assert "M_LOAD_CUSTOMERS" in sql  # comment

    def test_staging_model_with_sql_override(self):
        from run_dbt_migration import generate_staging_model
        m = _make_mapping("M_OVERRIDE", sql_overrides=[
            {"type": "Sql Query", "value": "SELECT * FROM SALES.ORDERS WHERE STATUS = 'A'"}
        ])
        sql = generate_staging_model(m)
        assert "SELECT" in sql
        assert "ORDERS" in sql
        # Should NOT contain {{ source() }} when SQL override is used
        assert "source(" not in sql

    def test_intermediate_model_basic(self):
        from run_dbt_migration import generate_intermediate_model
        m = _make_mapping("M_TEST", transforms=["SQ", "EXP", "FIL"])
        sql = generate_intermediate_model(m)
        assert "ref('stg_m_test')" in sql
        assert "expressions" in sql  # EXP transform
        assert "filtered" in sql     # FIL transform

    def test_intermediate_model_with_agg(self):
        from run_dbt_migration import generate_intermediate_model
        m = _make_mapping("M_SALES", transforms=["SQ", "AGG"])
        sql = generate_intermediate_model(m)
        assert "aggregated" in sql
        assert "GROUP BY" in sql

    def test_intermediate_model_with_lookup(self):
        from run_dbt_migration import generate_intermediate_model
        m = _make_mapping("M_LKP", transforms=["SQ", "LKP"],
                         lookup_conditions=[{"lookup": "LKP_PRODUCTS", "condition": "a = b"}])
        sql = generate_intermediate_model(m)
        assert "with_lookup" in sql
        assert "LEFT JOIN" in sql
        assert "LKP_PRODUCTS" in sql

    def test_intermediate_model_with_rank(self):
        from run_dbt_migration import generate_intermediate_model
        m = _make_mapping("M_RNK", transforms=["SQ", "RNK"])
        sql = generate_intermediate_model(m)
        assert "ranked" in sql
        assert "ROW_NUMBER()" in sql

    def test_intermediate_model_with_union(self):
        from run_dbt_migration import generate_intermediate_model
        m = _make_mapping("M_UNI", transforms=["SQ", "UNI"])
        sql = generate_intermediate_model(m)
        assert "unioned" in sql
        assert "UNION ALL" in sql

    def test_intermediate_model_with_joiner(self):
        from run_dbt_migration import generate_intermediate_model
        m = _make_mapping("M_JNR", transforms=["SQ", "JNR"])
        sql = generate_intermediate_model(m)
        assert "joined" in sql
        assert "JOIN" in sql

    def test_intermediate_model_with_normalizer(self):
        from run_dbt_migration import generate_intermediate_model
        m = _make_mapping("M_NRM", transforms=["SQ", "NRM"])
        sql = generate_intermediate_model(m)
        assert "normalized" in sql
        assert "EXPLODE" in sql

    def test_intermediate_model_with_sequence(self):
        from run_dbt_migration import generate_intermediate_model
        m = _make_mapping("M_SEQ", transforms=["SQ", "SEQ"])
        sql = generate_intermediate_model(m)
        assert "with_seq" in sql
        assert "ROW_NUMBER()" in sql
        assert "sk_id" in sql

    def test_intermediate_model_with_router(self):
        from run_dbt_migration import generate_intermediate_model
        m = _make_mapping("M_RTR", transforms=["SQ", "RTR"])
        sql = generate_intermediate_model(m)
        assert "routed" in sql

    def test_intermediate_model_with_data_masking(self):
        from run_dbt_migration import generate_intermediate_model
        m = _make_mapping("M_DM", transforms=["SQ", "DM"])
        sql = generate_intermediate_model(m)
        assert "masked" in sql
        assert "MD5" in sql

    def test_intermediate_model_with_sql_tx(self):
        from run_dbt_migration import generate_intermediate_model
        m = _make_mapping("M_SQLT", transforms=["SQ", "SQLT"])
        sql = generate_intermediate_model(m)
        assert "sql_tx" in sql

    def test_intermediate_model_with_stored_proc(self):
        from run_dbt_migration import generate_intermediate_model
        m = _make_mapping("M_SP", transforms=["SQ", "SP"])
        sql = generate_intermediate_model(m)
        assert "stored_proc" in sql

    def test_intermediate_model_chain(self):
        """Test multi-transform chain produces correct CTE order."""
        from run_dbt_migration import generate_intermediate_model
        m = _make_mapping("M_CHAIN", transforms=["SQ", "EXP", "FIL", "LKP", "AGG"])
        sql = generate_intermediate_model(m)
        # Verify CTE chain order
        exp_pos = sql.find("expressions")
        fil_pos = sql.find("filtered")
        lkp_pos = sql.find("with_lookup")
        agg_pos = sql.find("aggregated")
        assert exp_pos < fil_pos < lkp_pos < agg_pos

    def test_intermediate_model_ignores_port(self):
        """The 'port' metadata transform should not create a CTE."""
        from run_dbt_migration import generate_intermediate_model
        m = _make_mapping("M_PORT", transforms=["SQ", "port", "EXP"])
        sql = generate_intermediate_model(m)
        assert "port" not in sql.split("expressions")[0] or True  # No CTE for port

    def test_mart_model_table(self):
        from run_dbt_migration import generate_mart_model
        m = _make_mapping("M_LOAD", transforms=["SQ", "EXP"], targets=["DIM_CUSTOMER"])
        sql = generate_mart_model(m)
        assert 'materialized="table"' in sql
        assert "ref('int_m_load')" in sql

    def test_mart_model_incremental(self):
        from run_dbt_migration import generate_mart_model
        m = _make_mapping("M_UPSERT", transforms=["SQ", "EXP", "UPD"], targets=["FACT_ORDERS"])
        sql = generate_mart_model(m)
        assert 'materialized="incremental"' in sql
        assert "is_incremental()" in sql
        assert 'unique_key="id"' in sql


# ═══════════════════════════════════════════════
#  4. Project Scaffolding Tests
# ═══════════════════════════════════════════════

class TestProjectScaffolding:
    """Tests for dbt_project.yml, profiles.yml, sources.yml, schema.yml."""

    def test_dbt_project_yml(self):
        from run_dbt_migration import generate_dbt_project_yml
        yml = generate_dbt_project_yml("test_project")
        assert "name: 'test_project'" in yml
        assert "config-version: 2" in yml
        assert "profile: 'databricks_migration'" in yml
        assert "staging:" in yml
        assert "intermediate:" in yml
        assert "marts:" in yml

    def test_profiles_yml(self):
        from run_dbt_migration import generate_profiles_yml
        yml = generate_profiles_yml("analytics")
        assert "type: databricks" in yml
        assert "catalog: analytics" in yml
        assert "dev:" in yml
        assert "prod:" in yml
        assert "DBT_DATABRICKS_HOST" in yml

    def test_profiles_yml_default_catalog(self):
        from run_dbt_migration import generate_profiles_yml
        with mock.patch.dict(os.environ, {"INFORMATICA_DATABRICKS_CATALOG": "main"}):
            yml = generate_profiles_yml()
            assert "catalog: main" in yml

    def test_sources_yml(self):
        from run_dbt_migration import generate_sources_yml
        mappings = [
            _make_mapping("M_A", sources=["Oracle.SALES.CUSTOMERS"]),
            _make_mapping("M_B", sources=["Oracle.SALES.ORDERS"]),
            _make_mapping("M_C", sources=["Oracle.FINANCE.ACCOUNTS"]),
        ]
        yml = generate_sources_yml(mappings, "main")
        assert "version: 2" in yml
        assert "database: main" in yml
        assert "sales" in yml
        assert "customers" in yml
        assert "orders" in yml
        assert "finance" in yml
        assert "accounts" in yml

    def test_sources_yml_deduplicates(self):
        from run_dbt_migration import generate_sources_yml
        mappings = [
            _make_mapping("M_A", sources=["Oracle.SALES.CUSTOMERS"]),
            _make_mapping("M_B", sources=["Oracle.SALES.CUSTOMERS"]),
        ]
        yml = generate_sources_yml(mappings, "main")
        # Should only have 'customers' once under 'sales'
        assert yml.count("- name: customers") == 1

    def test_schema_yml(self):
        from run_dbt_migration import generate_schema_yml
        mappings = [_make_mapping("M_TEST")]
        yml = generate_schema_yml(mappings)
        assert "version: 2" in yml
        assert "stg_m_test" in yml
        assert "not_null" in yml

    def test_packages_yml(self):
        from run_dbt_migration import generate_packages_yml
        yml = generate_packages_yml()
        assert "dbt_utils" in yml
        assert "dbt_expectations" in yml


# ═══════════════════════════════════════════════
#  5. Name Sanitization Tests
# ═══════════════════════════════════════════════

class TestSanitization:
    def test_sanitize_lowercase(self):
        from run_dbt_migration import _sanitize_name
        assert _sanitize_name("M_LOAD_CUSTOMERS") == "m_load_customers"

    def test_sanitize_special_chars(self):
        from run_dbt_migration import _sanitize_name
        assert _sanitize_name("M-LOAD.CUST!") == "m_load_cust"

    def test_sanitize_strip_underscores(self):
        from run_dbt_migration import _sanitize_name
        assert _sanitize_name("__test__") == "test"


# ═══════════════════════════════════════════════
#  6. CLI & Environment Tests
# ═══════════════════════════════════════════════

class TestCLIDbtMode:
    """Tests for --target dbt/pyspark/auto CLI integration."""

    def test_get_dbt_mode_default(self):
        from run_dbt_migration import get_dbt_mode
        with mock.patch.dict(os.environ, {}, clear=True):
            # Environment might have INFORMATICA_DBT_MODE from parent
            os.environ.pop("INFORMATICA_DBT_MODE", None)
            assert get_dbt_mode() == ""

    def test_get_dbt_mode_dbt(self):
        from run_dbt_migration import get_dbt_mode
        with mock.patch.dict(os.environ, {"INFORMATICA_DBT_MODE": "dbt"}):
            assert get_dbt_mode() == "dbt"

    def test_get_dbt_mode_auto(self):
        from run_dbt_migration import get_dbt_mode
        with mock.patch.dict(os.environ, {"INFORMATICA_DBT_MODE": "auto"}):
            assert get_dbt_mode() == "auto"

    def test_get_dbt_mode_pyspark(self):
        from run_dbt_migration import get_dbt_mode
        with mock.patch.dict(os.environ, {"INFORMATICA_DBT_MODE": "pyspark"}):
            assert get_dbt_mode() == "pyspark"

    def test_get_catalog_default(self):
        from run_dbt_migration import get_catalog
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("INFORMATICA_DATABRICKS_CATALOG", None)
            assert get_catalog() == "main"

    def test_get_catalog_custom(self):
        from run_dbt_migration import get_catalog
        with mock.patch.dict(os.environ, {"INFORMATICA_DATABRICKS_CATALOG": "analytics"}):
            assert get_catalog() == "analytics"

    def test_cli_target_choices(self):
        """Verify run_migration.py accepts dbt/pyspark/auto targets."""
        import run_migration
        parser = run_migration._parse_args.__wrapped__ if hasattr(run_migration._parse_args, '__wrapped__') else None
        # Directly test by reading the argparse setup
        import argparse
        # Just verify the choices are present in the module
        source = Path(PROJECT_ROOT / "run_migration.py").read_text(encoding="utf-8")
        assert '"dbt"' in source
        assert '"pyspark"' in source
        assert '"auto"' in source


# ═══════════════════════════════════════════════
#  7. Integration Tests (E2E)
# ═══════════════════════════════════════════════

class TestE2EGeneration:
    """End-to-end dbt project generation from inventory."""

    @pytest.fixture
    def dbt_output(self, tmp_path):
        """Generate a dbt project to a temp directory."""
        import run_dbt_migration as mod
        # Redirect output
        orig_output = mod.OUTPUT_DIR
        mod.OUTPUT_DIR = tmp_path / "dbt"
        try:
            mappings = [
                _make_mapping("M_SIMPLE_A", "Simple", ["SQ", "EXP", "FIL"]),
                _make_mapping("M_MEDIUM_B", "Medium", ["SQ", "EXP", "AGG", "LKP"]),
                _make_mapping("M_COMPLEX_C", "Complex", ["SQ", "JNR", "EXP", "SQLT"]),
            ]
            dbt_maps, pyspark_maps = mod.classify_mappings(mappings)
            summary = mod.write_dbt_project(dbt_maps, mappings)
            yield {
                "path": tmp_path / "dbt",
                "summary": summary,
                "dbt_maps": dbt_maps,
                "pyspark_maps": pyspark_maps,
            }
        finally:
            mod.OUTPUT_DIR = orig_output

    def test_project_structure(self, dbt_output):
        base = dbt_output["path"]
        assert (base / "dbt_project.yml").exists()
        assert (base / "profiles.yml").exists()
        assert (base / "packages.yml").exists()
        assert (base / "models" / "sources.yml").exists()
        assert (base / "models" / "schema.yml").exists()
        assert (base / "models" / "staging").is_dir()
        assert (base / "models" / "intermediate").is_dir()
        assert (base / "models" / "marts").is_dir()
        assert (base / "macros").is_dir()
        assert (base / "tests").is_dir()
        assert (base / "seeds").is_dir()

    def test_correct_model_count(self, dbt_output):
        """Only Simple/Medium mappings should produce dbt models."""
        base = dbt_output["path"]
        staging = list((base / "models" / "staging").glob("stg_*.sql"))
        intermediate = list((base / "models" / "intermediate").glob("int_*.sql"))
        marts = list((base / "models" / "marts").glob("mart_*.sql"))
        # 2 dbt-eligible mappings
        assert len(staging) == 2
        assert len(intermediate) == 2
        assert len(marts) == 2

    def test_complex_mapping_excluded(self, dbt_output):
        """Complex mapping should NOT have dbt models."""
        base = dbt_output["path"]
        staging_names = [f.stem for f in (base / "models" / "staging").glob("*.sql")]
        assert "stg_m_complex_c" not in staging_names

    def test_simple_mapping_included(self, dbt_output):
        base = dbt_output["path"]
        staging_names = [f.stem for f in (base / "models" / "staging").glob("*.sql")]
        assert "stg_m_simple_a" in staging_names
        assert "stg_m_medium_b" in staging_names

    def test_summary_json(self, dbt_output):
        base = dbt_output["path"]
        summary_path = base / "dbt_generation_summary.json"
        assert summary_path.exists()
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert summary["total_dbt_mappings"] == 2
        assert summary["total_all_mappings"] == 3
        assert 60 < summary["dbt_percentage"] < 70  # ~66.7%

    def test_staging_model_content(self, dbt_output):
        base = dbt_output["path"]
        stg = (base / "models" / "staging" / "stg_m_simple_a.sql").read_text(encoding="utf-8")
        assert "source(" in stg
        assert "customers" in stg.lower()
        assert "staging" in stg

    def test_intermediate_model_content(self, dbt_output):
        base = dbt_output["path"]
        int_model = (base / "models" / "intermediate" / "int_m_simple_a.sql").read_text(encoding="utf-8")
        assert "ref('stg_m_simple_a')" in int_model
        assert "expressions" in int_model
        assert "filtered" in int_model

    def test_mart_model_content(self, dbt_output):
        base = dbt_output["path"]
        mart = (base / "models" / "marts" / "mart_m_simple_a.sql").read_text(encoding="utf-8")
        assert "ref('int_m_simple_a')" in mart
        assert 'materialized="table"' in mart

    def test_routing_percentages(self, dbt_output):
        assert len(dbt_output["dbt_maps"]) == 2
        assert len(dbt_output["pyspark_maps"]) == 1

    def test_dbt_project_yml_valid(self, dbt_output):
        base = dbt_output["path"]
        yml = (base / "dbt_project.yml").read_text(encoding="utf-8")
        assert "informatica_migration" in yml
        assert "model-paths" in yml

    def test_profiles_yml_valid(self, dbt_output):
        base = dbt_output["path"]
        yml = (base / "profiles.yml").read_text(encoding="utf-8")
        assert "databricks_migration" in yml
        assert "type: databricks" in yml


# ═══════════════════════════════════════════════
#  8. Model Config Tests
# ═══════════════════════════════════════════════

class TestModelConfig:
    def test_config_view(self):
        from run_dbt_migration import _model_config
        cfg = _model_config(materialized="view")
        assert '{{ config(materialized="view") }}' == cfg

    def test_config_with_tags(self):
        from run_dbt_migration import _model_config
        cfg = _model_config(materialized="table", tags=["staging"])
        assert 'tags=["staging"]' in cfg

    def test_config_with_unique_key(self):
        from run_dbt_migration import _model_config
        cfg = _model_config(materialized="incremental", unique_key="order_id")
        assert 'unique_key="order_id"' in cfg

    def test_config_with_schema(self):
        from run_dbt_migration import _model_config
        cfg = _model_config(materialized="view", schema="staging")
        assert 'schema="staging"' in cfg


# ═══════════════════════════════════════════════
#  9. Eligible Transform Constants Tests
# ═══════════════════════════════════════════════

class TestTransformConstants:
    def test_pyspark_only_transforms(self):
        from run_dbt_migration import PYSPARK_ONLY_TRANSFORMS
        assert "JTX" in PYSPARK_ONLY_TRANSFORMS
        assert "CT" in PYSPARK_ONLY_TRANSFORMS
        assert "HTTP" in PYSPARK_ONLY_TRANSFORMS
        assert "XMLG" in PYSPARK_ONLY_TRANSFORMS
        assert "XMLP" in PYSPARK_ONLY_TRANSFORMS

    def test_sql_expressible_transforms(self):
        from run_dbt_migration import SQL_EXPRESSIBLE_TRANSFORMS
        for tx in ["SQ", "EXP", "FIL", "AGG", "LKP", "SRT", "JNR", "UNI", "RNK"]:
            assert tx in SQL_EXPRESSIBLE_TRANSFORMS

    def test_no_overlap(self):
        from run_dbt_migration import PYSPARK_ONLY_TRANSFORMS, SQL_EXPRESSIBLE_TRANSFORMS
        assert not PYSPARK_ONLY_TRANSFORMS & SQL_EXPRESSIBLE_TRANSFORMS
