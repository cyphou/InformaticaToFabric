"""
Phase 8 — Sprints 71–73 Tests

Sprint 71: Query Optimization & Partition Strategy
Sprint 72: Advanced PL/SQL Conversion Engine
Sprint 73: Dynamic SQL & Complex SQL Patterns
"""

import json
import os
import sys
import textwrap
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ═══════════════════════════════════════════════
#  Sprint 71 — Query Optimization & Partition Strategy
# ═══════════════════════════════════════════════

from run_schema_generator import (
    recommend_partition_strategy,
    generate_optimize_statements,
)
from run_notebook_migration import (
    recommend_spark_config,
    detect_broadcast_candidates,
    recommend_materialization,
)
from run_sql_migration import annotate_query_performance


class TestPartitionStrategy:
    """Sprint 71: Partition strategy recommender."""

    def test_date_column_recommends_range(self):
        table = {"name": "orders", "columns": [
            {"name": "order_id", "type": "BIGINT"},
            {"name": "order_date", "type": "TIMESTAMP"},
        ]}
        result = recommend_partition_strategy(table)
        assert result["partition_type"] == "range"
        assert "order_date" in result["partition_columns"]

    def test_no_date_column_recommends_hash_with_id(self):
        table = {"name": "customers", "columns": [
            {"name": "customer_id", "type": "BIGINT"},
            {"name": "name", "type": "STRING"},
        ]}
        result = recommend_partition_strategy(table)
        assert result["partition_type"] == "hash"
        assert "customer_id" in result["partition_columns"]

    def test_category_column_detected(self):
        table = {"name": "sales", "columns": [
            {"name": "region", "type": "STRING"},
            {"name": "product_id", "type": "BIGINT"},
        ]}
        result = recommend_partition_strategy(table)
        assert result["partition_type"] == "hash"
        assert "region" in result["partition_columns"]

    def test_no_suitable_columns_returns_none(self):
        table = {"name": "misc", "columns": [
            {"name": "value", "type": "STRING"},
            {"name": "description", "type": "STRING"},
        ]}
        result = recommend_partition_strategy(table)
        assert result["partition_type"] == "none"
        assert result["partition_columns"] == []

    def test_zorder_columns_exclude_partition_key(self):
        table = {"name": "events", "columns": [
            {"name": "event_date", "type": "TIMESTAMP"},
            {"name": "event_id", "type": "BIGINT"},
            {"name": "user_id", "type": "BIGINT"},
        ]}
        result = recommend_partition_strategy(table)
        assert "event_date" in result["partition_columns"]
        assert "event_date" not in result["zorder_columns"]

    def test_multiple_date_columns_picks_first(self):
        table = {"name": "txn", "columns": [
            {"name": "transaction_date", "type": "TIMESTAMP"},
            {"name": "created_date", "type": "TIMESTAMP"},
        ]}
        result = recommend_partition_strategy(table)
        assert len(result["partition_columns"]) == 1

    def test_high_benefit_for_date_partition(self):
        table = {"name": "logs", "columns": [
            {"name": "log_date", "type": "TIMESTAMP"},
        ]}
        result = recommend_partition_strategy(table)
        assert result["estimated_benefit"] == "low"  # log_date not in priority list
        table2 = {"name": "logs", "columns": [
            {"name": "event_date", "type": "TIMESTAMP"},
        ]}
        result2 = recommend_partition_strategy(table2)
        assert result2["estimated_benefit"] == "high"


class TestGenerateOptimizeStatements:
    """Sprint 71: OPTIMIZE / ZORDER / ANALYZE TABLE generation."""

    def test_generates_optimize_for_table(self):
        tables = [{"name": "orders", "tier": "silver", "columns": [
            {"name": "order_id", "type": "BIGINT"},
        ]}]
        stmts = generate_optimize_statements(tables)
        assert any("OPTIMIZE" in s for s in stmts)
        assert any("ANALYZE TABLE" in s for s in stmts)

    def test_zorder_for_id_columns(self):
        tables = [{"name": "customers", "tier": "silver", "columns": [
            {"name": "customer_id", "type": "BIGINT"},
            {"name": "order_id", "type": "BIGINT"},
        ]}]
        stmts = generate_optimize_statements(tables)
        zorder_stmts = [s for s in stmts if "ZORDER" in s]
        assert len(zorder_stmts) > 0
        assert "customer_id" in zorder_stmts[0] or "order_id" in zorder_stmts[0]

    def test_databricks_uses_catalog_prefix(self):
        os.environ["INFORMATICA_MIGRATION_TARGET"] = "databricks"
        os.environ["INFORMATICA_DATABRICKS_CATALOG"] = "test_catalog"
        try:
            tables = [{"name": "t1", "tier": "gold", "columns": [
                {"name": "id", "type": "BIGINT"},
            ]}]
            stmts = generate_optimize_statements(tables)
            assert any("test_catalog.gold.t1" in s for s in stmts)
        finally:
            os.environ["INFORMATICA_MIGRATION_TARGET"] = "fabric"
            os.environ.pop("INFORMATICA_DATABRICKS_CATALOG", None)


class TestSparkConfigTuner:
    """Sprint 71: Spark configuration tuning based on complexity."""

    def test_simple_mapping_200_partitions(self):
        mapping = {"complexity": "Simple", "transformations": ["SQ", "EXP"], "sources": ["src"]}
        config = recommend_spark_config(mapping)
        assert config["spark.sql.shuffle.partitions"] == "200"
        assert config["spark.sql.adaptive.enabled"] == "true"

    def test_complex_mapping_800_partitions(self):
        mapping = {"complexity": "Complex", "transformations": ["SQ"] * 12, "sources": ["a", "b"]}
        config = recommend_spark_config(mapping)
        assert config["spark.sql.shuffle.partitions"] == "800"

    def test_medium_mapping_400_partitions(self):
        mapping = {"complexity": "Medium", "transformations": ["SQ"] * 7, "sources": ["src"]}
        config = recommend_spark_config(mapping)
        assert config["spark.sql.shuffle.partitions"] == "400"

    def test_custom_mapping_high_broadcast(self):
        mapping = {"complexity": "Custom", "transformations": ["SQ"] * 16, "sources": ["s"]}
        config = recommend_spark_config(mapping)
        assert int(config["spark.sql.autoBroadcastJoinThreshold"]) >= 268435456

    def test_multi_source_bumps_shuffle(self):
        mapping = {"complexity": "Simple", "transformations": ["SQ"], "sources": ["a", "b", "c", "d"]}
        config = recommend_spark_config(mapping)
        assert int(config["spark.sql.shuffle.partitions"]) >= 600

    def test_aqe_always_enabled(self):
        mapping = {"complexity": "Simple", "transformations": [], "sources": []}
        config = recommend_spark_config(mapping)
        assert config["spark.sql.adaptive.enabled"] == "true"
        assert config["spark.sql.adaptive.coalescePartitions.enabled"] == "true"


class TestBroadcastDetection:
    """Sprint 71: Broadcast join detection for lookups."""

    def test_lkp_produces_broadcast_candidate(self):
        mapping = {
            "transformations": ["SQ", "LKP"],
            "lookup_conditions": [{"lookup": "DIM_PRODUCT", "condition": "ID = ID"}],
        }
        candidates = detect_broadcast_candidates(mapping)
        assert len(candidates) >= 1
        assert candidates[0]["hint"] == "broadcast"

    def test_ulkp_produces_broadcast_candidate(self):
        mapping = {
            "transformations": ["SQ", "ULKP"],
            "lookup_conditions": [],
        }
        candidates = detect_broadcast_candidates(mapping)
        assert len(candidates) >= 1

    def test_no_lookup_no_candidates(self):
        mapping = {"transformations": ["SQ", "EXP"], "lookup_conditions": []}
        candidates = detect_broadcast_candidates(mapping)
        assert len(candidates) == 0


class TestMaterializationAdvisor:
    """Sprint 71: Cache/persist/checkpoint recommendations."""

    def test_multi_target_recommends_cache(self):
        mapping = {"transformations": ["SQ", "EXP"], "targets": ["T1", "T2", "T3"]}
        recs = recommend_materialization(mapping)
        assert any(r["action"] == "cache" for r in recs)

    def test_many_expensive_ops_recommends_persist(self):
        mapping = {"transformations": ["SQ", "JNR", "AGG", "RNK", "UNI"], "targets": ["T1"]}
        recs = recommend_materialization(mapping)
        assert any(r["action"] == "persist" for r in recs)

    def test_long_pipeline_recommends_checkpoint(self):
        mapping = {"transformations": ["SQ"] + ["EXP"] * 12, "targets": ["T1"]}
        recs = recommend_materialization(mapping)
        assert any(r["action"] == "checkpoint" for r in recs)

    def test_simple_pipeline_no_recommendations(self):
        mapping = {"transformations": ["SQ", "EXP"], "targets": ["T1"]}
        recs = recommend_materialization(mapping)
        assert len(recs) == 0


class TestQueryPerformanceAnnotation:
    """Sprint 71: SQL performance annotation."""

    def test_select_star_flagged(self):
        sql = "SELECT * FROM orders"
        result = annotate_query_performance(sql)
        assert "PERF: SELECT *" in result

    def test_cross_join_flagged(self):
        sql = "SELECT a.*, b.* FROM a CROSS JOIN b"
        result = annotate_query_performance(sql)
        assert "CROSS JOIN" in result

    def test_order_by_without_limit_flagged(self):
        sql = "SELECT id FROM orders ORDER BY order_date"
        result = annotate_query_performance(sql)
        assert "ORDER BY without LIMIT" in result

    def test_order_by_with_limit_not_flagged(self):
        sql = "SELECT id FROM orders ORDER BY order_date LIMIT 100"
        result = annotate_query_performance(sql)
        assert "ORDER BY without LIMIT" not in result

    def test_correlated_subquery_flagged(self):
        sql = "SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.id)"
        result = annotate_query_performance(sql)
        assert "Correlated subquery" in result

    def test_union_without_all_flagged(self):
        sql = "SELECT id FROM a UNION SELECT id FROM b"
        result = annotate_query_performance(sql)
        assert "UNION requires dedup" in result

    def test_union_all_not_flagged(self):
        sql = "SELECT id FROM a UNION ALL SELECT id FROM b"
        result = annotate_query_performance(sql)
        assert "UNION requires dedup" not in result

    def test_large_in_list_flagged(self):
        values = ", ".join([str(i) for i in range(25)])
        sql = f"SELECT * FROM t WHERE id IN ({values})"
        result = annotate_query_performance(sql)
        assert "IN list has" in result

    def test_no_issues_no_annotation(self):
        sql = "SELECT id, name FROM customers WHERE status = 'ACTIVE' LIMIT 10"
        result = annotate_query_performance(sql)
        assert "PERF" not in result

    def test_distinct_flagged(self):
        sql = "SELECT DISTINCT customer_id FROM orders"
        result = annotate_query_performance(sql)
        assert "DISTINCT" in result


# ═══════════════════════════════════════════════
#  Sprint 72 — Advanced PL/SQL Conversion
# ═══════════════════════════════════════════════

from run_sql_migration import (
    convert_cursor_to_pyspark,
    convert_bulk_collect,
    convert_forall,
    convert_exception_blocks,
    convert_package_state,
)


class TestCursorConversion:
    """Sprint 72: PL/SQL cursor → PySpark DataFrame."""

    def test_explicit_cursor_declaration(self):
        plsql = "CURSOR c_orders IS SELECT * FROM orders;"
        result = convert_cursor_to_pyspark(plsql)
        assert "df_c_orders" in result
        assert "spark.sql" in result

    def test_for_in_cursor_loop(self):
        plsql = textwrap.dedent("""\
            CURSOR c_emp IS SELECT * FROM employees;
            FOR rec IN c_emp LOOP
                DBMS_OUTPUT.PUT_LINE(rec.name);
            END LOOP;""")
        result = convert_cursor_to_pyspark(plsql)
        assert "df_c_emp" in result
        assert "for rec in" in result
        assert ".collect()" in result

    def test_inline_cursor_loop(self):
        plsql = textwrap.dedent("""\
            FOR rec IN (SELECT id, name FROM customers) LOOP
                NULL;
            END LOOP;""")
        result = convert_cursor_to_pyspark(plsql)
        assert "df_inline" in result
        assert "spark.sql" in result


class TestBulkCollectConversion:
    """Sprint 72: BULK COLLECT → DataFrame."""

    def test_basic_bulk_collect(self):
        plsql = "SELECT id, name BULK COLLECT INTO l_employees FROM employees;"
        result = convert_bulk_collect(plsql)
        assert "df_l_employees" in result
        assert "spark.sql" in result

    def test_no_bulk_collect_unchanged(self):
        plsql = "SELECT id FROM employees;"
        result = convert_bulk_collect(plsql)
        assert result == plsql


class TestForallConversion:
    """Sprint 72: FORALL batch DML → DataFrame write."""

    def test_forall_insert(self):
        plsql = "FORALL i IN 1..l_data.COUNT INSERT INTO staging_table"
        result = convert_forall(plsql)
        assert "df_batch.write" in result
        assert "staging_table" in result

    def test_forall_update(self):
        plsql = "FORALL i IN 1..l_data.COUNT UPDATE target_table"
        result = convert_forall(plsql)
        assert "MERGE" in result or "DeltaTable" in result

    def test_no_forall_unchanged(self):
        plsql = "INSERT INTO t VALUES (1);"
        result = convert_forall(plsql)
        assert result == plsql


class TestExceptionBlockConversion:
    """Sprint 72: PL/SQL EXCEPTION WHEN → try/except."""

    def test_no_data_found_mapped(self):
        plsql = textwrap.dedent("""\
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('Not found');
            """)
        result = convert_exception_blocks(plsql)
        assert "IndexError" in result or "Exception" in result

    def test_others_maps_to_exception(self):
        plsql = textwrap.dedent("""\
            EXCEPTION
            WHEN OTHERS THEN
                RAISE;
            """)
        result = convert_exception_blocks(plsql)
        assert "except" in result.lower() or "Exception" in result


class TestPackageStateConversion:
    """Sprint 72: Package-level variables → Python module state."""

    def test_varchar_variable(self):
        plsql = "g_status VARCHAR2(20) := 'ACTIVE';"
        result = convert_package_state(plsql)
        assert "g_status" in result
        assert "'ACTIVE'" in result

    def test_number_variable(self):
        plsql = "g_count NUMBER := 0;"
        result = convert_package_state(plsql)
        assert "g_count" in result
        assert "0" in result


# ═══════════════════════════════════════════════
#  Sprint 73 — Dynamic SQL & Complex SQL Patterns
# ═══════════════════════════════════════════════

from run_sql_migration import (
    convert_execute_immediate,
    convert_connect_by,
    convert_pivot_unpivot,
    convert_correlated_subquery,
    convert_temporal_tables,
)


class TestExecuteImmediate:
    """Sprint 73: EXECUTE IMMEDIATE → spark.sql()."""

    def test_literal_sql(self):
        sql = "EXECUTE IMMEDIATE 'DROP TABLE temp_staging';"
        result = convert_execute_immediate(sql)
        assert "spark.sql" in result
        assert "DROP TABLE" in result

    def test_variable_sql(self):
        sql = "EXECUTE IMMEDIATE v_sql_stmt;"
        result = convert_execute_immediate(sql)
        assert "spark.sql(v_sql_stmt)" in result

    def test_with_using_clause(self):
        sql = "EXECUTE IMMEDIATE 'INSERT INTO t VALUES (:1, :2)' USING v_id, v_name;"
        result = convert_execute_immediate(sql)
        assert "spark.sql" in result


class TestConnectBy:
    """Sprint 73: CONNECT BY → recursive CTE."""

    def test_basic_hierarchy(self):
        sql = "SELECT id, name FROM employees START WITH manager_id IS NULL CONNECT BY PRIOR id = manager_id"
        result = convert_connect_by(sql)
        assert "WITH RECURSIVE" in result
        assert "hierarchy" in result
        assert "UNION ALL" in result

    def test_nocycle(self):
        sql = "SELECT id FROM org START WITH parent IS NULL CONNECT BY NOCYCLE PRIOR id = parent"
        result = convert_connect_by(sql)
        assert "WITH RECURSIVE" in result


class TestPivotUnpivot:
    """Sprint 73: Oracle PIVOT/UNPIVOT → Spark SQL."""

    def test_pivot_conversion(self):
        sql = "PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))"
        result = convert_pivot_unpivot(sql)
        assert "PIVOT" in result
        assert "Spark SQL native" in result

    def test_unpivot_to_stack(self):
        sql = "UNPIVOT (value FOR metric IN (col1, col2, col3))"
        result = convert_pivot_unpivot(sql)
        assert "stack" in result.lower()


class TestCorrelatedSubquery:
    """Sprint 73: Correlated EXISTS → JOIN."""

    def test_exists_to_semi_join(self):
        sql = "WHERE EXISTS (SELECT 1 FROM orders o WHERE c.customer_id = o.customer_id)"
        result = convert_correlated_subquery(sql)
        assert "LEFT SEMI JOIN" in result

    def test_not_exists_to_anti_join(self):
        sql = "WHERE NOT EXISTS (SELECT 1 FROM blacklist b WHERE u.email = b.email)"
        result = convert_correlated_subquery(sql)
        assert "LEFT ANTI JOIN" in result


class TestTemporalTables:
    """Sprint 73: SQL Server temporal → Delta time-travel."""

    def test_system_time_as_of(self):
        sql = "SELECT * FROM employees FOR SYSTEM_TIME AS OF '2024-01-01'"
        result = convert_temporal_tables(sql)
        assert "TIMESTAMP AS OF" in result

    def test_system_time_between(self):
        sql = "SELECT * FROM emp FOR SYSTEM_TIME BETWEEN '2024-01-01' AND '2024-06-30'"
        result = convert_temporal_tables(sql)
        assert "TIMESTAMP AS OF" in result
        assert "Delta time-travel" in result

    def test_no_temporal_unchanged(self):
        sql = "SELECT * FROM employees WHERE hire_date > '2024-01-01'"
        result = convert_temporal_tables(sql)
        assert result == sql
