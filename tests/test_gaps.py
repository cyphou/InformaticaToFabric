"""
Sprint 20 — Gap Remediation P1/P2 Tests
Tests for session config, scheduler cron, global temp tables,
materialized views, database links, and SQL conversion rules.
"""

import re
import xml.etree.ElementTree as ET

import pytest

from run_assessment import (
    detect_db_links,
    detect_global_temp_tables,
    detect_materialized_views,
    parse_scheduler_cron,
    parse_session_config,
    parse_sql_file,
)
from run_sql_migration import convert_sql


# ─────────────────────────────────────────────
#  parse_session_config
# ─────────────────────────────────────────────

class TestParseSessionConfig:
    """Tests for parse_session_config()."""

    def _make_root(self, xml_str):
        return ET.fromstring(xml_str)

    def test_extracts_dtm_buffer_size(self):
        root = self._make_root("""
        <WORKFLOW>
          <SESSION NAME="s_load">
            <ATTRIBUTE NAME="DTM buffer size" VALUE="20000000"/>
          </SESSION>
        </WORKFLOW>
        """)
        configs = parse_session_config(root)
        assert len(configs) == 1
        assert configs[0]["infa_property"] == "DTM buffer size"
        assert configs[0]["infa_value"] == "20000000"
        assert configs[0]["spark_property"] == "spark.sql.shuffle.partitions"
        assert configs[0]["session"] == "s_load"

    def test_extracts_commit_interval(self):
        root = self._make_root("""
        <WORKFLOW>
          <SESSION NAME="s_etl">
            <ATTRIBUTE NAME="Commit Interval" VALUE="10000"/>
          </SESSION>
        </WORKFLOW>
        """)
        configs = parse_session_config(root)
        assert len(configs) == 1
        assert configs[0]["infa_property"] == "Commit Interval"
        assert "delta" in configs[0]["spark_property"].lower() or "optimize" in configs[0]["spark_property"].lower()

    def test_extracts_lookup_cache_size(self):
        root = self._make_root("""
        <WORKFLOW>
          <SESSION NAME="s_lookup">
            <ATTRIBUTE NAME="Lookup Cache Size" VALUE="5000000"/>
          </SESSION>
        </WORKFLOW>
        """)
        configs = parse_session_config(root)
        assert len(configs) == 1
        assert configs[0]["infa_property"] == "Lookup Cache Size"
        assert "broadcast" in configs[0]["spark_property"].lower()

    def test_extracts_multiple_attributes(self):
        root = self._make_root("""
        <WORKFLOW>
          <SESSION NAME="s_multi">
            <ATTRIBUTE NAME="DTM buffer size" VALUE="10000000"/>
            <ATTRIBUTE NAME="Commit Interval" VALUE="5000"/>
            <ATTRIBUTE NAME="Sorter Cache Size" VALUE="2000000"/>
          </SESSION>
        </WORKFLOW>
        """)
        configs = parse_session_config(root)
        assert len(configs) == 3
        props = {c["infa_property"] for c in configs}
        assert "DTM buffer size" in props
        assert "Commit Interval" in props
        assert "Sorter Cache Size" in props

    def test_extracts_config_reference(self):
        root = self._make_root("""
        <WORKFLOW>
          <SESSION NAME="s_ref">
            <CONFIGREFERENCE REFOBJECTNAME="BigDataConfig"/>
          </SESSION>
        </WORKFLOW>
        """)
        configs = parse_session_config(root)
        assert len(configs) == 1
        assert configs[0]["infa_value"] == "BigDataConfig"
        assert "BigDataConfig" in configs[0]["note"]

    def test_ignores_unknown_attributes(self):
        root = self._make_root("""
        <WORKFLOW>
          <SESSION NAME="s_unknown">
            <ATTRIBUTE NAME="Unknown Property" VALUE="foo"/>
          </SESSION>
        </WORKFLOW>
        """)
        configs = parse_session_config(root)
        assert len(configs) == 0

    def test_ignores_empty_values(self):
        root = self._make_root("""
        <WORKFLOW>
          <SESSION NAME="s_empty">
            <ATTRIBUTE NAME="DTM buffer size" VALUE=""/>
          </SESSION>
        </WORKFLOW>
        """)
        configs = parse_session_config(root)
        assert len(configs) == 0

    def test_multiple_sessions(self):
        root = self._make_root("""
        <WORKFLOW>
          <SESSION NAME="s_first">
            <ATTRIBUTE NAME="DTM buffer size" VALUE="10000000"/>
          </SESSION>
          <SESSION NAME="s_second">
            <ATTRIBUTE NAME="Commit Interval" VALUE="5000"/>
          </SESSION>
        </WORKFLOW>
        """)
        configs = parse_session_config(root)
        assert len(configs) == 2
        sessions = {c["session"] for c in configs}
        assert sessions == {"s_first", "s_second"}

    def test_empty_workflow(self):
        root = self._make_root("<WORKFLOW/>")
        configs = parse_session_config(root)
        assert configs == []

    def test_treat_source_rows_as(self):
        root = self._make_root("""
        <WORKFLOW>
          <SESSION NAME="s_update">
            <ATTRIBUTE NAME="Treat Source Rows As" VALUE="DD_UPDATE"/>
          </SESSION>
        </WORKFLOW>
        """)
        configs = parse_session_config(root)
        assert len(configs) == 1
        assert configs[0]["spark_property"] == "merge_strategy"


# ─────────────────────────────────────────────
#  parse_scheduler_cron
# ─────────────────────────────────────────────

class TestParseSchedulerCron:
    """Tests for parse_scheduler_cron()."""

    def test_daily(self):
        result = parse_scheduler_cron("DAILY")
        assert result["cron"] == "0 0 2 * * *"
        assert "daily" in result["note"].lower()

    def test_hourly(self):
        result = parse_scheduler_cron("HOURLY")
        assert result["cron"] == "0 0 * * * *"

    def test_weekly(self):
        result = parse_scheduler_cron("WEEKLY")
        assert result["cron"] == "0 0 2 * * 1"

    def test_monthly(self):
        result = parse_scheduler_cron("MONTHLY")
        assert result["cron"] == "0 0 2 1 * *"

    def test_case_insensitive(self):
        result = parse_scheduler_cron("daily_load")
        assert result["cron"] == "0 0 2 * * *"

    def test_time_am(self):
        result = parse_scheduler_cron("SCHED_06AM")
        assert "6" in result["cron"]

    def test_time_pm(self):
        result = parse_scheduler_cron("RUN_2PM")
        assert "14" in result["cron"]

    def test_four_digit_time(self):
        result = parse_scheduler_cron("LOAD_0600")
        assert "6" in result["cron"]
        assert "0" in result["cron"]

    def test_empty_string(self):
        result = parse_scheduler_cron("")
        assert result["cron"] == ""
        assert "no schedule" in result["note"].lower()

    def test_none(self):
        result = parse_scheduler_cron(None)
        assert result["cron"] == ""

    def test_unrecognized(self):
        result = parse_scheduler_cron("CUSTOM_XYZ")
        assert result["cron"] == ""
        assert "manual" in result["note"].lower()
        assert "CUSTOM_XYZ" in result["note"]

    def test_daily_with_time_suffix(self):
        result = parse_scheduler_cron("DAILY_02AM")
        # Should match DAILY keyword
        assert result["cron"] != ""


# ─────────────────────────────────────────────
#  detect_global_temp_tables
# ─────────────────────────────────────────────

class TestDetectGlobalTempTables:
    """Tests for detect_global_temp_tables()."""

    def test_detects_create_gtt(self):
        sql = "CREATE GLOBAL TEMPORARY TABLE tmp_staging (id NUMBER, name VARCHAR2(100))"
        gtts = detect_global_temp_tables(sql)
        assert len(gtts) >= 1
        assert gtts[0]["table"] == "tmp_staging"
        assert "temp view" in gtts[0]["spark_equivalent"].lower() or "TempView" in gtts[0]["spark_equivalent"]

    def test_detects_on_commit_preserve(self):
        sql = "CREATE GLOBAL TEMPORARY TABLE t1 (id NUMBER) ON COMMIT PRESERVE ROWS"
        gtts = detect_global_temp_tables(sql)
        # Should detect both the CREATE and ON COMMIT
        tables = [g["table"] for g in gtts]
        assert "t1" in tables
        notes = " ".join(g["note"] for g in gtts)
        assert "preserve" in notes.lower()

    def test_detects_on_commit_delete(self):
        sql = "CREATE GLOBAL TEMPORARY TABLE t2 (x INT) ON COMMIT DELETE ROWS"
        gtts = detect_global_temp_tables(sql)
        notes = " ".join(g["note"] for g in gtts)
        assert "delete" in notes.lower() or "unpersist" in notes.lower()

    def test_multiple_gtts(self):
        sql = """
        CREATE GLOBAL TEMPORARY TABLE tmp_a (id INT);
        CREATE GLOBAL TEMPORARY TABLE tmp_b (id INT);
        """
        gtts = detect_global_temp_tables(sql)
        tables = [g["table"] for g in gtts if g["table"] not in ("(from ON COMMIT clause)",)]
        assert "tmp_a" in tables
        assert "tmp_b" in tables

    def test_no_gtt(self):
        sql = "CREATE TABLE regular_table (id INT)"
        gtts = detect_global_temp_tables(sql)
        assert len(gtts) == 0

    def test_line_number_tracking(self):
        sql = "SELECT 1;\nSELECT 2;\nCREATE GLOBAL TEMPORARY TABLE tmp_x (id INT)"
        gtts = detect_global_temp_tables(sql)
        assert gtts[0]["line"] == 3

    def test_empty_sql(self):
        assert detect_global_temp_tables("") == []


# ─────────────────────────────────────────────
#  detect_materialized_views
# ─────────────────────────────────────────────

class TestDetectMaterializedViews:
    """Tests for detect_materialized_views()."""

    def test_detects_mv(self):
        sql = "CREATE MATERIALIZED VIEW mv_sales AS SELECT * FROM orders"
        mvs = detect_materialized_views(sql)
        assert len(mvs) == 1
        assert mvs[0]["name"] == "mv_sales"
        assert "delta" in mvs[0]["spark_equivalent"].lower()

    def test_multiple_mvs(self):
        sql = """
        CREATE MATERIALIZED VIEW mv_a AS SELECT 1;
        CREATE MATERIALIZED VIEW mv_b AS SELECT 2;
        """
        mvs = detect_materialized_views(sql)
        names = [mv["name"] for mv in mvs]
        assert "mv_a" in names
        assert "mv_b" in names

    def test_no_mv(self):
        sql = "CREATE VIEW regular_view AS SELECT 1"
        mvs = detect_materialized_views(sql)
        assert len(mvs) == 0

    def test_line_number(self):
        sql = "-- header\nCREATE MATERIALIZED VIEW mv_test AS SELECT 1"
        mvs = detect_materialized_views(sql)
        assert mvs[0]["line"] == 2

    def test_empty_sql(self):
        assert detect_materialized_views("") == []


# ─────────────────────────────────────────────
#  detect_db_links
# ─────────────────────────────────────────────

class TestDetectDbLinks:
    """Tests for detect_db_links()."""

    def test_detects_dblink_in_from_clause(self):
        sql = "SELECT * FROM employees@remote_db WHERE dept_id = 10"
        links = detect_db_links(sql)
        assert len(links) >= 1
        link_names = [l["db_link"] for l in links]
        assert "remote_db" in link_names

    def test_detects_dblink_in_join(self):
        sql = "SELECT a.id FROM local_t a JOIN remote_t@prod_link b ON a.id = b.id"
        links = detect_db_links(sql)
        link_names = [l["db_link"] for l in links]
        assert "prod_link" in link_names

    def test_includes_jdbc_suggestion(self):
        sql = "SELECT col FROM table@mylink"
        links = detect_db_links(sql)
        if links:
            notes = " ".join(l["note"] for l in links)
            assert "jdbc" in notes.lower()

    def test_no_dblink(self):
        sql = "SELECT * FROM employees WHERE dept_id = 10"
        links = detect_db_links(sql)
        assert len(links) == 0

    def test_empty_sql(self):
        assert detect_db_links("") == []


# ─────────────────────────────────────────────
#  SQL Conversion — GTT, MV, DB Link rules
# ─────────────────────────────────────────────

class TestSqlConversionGapRules:
    """Tests for new Oracle conversion rules in run_sql_migration."""

    def test_gtt_converted_to_temp_view(self):
        sql = "CREATE GLOBAL TEMPORARY TABLE tmp_staging (id NUMBER)"
        result = convert_sql(sql, "oracle")
        assert "CREATE OR REPLACE TEMP VIEW" in result
        assert "tmp_staging" in result

    def test_on_commit_clause_removed(self):
        sql = "CREATE GLOBAL TEMPORARY TABLE t (id INT) ON COMMIT PRESERVE ROWS"
        result = convert_sql(sql, "oracle")
        assert "ON COMMIT" not in result or "removed" in result.lower()

    def test_on_commit_delete_rows_removed(self):
        sql = "CREATE GLOBAL TEMPORARY TABLE t (id INT) ON COMMIT DELETE ROWS"
        result = convert_sql(sql, "oracle")
        assert "ON COMMIT" not in result or "removed" in result.lower()

    def test_mv_flagged_as_todo(self):
        sql = "CREATE MATERIALIZED VIEW mv_summary AS SELECT COUNT(*) FROM orders"
        result = convert_sql(sql, "oracle")
        assert "TODO" in result
        assert "Delta" in result or "delta" in result.lower() or "mv_summary" in result

    def test_dblink_flagged_as_todo(self):
        sql = "SELECT * FROM employees@remote_db WHERE 1=1"
        result = convert_sql(sql, "oracle")
        assert "TODO" in result or "DB link" in result or "jdbc" in result.lower()

    def test_existing_oracle_rules_still_work(self):
        """Ensure new rules don't break existing conversions."""
        sql = "SELECT NVL(col, 0), SYSDATE FROM DUAL"
        result = convert_sql(sql, "oracle")
        assert "COALESCE" in result
        assert "current_timestamp()" in result
        assert "DUAL" not in result


# ─────────────────────────────────────────────
#  parse_sql_file — new fields
# ─────────────────────────────────────────────

class TestParseSqlFileGapFields:
    """Tests that parse_sql_file returns global_temp_tables, materialized_views, db_links."""

    def test_returns_gtt_field(self, tmp_path, monkeypatch):
        import run_assessment
        monkeypatch.setattr(run_assessment, "WORKSPACE", tmp_path)
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("CREATE GLOBAL TEMPORARY TABLE tmp_x (id INT);")
        result = parse_sql_file(sql_file)
        assert "global_temp_tables" in result
        assert len(result["global_temp_tables"]) >= 1

    def test_returns_mv_field(self, tmp_path, monkeypatch):
        import run_assessment
        monkeypatch.setattr(run_assessment, "WORKSPACE", tmp_path)
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("CREATE MATERIALIZED VIEW mv_x AS SELECT 1;")
        result = parse_sql_file(sql_file)
        assert "materialized_views" in result
        assert len(result["materialized_views"]) >= 1

    def test_returns_dblinks_field(self, tmp_path, monkeypatch):
        import run_assessment
        monkeypatch.setattr(run_assessment, "WORKSPACE", tmp_path)
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT * FROM employees@remote_db WHERE 1=1;")
        result = parse_sql_file(sql_file)
        assert "db_links" in result

    def test_empty_fields_for_simple_sql(self, tmp_path, monkeypatch):
        import run_assessment
        monkeypatch.setattr(run_assessment, "WORKSPACE", tmp_path)
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1 FROM dual;")
        result = parse_sql_file(sql_file)
        assert result.get("global_temp_tables") == []
        assert result.get("materialized_views") == []
        assert result.get("db_links") == []


# ─────────────────────────────────────────────
#  Pipeline trigger integration
# ─────────────────────────────────────────────

class TestPipelineScheduleTrigger:
    """Tests that schedule_cron is reflected in pipeline generation."""

    def test_pipeline_includes_trigger(self):
        from run_pipeline_migration import generate_pipeline

        workflow = {
            "name": "WF_DAILY",
            "sessions": ["S_LOAD"],
            "session_to_mapping": {"S_LOAD": "M_LOAD"},
            "dependencies": {"S_LOAD": ["Start"]},
            "has_timer": False,
            "has_decision": False,
            "decision_tasks": [],
            "email_tasks": [],
            "schedule": "DAILY",
            "schedule_cron": {"cron": "0 0 2 * * *", "note": "Daily at 2 AM UTC"},
        }
        mappings_by_name = {"M_LOAD": {"parameters": []}}
        pipeline = generate_pipeline(workflow, mappings_by_name)
        trigger = pipeline["properties"].get("trigger")
        assert trigger is not None
        assert trigger["type"] == "ScheduleTrigger"
        assert trigger["typeProperties"]["recurrence"]["cron"] == "0 0 2 * * *"

    def test_pipeline_no_trigger_when_empty_cron(self):
        from run_pipeline_migration import generate_pipeline

        workflow = {
            "name": "WF_MANUAL",
            "sessions": ["S_LOAD"],
            "session_to_mapping": {"S_LOAD": "M_LOAD"},
            "dependencies": {"S_LOAD": ["Start"]},
            "has_timer": False,
            "has_decision": False,
            "decision_tasks": [],
            "email_tasks": [],
            "schedule": "",
            "schedule_cron": {"cron": "", "note": "No schedule defined"},
        }
        mappings_by_name = {"M_LOAD": {"parameters": []}}
        pipeline = generate_pipeline(workflow, mappings_by_name)
        assert "trigger" not in pipeline["properties"]

    def test_pipeline_no_trigger_when_no_schedule_cron(self):
        from run_pipeline_migration import generate_pipeline

        workflow = {
            "name": "WF_LEGACY",
            "sessions": ["S_LOAD"],
            "session_to_mapping": {"S_LOAD": "M_LOAD"},
            "dependencies": {"S_LOAD": ["Start"]},
            "has_timer": False,
            "has_decision": False,
            "decision_tasks": [],
            "email_tasks": [],
            "schedule": "DAILY",
        }
        mappings_by_name = {"M_LOAD": {"parameters": []}}
        pipeline = generate_pipeline(workflow, mappings_by_name)
        # No schedule_cron key → no trigger
        assert "trigger" not in pipeline["properties"]
