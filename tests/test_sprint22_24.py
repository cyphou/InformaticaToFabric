"""
Sprints 22-24 — IICS Gap Closure, Multi-DB Support, Coverage Expansion
Tests for: DQ Task parsing, App Integration parsing, Teradata/DB2/MySQL/PostgreSQL
detection & conversion, and deep coverage paths.
"""

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from run_assessment import (
    detect_source_db_type,
    parse_iics_app_integration,
    parse_iics_dq_tasks,
)
from run_sql_migration import convert_sql


# ═══════════════════════════════════════════════
#  Sprint 22 — IICS Data Quality Task Parser
# ═══════════════════════════════════════════════


class TestIicsDqTaskParser:
    """Tests for parse_iics_dq_tasks()."""

    def test_parses_dq_task(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="DQ_VALIDATE_EMAILS" objectType="com.infa.deployment.dataqualitytask">
    <dAttribute name="ruleSet" value="EMAIL_VALIDATION_RULES"/>
    <dAttribute name="profileName" value="CRM_EMAIL_PROFILE"/>
    <dTemplate name="src_contacts" objectType="field">
      <dAttribute name="sourceConnection" value="CONN_ORACLE"/>
    </dTemplate>
    <dTemplate name="tgt_clean" objectType="field">
      <dAttribute name="targetConnection" value="CONN_LAKEHOUSE"/>
    </dTemplate>
  </dTemplate>
</exportMetadata>'''
        f = tmp_path / "dq.xml"
        f.write_text(xml)
        tasks = parse_iics_dq_tasks(f)
        assert len(tasks) == 1
        t = tasks[0]
        assert t["name"] == "DQ_VALIDATE_EMAILS"
        assert t["iics_type"] == "DataQualityTask"
        assert t["complexity"] == "Complex"
        assert "DQ" in t["transformations"]
        assert t["rule_set"] == "EMAIL_VALIDATION_RULES"
        assert t["profile_name"] == "CRM_EMAIL_PROFILE"

    def test_dq_task_sources_targets(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="DQ_CLEAN_ADDRESSES" objectType="com.infa.deployment.dqtask">
    <dTemplate name="src_addr" objectType="field">
      <dAttribute name="sourceConnection" value="CONN_SALESFORCE"/>
    </dTemplate>
    <dTemplate name="tgt_cleaned" objectType="field">
      <dAttribute name="targetConnection" value="CONN_DELTA"/>
    </dTemplate>
  </dTemplate>
</exportMetadata>'''
        f = tmp_path / "dq2.xml"
        f.write_text(xml)
        tasks = parse_iics_dq_tasks(f)
        assert len(tasks) == 1
        assert "CONN_SALESFORCE" in tasks[0]["sources"]
        assert "CONN_DELTA" in tasks[0]["targets"]

    def test_dq_task_alternative_object_type(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="DQ_CHK" objectType="com.infa.deployment.dataquality">
  </dTemplate>
</exportMetadata>'''
        f = tmp_path / "dq3.xml"
        f.write_text(xml)
        tasks = parse_iics_dq_tasks(f)
        assert len(tasks) == 1
        assert tasks[0]["iics_type"] == "DataQualityTask"

    def test_empty_returns_empty(self, tmp_path):
        f = tmp_path / "empty.xml"
        f.write_text('<?xml version="1.0"?>\n<exportMetadata/>')
        assert parse_iics_dq_tasks(f) == []

    def test_non_dq_ignored(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="TF_SIMPLE" objectType="com.infa.deployment.taskflow"/>
</exportMetadata>'''
        f = tmp_path / "non_dq.xml"
        f.write_text(xml)
        assert parse_iics_dq_tasks(f) == []

    def test_dq_format_is_iics(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="DQ_X" objectType="com.infa.deployment.dataqualitytask"/>
</exportMetadata>'''
        f = tmp_path / "dq_fmt.xml"
        f.write_text(xml)
        tasks = parse_iics_dq_tasks(f)
        assert tasks[0]["format"] == "iics"

    def test_dq_default_transformations(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="DQ_Y" objectType="com.infa.deployment.dataqualitytask"/>
</exportMetadata>'''
        f = tmp_path / "dq_xf.xml"
        f.write_text(xml)
        tasks = parse_iics_dq_tasks(f)
        assert set(tasks[0]["transformations"]) == {"SQ", "DQ", "TGT"}

    def test_multiple_dq_tasks(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="DQ_A" objectType="com.infa.deployment.dataqualitytask"/>
  <dTemplate name="DQ_B" objectType="com.infa.deployment.dqtask"/>
</exportMetadata>'''
        f = tmp_path / "multi_dq.xml"
        f.write_text(xml)
        tasks = parse_iics_dq_tasks(f)
        assert len(tasks) == 2
        names = {t["name"] for t in tasks}
        assert names == {"DQ_A", "DQ_B"}


# ═══════════════════════════════════════════════
#  Sprint 22 — IICS Application Integration Parser
# ═══════════════════════════════════════════════


class TestIicsAppIntegration:
    """Tests for parse_iics_app_integration()."""

    def test_parses_app_integration(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="AI_ORDER_EVENTS" objectType="com.infa.deployment.appintegration">
    <dAttribute name="eventType" value="REST_API"/>
    <dAttribute name="serviceUrl" value="https://api.example.com/orders"/>
    <dTemplate name="step_validate" objectType="step">
      <dAttribute name="type" value="mapping"/>
      <dAttribute name="mappingName" value="m_validate_order"/>
    </dTemplate>
    <dTemplate name="step_route" objectType="step">
      <dAttribute name="type" value="decision"/>
    </dTemplate>
    <dLink from="Start" to="step_validate"/>
    <dLink from="step_validate" to="step_route"/>
  </dTemplate>
</exportMetadata>'''
        f = tmp_path / "ai.xml"
        f.write_text(xml)
        tasks = parse_iics_app_integration(f)
        assert len(tasks) == 1
        t = tasks[0]
        assert t["name"] == "AI_ORDER_EVENTS"
        assert t["iics_type"] == "ApplicationIntegration"
        assert t["event_type"] == "REST_API"
        assert t["service_url"] == "https://api.example.com/orders"

    def test_app_integration_steps(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="AI_TEST" objectType="com.infa.deployment.applicationintegration">
    <dTemplate name="step_a" objectType="step">
      <dAttribute name="type" value="mapping"/>
      <dAttribute name="mappingName" value="m_a"/>
    </dTemplate>
    <dTemplate name="step_b" objectType="step">
      <dAttribute name="type" value="command"/>
    </dTemplate>
  </dTemplate>
</exportMetadata>'''
        f = tmp_path / "ai2.xml"
        f.write_text(xml)
        tasks = parse_iics_app_integration(f)
        assert len(tasks) == 1
        assert "step_a" in tasks[0]["sessions"]
        assert "step_b" in tasks[0]["sessions"]

    def test_app_integration_links(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="AI_LINK" objectType="com.infa.deployment.appintegration">
    <dLink from="Start" to="step_a"/>
    <dLink from="step_a" to="End"/>
  </dTemplate>
</exportMetadata>'''
        f = tmp_path / "ai_link.xml"
        f.write_text(xml)
        tasks = parse_iics_app_integration(f)
        assert len(tasks[0]["links"]) == 2

    def test_app_integration_mapping_refs(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="AI_MAP" objectType="com.infa.deployment.serviceconnector">
    <dTemplate name="st_load" objectType="step">
      <dAttribute name="type" value="mapping"/>
      <dAttribute name="mappingName" value="m_load_data"/>
    </dTemplate>
  </dTemplate>
</exportMetadata>'''
        f = tmp_path / "ai_map.xml"
        f.write_text(xml)
        tasks = parse_iics_app_integration(f)
        assert tasks[0]["session_to_mapping"]["st_load"] == "m_load_data"

    def test_empty_returns_empty(self, tmp_path):
        f = tmp_path / "empty.xml"
        f.write_text('<?xml version="1.0"?>\n<exportMetadata/>')
        assert parse_iics_app_integration(f) == []

    def test_non_ai_ignored(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="TF_X" objectType="com.infa.deployment.taskflow"/>
</exportMetadata>'''
        f = tmp_path / "non_ai.xml"
        f.write_text(xml)
        assert parse_iics_app_integration(f) == []

    def test_format_is_iics(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="AI_FMT" objectType="com.infa.deployment.appintegration"/>
</exportMetadata>'''
        f = tmp_path / "ai_fmt.xml"
        f.write_text(xml)
        tasks = parse_iics_app_integration(f)
        assert tasks[0]["format"] == "iics"

    def test_default_event_type(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="AI_DEF" objectType="com.infa.deployment.appintegration"/>
</exportMetadata>'''
        f = tmp_path / "ai_def.xml"
        f.write_text(xml)
        tasks = parse_iics_app_integration(f)
        assert tasks[0]["event_type"] == ""

    def test_multiple_app_integrations(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="AI_1" objectType="com.infa.deployment.appintegration"/>
  <dTemplate name="AI_2" objectType="com.infa.deployment.applicationintegration"/>
</exportMetadata>'''
        f = tmp_path / "multi_ai.xml"
        f.write_text(xml)
        tasks = parse_iics_app_integration(f)
        assert len(tasks) == 2


# ═══════════════════════════════════════════════
#  Sprint 23 — Teradata Detection & Conversion
# ═══════════════════════════════════════════════


class TestTeradataDetection:
    """Test detect_source_db_type() for Teradata SQL."""

    def test_teradata_qualify(self):
        sql = "SELECT * FROM t QUALIFY ROW_NUMBER() OVER (ORDER BY ts) = 1;\nCOLLECT STATISTICS ON t;\nSEL col1 FROM t SAMPLE 100"
        assert detect_source_db_type(sql) == "teradata"

    def test_teradata_collect_stats(self):
        sql = "COLLECT STATISTICS ON my_table COLUMN (id, name);\nSELECT * FROM my_table SAMPLE 100"
        assert detect_source_db_type(sql) == "teradata"

    def test_teradata_volatile_table(self):
        sql = "CREATE VOLATILE TABLE tmp AS (SELECT 1 AS x) WITH DATA;\nSEL x FROM tmp"
        assert detect_source_db_type(sql) == "teradata"

    def test_teradata_zeroifnull(self):
        sql = "SELECT ZEROIFNULL(amount), NULLIFZERO(qty) FROM orders"
        assert detect_source_db_type(sql) == "teradata"

    def test_teradata_format(self):
        sql = "SELECT hire_date (FORMAT 'YYYY-MM-DD'), CASESPECIFIC name FROM employees"
        assert detect_source_db_type(sql) == "teradata"

    def test_teradata_multiset(self):
        sql = "CREATE MULTISET TABLE db.tbl (id INT, name VARCHAR(100)) PRIMARY INDEX (id);\nCOLLECT STATS ON db.tbl"
        assert detect_source_db_type(sql) == "teradata"


class TestTeradataConversion:
    """Test convert_sql() with db_type='teradata'."""

    def test_zeroifnull(self):
        result = convert_sql("SELECT ZEROIFNULL(amount) FROM t", "teradata")
        assert "COALESCE(amount, 0)" in result

    def test_nullifzero(self):
        result = convert_sql("SELECT NULLIFZERO(qty) FROM t", "teradata")
        assert "CASE WHEN qty = 0 THEN NULL ELSE qty END" in result

    def test_volatile_table(self):
        result = convert_sql("CREATE VOLATILE TABLE tmp_data AS (SELECT 1)", "teradata")
        assert "TEMP VIEW" in result

    def test_sel_to_select(self):
        result = convert_sql("\nSEL col1, col2 FROM tbl", "teradata")
        assert "SELECT" in result

    def test_qualify_todo(self):
        result = convert_sql("SELECT * FROM t QUALIFY ROW_NUMBER() OVER() = 1", "teradata")
        assert "TODO" in result

    def test_collect_stats_todo(self):
        result = convert_sql("COLLECT STATISTICS ON my_table", "teradata")
        assert "TODO" in result or "ANALYZE" in result

    def test_sample_to_tablesample(self):
        result = convert_sql("SELECT * FROM t SAMPLE 50", "teradata")
        assert "TABLESAMPLE" in result

    def test_date_literal(self):
        result = convert_sql("SELECT DATE '2024-01-15' FROM t", "teradata")
        assert "TO_DATE" in result

    def test_format_removal(self):
        result = convert_sql("SELECT col (FORMAT 'YYYY-MM-DD') FROM t", "teradata")
        assert "FORMAT" not in result or "removed" in result.lower()

    def test_casespecific_removal(self):
        result = convert_sql("SELECT name (CASESPECIFIC) FROM t", "teradata")
        assert "CASESPECIFIC" not in result or "removed" in result.lower()

    def test_byteint_to_tinyint(self):
        result = convert_sql("CREATE TABLE t (flag BYTEINT)", "teradata")
        assert "TINYINT" in result

    def test_multiset_table(self):
        result = convert_sql("CREATE MULTISET TABLE db.tbl (id INT)", "teradata")
        assert "CREATE TABLE" in result
        assert "MULTISET" not in result


# ═══════════════════════════════════════════════
#  Sprint 23 — DB2 Detection & Conversion
# ═══════════════════════════════════════════════


class TestDB2Detection:
    """Test detect_source_db_type() for DB2 SQL."""

    def test_db2_fetch_first(self):
        sql = "SELECT * FROM t FETCH FIRST 10 ROWS ONLY"
        assert detect_source_db_type(sql) == "db2"

    def test_db2_value_function(self):
        sql = "SELECT VALUE(name, 'N/A'), CURRENT DATE FROM t WITH UR"
        assert detect_source_db_type(sql) == "db2"

    def test_db2_rrn(self):
        sql = "SELECT RRN(t), DAYS(hire_date) FROM t FETCH FIRST 5 ROWS ONLY"
        assert detect_source_db_type(sql) == "db2"

    def test_db2_dayofweek(self):
        sql = "SELECT DAYOFWEEK(CURRENT DATE), CURRENT TIMESTAMP FROM t WITH UR"
        assert detect_source_db_type(sql) == "db2"


class TestDB2Conversion:
    """Test convert_sql() with db_type='db2'."""

    def test_value_to_coalesce(self):
        result = convert_sql("SELECT VALUE(name, 'N/A') FROM t", "db2")
        assert "COALESCE(" in result

    def test_fetch_first_to_limit(self):
        result = convert_sql("SELECT * FROM t FETCH FIRST 10 ROWS ONLY", "db2")
        assert "LIMIT 10" in result

    def test_with_ur_removal(self):
        result = convert_sql("SELECT * FROM t WITH UR", "db2")
        assert "WITH UR" not in result or "removed" in result.lower()

    def test_rrn_conversion(self):
        result = convert_sql("SELECT RRN(t) FROM t", "db2")
        assert "monotonically_increasing_id" in result

    def test_days_conversion(self):
        result = convert_sql("SELECT DAYS(hire_date) FROM t", "db2")
        assert "DATEDIFF" in result

    def test_current_date(self):
        result = convert_sql("SELECT CURRENT DATE FROM SYSIBM.SYSDUMMY1", "db2")
        assert "current_date()" in result

    def test_current_timestamp(self):
        result = convert_sql("SELECT CURRENT TIMESTAMP FROM t", "db2")
        assert "current_timestamp()" in result

    def test_graphic_type(self):
        result = convert_sql("CREATE TABLE t (name GRAPHIC(20))", "db2")
        assert "STRING" in result

    def test_with_cs_removal(self):
        result = convert_sql("SELECT * FROM t WITH CS", "db2")
        assert "WITH CS" not in result or "removed" in result.lower()


# ═══════════════════════════════════════════════
#  Sprint 23 — MySQL Detection & Conversion
# ═══════════════════════════════════════════════


class TestMySQLDetection:
    """Test detect_source_db_type() for MySQL SQL."""

    def test_mysql_ifnull_limit(self):
        sql = "SELECT IFNULL(name, 'X'), NOW() FROM t LIMIT 10"
        assert detect_source_db_type(sql) == "mysql"

    def test_mysql_backtick(self):
        sql = "SELECT `user_id`, `name` FROM `users` WHERE IFNULL(`status`, 0) = 1"
        assert detect_source_db_type(sql) == "mysql"

    def test_mysql_group_concat(self):
        sql = "SELECT GROUP_CONCAT(tag), DATE_FORMAT(created, '%Y-%m-%d') FROM posts LIMIT 100"
        assert detect_source_db_type(sql) == "mysql"

    def test_mysql_auto_increment(self):
        sql = "CREATE TABLE t (id INT AUTO_INCREMENT, name VARCHAR(100)) ENGINE=InnoDB DEFAULT CHARSET=utf8"
        assert detect_source_db_type(sql) == "mysql"


class TestMySQLConversion:
    """Test convert_sql() with db_type='mysql'."""

    def test_ifnull_to_coalesce(self):
        result = convert_sql("SELECT IFNULL(name, 'X') FROM t", "mysql")
        assert "COALESCE(" in result

    def test_now_to_current_timestamp(self):
        result = convert_sql("SELECT NOW() FROM t", "mysql")
        assert "current_timestamp()" in result

    def test_group_concat(self):
        result = convert_sql("SELECT GROUP_CONCAT(tag) FROM t", "mysql")
        assert "COLLECT_LIST" in result

    def test_backtick_removal(self):
        result = convert_sql("SELECT `name` FROM `users`", "mysql")
        assert "`" not in result
        assert "name" in result

    def test_auto_increment_removal(self):
        result = convert_sql("CREATE TABLE t (id INT AUTO_INCREMENT)", "mysql")
        assert "AUTO_INCREMENT" not in result or "removed" in result.lower()

    def test_engine_clause_removal(self):
        result = convert_sql("CREATE TABLE t (id INT) ENGINE=InnoDB", "mysql")
        assert "ENGINE" not in result

    def test_charset_removal(self):
        result = convert_sql("CREATE TABLE t (id INT) DEFAULT CHARSET=utf8", "mysql")
        assert "CHARSET" not in result

    def test_tinyint1_to_boolean(self):
        result = convert_sql("CREATE TABLE t (active TINYINT(1))", "mysql")
        assert "BOOLEAN" in result

    def test_datetime_to_timestamp(self):
        result = convert_sql("CREATE TABLE t (created DATETIME)", "mysql")
        assert "TIMESTAMP" in result

    def test_unsigned_removal(self):
        result = convert_sql("CREATE TABLE t (qty INT UNSIGNED)", "mysql")
        assert "UNSIGNED" not in result


# ═══════════════════════════════════════════════
#  Sprint 23 — PostgreSQL Detection & Conversion
# ═══════════════════════════════════════════════


class TestPostgreSQLDetection:
    """Test detect_source_db_type() for PostgreSQL SQL."""

    def test_postgresql_double_colon(self):
        sql = "SELECT name::text, created::date FROM t WHERE name ILIKE '%test%'"
        assert detect_source_db_type(sql) == "postgresql"

    def test_postgresql_serial(self):
        sql = "CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT, active BOOLEAN)"
        assert detect_source_db_type(sql) == "postgresql"

    def test_postgresql_array_agg(self):
        sql = "SELECT ARRAY_AGG(tag), GENERATE_SERIES(1, 10) FROM t RETURNING id"
        assert detect_source_db_type(sql) == "postgresql"

    def test_postgresql_pg_catalog(self):
        sql = "SELECT * FROM PG_CATALOG.pg_tables WHERE name ILIKE 'test%'"
        assert detect_source_db_type(sql) == "postgresql"


class TestPostgreSQLConversion:
    """Test convert_sql() with db_type='postgresql'."""

    def test_double_colon_cast(self):
        result = convert_sql("SELECT name::varchar FROM t", "postgresql")
        assert "CAST(name AS varchar)" in result

    def test_ilike_todo(self):
        result = convert_sql("SELECT * FROM t WHERE name ILIKE '%test%'", "postgresql")
        assert "LIKE" in result
        assert "TODO" in result

    def test_array_agg(self):
        result = convert_sql("SELECT ARRAY_AGG(tag) FROM t", "postgresql")
        assert "COLLECT_LIST" in result

    def test_string_to_array(self):
        result = convert_sql("SELECT STRING_TO_ARRAY(tags, ',') FROM t", "postgresql")
        assert "SPLIT" in result

    def test_serial_to_int(self):
        result = convert_sql("CREATE TABLE t (id SERIAL)", "postgresql")
        assert "INT" in result
        assert "SERIAL" not in result.split("INT")[0]  # SERIAL replaced

    def test_bigserial_to_bigint(self):
        result = convert_sql("CREATE TABLE t (id BIGSERIAL)", "postgresql")
        assert "BIGINT" in result

    def test_text_to_string(self):
        result = convert_sql("CREATE TABLE t (note TEXT)", "postgresql")
        assert "STRING" in result

    def test_returning_todo(self):
        result = convert_sql("INSERT INTO t VALUES (1) RETURNING id", "postgresql")
        assert "TODO" in result

    def test_on_conflict_todo(self):
        result = convert_sql("INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET x=1", "postgresql")
        assert "TODO" in result

    def test_bytea_to_binary(self):
        result = convert_sql("CREATE TABLE t (data BYTEA)", "postgresql")
        assert "BINARY" in result

    def test_pg_catalog_todo(self):
        result = convert_sql("SELECT * FROM PG_CATALOG.pg_tables", "postgresql")
        assert "TODO" in result


# ═══════════════════════════════════════════════
#  Sprint 23 — Cross-DB Detection Edge Cases
# ═══════════════════════════════════════════════


class TestCrossDbDetection:
    """Edge cases for detect_source_db_type()."""

    def test_unknown_for_ansi_sql(self):
        sql = "SELECT id, name FROM users WHERE active = 1"
        assert detect_source_db_type(sql) == "unknown"

    def test_oracle_still_wins_with_classic_patterns(self):
        sql = "SELECT NVL(name, 'X'), SYSDATE, DECODE(status, 1, 'A') FROM dual"
        assert detect_source_db_type(sql) == "oracle"

    def test_sqlserver_still_wins_with_classic_patterns(self):
        sql = "SELECT ISNULL(name, 'X'), GETDATE(), CHARINDEX('a', name) FROM dbo.users WITH (NOLOCK)"
        assert detect_source_db_type(sql) == "sqlserver"

    def test_teradata_vs_oracle(self):
        # Teradata-specific: QUALIFY + SAMPLE + COLLECT STATS
        sql = "SELECT * FROM t QUALIFY ROW_NUMBER() OVER () = 1 SAMPLE 10;\nCOLLECT STATISTICS ON t"
        result = detect_source_db_type(sql)
        assert result == "teradata"

    def test_db2_vs_oracle(self):
        # DB2-specific: FETCH FIRST + VALUE + WITH UR
        sql = "SELECT VALUE(name, 'N/A') FROM t FETCH FIRST 5 ROWS ONLY WITH UR"
        result = detect_source_db_type(sql)
        assert result == "db2"


# ═══════════════════════════════════════════════
#  Sprint 23 — SQL Migration Header Labels
# ═══════════════════════════════════════════════


class TestSqlMigrationHeaders:
    """Test _header() produces correct labels for all DB types."""

    def test_all_db_type_labels(self):
        from run_sql_migration import DB_TYPE_LABELS
        expected = {"oracle", "sqlserver", "teradata", "db2", "mysql", "postgresql"}
        assert set(DB_TYPE_LABELS.keys()) == expected

    def test_header_teradata(self):
        from run_sql_migration import _header
        h = _header("test.sql", "teradata")
        assert "TERADATA" in h
        assert "Teradata → Spark SQL" in h

    def test_header_db2(self):
        from run_sql_migration import _header
        h = _header("test.sql", "db2")
        assert "DB2" in h

    def test_header_mysql(self):
        from run_sql_migration import _header
        h = _header("test.sql", "mysql")
        assert "MYSQL" in h

    def test_header_postgresql(self):
        from run_sql_migration import _header
        h = _header("test.sql", "postgresql")
        assert "POSTGRESQL" in h

    def test_header_unknown_fallback(self):
        from run_sql_migration import _header
        h = _header("test.sql", "xyz")
        assert "Auto-detected" in h


# ═══════════════════════════════════════════════
#  Sprint 24 — Coverage Expansion: Assessment Patterns
# ═══════════════════════════════════════════════


class TestAssessmentPatternDicts:
    """Verify pattern dicts exist and compile correctly."""

    def test_oracle_patterns_compile(self):
        from run_assessment import ORACLE_PATTERNS
        for name, pat in ORACLE_PATTERNS.items():
            assert re.compile(pat, re.IGNORECASE), f"Failed to compile {name}"

    def test_sqlserver_patterns_compile(self):
        from run_assessment import SQLSERVER_PATTERNS
        for name, pat in SQLSERVER_PATTERNS.items():
            assert re.compile(pat, re.IGNORECASE), f"Failed to compile {name}"

    def test_teradata_patterns_compile(self):
        from run_assessment import TERADATA_PATTERNS
        for name, pat in TERADATA_PATTERNS.items():
            assert re.compile(pat, re.IGNORECASE), f"Failed to compile {name}"
        assert len(TERADATA_PATTERNS) >= 10

    def test_db2_patterns_compile(self):
        from run_assessment import DB2_PATTERNS
        for name, pat in DB2_PATTERNS.items():
            assert re.compile(pat, re.IGNORECASE), f"Failed to compile {name}"
        assert len(DB2_PATTERNS) >= 8

    def test_mysql_patterns_compile(self):
        from run_assessment import MYSQL_PATTERNS
        for name, pat in MYSQL_PATTERNS.items():
            assert re.compile(pat, re.IGNORECASE), f"Failed to compile {name}"
        assert len(MYSQL_PATTERNS) >= 8

    def test_postgresql_patterns_compile(self):
        from run_assessment import POSTGRESQL_PATTERNS
        for name, pat in POSTGRESQL_PATTERNS.items():
            assert re.compile(pat, re.IGNORECASE), f"Failed to compile {name}"
        assert len(POSTGRESQL_PATTERNS) >= 8

    def test_dq_in_transformation_abbrev(self):
        from run_assessment import TRANSFORMATION_ABBREV
        assert "Data Quality" in TRANSFORMATION_ABBREV
        assert TRANSFORMATION_ABBREV["Data Quality"] == "DQ"


class TestSqlRuleCompilation:
    """Verify all SQL conversion rule lists compile."""

    def test_teradata_rules_exist(self):
        from run_sql_migration import TERADATA_RULES
        assert len(TERADATA_RULES) >= 10

    def test_db2_rules_exist(self):
        from run_sql_migration import DB2_RULES
        assert len(DB2_RULES) >= 10

    def test_mysql_rules_exist(self):
        from run_sql_migration import MYSQL_RULES
        assert len(MYSQL_RULES) >= 10

    def test_postgresql_rules_exist(self):
        from run_sql_migration import POSTGRESQL_RULES
        assert len(POSTGRESQL_RULES) >= 10


# ═══════════════════════════════════════════════
#  Sprint 24 — Coverage: parse_sql_file with new DB types
# ═══════════════════════════════════════════════


class TestParseSqlFileMultiDb:
    """Test parse_sql_file returns constructs for all DB types."""

    @pytest.fixture(autouse=True)
    def _patch_workspace(self, tmp_path, monkeypatch):
        import run_assessment
        monkeypatch.setattr(run_assessment, "WORKSPACE", tmp_path)
        self._tmp = tmp_path

    def test_teradata_constructs_returned(self):
        from run_assessment import parse_sql_file
        sql_file = self._tmp / "td.sql"
        sql_file.write_text("SELECT ZEROIFNULL(x), NULLIFZERO(y) FROM t;\nCOLLECT STATISTICS ON t")
        result = parse_sql_file(sql_file)
        assert result["db_type"] == "teradata"
        assert len(result["teradata_constructs"]) >= 2

    def test_db2_constructs_returned(self):
        from run_assessment import parse_sql_file
        sql_file = self._tmp / "db2.sql"
        sql_file.write_text("SELECT VALUE(name, 'X'), CURRENT DATE FROM t FETCH FIRST 10 ROWS ONLY WITH UR")
        result = parse_sql_file(sql_file)
        assert result["db_type"] == "db2"
        assert len(result["db2_constructs"]) >= 2

    def test_mysql_constructs_returned(self):
        from run_assessment import parse_sql_file
        sql_file = self._tmp / "my.sql"
        sql_file.write_text("SELECT IFNULL(`name`, 'X'), NOW() FROM `users` LIMIT 5")
        result = parse_sql_file(sql_file)
        assert result["db_type"] == "mysql"
        assert len(result["mysql_constructs"]) >= 2

    def test_postgresql_constructs_returned(self):
        from run_assessment import parse_sql_file
        sql_file = self._tmp / "pg.sql"
        sql_file.write_text("SELECT name::varchar FROM t WHERE name ILIKE '%test%' AND id::int > 0")
        result = parse_sql_file(sql_file)
        assert result["db_type"] == "postgresql"
        assert len(result["postgresql_constructs"]) >= 2

    def test_all_construct_keys_present(self):
        from run_assessment import parse_sql_file
        sql_file = self._tmp / "ansi.sql"
        sql_file.write_text("SELECT 1")
        result = parse_sql_file(sql_file)
        for key in ["oracle_constructs", "sqlserver_constructs", "teradata_constructs",
                     "db2_constructs", "mysql_constructs", "postgresql_constructs"]:
            assert key in result


# ═══════════════════════════════════════════════
#  Sprint 24 — Coverage: convert_sql unknown db_type
# ═══════════════════════════════════════════════


class TestConvertSqlFallback:
    """Test convert_sql with unknown db_type falls back to Oracle."""

    def test_unknown_uses_oracle_rules(self):
        result = convert_sql("SELECT NVL(x, 0) FROM dual", "unknown_db")
        assert "COALESCE" in result

    def test_default_oracle(self):
        result = convert_sql("SELECT SYSDATE FROM dual")
        assert "current_timestamp()" in result


# ═══════════════════════════════════════════════
#  Sprint 24 — Coverage: Edge cases for new parsers
# ═══════════════════════════════════════════════


class TestDqTaskEdgeCases:
    """Edge cases for parse_iics_dq_tasks."""

    def test_malformed_xml(self, tmp_path):
        f = tmp_path / "bad.xml"
        f.write_text("NOT XML AT ALL")
        assert parse_iics_dq_tasks(f) == []

    def test_no_name_attribute(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate objectType="com.infa.deployment.dataqualitytask"/>
</exportMetadata>'''
        f = tmp_path / "noname.xml"
        f.write_text(xml)
        tasks = parse_iics_dq_tasks(f)
        assert len(tasks) == 1
        assert tasks[0]["name"] == ""


class TestAppIntegrationEdgeCases:
    """Edge cases for parse_iics_app_integration."""

    def test_malformed_xml(self, tmp_path):
        f = tmp_path / "bad.xml"
        f.write_text("THIS IS NOT XML")
        assert parse_iics_app_integration(f) == []

    def test_no_steps(self, tmp_path):
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="AI_EMPTY" objectType="com.infa.deployment.appintegration"/>
</exportMetadata>'''
        f = tmp_path / "ai_empty.xml"
        f.write_text(xml)
        tasks = parse_iics_app_integration(f)
        assert len(tasks) == 1
        assert tasks[0]["sessions"] == []
        assert tasks[0]["links"] == []
