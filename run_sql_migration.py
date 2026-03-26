"""
SQL Migration — Phase 1
Reads SQL files from input/sql/ and mapping SQL overrides from inventory.json,
applies Oracle/SQL Server → Spark SQL regex-based conversions.

Outputs:
  output/sql/SQL_<name>.sql          — converted standalone SQL files
  output/sql/SQL_OVERRIDES_<name>.sql — converted SQL overrides per mapping

Usage:
    python run_sql_migration.py                          # uses output/inventory/inventory.json
    python run_sql_migration.py path/to/inventory.json
"""

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
INPUT_DIR = WORKSPACE / "input"
OUTPUT_DIR = WORKSPACE / "output" / "sql"
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
#  Oracle → Spark SQL conversion rules
# ─────────────────────────────────────────────

ORACLE_REPLACEMENTS = [
    # Functions (order matters — more specific patterns first)
    (r"\bNVL2\s*\(([^,]+),\s*([^,]+),\s*([^)]+)\)", r"CASE WHEN \1 IS NOT NULL THEN \2 ELSE \3 END"),
    (r"\bNVL\s*\(", "COALESCE("),
    (r"\bSYSTIMESTAMP\b", "current_timestamp()"),
    (r"\bSYSDATE\b", "current_timestamp()"),
    (r"\bTO_NUMBER\s*\(([^)]+)\)", r"CAST(\1 AS DECIMAL)"),
    (r"\bSUBSTR\s*\(", "SUBSTRING("),
    (r"\bREGEXP_LIKE\s*\(([^,]+),\s*([^)]+)\)", r"\1 RLIKE \2"),
    (r"\bLISTAGG\s*\(([^,]+),\s*([^)]+)\)", r"CONCAT_WS(\2, COLLECT_LIST(\1))"),
    (r"\bWM_CONCAT\s*\(([^)]+)\)", r"CONCAT_WS(',', COLLECT_LIST(\1))"),
    # Date formats: Oracle YYYY → Spark yyyy, MM stays, DD → dd, HH24 → HH
    (r"'YYYY-MM-DD HH24:MI:SS'", "'yyyy-MM-dd HH:mm:ss'"),
    (r"'YYYY-MM-DD'", "'yyyy-MM-dd'"),
    (r"'YYYYMMDD'", "'yyyyMMdd'"),
    (r"'YYYY'", "'yyyy'"),
    (r"'DD-MON-YYYY'", "'dd-MMM-yyyy'"),
    (r"'MM/DD/YYYY'", "'MM/dd/yyyy'"),
    # TRUNC(date, 'MM') and TRUNC(date)
    (r"\bTRUNC\s*\(([^,)]+),\s*'MM'\s*\)", r"date_trunc('month', \1)"),
    (r"\bTRUNC\s*\(([^,)]+),\s*'YYYY'\s*\)", r"date_trunc('year', \1)"),
    (r"\bTRUNC\s*\(([^,)]+)\)", r"date_trunc('day', \1)"),
    # DUAL removal
    (r"\bFROM\s+DUAL\b", ""),
    # Outer join (+) syntax — basic cases only
    (r"(\w+\.\w+)\s*=\s*(\w+\.\w+)\s*\(\+\)", r"\1 = \2  -- TODO: Convert to LEFT JOIN"),
    (r"(\w+\.\w+)\s*\(\+\)\s*=\s*(\w+\.\w+)", r"\1 = \2  -- TODO: Convert to RIGHT JOIN"),
    # Data types
    (r"\bVARCHAR2\s*\(\d+\)", "STRING"),
    (r"\bNUMBER\s*\(\d+,\s*\d+\)", "DECIMAL"),
    (r"\bNUMBER\b", "DECIMAL"),
    (r"\bCLOB\b", "STRING"),
    (r"\bBLOB\b", "BINARY"),
    # Global Temporary Tables → Spark temp views
    (r"\bCREATE\s+GLOBAL\s+TEMPORARY\s+TABLE\s+(\w+)", r"CREATE OR REPLACE TEMP VIEW \1  -- GTT converted to Spark temp view"),
    (r"\bON\s+COMMIT\s+(?:PRESERVE|DELETE)\s+ROWS", "-- ON COMMIT clause removed (Spark temp views are session-scoped)"),
    # Materialized Views → Delta tables
    (r"\bCREATE\s+MATERIALIZED\s+VIEW\s+(\w+)", r"-- TODO: Replace with Delta table '\1' + scheduled notebook refresh"),
    # Database links → JDBC
    (r"(\w+)@(\w+)", r"\1  -- TODO: Replace @\2 DB link with spark.read.jdbc()"),
    # Oracle packages → TODO markers
    (r"\bDBMS_OUTPUT\.PUT_LINE\s*\(([^)]+)\)", r"-- TODO: print(\1) in PySpark notebook"),
    (r"\bDBMS_\w+\.\w+", "-- TODO: Replace Oracle DBMS package call"),
    (r"\bUTL_\w+\.\w+", "-- TODO: Replace Oracle UTL package call"),
    # Sprint 31: Oracle Object Types → StructType
    (r"\bCREATE\s+(?:OR\s+REPLACE\s+)?TYPE\s+(\w+)\s+AS\s+OBJECT\b",
     r"-- TODO: Flatten Oracle Object Type '\1' to StructType or individual columns"),
    (r"\bCREATE\s+(?:OR\s+REPLACE\s+)?TYPE\s+(\w+)\s+AS\s+TABLE\s+OF\b",
     r"-- TODO: Replace Oracle collection type '\1' with ArrayType"),
    # Sprint 33: Advanced PL/SQL → PySpark patterns
    # Dynamic SQL
    (r"\bEXECUTE\s+IMMEDIATE\s+'([^']+)'",
     r"spark.sql('\1')  -- Dynamic SQL extracted from EXECUTE IMMEDIATE"),
    (r"\bEXECUTE\s+IMMEDIATE\s+(\w+)",
     r"spark.sql(\1)  -- TODO: Dynamic SQL variable; review constructed query"),
    (r"\bDBMS_SQL\.\w+", "-- TODO: Replace DBMS_SQL with spark.sql() or DataFrame API"),
    # PL/SQL Cursor → PySpark iterator
    (r"\bCURSOR\s+(\w+)\s+IS\s+SELECT\b",
     r"# PySpark: df_\1 = spark.sql('SELECT"),
    (r"\bOPEN\s+(\w+)\s*;", r"# Cursor '\1' opened — use DataFrame instead"),
    (r"\bFETCH\s+(\w+)\s+INTO\b", r"# Cursor FETCH \1 — use .collect() or .foreach()"),
    (r"\bCLOSE\s+(\w+)\s*;", r"# Cursor '\1' closed — no action needed with DataFrames"),
    (r"\bFOR\s+(\w+)\s+IN\s+(\w+)\s+LOOP",
     r"for \1 in df_\2.collect():  # Cursor loop → DataFrame collect"),
    # BULK COLLECT → DataFrame
    (r"\bBULK\s+COLLECT\s+INTO\s+(\w+)",
     r"# BULK COLLECT → DataFrame: \1 = spark.sql('...').collect()  # Small datasets only"),
    (r"\bFORALL\s+(\w+)\s+IN\s+\d+\s*\.\.\s*(\w+)\.COUNT",
     r"# FORALL batch DML → Use DataFrame .write or MERGE for batch operations"),
    # CONNECT BY → Recursive CTE
    (r"\bCONNECT\s+BY\s+(?:NOCYCLE\s+)?PRIOR\s+(\w+)\s*=\s*(\w+)",
     r"-- TODO: CONNECT BY → Recursive CTE pattern:\n-- WITH RECURSIVE cte AS (\n--   SELECT * FROM table WHERE \2 IS NULL  -- root nodes\n--   UNION ALL\n--   SELECT t.* FROM table t JOIN cte ON t.\2 = cte.\1\n-- ) SELECT * FROM cte"),
    (r"\bSTART\s+WITH\s+(.+?)(?=\s+CONNECT\s+BY|\s*;|\s*\))",
     r"-- START WITH \1 → use as the anchor in WITH RECURSIVE"),
    # EXCEPTION WHEN → try/except
    (r"\bEXCEPTION\s+WHEN\s+(\w+)\s+THEN",
     r"except Exception as e:  # PL/SQL EXCEPTION WHEN \1"),
    (r"\bRAISE_APPLICATION_ERROR\s*\(([^,]+),\s*([^)]+)\)",
     r"raise ValueError(\2)  # Oracle RAISE_APPLICATION_ERROR code=\1"),
    (r"\bRAISE\s*;", "raise  # Re-raise current exception"),
]

# Compile Oracle patterns
ORACLE_RULES = [(re.compile(pat, re.IGNORECASE), repl) for pat, repl in ORACLE_REPLACEMENTS]

# ─────────────────────────────────────────────
#  SQL Server → Spark SQL conversion rules
# ─────────────────────────────────────────────

SQLSERVER_REPLACEMENTS = [
    (r"\bGETDATE\s*\(\s*\)", "current_timestamp()"),
    (r"\bSYSDATETIME\s*\(\s*\)", "current_timestamp()"),
    (r"\bISNULL\s*\(", "COALESCE("),
    (r"\bCHARINDEX\s*\(([^,]+),\s*([^)]+)\)", r"LOCATE(\1, \2)"),
    (r"\bLEN\s*\(([^)]+)\)", r"LENGTH(RTRIM(\1))"),
    (r"\bDATALENGTH\s*\(([^)]+)\)", r"LENGTH(\1)"),
    (r"\bSTRING_AGG\s*\(([^,]+),\s*([^)]+)\)", r"CONCAT_WS(\2, COLLECT_LIST(\1))"),
    (r"\bIIF\s*\(([^,]+),\s*([^,]+),\s*([^)]+)\)", r"CASE WHEN \1 THEN \2 ELSE \3 END"),
    (r"\bNEWID\s*\(\s*\)", "uuid()"),
    # TOP n → LIMIT n (simple cases; complex cases flagged)
    (r"\bSELECT\s+TOP\s+(\d+)\b", r"SELECT /* TOP \1 → add LIMIT \1 at end */"),
    # Type conversions
    (r"\bNVARCHAR\s*\(\s*\w+\s*\)", "STRING"),
    (r"\bNCHAR\s*\(\s*\d+\s*\)", "STRING"),
    (r"\bNVARCHAR\b", "STRING"),
    (r"\bBIT\b", "BOOLEAN"),
    (r"\bDATETIME2?\b", "TIMESTAMP"),
    # Hints removal
    (r"\bWITH\s*\(\s*NOLOCK\s*\)", ""),
    (r"\(\s*NOLOCK\s*\)", ""),
    # Identity
    (r"\b@@IDENTITY\b", "monotonically_increasing_id()"),
    (r"\bSCOPE_IDENTITY\s*\(\s*\)", "monotonically_increasing_id()"),
    # CROSS/OUTER APPLY → TODO
    (r"\bCROSS\s+APPLY\b", "-- TODO: CROSS APPLY → LATERAL VIEW EXPLODE or lateral join"),
    (r"\bOUTER\s+APPLY\b", "-- TODO: OUTER APPLY → LATERAL VIEW OUTER EXPLODE or left lateral join"),
    # Temp tables
    (r"#(\w+)", r"temp_\1  -- TODO: use createOrReplaceTempView"),
    # @@ERROR
    (r"\b@@ERROR\b", "-- TODO: Replace @@ERROR with Python try/except"),
]

SQLSERVER_RULES = [(re.compile(pat, re.IGNORECASE), repl) for pat, repl in SQLSERVER_REPLACEMENTS]

# ─────────────────────────────────────────────
#  Teradata → Spark SQL conversion rules (Sprint 23)
# ─────────────────────────────────────────────

TERADATA_REPLACEMENTS = [
    # Functions
    (r"\bZEROIFNULL\s*\(([^)]+)\)", r"COALESCE(\1, 0)"),
    (r"\bNULLIFZERO\s*\(([^)]+)\)", r"CASE WHEN \1 = 0 THEN NULL ELSE \1 END"),
    (r"\bSTRTOK\s*\(([^,]+),\s*([^,]+),\s*(\d+)\)", r"SPLIT(\1, \2)[\3 - 1]"),
    # Date/time
    (r"\bDATE\s*'(\d{4}-\d{2}-\d{2})'", r"TO_DATE('\1', 'yyyy-MM-dd')"),
    (r"\bCURRENT_DATE\b", "current_date()"),
    (r"\bCURRENT_TIMESTAMP\s*\(\d*\)", "current_timestamp()"),
    # QUALIFY → HAVING-like window filter (needs manual review)
    (r"\bQUALIFY\b", "-- TODO: QUALIFY clause → use subquery with ROW_NUMBER() and filter"),
    # COLLECT STATISTICS → no-op in Spark (ANALYZE TABLE instead)
    (r"\bCOLLECT\s+STAT(?:ISTIC)?S?\b[^;]*", "-- TODO: Replace COLLECT STATISTICS with ANALYZE TABLE"),
    # VOLATILE TABLE → temp view
    (r"\bCREATE\s+VOLATILE\s+TABLE\s+(\w+)", r"CREATE OR REPLACE TEMP VIEW \1  -- Volatile table converted"),
    (r"\bCREATE\s+(?:MULTISET|SET)\s+TABLE\b", "CREATE TABLE"),
    # SEL → SELECT (Teradata shorthand)
    (r"(?:^|\n)(\s*)SEL\s+", r"\n\1SELECT "),
    # FORMAT clause removal
    (r"\bFORMAT\s+'[^']*'", "-- FORMAT removed (use date_format in Spark)"),
    # CASESPECIFIC removal
    (r"\bCASESPECIFIC\b", "-- CASESPECIFIC removed"),
    # TITLE clause removal
    (r"\bTITLE\s+'[^']*'", ""),
    # SAMPLE → TABLESAMPLE
    (r"\bSAMPLE\s+(\d+)\b", r"TABLESAMPLE (\1 ROWS)"),
    # Type conversions
    (r"\bBYTEINT\b", "TINYINT"),
    (r"\bINTEGER\b", "INT"),
]

TERADATA_RULES = [(re.compile(pat, re.IGNORECASE), repl) for pat, repl in TERADATA_REPLACEMENTS]

# ─────────────────────────────────────────────
#  DB2 → Spark SQL conversion rules (Sprint 23)
# ─────────────────────────────────────────────

DB2_REPLACEMENTS = [
    # Functions
    (r"\bVALUE\s*\(", "COALESCE("),
    (r"\bRRN\s*\(([^)]+)\)", r"monotonically_increasing_id()  -- RRN(\1) converted"),
    (r"\bDAYS\s*\(([^)]+)\)", r"DATEDIFF(current_date(), \1)"),
    (r"\bDAYOFWEEK\s*\(([^)]+)\)", r"DAYOFWEEK(\1)"),
    (r"\bMIDNIGHT_SECONDS\s*\(([^)]+)\)", r"(HOUR(\1) * 3600 + MINUTE(\1) * 60 + SECOND(\1))"),
    (r"\bDIGITS\s*\(([^)]+)\)", r"LPAD(CAST(\1 AS STRING), 10, '0')"),
    # Date/time
    (r"\bCURRENT\s+DATE\b", "current_date()"),
    (r"\bCURRENT\s+TIMESTAMP\b", "current_timestamp()"),
    # FETCH FIRST → LIMIT
    (r"\bFETCH\s+FIRST\s+(\d+)\s+ROWS?\s+ONLY\b", r"LIMIT \1"),
    # WITH UR (uncommitted read) → remove
    (r"\bWITH\s+UR\b", "-- WITH UR removed (Spark has no isolation levels in SQL)"),
    (r"\bWITH\s+CS\b", "-- WITH CS removed"),
    (r"\bWITH\s+RR\b", "-- WITH RR removed"),
    # OPTIMIZE FOR → remove
    (r"\bOPTIMIZE\s+FOR\s+\d+\s+ROWS?\b", ""),
    # Type conversions
    (r"\bDECIMAL\s*\((\d+),\s*(\d+)\)", r"DECIMAL(\1, \2)"),
    (r"\bGRAPHIC\s*\(\d+\)", "STRING"),
    (r"\bVARGRAPHIC\s*\(\d+\)", "STRING"),
    (r"\bDBCLOB\b", "STRING"),
]

DB2_RULES = [(re.compile(pat, re.IGNORECASE), repl) for pat, repl in DB2_REPLACEMENTS]

# ─────────────────────────────────────────────
#  MySQL → Spark SQL conversion rules (Sprint 23)
# ─────────────────────────────────────────────

MYSQL_REPLACEMENTS = [
    # Functions
    (r"\bIFNULL\s*\(", "COALESCE("),
    (r"\bNOW\s*\(\s*\)", "current_timestamp()"),
    (r"\bCURDATE\s*\(\s*\)", "current_date()"),
    (r"\bGROUP_CONCAT\s*\(([^)]+)\)", r"CONCAT_WS(',', COLLECT_LIST(\1))"),
    (r"\bDATE_FORMAT\s*\(([^,]+),\s*([^)]+)\)", r"date_format(\1, \2)"),
    (r"\bSTR_TO_DATE\s*\(([^,]+),\s*([^)]+)\)", r"to_date(\1, \2)"),
    (r"\bUNIX_TIMESTAMP\s*\(\s*\)", "unix_timestamp()"),
    # LIMIT is already Spark-compatible — keep as-is
    # AUTO_INCREMENT → monotonically_increasing_id
    (r"\bAUTO_INCREMENT\b", "-- AUTO_INCREMENT removed (use monotonically_increasing_id() in PySpark)"),
    # Backtick identifiers → standard identifiers
    (r"`(\w+)`", r"\1"),
    # Type conversions
    (r"\bUNSIGNED\b", ""),
    (r"\bTINYINT\s*\(1\)", "BOOLEAN"),
    (r"\bDATETIME\b", "TIMESTAMP"),
    (r"\bMEDIUMTEXT\b", "STRING"),
    (r"\bLONGTEXT\b", "STRING"),
    (r"\bENUM\s*\([^)]+\)", "STRING"),
    # ENGINE= clause removal
    (r"\bENGINE\s*=\s*\w+", ""),
    # CHARSET clause removal
    (r"\bDEFAULT\s+CHARSET\s*=\s*\w+", ""),
]

MYSQL_RULES = [(re.compile(pat, re.IGNORECASE), repl) for pat, repl in MYSQL_REPLACEMENTS]

# ─────────────────────────────────────────────
#  PostgreSQL → Spark SQL conversion rules (Sprint 23)
# ─────────────────────────────────────────────

POSTGRESQL_REPLACEMENTS = [
    # :: type cast → CAST
    (r"(\w+)::(\w+)", r"CAST(\1 AS \2)"),
    # Functions
    (r"\bILIKE\b", "LIKE  -- TODO: ILIKE → case-insensitive; use LOWER() on both sides"),
    (r"\bGENERATE_SERIES\s*\(([^)]+)\)", r"SEQUENCE(\1)  -- TODO: Replace with EXPLODE(SEQUENCE(...))"),
    (r"\bARRAY_AGG\s*\(([^)]+)\)", r"COLLECT_LIST(\1)"),
    (r"\bSTRING_TO_ARRAY\s*\(([^,]+),\s*([^)]+)\)", r"SPLIT(\1, \2)"),
    (r"\bSTRING_AGG\s*\(([^,]+),\s*([^)]+)\)", r"CONCAT_WS(\2, COLLECT_LIST(\1))"),
    (r"\bCOALESCE\s*\(", "COALESCE("),  # Already compatible, keep
    # ON CONFLICT → MERGE
    (r"\bON\s+CONFLICT\b[^;]*", "-- TODO: ON CONFLICT (upsert) → convert to MERGE INTO"),
    # RETURNING → not supported in Spark
    (r"\bRETURNING\b[^;]*", "-- TODO: RETURNING clause not supported in Spark SQL"),
    # SERIAL → BIGINT (auto-increment via PySpark)
    (r"\bBIGSERIAL\b", "BIGINT  -- TODO: auto-increment via monotonically_increasing_id()"),
    (r"\bSERIAL\b", "INT  -- TODO: auto-increment via monotonically_increasing_id()"),
    # Type conversions
    (r"\bTEXT\b", "STRING"),
    (r"\bBOOLEAN\b", "BOOLEAN"),
    (r"\bBYTEA\b", "BINARY"),
    (r"\bINTERVAL\s+'([^']+)'", r"INTERVAL \1"),
    (r"\bPG_CATALOG\.\w+", "-- TODO: Replace pg_catalog reference"),
]

POSTGRESQL_RULES = [(re.compile(pat, re.IGNORECASE), repl) for pat, repl in POSTGRESQL_REPLACEMENTS]

# DECODE is special — needs argument-count-aware conversion
DECODE_PATTERN = re.compile(r"\bDECODE\s*\(", re.IGNORECASE)


def _convert_decode(sql):
    """Convert Oracle DECODE(expr, val1, res1, val2, res2, ..., default) → CASE WHEN."""
    result = []
    i = 0
    while i < len(sql):
        m = DECODE_PATTERN.search(sql, i)
        if not m:
            result.append(sql[i:])
            break
        result.append(sql[i:m.start()])
        # Find matching closing paren
        depth = 1
        start = m.end()
        j = start
        while j < len(sql) and depth > 0:
            if sql[j] == '(':
                depth += 1
            elif sql[j] == ')':
                depth -= 1
            j += 1
        args_str = sql[start:j - 1]
        # Split on commas (respecting nested parens)
        args = _split_args(args_str)
        if len(args) >= 3:
            expr = args[0].strip()
            case_parts = ["CASE"]
            idx = 1
            while idx + 1 < len(args):
                case_parts.append(f" WHEN {expr} = {args[idx].strip()} THEN {args[idx + 1].strip()}")
                idx += 2
            if idx < len(args):
                case_parts.append(f" ELSE {args[idx].strip()}")
            case_parts.append(" END")
            result.append("".join(case_parts))
        else:
            result.append(m.group() + args_str + ")")
        i = j
    return "".join(result)


def _split_args(s):
    """Split comma-separated args respecting nested parentheses and quotes."""
    args = []
    depth = 0
    in_str = False
    current = []
    for ch in s:
        if ch == "'" and depth == 0:
            in_str = not in_str
        if not in_str:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == ',' and depth == 0:
                args.append("".join(current))
                current = []
                continue
        current.append(ch)
    if current:
        args.append("".join(current))
    return args


def convert_sql(sql_text, db_type="oracle"):
    """Apply regex-based conversions to SQL text."""
    result = sql_text

    if db_type == "oracle":
        result = _convert_decode(result)
        for pattern, replacement in ORACLE_RULES:
            result = pattern.sub(replacement, result)
    elif db_type == "sqlserver":
        for pattern, replacement in SQLSERVER_RULES:
            result = pattern.sub(replacement, result)
    elif db_type == "teradata":
        for pattern, replacement in TERADATA_RULES:
            result = pattern.sub(replacement, result)
    elif db_type == "db2":
        for pattern, replacement in DB2_RULES:
            result = pattern.sub(replacement, result)
    elif db_type == "mysql":
        for pattern, replacement in MYSQL_RULES:
            result = pattern.sub(replacement, result)
    elif db_type == "postgresql":
        for pattern, replacement in POSTGRESQL_RULES:
            result = pattern.sub(replacement, result)
    else:
        # Unknown DB — apply Oracle rules as default
        result = _convert_decode(result)
        for pattern, replacement in ORACLE_RULES:
            result = pattern.sub(replacement, result)

    return result


DB_TYPE_LABELS = {
    "oracle": "Oracle → Spark SQL",
    "sqlserver": "SQL Server → Spark SQL",
    "teradata": "Teradata → Spark SQL",
    "db2": "DB2 → Spark SQL",
    "mysql": "MySQL → Spark SQL",
    "postgresql": "PostgreSQL → Spark SQL",
}


def _header(original_path, db_type):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rules = DB_TYPE_LABELS.get(db_type, "Auto-detected → Spark SQL")
    return (
        f"-- {'=' * 76}\n"
        f"-- Converted from: {original_path}\n"
        f"-- DB Type: {db_type.upper()}\n"
        f"-- Conversion: {rules}\n"
        f"-- Date: {now}\n"
        f"-- Agent: sql-migration (automated)\n"
        f"-- {'=' * 76}\n"
        f"-- Review all TODO comments before deploying to Fabric.\n"
        f"-- {'=' * 76}\n\n"
    )


def convert_sql_file(input_path, db_type, output_path):
    """Read a SQL file, convert, and write output."""
    original = input_path.read_text(encoding="utf-8", errors="replace")
    converted = convert_sql(original, db_type)
    header = _header(str(input_path), db_type)
    output_path.write_text(header + converted, encoding="utf-8")
    return output_path


def convert_sql_overrides(mapping_name, overrides, db_type="oracle"):
    """Convert SQL overrides from a mapping and write a combined file."""
    if not overrides:
        return None
    lines = [
        f"-- {'=' * 76}",
        f"-- SQL Overrides for mapping: {mapping_name}",
        f"-- DB Type: {db_type.upper()} → Spark SQL",
        f"-- Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
        "-- Agent: sql-migration (automated)",
        f"-- {'=' * 76}",
        "",
    ]
    for i, ovr in enumerate(overrides, 1):
        ovr_type = ovr.get("type", "Unknown")
        original = ovr.get("value", "")
        converted = convert_sql(original, db_type)
        lines.append(f"-- Override #{i}: {ovr_type}")
        lines.append("-- Original:")
        for ol in original.split("\n"):
            lines.append(f"--   {ol}")
        lines.append("-- Converted:")
        lines.append(converted)
        lines.append("")
    out_path = OUTPUT_DIR / f"SQL_OVERRIDES_{mapping_name}.sql"
    out_path.write_text("\n".join(lines), encoding="utf-8")
    return out_path


def main():
    inv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else INVENTORY_PATH
    if not inv_path.exists():
        print(f"ERROR: {inv_path} not found. Run run_assessment.py first.")
        sys.exit(1)

    with open(inv_path, encoding="utf-8") as f:
        inv = json.load(f)

    target = os.environ.get("INFORMATICA_MIGRATION_TARGET", "fabric")
    target_label = "Databricks Spark SQL" if target == "databricks" else "Fabric Spark SQL"

    print("=" * 60)
    print(f"  SQL Migration — Phase 1 [{target_label}]")
    print("=" * 60)
    print()

    converted_count = 0

    # 1. Convert standalone SQL files
    print("[1/2] Converting standalone SQL files...")
    for sf in inv.get("sql_files", []):
        src_path = WORKSPACE / sf["path"]
        if not src_path.exists():
            print(f"  SKIP: {sf['file']} (file not found)")
            continue
        db_type = sf.get("db_type", "oracle")
        out_name = f"SQL_{src_path.stem}.sql"
        out_path = OUTPUT_DIR / out_name
        convert_sql_file(src_path, db_type, out_path)
        converted_count += 1
        print(f"  ✅ {sf['file']} ({db_type}) → {out_name}")
    print()

    # 2. Convert SQL overrides from mappings
    print("[2/2] Converting mapping SQL overrides...")
    override_count = 0
    for m in inv.get("mappings", []):
        overrides = m.get("sql_overrides", [])
        if not overrides:
            continue
        # Detect db_type from sources (simple heuristic)
        sources = " ".join(m.get("sources", [])).lower()
        if "sqlserver" in sources or "mssql" in sources:
            db_type = "sqlserver"
        elif "teradata" in sources:
            db_type = "teradata"
        elif "db2" in sources:
            db_type = "db2"
        elif "mysql" in sources:
            db_type = "mysql"
        elif "postgres" in sources or "postgresql" in sources:
            db_type = "postgresql"
        else:
            db_type = "oracle"
        out_path = convert_sql_overrides(m["name"], overrides, db_type)
        if out_path:
            override_count += 1
            converted_count += 1
            print(f"  ✅ {m['name']} ({len(overrides)} overrides) → {out_path.name}")
    print()

    print("=" * 60)
    print(f"  SQL files converted: {converted_count}")
    print(f"    Standalone: {len(inv.get('sql_files', []))}")
    print(f"    Override files: {override_count}")
    print(f"  Output: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
