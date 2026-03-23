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
import re
import sys
from pathlib import Path
from datetime import datetime, timezone

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
    # Oracle packages → TODO markers
    (r"\bDBMS_OUTPUT\.PUT_LINE\s*\(([^)]+)\)", r"-- TODO: print(\1) in PySpark notebook"),
    (r"\bDBMS_\w+\.\w+", "-- TODO: Replace Oracle DBMS package call"),
    (r"\bUTL_\w+\.\w+", "-- TODO: Replace Oracle UTL package call"),
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
        # DECODE first (complex multi-arg)
        result = _convert_decode(result)
        for pattern, replacement in ORACLE_RULES:
            result = pattern.sub(replacement, result)
    elif db_type == "sqlserver":
        for pattern, replacement in SQLSERVER_RULES:
            result = pattern.sub(replacement, result)
    else:
        # Unknown DB — apply Oracle rules as default
        result = _convert_decode(result)
        for pattern, replacement in ORACLE_RULES:
            result = pattern.sub(replacement, result)

    return result


def _header(original_path, db_type):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rules = "Oracle → Spark SQL" if db_type == "oracle" else "SQL Server → Spark SQL" if db_type == "sqlserver" else "Auto-detected → Spark SQL"
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
        f"-- Agent: sql-migration (automated)",
        f"-- {'=' * 76}",
        "",
    ]
    for i, ovr in enumerate(overrides, 1):
        ovr_type = ovr.get("type", "Unknown")
        original = ovr.get("value", "")
        converted = convert_sql(original, db_type)
        lines.append(f"-- Override #{i}: {ovr_type}")
        lines.append(f"-- Original:")
        for ol in original.split("\n"):
            lines.append(f"--   {ol}")
        lines.append(f"-- Converted:")
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

    with open(inv_path, "r", encoding="utf-8") as f:
        inv = json.load(f)

    print("=" * 60)
    print("  SQL Migration — Phase 1")
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
        sources = " ".join(m.get("sources", []))
        if "sqlserver" in sources.lower() or "mssql" in sources.lower():
            db_type = "sqlserver"
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
