"""
Security Migration — Sprint 83 (Phase 12)
Extracts Informatica security rules (session filters, user groups, data masking)
and generates:
  - Fabric RLS policies (CREATE SECURITY POLICY)
  - Databricks row filters (ALTER TABLE SET ROW FILTER)
  - Column masking functions (dynamic data masking for PII)
  - Security policy audit report
"""

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output"
INVENTORY_DIR = OUTPUT_DIR / "inventory"
SECURITY_DIR = OUTPUT_DIR / "security"

# ─────────────────────────────────────────────
#  Security Rule Extraction
# ─────────────────────────────────────────────

# Informatica session-level security filter patterns
_SESSION_FILTER_PATTERNS = [
    re.compile(r"(?i)where\s+.*?\buser\b", re.DOTALL),
    re.compile(r"(?i)where\s+.*?\brole\b", re.DOTALL),
    re.compile(r"(?i)where\s+.*?\btenant", re.DOTALL),
    re.compile(r"(?i)where\s+.*?\bdepartment\b", re.DOTALL),
    re.compile(r"(?i)where\s+.*?\bregion\b", re.DOTALL),
]

# Column masking patterns indexed by PII category
MASKING_FUNCTIONS = {
    "SSN": {
        "fabric": "CONCAT('***-**-', RIGHT(CAST({col} AS VARCHAR(11)), 4))",
        "databricks": "concat('***-**-', right(cast({col} as string), 4))",
        "description": "Partial mask — last 4 digits visible",
    },
    "EMAIL": {
        "fabric": "CONCAT(LEFT({col}, 1), '****@', RIGHT({col}, LEN({col}) - CHARINDEX('@', {col})))",
        "databricks": "concat(left({col}, 1), '****@', substring_index({col}, '@', -1))",
        "description": "Partial mask — first char + domain visible",
    },
    "PHONE": {
        "fabric": "CONCAT('***-***-', RIGHT({col}, 4))",
        "databricks": "concat('***-***-', right({col}, 4))",
        "description": "Partial mask — last 4 digits visible",
    },
    "CREDIT_CARD": {
        "fabric": "CONCAT('****-****-****-', RIGHT(CAST({col} AS VARCHAR(19)), 4))",
        "databricks": "concat('****-****-****-', right(cast({col} as string), 4))",
        "description": "Partial mask — last 4 digits visible",
    },
    "NAME": {
        "fabric": "CONCAT(LEFT({col}, 1), REPLICATE('*', LEN({col}) - 1))",
        "databricks": "concat(left({col}, 1), repeat('*', length({col}) - 1))",
        "description": "Partial mask — first char visible",
    },
    "ADDRESS": {
        "fabric": "'[REDACTED]'",
        "databricks": "'[REDACTED]'",
        "description": "Full redaction",
    },
    "DOB": {
        "fabric": "CONCAT(YEAR({col}), '-01-01')",
        "databricks": "concat(year({col}), '-01-01')",
        "description": "Year only — month/day redacted",
    },
    "PASSPORT": {
        "fabric": "CONCAT(LEFT({col}, 2), REPLICATE('*', LEN({col}) - 2))",
        "databricks": "concat(left({col}, 2), repeat('*', length({col}) - 2))",
        "description": "Partial mask — first 2 chars visible",
    },
    "IP_ADDRESS": {
        "fabric": "'***.***.***.***'",
        "databricks": "'***.***.***.***'",
        "description": "Full redaction",
    },
    "NATIONAL_ID": {
        "fabric": "CONCAT('****', RIGHT({col}, 4))",
        "databricks": "concat('****', right({col}, 4))",
        "description": "Partial mask — last 4 chars visible",
    },
}


def extract_security_rules(inventory_path=None):
    """Extract security rules from inventory: session filters, user-group hints, DM transforms."""
    inv_path = inventory_path or (INVENTORY_DIR / "inventory.json")
    if not Path(inv_path).exists():
        return {"filters": [], "masking_transforms": [], "user_groups": [], "pii_findings": []}

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    filters = []
    masking_transforms = []
    user_groups = set()

    for mapping in inventory.get("mappings", []):
        name = mapping.get("name", "UNKNOWN")

        # Check SQL overrides for session-level security filters
        for override in mapping.get("sql_overrides", []):
            for pat in _SESSION_FILTER_PATTERNS:
                if pat.search(override):
                    filters.append({
                        "mapping": name,
                        "type": "session_filter",
                        "expression": override.strip(),
                    })
                    # Extract referenced group/role names
                    for grp in re.findall(r"(?i)(?:role|group|user)\s*=\s*'([^']+)'", override):
                        user_groups.add(grp)
                    break

        # Check for Data Masking (DM) transformations
        for tx in mapping.get("transformations", []):
            if tx in ("DM",):
                masking_transforms.append({
                    "mapping": name,
                    "type": "data_masking_transform",
                })

        # Check filter transforms for security-style predicates
        for tx in mapping.get("transformations", []):
            if tx == "FIL":
                filters.append({
                    "mapping": name,
                    "type": "filter_transform",
                    "expression": f"FIL transform in {name}",
                })

    # Also pull PII findings from inventory
    pii_findings = inventory.get("pii_findings", [])

    return {
        "filters": filters,
        "masking_transforms": masking_transforms,
        "user_groups": sorted(user_groups),
        "pii_findings": pii_findings,
    }


# ─────────────────────────────────────────────
#  Fabric RLS Generator
# ─────────────────────────────────────────────

def generate_fabric_rls(security_rules, schema="dbo"):
    """Generate Fabric Warehouse RLS policies: CREATE SECURITY POLICY + filter predicates."""
    lines = [
        "-- Fabric Row-Level Security (RLS) Policies",
        f"-- Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "-- Based on Informatica session-level security filters",
        "",
    ]

    tables_with_rls = set()
    filters = security_rules.get("filters", [])

    if not filters:
        lines.append("-- No security filters detected in Informatica mappings.")
        lines.append("-- Consider adding RLS policies manually for sensitive tables.")
        return "\n".join(lines), tables_with_rls

    # Create inline TVF for row filter
    lines.append("-- Step 1: Create filter predicate function")
    lines.append(f"CREATE FUNCTION {schema}.fn_security_filter(@user_name NVARCHAR(256))")
    lines.append("RETURNS TABLE")
    lines.append("WITH SCHEMABINDING")
    lines.append("AS")
    lines.append("RETURN SELECT 1 AS result")
    lines.append("    WHERE @user_name = USER_NAME()")
    lines.append("       OR IS_MEMBER('db_owner') = 1;")
    lines.append("GO")
    lines.append("")

    # Group filters by mapping → derive table names
    mapping_filters = {}
    for f in filters:
        m = f["mapping"]
        if m not in mapping_filters:
            mapping_filters[m] = []
        mapping_filters[m].append(f)

    lines.append("-- Step 2: Create security policies per table")
    for mapping_name, mf_list in sorted(mapping_filters.items()):
        table_name = mapping_name.replace("M_", "").lower()
        full_table = f"{schema}.{table_name}"
        tables_with_rls.add(full_table)

        policy_name = f"RLS_{table_name}"
        lines.append(f"-- Policy for {full_table} (from mapping {mapping_name})")
        lines.append(f"CREATE SECURITY POLICY {schema}.{policy_name}")
        lines.append(f"    ADD FILTER PREDICATE {schema}.fn_security_filter(modified_by)")
        lines.append(f"    ON {full_table}")
        lines.append("    WITH (STATE = ON);")
        lines.append("GO")
        lines.append("")

    return "\n".join(lines), tables_with_rls


# ─────────────────────────────────────────────
#  Databricks RLS Generator
# ─────────────────────────────────────────────

def generate_databricks_rls(security_rules, catalog="main", schema="silver"):
    """Generate Databricks Unity Catalog row filters: ALTER TABLE SET ROW FILTER + Python UDF."""
    lines = [
        "-- Databricks Row-Level Security (Unity Catalog Row Filters)",
        f"-- Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "-- Based on Informatica session-level security filters",
        "",
    ]

    tables_with_rls = set()
    filters = security_rules.get("filters", [])

    if not filters:
        lines.append("-- No security filters detected in Informatica mappings.")
        return "\n".join(lines), tables_with_rls

    # Create row filter UDF
    lines.append("-- Step 1: Create row filter function")
    lines.append(f"CREATE OR REPLACE FUNCTION {catalog}.{schema}.row_filter(owner_col STRING)")
    lines.append("RETURNS BOOLEAN")
    lines.append("RETURN (owner_col = current_user())")
    lines.append("    OR (is_account_group_member('data-admins'));")
    lines.append("")

    # Apply row filter to tables derived from mappings
    mapping_names = sorted(set(f["mapping"] for f in filters))
    lines.append("-- Step 2: Apply row filters to tables")
    for mapping_name in mapping_names:
        table_name = mapping_name.replace("M_", "").lower()
        full_table = f"{catalog}.{schema}.{table_name}"
        tables_with_rls.add(full_table)

        lines.append(f"ALTER TABLE {full_table}")
        lines.append(f"    SET ROW FILTER {catalog}.{schema}.row_filter ON (modified_by);")
        lines.append("")

    return "\n".join(lines), tables_with_rls


# ─────────────────────────────────────────────
#  Column Masking Generator
# ─────────────────────────────────────────────

def generate_column_masking(pii_findings, target="fabric", catalog="main", schema="silver"):
    """Generate column masking SQL for PII columns.

    Args:
        pii_findings: list of dicts from detect_pii_columns().
        target: 'fabric' or 'databricks'.
        catalog: Databricks catalog name (ignored for Fabric).
        schema: schema name.

    Returns:
        (sql_text, masked_columns_count)
    """
    if target == "fabric":
        return _generate_fabric_masking(pii_findings, schema)
    else:
        return _generate_databricks_masking(pii_findings, catalog, schema)


def _generate_fabric_masking(pii_findings, schema="dbo"):
    """Generate Fabric dynamic data masking ALTER TABLE statements."""
    lines = [
        "-- Fabric Dynamic Data Masking",
        f"-- Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
    ]

    if not pii_findings:
        lines.append("-- No PII columns detected.")
        return "\n".join(lines), 0

    # Group by table (mapping → table)
    by_table = {}
    for finding in pii_findings:
        table = finding["mapping"].replace("M_", "").lower()
        if table not in by_table:
            by_table[table] = []
        by_table[table].append(finding)

    masked = 0
    for table_name, findings in sorted(by_table.items()):
        full_table = f"{schema}.{table_name}"
        lines.append(f"-- Table: {full_table}")
        for f in findings:
            col = f["field"]
            category = f["pii_category"]
            mask_info = MASKING_FUNCTIONS.get(category)
            if mask_info:
                mask_expr = mask_info["fabric"].format(col=col)
                lines.append(f"-- {category}: {mask_info['description']}")
                lines.append(f"ALTER TABLE {full_table}")
                lines.append(f"    ALTER COLUMN {col} ADD MASKED WITH (FUNCTION = 'partial(0,\"***\",0)');")
                masked += 1
        lines.append("")

    return "\n".join(lines), masked


def _generate_databricks_masking(pii_findings, catalog="main", schema="silver"):
    """Generate Databricks Unity Catalog column masks."""
    lines = [
        "-- Databricks Column Masking (Unity Catalog)",
        f"-- Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
    ]

    if not pii_findings:
        lines.append("-- No PII columns detected.")
        return "\n".join(lines), 0

    # Create a masking function per PII category
    created_functions = set()
    for finding in pii_findings:
        category = finding["pii_category"]
        if category in created_functions:
            continue
        mask_info = MASKING_FUNCTIONS.get(category)
        if not mask_info:
            continue

        func_name = f"mask_{category.lower()}"
        mask_expr = mask_info["databricks"].format(col="val")
        lines.append(f"CREATE OR REPLACE FUNCTION {catalog}.{schema}.{func_name}(val STRING)")
        lines.append(f"RETURNS STRING")
        lines.append(f"RETURN CASE")
        lines.append(f"    WHEN is_account_group_member('data-admins') THEN val")
        lines.append(f"    ELSE {mask_expr}")
        lines.append(f"END;")
        lines.append("")
        created_functions.add(category)

    # Apply masks to columns
    by_table = {}
    for finding in pii_findings:
        table = finding["mapping"].replace("M_", "").lower()
        if table not in by_table:
            by_table[table] = []
        by_table[table].append(finding)

    masked = 0
    lines.append("-- Apply column masks to tables")
    for table_name, findings in sorted(by_table.items()):
        full_table = f"{catalog}.{schema}.{table_name}"
        for f in findings:
            col = f["field"]
            category = f["pii_category"]
            func_name = f"mask_{category.lower()}"
            lines.append(f"ALTER TABLE {full_table}")
            lines.append(f"    ALTER COLUMN {col} SET MASK {catalog}.{schema}.{func_name};")
            masked += 1
        lines.append("")

    return "\n".join(lines), masked


# ─────────────────────────────────────────────
#  Security Policy Audit Report
# ─────────────────────────────────────────────

def generate_security_audit(security_rules, rls_tables_fabric=None, rls_tables_databricks=None,
                            masked_count=0, total_tables=0, total_pii=0):
    """Generate a security policy audit report (JSON + Markdown)."""
    rls_fabric = rls_tables_fabric or set()
    rls_databricks = rls_tables_databricks or set()

    rls_coverage = 0
    if total_tables > 0:
        rls_coverage = round(len(rls_fabric | rls_databricks) / total_tables, 3)

    masking_coverage = 0
    if total_pii > 0:
        masking_coverage = round(masked_count / total_pii, 3)

    audit = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "security_filters_found": len(security_rules.get("filters", [])),
        "masking_transforms_found": len(security_rules.get("masking_transforms", [])),
        "user_groups_detected": security_rules.get("user_groups", []),
        "pii_columns_found": total_pii,
        "rls_policies": {
            "fabric_tables": sorted(rls_fabric),
            "databricks_tables": sorted(rls_databricks),
            "total_covered": len(rls_fabric | rls_databricks),
        },
        "column_masking": {
            "masked_columns": masked_count,
            "coverage": masking_coverage,
        },
        "overall": {
            "total_tables": total_tables,
            "rls_coverage": rls_coverage,
            "masking_coverage": masking_coverage,
        },
    }

    # Generate markdown summary
    md_lines = [
        "# Security Policy Audit Report",
        "",
        f"**Generated:** {audit['generated']}",
        "",
        "## Summary",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Security filters found | {audit['security_filters_found']} |",
        f"| Data masking transforms | {audit['masking_transforms_found']} |",
        f"| User groups detected | {len(audit['user_groups_detected'])} |",
        f"| PII columns detected | {total_pii} |",
        f"| Tables with RLS | {audit['rls_policies']['total_covered']} |",
        f"| Columns masked | {masked_count} |",
        f"| RLS coverage | {rls_coverage:.1%} |",
        f"| Masking coverage | {masking_coverage:.1%} |",
        "",
        "## RLS Policies",
        "",
    ]

    if rls_fabric:
        md_lines.append("### Fabric RLS")
        for t in sorted(rls_fabric):
            md_lines.append(f"- `{t}`")
        md_lines.append("")

    if rls_databricks:
        md_lines.append("### Databricks Row Filters")
        for t in sorted(rls_databricks):
            md_lines.append(f"- `{t}`")
        md_lines.append("")

    if audit["user_groups_detected"]:
        md_lines.append("## Detected User Groups")
        md_lines.append("")
        for g in audit["user_groups_detected"]:
            md_lines.append(f"- `{g}`")
        md_lines.append("")

    return audit, "\n".join(md_lines)


# ─────────────────────────────────────────────
#  Main Entry Point
# ─────────────────────────────────────────────

def run_security_migration(inventory_path=None, target=None):
    """Run full security migration: extract rules, generate RLS + masking, produce audit."""
    target = target or os.environ.get("INFORMATICA_MIGRATION_TARGET", "fabric")

    SECURITY_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("  Security Migration — Sprint 83")
    print("=" * 60)

    # 1. Extract security rules
    print("\n📋 Extracting security rules from inventory...")
    security_rules = extract_security_rules(inventory_path)
    print(f"   Filters: {len(security_rules['filters'])}")
    print(f"   Masking transforms: {len(security_rules['masking_transforms'])}")
    print(f"   User groups: {len(security_rules['user_groups'])}")
    print(f"   PII findings: {len(security_rules['pii_findings'])}")

    # Count total tables from inventory
    inv_path = inventory_path or (INVENTORY_DIR / "inventory.json")
    total_tables = 0
    if Path(inv_path).exists():
        with open(inv_path, encoding="utf-8") as f:
            inv = json.load(f)
        total_tables = len(inv.get("mappings", []))

    # 2. Generate RLS policies
    rls_fabric_tables = set()
    rls_databricks_tables = set()

    if target in ("fabric", "both"):
        print("\n🔒 Generating Fabric RLS policies...")
        fabric_rls, rls_fabric_tables = generate_fabric_rls(security_rules)
        out_path = SECURITY_DIR / "fabric_rls.sql"
        out_path.write_text(fabric_rls, encoding="utf-8")
        print(f"   → {out_path.name} ({len(rls_fabric_tables)} tables)")

    if target in ("databricks", "both"):
        print("\n🔒 Generating Databricks row filters...")
        dbx_rls, rls_databricks_tables = generate_databricks_rls(security_rules)
        out_path = SECURITY_DIR / "databricks_row_filters.sql"
        out_path.write_text(dbx_rls, encoding="utf-8")
        print(f"   → {out_path.name} ({len(rls_databricks_tables)} tables)")

    # For 'both' or unrecognized target, generate both
    if target not in ("fabric", "databricks", "both"):
        fabric_rls, rls_fabric_tables = generate_fabric_rls(security_rules)
        (SECURITY_DIR / "fabric_rls.sql").write_text(fabric_rls, encoding="utf-8")
        dbx_rls, rls_databricks_tables = generate_databricks_rls(security_rules)
        (SECURITY_DIR / "databricks_row_filters.sql").write_text(dbx_rls, encoding="utf-8")

    # 3. Generate column masking
    pii = security_rules.get("pii_findings", [])
    total_pii = len(pii)
    masked_count = 0

    if pii:
        print(f"\n🎭 Generating column masking for {total_pii} PII columns...")
        if target in ("fabric", "both") or target not in ("databricks",):
            fabric_mask, fc = generate_column_masking(pii, target="fabric")
            (SECURITY_DIR / "fabric_column_masking.sql").write_text(fabric_mask, encoding="utf-8")
            masked_count = max(masked_count, fc)
            print(f"   → fabric_column_masking.sql ({fc} columns)")

        if target in ("databricks", "both") or target not in ("fabric",):
            dbx_mask, dc = generate_column_masking(pii, target="databricks")
            (SECURITY_DIR / "databricks_column_masking.sql").write_text(dbx_mask, encoding="utf-8")
            masked_count = max(masked_count, dc)
            print(f"   → databricks_column_masking.sql ({dc} columns)")

    # 4. Generate audit report
    print("\n📊 Generating security audit report...")
    audit_json, audit_md = generate_security_audit(
        security_rules,
        rls_tables_fabric=rls_fabric_tables,
        rls_tables_databricks=rls_databricks_tables,
        masked_count=masked_count,
        total_tables=total_tables,
        total_pii=total_pii,
    )

    (SECURITY_DIR / "security_audit.json").write_text(
        json.dumps(audit_json, indent=2), encoding="utf-8"
    )
    (SECURITY_DIR / "security_audit.md").write_text(audit_md, encoding="utf-8")
    print(f"   → security_audit.json + security_audit.md")

    print(f"\n✅ Security migration complete — {SECURITY_DIR}")
    return audit_json


def main():
    run_security_migration()


if __name__ == "__main__":
    main()
