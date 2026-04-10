"""
Compliance — Sprint 84 (Phase 12)
GDPR/CCPA compliance: PII column classification, retention policy generation,
right-to-erasure templates, data residency validation, compliance reporting.
"""

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output"
INVENTORY_DIR = OUTPUT_DIR / "inventory"
COMPLIANCE_DIR = OUTPUT_DIR / "compliance"

# Import PII patterns from assessment if available, else define locally
try:
    from run_assessment import PII_COLUMN_PATTERNS, SENSITIVITY_LEVELS, detect_pii_columns
except ImportError:
    PII_COLUMN_PATTERNS = {
        "SSN": r"(?i)\b(ssn|social_security|soc_sec)\b",
        "EMAIL": r"(?i)\b(email|e_mail|email_addr)\b",
        "PHONE": r"(?i)\b(phone|mobile|cell|fax|tel)\b",
        "ADDRESS": r"(?i)\b(address|addr|street|city|zip|postal)\b",
        "NAME": r"(?i)\b(first_name|last_name|full_name|fname|lname|surname)\b",
        "DOB": r"(?i)\b(dob|date_of_birth|birth_date|birthdate)\b",
        "CREDIT_CARD": r"(?i)\b(credit_card|card_num|cc_num|card_number)\b",
        "PASSPORT": r"(?i)\b(passport|passport_no|passport_num)\b",
        "IP_ADDRESS": r"(?i)\b(ip_addr|ip_address|client_ip|source_ip)\b",
        "NATIONAL_ID": r"(?i)\b(national_id|id_number|citizen_id|tax_id|tin)\b",
    }
    SENSITIVITY_LEVELS = {
        "SSN": "Highly Confidential", "CREDIT_CARD": "Highly Confidential",
        "PASSPORT": "Highly Confidential", "NATIONAL_ID": "Highly Confidential",
        "DOB": "Confidential", "EMAIL": "Confidential", "PHONE": "Confidential",
        "NAME": "Internal", "ADDRESS": "Internal", "IP_ADDRESS": "Internal",
    }

    def detect_pii_columns(mappings):
        pii_findings = []
        compiled = {cat: re.compile(pat) for cat, pat in PII_COLUMN_PATTERNS.items()}
        for m in mappings:
            all_fields = list(m.get("sources", [])) + list(m.get("targets", []))
            for lineage in m.get("field_lineage", []):
                all_fields.append(lineage.get("source_field", ""))
                all_fields.append(lineage.get("target_field", ""))
            for field in all_fields:
                for category, pattern in compiled.items():
                    if pattern.search(field):
                        pii_findings.append({
                            "mapping": m["name"], "field": field,
                            "pii_category": category,
                            "sensitivity": SENSITIVITY_LEVELS.get(category, "Internal"),
                        })
        return pii_findings


# ─────────────────────────────────────────────
#  Retention Policy Configuration
# ─────────────────────────────────────────────

RETENTION_POLICIES = {
    "Highly Confidential": {
        "retention_days": 365,
        "delta_retention": "interval 365 days",
        "archive_after_days": 180,
        "description": "1-year retention — highly sensitive PII",
    },
    "Confidential": {
        "retention_days": 730,
        "delta_retention": "interval 730 days",
        "archive_after_days": 365,
        "description": "2-year retention — confidential data",
    },
    "Internal": {
        "retention_days": 1095,
        "delta_retention": "interval 1095 days",
        "archive_after_days": 730,
        "description": "3-year retention — internal data",
    },
    "Public": {
        "retention_days": 2555,
        "delta_retention": "interval 2555 days",
        "archive_after_days": 1825,
        "description": "7-year retention — public data (default)",
    },
}

# Allowed regions for sensitive data
COMPLIANT_REGIONS = {
    "Highly Confidential": ["eastus", "westus", "westeurope", "northeurope"],
    "Confidential": ["eastus", "westus", "westeurope", "northeurope", "centralus", "uksouth"],
    "Internal": None,  # Any region
    "Public": None,
}


# ─────────────────────────────────────────────
#  PII Column Classification
# ─────────────────────────────────────────────

def classify_pii_columns(inventory_path=None):
    """Classify all columns across mappings into PII categories with sensitivity levels."""
    inv_path = inventory_path or (INVENTORY_DIR / "inventory.json")
    if not Path(inv_path).exists():
        return {"classifications": [], "summary": {"total_columns": 0, "pii_columns": 0}}

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    mappings = inventory.get("mappings", [])

    # Use existing PII findings or detect
    pii_findings = inventory.get("pii_findings", [])
    if not pii_findings:
        pii_findings = detect_pii_columns(mappings)

    # Build classification report
    classifications = []
    total_columns = 0
    for m in mappings:
        all_fields = list(m.get("sources", [])) + list(m.get("targets", []))
        for lineage in m.get("field_lineage", []):
            all_fields.append(lineage.get("source_field", ""))
            all_fields.append(lineage.get("target_field", ""))
        total_columns += len(set(all_fields))

    for finding in pii_findings:
        classifications.append({
            "mapping": finding["mapping"],
            "column": finding["field"],
            "pii_category": finding["pii_category"],
            "sensitivity": finding.get("sensitivity", SENSITIVITY_LEVELS.get(finding["pii_category"], "Internal")),
            "gdpr_relevant": finding["pii_category"] in ("SSN", "EMAIL", "NAME", "DOB", "ADDRESS", "PHONE", "NATIONAL_ID", "PASSPORT"),
            "ccpa_relevant": finding["pii_category"] in ("SSN", "EMAIL", "NAME", "ADDRESS", "PHONE", "IP_ADDRESS"),
        })

    return {
        "classifications": classifications,
        "summary": {
            "total_columns_scanned": total_columns,
            "pii_columns": len(classifications),
            "by_category": _count_by_key(classifications, "pii_category"),
            "by_sensitivity": _count_by_key(classifications, "sensitivity"),
            "gdpr_relevant": sum(1 for c in classifications if c["gdpr_relevant"]),
            "ccpa_relevant": sum(1 for c in classifications if c["ccpa_relevant"]),
        },
    }


def _count_by_key(items, key):
    counts = {}
    for item in items:
        v = item.get(key, "Unknown")
        counts[v] = counts.get(v, 0) + 1
    return counts


# ─────────────────────────────────────────────
#  Retention Policy Generator
# ─────────────────────────────────────────────

def generate_retention_policies(classification_result, target="fabric", catalog="main", schema="silver"):
    """Generate Delta Lake retention properties per table based on data classification."""
    lines = []
    if target == "databricks":
        lines.append(f"-- Delta Lake Retention Policies ({catalog}.{schema})")
    else:
        lines.append("-- Delta Lake Retention Policies (Fabric Lakehouse)")
    lines.append(f"-- Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("")

    classifications = classification_result.get("classifications", [])
    if not classifications:
        lines.append("-- No PII columns detected — using default retention.")
        return "\n".join(lines), {}

    # Determine highest sensitivity per table
    table_sensitivity = {}
    for c in classifications:
        table = c["mapping"].replace("M_", "").lower()
        current = table_sensitivity.get(table, "Public")
        if _sensitivity_rank(c["sensitivity"]) > _sensitivity_rank(current):
            table_sensitivity[table] = c["sensitivity"]

    policies = {}
    for table_name, sensitivity in sorted(table_sensitivity.items()):
        policy = RETENTION_POLICIES.get(sensitivity, RETENTION_POLICIES["Public"])
        policies[table_name] = {
            "sensitivity": sensitivity,
            "retention_days": policy["retention_days"],
        }

        if target == "databricks":
            full_table = f"{catalog}.{schema}.{table_name}"
        else:
            full_table = table_name

        lines.append(f"-- Table: {full_table} (Sensitivity: {sensitivity})")
        lines.append(f"-- {policy['description']}")
        lines.append(f"ALTER TABLE {full_table}")
        lines.append(f"    SET TBLPROPERTIES (")
        lines.append(f"        'delta.deletedFileRetentionDuration' = '{policy['delta_retention']}',")
        lines.append(f"        'delta.logRetentionDuration' = '{policy['delta_retention']}',")
        lines.append(f"        'compliance.sensitivity' = '{sensitivity}',")
        lines.append(f"        'compliance.retention_days' = '{policy['retention_days']}'")
        lines.append(f"    );")
        lines.append("")

    return "\n".join(lines), policies


def _sensitivity_rank(level):
    return {"Public": 0, "Internal": 1, "Confidential": 2, "Highly Confidential": 3}.get(level, 0)


# ─────────────────────────────────────────────
#  Right-to-Erasure Template (GDPR Art. 17)
# ─────────────────────────────────────────────

def generate_erasure_notebook(classification_result, target="fabric", catalog="main", schema="silver"):
    """Generate a GDPR right-to-erasure PySpark notebook."""
    classifications = classification_result.get("classifications", [])

    # Build list of tables with GDPR-relevant PII
    gdpr_tables = {}
    for c in classifications:
        if c.get("gdpr_relevant"):
            table = c["mapping"].replace("M_", "").lower()
            if table not in gdpr_tables:
                gdpr_tables[table] = []
            gdpr_tables[table].append(c["column"])

    if target == "databricks":
        spark_import = "# Databricks notebook\nfrom pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()"
        table_prefix = f"{catalog}.{schema}"
    else:
        spark_import = "# Fabric notebook\nfrom pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()"
        table_prefix = schema

    lines = [
        '"""',
        "GDPR Right-to-Erasure Notebook (Article 17)",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "Usage: Set subject_id widget/parameter before running.",
        "This notebook deletes all records matching the data subject ID",
        "across all tables with GDPR-relevant PII columns.",
        '"""',
        "",
        spark_import,
        "",
        "# --- Configuration ---",
        'subject_id = spark.conf.get("spark.erasure.subject_id", "")',
        'if not subject_id:',
        '    raise ValueError("subject_id must be set before running erasure")',
        "",
        "audit_records = []",
        "",
    ]

    if not gdpr_tables:
        lines.append("# No GDPR-relevant tables detected")
        lines.append("print('No tables with GDPR-relevant PII found.')")
    else:
        lines.append(f"# Erasure across {len(gdpr_tables)} GDPR-relevant tables")
        lines.append("")

        for table_name, columns in sorted(gdpr_tables.items()):
            full_table = f"{table_prefix}.{table_name}"
            # Use the first PII column as the identifier column
            id_col = columns[0]
            lines.append(f"# --- Table: {full_table} ---")
            lines.append(f'try:')
            lines.append(f'    count_before = spark.table("{full_table}").count()')
            lines.append(f'    spark.sql(f"DELETE FROM {full_table} WHERE {id_col} = \'{{subject_id}}\'")')
            lines.append(f'    count_after = spark.table("{full_table}").count()')
            lines.append(f'    deleted = count_before - count_after')
            lines.append(f'    audit_records.append({{"table": "{full_table}", "column": "{id_col}", "deleted": deleted, "status": "success"}})')
            lines.append(f'    print(f"  ✅ {full_table}: {{deleted}} records deleted")')
            lines.append(f'except Exception as e:')
            lines.append(f'    audit_records.append({{"table": "{full_table}", "column": "{id_col}", "deleted": 0, "status": f"error: {{e}}"}})')
            lines.append(f'    print(f"  ❌ {full_table}: {{e}}")')
            lines.append("")

    # Audit logging
    lines.extend([
        "# --- Audit Log ---",
        "import json",
        "from datetime import datetime, timezone",
        "audit = {",
        '    "operation": "gdpr_erasure",',
        '    "subject_id_hash": hash(subject_id),  # Do not log actual subject_id',
        '    "timestamp": datetime.now(timezone.utc).isoformat(),',
        '    "tables_processed": len(audit_records),',
        '    "total_deleted": sum(r["deleted"] for r in audit_records),',
        '    "details": audit_records,',
        "}",
        'print(json.dumps(audit, indent=2))',
    ])

    return "\n".join(lines), len(gdpr_tables)


# ─────────────────────────────────────────────
#  Data Residency Validator
# ─────────────────────────────────────────────

def validate_data_residency(classification_result, workspace_region="eastus"):
    """Check if sensitive data tables are hosted in compliant regions."""
    classifications = classification_result.get("classifications", [])

    findings = []
    table_sensitivity = {}
    for c in classifications:
        table = c["mapping"].replace("M_", "").lower()
        current = table_sensitivity.get(table, "Public")
        if _sensitivity_rank(c["sensitivity"]) > _sensitivity_rank(current):
            table_sensitivity[table] = c["sensitivity"]

    for table_name, sensitivity in sorted(table_sensitivity.items()):
        allowed = COMPLIANT_REGIONS.get(sensitivity)
        if allowed is None:
            # Any region is fine
            findings.append({
                "table": table_name,
                "sensitivity": sensitivity,
                "region": workspace_region,
                "compliant": True,
                "message": "No region restriction",
            })
        elif workspace_region.lower() in [r.lower() for r in allowed]:
            findings.append({
                "table": table_name,
                "sensitivity": sensitivity,
                "region": workspace_region,
                "compliant": True,
                "message": f"Region {workspace_region} is compliant for {sensitivity}",
            })
        else:
            findings.append({
                "table": table_name,
                "sensitivity": sensitivity,
                "region": workspace_region,
                "compliant": False,
                "message": f"⚠️ Region {workspace_region} NOT in allowed list for {sensitivity}: {allowed}",
            })

    compliant_count = sum(1 for f in findings if f["compliant"])
    total = len(findings)

    return {
        "findings": findings,
        "summary": {
            "total_tables": total,
            "compliant": compliant_count,
            "non_compliant": total - compliant_count,
            "compliance_rate": round(compliant_count / total, 3) if total > 0 else 1.0,
            "workspace_region": workspace_region,
        },
    }


# ─────────────────────────────────────────────
#  Compliance Report
# ─────────────────────────────────────────────

def generate_compliance_report(classification_result, residency_result, retention_policies,
                               erasure_table_count=0):
    """Generate a compliance dashboard report (JSON + Markdown)."""
    cls_summary = classification_result.get("summary", {})
    res_summary = residency_result.get("summary", {})

    # GDPR readiness score (0-100)
    scores = []
    # PII classified?
    scores.append(100 if cls_summary.get("pii_columns", 0) > 0 else 50)
    # Retention policies set?
    scores.append(100 if retention_policies else 0)
    # Erasure template generated?
    scores.append(100 if erasure_table_count > 0 else 0)
    # Data residency compliant?
    scores.append(int(res_summary.get("compliance_rate", 0) * 100))

    gdpr_score = round(sum(scores) / len(scores)) if scores else 0

    report = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "gdpr_readiness_score": gdpr_score,
        "pii_classification": cls_summary,
        "data_residency": res_summary,
        "retention_policies_count": len(retention_policies),
        "erasure_tables_count": erasure_table_count,
    }

    # Markdown report
    md = [
        "# Compliance Report — GDPR / CCPA",
        "",
        f"**Generated:** {report['generated']}",
        f"**GDPR Readiness Score:** {gdpr_score}/100",
        "",
        "## PII Classification Summary",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total columns scanned | {cls_summary.get('total_columns_scanned', 0)} |",
        f"| PII columns detected | {cls_summary.get('pii_columns', 0)} |",
        f"| GDPR-relevant columns | {cls_summary.get('gdpr_relevant', 0)} |",
        f"| CCPA-relevant columns | {cls_summary.get('ccpa_relevant', 0)} |",
        "",
        "### By Category",
        "",
    ]
    for cat, count in sorted(cls_summary.get("by_category", {}).items()):
        md.append(f"- **{cat}**: {count}")
    md.append("")

    md.extend([
        "## Data Residency",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Region | {res_summary.get('workspace_region', 'unknown')} |",
        f"| Compliant tables | {res_summary.get('compliant', 0)} |",
        f"| Non-compliant tables | {res_summary.get('non_compliant', 0)} |",
        f"| Compliance rate | {res_summary.get('compliance_rate', 0):.1%} |",
        "",
        "## Retention Policies",
        "",
        f"Tables with retention policies: **{len(retention_policies)}**",
        "",
        "## GDPR Erasure",
        "",
        f"Tables covered by erasure template: **{erasure_table_count}**",
        "",
    ])

    # Non-compliant warnings
    non_compliant = [f for f in residency_result.get("findings", []) if not f["compliant"]]
    if non_compliant:
        md.append("## ⚠️ Data Residency Warnings")
        md.append("")
        for nc in non_compliant:
            md.append(f"- **{nc['table']}** ({nc['sensitivity']}): {nc['message']}")
        md.append("")

    return report, "\n".join(md)


# ─────────────────────────────────────────────
#  Main Entry Point
# ─────────────────────────────────────────────

def run_compliance(inventory_path=None, target=None, workspace_region="eastus"):
    """Run full compliance pipeline: classify → retention → erasure → residency → report."""
    target = target or os.environ.get("INFORMATICA_MIGRATION_TARGET", "fabric")

    COMPLIANCE_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("  Compliance — Sprint 84 (GDPR/CCPA)")
    print("=" * 60)

    # 1. PII classification
    print("\n📋 Classifying PII columns...")
    classification = classify_pii_columns(inventory_path)
    cls_summary = classification["summary"]
    print(f"   PII columns: {cls_summary['pii_columns']}")
    print(f"   GDPR-relevant: {cls_summary.get('gdpr_relevant', 0)}")
    print(f"   CCPA-relevant: {cls_summary.get('ccpa_relevant', 0)}")

    (COMPLIANCE_DIR / "pii_classification.json").write_text(
        json.dumps(classification, indent=2), encoding="utf-8"
    )

    # 2. Retention policies
    print("\n📅 Generating retention policies...")
    retention_sql, retention_policies = generate_retention_policies(
        classification, target=target
    )
    (COMPLIANCE_DIR / "retention_policies.sql").write_text(retention_sql, encoding="utf-8")
    print(f"   Tables with policies: {len(retention_policies)}")

    # 3. Right-to-erasure template
    print("\n🗑️  Generating GDPR erasure notebook...")
    erasure_code, erasure_count = generate_erasure_notebook(
        classification, target=target
    )
    (COMPLIANCE_DIR / "gdpr_erasure_notebook.py").write_text(erasure_code, encoding="utf-8")
    print(f"   GDPR tables covered: {erasure_count}")

    # 4. Data residency validation
    print(f"\n🌍 Validating data residency (region: {workspace_region})...")
    residency = validate_data_residency(classification, workspace_region=workspace_region)
    res_summary = residency["summary"]
    print(f"   Compliant: {res_summary['compliant']}/{res_summary['total_tables']}")
    if res_summary["non_compliant"] > 0:
        print(f"   ⚠️  Non-compliant: {res_summary['non_compliant']}")

    (COMPLIANCE_DIR / "data_residency.json").write_text(
        json.dumps(residency, indent=2), encoding="utf-8"
    )

    # 5. Compliance report
    print("\n📊 Generating compliance report...")
    report_json, report_md = generate_compliance_report(
        classification, residency, retention_policies, erasure_count
    )
    (COMPLIANCE_DIR / "compliance_report.json").write_text(
        json.dumps(report_json, indent=2), encoding="utf-8"
    )
    (COMPLIANCE_DIR / "compliance_report.md").write_text(report_md, encoding="utf-8")
    print(f"   GDPR readiness score: {report_json['gdpr_readiness_score']}/100")

    print(f"\n✅ Compliance analysis complete — {COMPLIANCE_DIR}")
    return report_json


def main():
    run_compliance()


if __name__ == "__main__":
    main()
