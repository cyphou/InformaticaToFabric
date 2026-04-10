"""
Tests for Phase 12 — Governance & Compliance (Sprints 83-85)
Sprint 83: Security Migration (RLS/CLS, column masking, audit)
Sprint 84: GDPR/CCPA Compliance (PII classification, retention, erasure, residency)
Sprint 85: Certification (gates, audit trail, evidence package, sign-off)
"""

import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

# ─────────────────────────────────────────────
#  Sprint 83 — Security Migration Tests
# ─────────────────────────────────────────────


class TestSecurityRuleExtraction(unittest.TestCase):
    """Tests for extract_security_rules()."""

    def test_import_security_migration(self):
        import security_migration
        self.assertTrue(hasattr(security_migration, "extract_security_rules"))
        self.assertTrue(hasattr(security_migration, "generate_fabric_rls"))
        self.assertTrue(hasattr(security_migration, "generate_databricks_rls"))
        self.assertTrue(hasattr(security_migration, "generate_column_masking"))
        self.assertTrue(hasattr(security_migration, "generate_security_audit"))

    def test_extract_no_inventory(self):
        from security_migration import extract_security_rules
        result = extract_security_rules("/nonexistent/path.json")
        self.assertEqual(result["filters"], [])
        self.assertEqual(result["masking_transforms"], [])
        self.assertEqual(result["user_groups"], [])

    def test_extract_with_security_filters(self):
        from security_migration import extract_security_rules
        inv = {
            "mappings": [
                {
                    "name": "M_CUSTOMER_LOAD",
                    "sql_overrides": ["SELECT * FROM customers WHERE user = 'admin'"],
                    "transformations": ["SQ", "EXP"],
                },
            ],
            "pii_findings": [],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = extract_security_rules(f.name)
        os.unlink(f.name)
        self.assertGreater(len(result["filters"]), 0)
        self.assertEqual(result["filters"][0]["mapping"], "M_CUSTOMER_LOAD")

    def test_extract_user_groups(self):
        from security_migration import extract_security_rules
        inv = {
            "mappings": [
                {
                    "name": "M_LOAD",
                    "sql_overrides": ["SELECT * FROM t WHERE role = 'data_engineers'"],
                    "transformations": [],
                },
            ],
            "pii_findings": [],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = extract_security_rules(f.name)
        os.unlink(f.name)
        self.assertIn("data_engineers", result["user_groups"])

    def test_extract_masking_transforms(self):
        from security_migration import extract_security_rules
        inv = {
            "mappings": [
                {
                    "name": "M_MASK",
                    "sql_overrides": [],
                    "transformations": ["DM", "EXP"],
                },
            ],
            "pii_findings": [],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = extract_security_rules(f.name)
        os.unlink(f.name)
        self.assertEqual(len(result["masking_transforms"]), 1)
        self.assertEqual(result["masking_transforms"][0]["type"], "data_masking_transform")

    def test_extract_filter_transforms(self):
        from security_migration import extract_security_rules
        inv = {
            "mappings": [
                {
                    "name": "M_FILT",
                    "sql_overrides": [],
                    "transformations": ["FIL"],
                },
            ],
            "pii_findings": [],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = extract_security_rules(f.name)
        os.unlink(f.name)
        self.assertTrue(any(f_item["type"] == "filter_transform" for f_item in result["filters"]))

    def test_extract_pii_findings_passthrough(self):
        from security_migration import extract_security_rules
        pii = [{"mapping": "M_X", "field": "ssn", "pii_category": "SSN", "sensitivity": "Highly Confidential"}]
        inv = {"mappings": [], "pii_findings": pii}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = extract_security_rules(f.name)
        os.unlink(f.name)
        self.assertEqual(len(result["pii_findings"]), 1)


class TestFabricRLS(unittest.TestCase):
    """Tests for generate_fabric_rls()."""

    def test_no_filters(self):
        from security_migration import generate_fabric_rls
        sql, tables = generate_fabric_rls({"filters": []})
        self.assertIn("No security filters detected", sql)
        self.assertEqual(len(tables), 0)

    def test_with_filters(self):
        from security_migration import generate_fabric_rls
        rules = {
            "filters": [
                {"mapping": "M_CUSTOMER", "type": "session_filter", "expression": "WHERE user = 'x'"},
            ],
        }
        sql, tables = generate_fabric_rls(rules)
        self.assertIn("CREATE SECURITY POLICY", sql)
        self.assertIn("fn_security_filter", sql)
        self.assertEqual(len(tables), 1)
        self.assertIn("dbo.customer", tables)

    def test_custom_schema(self):
        from security_migration import generate_fabric_rls
        rules = {"filters": [{"mapping": "M_ORDERS", "type": "filter_transform", "expression": "..."}]}
        sql, tables = generate_fabric_rls(rules, schema="silver")
        self.assertIn("silver.orders", sql)
        self.assertIn("silver.fn_security_filter", sql)


class TestDatabricksRLS(unittest.TestCase):
    """Tests for generate_databricks_rls()."""

    def test_no_filters(self):
        from security_migration import generate_databricks_rls
        sql, tables = generate_databricks_rls({"filters": []})
        self.assertIn("No security filters detected", sql)
        self.assertEqual(len(tables), 0)

    def test_with_filters(self):
        from security_migration import generate_databricks_rls
        rules = {
            "filters": [
                {"mapping": "M_ORDERS", "type": "session_filter", "expression": "..."},
            ],
        }
        sql, tables = generate_databricks_rls(rules)
        self.assertIn("ALTER TABLE", sql)
        self.assertIn("SET ROW FILTER", sql)
        self.assertIn("row_filter", sql)
        self.assertEqual(len(tables), 1)

    def test_custom_catalog_schema(self):
        from security_migration import generate_databricks_rls
        rules = {"filters": [{"mapping": "M_ITEMS", "type": "filter_transform", "expression": "..."}]}
        sql, tables = generate_databricks_rls(rules, catalog="analytics", schema="gold")
        self.assertIn("analytics.gold.items", sql)


class TestColumnMasking(unittest.TestCase):
    """Tests for generate_column_masking()."""

    def test_no_pii(self):
        from security_migration import generate_column_masking
        sql, count = generate_column_masking([], target="fabric")
        self.assertIn("No PII columns detected", sql)
        self.assertEqual(count, 0)

    def test_fabric_masking(self):
        from security_migration import generate_column_masking
        pii = [
            {"mapping": "M_CUST", "field": "ssn", "pii_category": "SSN", "sensitivity": "Highly Confidential"},
            {"mapping": "M_CUST", "field": "email_addr", "pii_category": "EMAIL", "sensitivity": "Confidential"},
        ]
        sql, count = generate_column_masking(pii, target="fabric")
        self.assertIn("ALTER TABLE", sql)
        self.assertIn("ADD MASKED", sql)
        self.assertEqual(count, 2)

    def test_databricks_masking(self):
        from security_migration import generate_column_masking
        pii = [
            {"mapping": "M_CUST", "field": "ssn", "pii_category": "SSN", "sensitivity": "Highly Confidential"},
        ]
        sql, count = generate_column_masking(pii, target="databricks")
        self.assertIn("CREATE OR REPLACE FUNCTION", sql)
        self.assertIn("SET MASK", sql)
        self.assertEqual(count, 1)

    def test_masking_functions_defined(self):
        from security_migration import MASKING_FUNCTIONS
        self.assertIn("SSN", MASKING_FUNCTIONS)
        self.assertIn("EMAIL", MASKING_FUNCTIONS)
        self.assertIn("CREDIT_CARD", MASKING_FUNCTIONS)
        for cat, info in MASKING_FUNCTIONS.items():
            self.assertIn("fabric", info)
            self.assertIn("databricks", info)
            self.assertIn("description", info)


class TestSecurityAudit(unittest.TestCase):
    """Tests for generate_security_audit()."""

    def test_empty_rules(self):
        from security_migration import generate_security_audit
        audit_json, audit_md = generate_security_audit(
            {"filters": [], "masking_transforms": [], "user_groups": []},
            total_tables=10, total_pii=0,
        )
        self.assertEqual(audit_json["security_filters_found"], 0)
        self.assertEqual(audit_json["overall"]["total_tables"], 10)
        self.assertIn("Security Policy Audit", audit_md)

    def test_with_data(self):
        from security_migration import generate_security_audit
        rules = {
            "filters": [{"mapping": "M_X"}],
            "masking_transforms": [{"mapping": "M_Y"}],
            "user_groups": ["admins", "analysts"],
        }
        audit_json, audit_md = generate_security_audit(
            rules,
            rls_tables_fabric={"dbo.x"},
            rls_tables_databricks={"main.silver.x"},
            masked_count=5,
            total_tables=10,
            total_pii=8,
        )
        self.assertEqual(audit_json["security_filters_found"], 1)
        self.assertEqual(audit_json["column_masking"]["masked_columns"], 5)
        self.assertGreater(audit_json["overall"]["rls_coverage"], 0)
        self.assertIn("admins", audit_md)

    def test_audit_markdown_has_sections(self):
        from security_migration import generate_security_audit
        _, md = generate_security_audit(
            {"filters": [{"m": 1}], "masking_transforms": [], "user_groups": ["eng"]},
            rls_tables_fabric={"dbo.t1"},
            total_tables=5, total_pii=3, masked_count=2,
        )
        self.assertIn("## Summary", md)
        self.assertIn("## RLS Policies", md)
        self.assertIn("## Detected User Groups", md)


class TestRunSecurityMigration(unittest.TestCase):
    """Tests for run_security_migration() end-to-end."""

    def test_run_with_no_inventory(self):
        from security_migration import run_security_migration
        result = run_security_migration(inventory_path="/nonexistent.json", target="fabric")
        self.assertIn("security_filters_found", result)


# ─────────────────────────────────────────────
#  Sprint 84 — Compliance Tests
# ─────────────────────────────────────────────


class TestPIIClassification(unittest.TestCase):
    """Tests for classify_pii_columns()."""

    def test_import_compliance(self):
        import compliance
        self.assertTrue(hasattr(compliance, "classify_pii_columns"))
        self.assertTrue(hasattr(compliance, "generate_retention_policies"))
        self.assertTrue(hasattr(compliance, "generate_erasure_notebook"))
        self.assertTrue(hasattr(compliance, "validate_data_residency"))
        self.assertTrue(hasattr(compliance, "generate_compliance_report"))

    def test_no_inventory(self):
        from compliance import classify_pii_columns
        result = classify_pii_columns("/nonexistent.json")
        self.assertEqual(result["summary"]["pii_columns"], 0)

    def test_with_pii_findings(self):
        from compliance import classify_pii_columns
        inv = {
            "mappings": [
                {"name": "M_LOAD", "sources": ["ssn", "email"], "targets": ["customer_id"],
                 "field_lineage": [], "transformations": []},
            ],
            "pii_findings": [
                {"mapping": "M_LOAD", "field": "ssn", "pii_category": "SSN", "sensitivity": "Highly Confidential"},
                {"mapping": "M_LOAD", "field": "email", "pii_category": "EMAIL", "sensitivity": "Confidential"},
            ],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = classify_pii_columns(f.name)
        os.unlink(f.name)
        self.assertEqual(result["summary"]["pii_columns"], 2)
        self.assertTrue(result["classifications"][0]["gdpr_relevant"])

    def test_gdpr_and_ccpa_flags(self):
        from compliance import classify_pii_columns
        inv = {
            "mappings": [],
            "pii_findings": [
                {"mapping": "M_A", "field": "ip_address", "pii_category": "IP_ADDRESS", "sensitivity": "Internal"},
            ],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = classify_pii_columns(f.name)
        os.unlink(f.name)
        # IP_ADDRESS is CCPA-relevant but not GDPR
        cls = result["classifications"][0]
        self.assertFalse(cls["gdpr_relevant"])
        self.assertTrue(cls["ccpa_relevant"])

    def test_by_category_counts(self):
        from compliance import classify_pii_columns
        inv = {
            "mappings": [],
            "pii_findings": [
                {"mapping": "M_A", "field": "ssn", "pii_category": "SSN", "sensitivity": "Highly Confidential"},
                {"mapping": "M_A", "field": "email", "pii_category": "EMAIL", "sensitivity": "Confidential"},
                {"mapping": "M_B", "field": "ssn2", "pii_category": "SSN", "sensitivity": "Highly Confidential"},
            ],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = classify_pii_columns(f.name)
        os.unlink(f.name)
        self.assertEqual(result["summary"]["by_category"]["SSN"], 2)
        self.assertEqual(result["summary"]["by_category"]["EMAIL"], 1)


class TestRetentionPolicies(unittest.TestCase):
    """Tests for generate_retention_policies()."""

    def test_no_classifications(self):
        from compliance import generate_retention_policies
        sql, policies = generate_retention_policies({"classifications": []})
        self.assertIn("No PII columns detected", sql)
        self.assertEqual(len(policies), 0)

    def test_fabric_retention(self):
        from compliance import generate_retention_policies
        cls = {"classifications": [
            {"mapping": "M_CUST", "column": "ssn", "pii_category": "SSN",
             "sensitivity": "Highly Confidential", "gdpr_relevant": True, "ccpa_relevant": True},
        ]}
        sql, policies = generate_retention_policies(cls, target="fabric")
        self.assertIn("ALTER TABLE", sql)
        self.assertIn("deletedFileRetentionDuration", sql)
        self.assertIn("cust", policies)
        self.assertEqual(policies["cust"]["retention_days"], 365)

    def test_databricks_retention(self):
        from compliance import generate_retention_policies
        cls = {"classifications": [
            {"mapping": "M_ORDERS", "column": "email", "pii_category": "EMAIL",
             "sensitivity": "Confidential", "gdpr_relevant": True, "ccpa_relevant": True},
        ]}
        sql, policies = generate_retention_policies(cls, target="databricks", catalog="prod")
        self.assertIn("prod.silver", sql)
        self.assertEqual(policies["orders"]["retention_days"], 730)

    def test_highest_sensitivity_wins(self):
        from compliance import generate_retention_policies
        cls = {"classifications": [
            {"mapping": "M_USERS", "column": "email", "sensitivity": "Confidential",
             "pii_category": "EMAIL", "gdpr_relevant": True, "ccpa_relevant": True},
            {"mapping": "M_USERS", "column": "ssn", "sensitivity": "Highly Confidential",
             "pii_category": "SSN", "gdpr_relevant": True, "ccpa_relevant": True},
        ]}
        _, policies = generate_retention_policies(cls)
        # Highly Confidential (365 days) should win over Confidential (730)
        self.assertEqual(policies["users"]["retention_days"], 365)


class TestErasureNotebook(unittest.TestCase):
    """Tests for generate_erasure_notebook()."""

    def test_no_gdpr_tables(self):
        from compliance import generate_erasure_notebook
        code, count = generate_erasure_notebook({"classifications": []})
        self.assertEqual(count, 0)
        self.assertIn("No GDPR-relevant tables", code)

    def test_fabric_erasure(self):
        from compliance import generate_erasure_notebook
        cls = {"classifications": [
            {"mapping": "M_CUST", "column": "email", "pii_category": "EMAIL",
             "sensitivity": "Confidential", "gdpr_relevant": True, "ccpa_relevant": True},
        ]}
        code, count = generate_erasure_notebook(cls, target="fabric")
        self.assertEqual(count, 1)
        self.assertIn("DELETE FROM", code)
        self.assertIn("audit_records", code)

    def test_databricks_erasure(self):
        from compliance import generate_erasure_notebook
        cls = {"classifications": [
            {"mapping": "M_USERS", "column": "ssn", "pii_category": "SSN",
             "sensitivity": "Highly Confidential", "gdpr_relevant": True, "ccpa_relevant": True},
        ]}
        code, count = generate_erasure_notebook(cls, target="databricks", catalog="prod")
        self.assertEqual(count, 1)
        self.assertIn("prod.silver", code)

    def test_erasure_has_audit_logging(self):
        from compliance import generate_erasure_notebook
        cls = {"classifications": [
            {"mapping": "M_X", "column": "email", "pii_category": "EMAIL",
             "sensitivity": "Confidential", "gdpr_relevant": True, "ccpa_relevant": True},
        ]}
        code, _ = generate_erasure_notebook(cls)
        self.assertIn("audit", code.lower())
        self.assertIn("gdpr_erasure", code)

    def test_erasure_requires_subject_id(self):
        from compliance import generate_erasure_notebook
        cls = {"classifications": [
            {"mapping": "M_X", "column": "ssn", "pii_category": "SSN",
             "sensitivity": "Highly Confidential", "gdpr_relevant": True, "ccpa_relevant": True},
        ]}
        code, _ = generate_erasure_notebook(cls)
        self.assertIn("subject_id", code)
        self.assertIn("ValueError", code)


class TestDataResidency(unittest.TestCase):
    """Tests for validate_data_residency()."""

    def test_empty_classifications(self):
        from compliance import validate_data_residency
        result = validate_data_residency({"classifications": []})
        self.assertEqual(result["summary"]["total_tables"], 0)
        self.assertEqual(result["summary"]["compliance_rate"], 1.0)

    def test_compliant_region(self):
        from compliance import validate_data_residency
        cls = {"classifications": [
            {"mapping": "M_X", "column": "ssn", "sensitivity": "Highly Confidential",
             "pii_category": "SSN", "gdpr_relevant": True, "ccpa_relevant": True},
        ]}
        result = validate_data_residency(cls, workspace_region="eastus")
        self.assertTrue(result["findings"][0]["compliant"])
        self.assertEqual(result["summary"]["compliance_rate"], 1.0)

    def test_non_compliant_region(self):
        from compliance import validate_data_residency
        cls = {"classifications": [
            {"mapping": "M_X", "column": "ssn", "sensitivity": "Highly Confidential",
             "pii_category": "SSN", "gdpr_relevant": True, "ccpa_relevant": True},
        ]}
        result = validate_data_residency(cls, workspace_region="southeastasia")
        self.assertFalse(result["findings"][0]["compliant"])
        self.assertEqual(result["summary"]["non_compliant"], 1)

    def test_internal_sensitivity_any_region(self):
        from compliance import validate_data_residency
        cls = {"classifications": [
            {"mapping": "M_Y", "column": "addr", "sensitivity": "Internal",
             "pii_category": "ADDRESS", "gdpr_relevant": True, "ccpa_relevant": False},
        ]}
        result = validate_data_residency(cls, workspace_region="southeastasia")
        self.assertTrue(result["findings"][0]["compliant"])

    def test_mixed_compliance(self):
        from compliance import validate_data_residency
        cls = {"classifications": [
            {"mapping": "M_A", "column": "ssn", "sensitivity": "Highly Confidential",
             "pii_category": "SSN", "gdpr_relevant": True, "ccpa_relevant": True},
            {"mapping": "M_B", "column": "name", "sensitivity": "Internal",
             "pii_category": "NAME", "gdpr_relevant": True, "ccpa_relevant": False},
        ]}
        result = validate_data_residency(cls, workspace_region="southeastasia")
        self.assertEqual(result["summary"]["total_tables"], 2)
        # M_A is non-compliant (Highly Confidential + southeastasia), M_B compliant (Internal)
        self.assertEqual(result["summary"]["non_compliant"], 1)
        self.assertEqual(result["summary"]["compliant"], 1)


class TestComplianceReport(unittest.TestCase):
    """Tests for generate_compliance_report()."""

    def test_report_structure(self):
        from compliance import generate_compliance_report
        cls_result = {"summary": {"total_columns_scanned": 50, "pii_columns": 5,
                                  "gdpr_relevant": 3, "ccpa_relevant": 4, "by_category": {"SSN": 2, "EMAIL": 3},
                                  "by_sensitivity": {"Highly Confidential": 2, "Confidential": 3}}}
        res_result = {"findings": [], "summary": {"total_tables": 0, "compliant": 0,
                                                   "non_compliant": 0, "compliance_rate": 1.0,
                                                   "workspace_region": "eastus"}}
        report_json, report_md = generate_compliance_report(cls_result, res_result, {"t1": {}}, 2)
        self.assertIn("gdpr_readiness_score", report_json)
        self.assertIsInstance(report_json["gdpr_readiness_score"], int)
        self.assertIn("Compliance Report", report_md)
        self.assertIn("GDPR Readiness Score", report_md)

    def test_gdpr_score_calculation(self):
        from compliance import generate_compliance_report
        cls_result = {"summary": {"total_columns_scanned": 10, "pii_columns": 3,
                                  "gdpr_relevant": 2, "ccpa_relevant": 2,
                                  "by_category": {}, "by_sensitivity": {}}}
        res_result = {"findings": [], "summary": {"total_tables": 0, "compliant": 0,
                                                   "non_compliant": 0, "compliance_rate": 1.0,
                                                   "workspace_region": "eastus"}}
        report, _ = generate_compliance_report(cls_result, res_result, {"t": {}}, 1)
        # All 4 scores should be 100 → average 100
        self.assertEqual(report["gdpr_readiness_score"], 100)

    def test_report_warnings_for_noncompliant(self):
        from compliance import generate_compliance_report
        cls_result = {"summary": {"total_columns_scanned": 10, "pii_columns": 1,
                                  "gdpr_relevant": 1, "ccpa_relevant": 1,
                                  "by_category": {}, "by_sensitivity": {}}}
        res_result = {
            "findings": [{"table": "t1", "sensitivity": "HC", "compliant": False, "message": "bad region"}],
            "summary": {"total_tables": 1, "compliant": 0, "non_compliant": 1,
                        "compliance_rate": 0.0, "workspace_region": "unknown"},
        }
        _, md = generate_compliance_report(cls_result, res_result, {}, 0)
        self.assertIn("Data Residency Warnings", md)


class TestRunCompliance(unittest.TestCase):
    """Tests for run_compliance() end-to-end."""

    def test_run_with_no_inventory(self):
        from compliance import run_compliance
        result = run_compliance(inventory_path="/nonexistent.json", target="fabric")
        self.assertIn("gdpr_readiness_score", result)


# ─────────────────────────────────────────────
#  Sprint 85 — Certification Tests
# ─────────────────────────────────────────────


class TestCertificationGates(unittest.TestCase):
    """Tests for the 6-gate certification system."""

    def test_import_certification(self):
        import certification
        self.assertTrue(hasattr(certification, "evaluate_gates"))
        self.assertTrue(hasattr(certification, "build_artifact_audit_trail"))
        self.assertTrue(hasattr(certification, "generate_evidence_package"))
        self.assertTrue(hasattr(certification, "generate_signoff_template"))

    def test_six_gates_defined(self):
        from certification import CERTIFICATION_GATES
        self.assertEqual(len(CERTIFICATION_GATES), 6)
        names = [g["name"] for g in CERTIFICATION_GATES]
        self.assertIn("Assessment", names)
        self.assertIn("Conversion", names)
        self.assertIn("Validation", names)
        self.assertIn("Security", names)
        self.assertIn("Performance", names)
        self.assertIn("Production", names)

    def test_evaluate_gates_returns_list(self):
        from certification import evaluate_gates
        results = evaluate_gates()
        self.assertEqual(len(results), 6)
        for g in results:
            self.assertIn("gate_id", g)
            self.assertIn("gate_name", g)
            self.assertIn("passed", g)
            self.assertIn("checks", g)
            self.assertIsInstance(g["passed"], bool)

    def test_gate_ids_sequential(self):
        from certification import evaluate_gates
        results = evaluate_gates()
        ids = [g["gate_id"] for g in results]
        self.assertEqual(ids, [1, 2, 3, 4, 5, 6])

    def test_gates_have_checks(self):
        from certification import CERTIFICATION_GATES
        for gate in CERTIFICATION_GATES:
            self.assertGreater(len(gate["checks"]), 0, f"Gate {gate['name']} has no checks")

    def test_no_critical_errors_check(self):
        from certification import _check_no_critical_errors
        # Without issues file, should return True
        result = _check_no_critical_errors()
        self.assertIsInstance(result, bool)


class TestAuditTrail(unittest.TestCase):
    """Tests for build_artifact_audit_trail()."""

    def test_build_returns_list(self):
        from certification import build_artifact_audit_trail
        trail = build_artifact_audit_trail()
        self.assertIsInstance(trail, list)

    def test_audit_entry_structure(self):
        from certification import _audit_entry
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write("# test")
            f.flush()
            entry = _audit_entry(f.name, "test")
        os.unlink(f.name)
        self.assertIn("artifact", entry)
        self.assertIn("type", entry)
        self.assertIn("size_bytes", entry)
        self.assertIn("created", entry)
        self.assertIn("modified", entry)
        self.assertEqual(entry["type"], "test")

    def test_audit_scans_multiple_directories(self):
        """build_artifact_audit_trail scans notebooks, pipelines, sql, validation, dbt, security, compliance."""
        from certification import build_artifact_audit_trail
        # Just verify it doesn't crash — actual content depends on output/ state
        trail = build_artifact_audit_trail()
        self.assertIsInstance(trail, list)


class TestEvidencePackage(unittest.TestCase):
    """Tests for generate_evidence_package()."""

    def test_generates_zip(self):
        from certification import generate_evidence_package, CERTIFICATION_DIR
        CERTIFICATION_DIR.mkdir(parents=True, exist_ok=True)
        zip_path = generate_evidence_package(
            gate_results=[{"gate_id": 1, "gate_name": "Test", "passed": True, "checks": {}, "description": "t"}],
            audit_trail=[],
        )
        self.assertTrue(Path(zip_path).exists())
        self.assertTrue(zip_path.endswith(".zip"))

    def test_zip_contains_required_files(self):
        from certification import generate_evidence_package, CERTIFICATION_DIR
        CERTIFICATION_DIR.mkdir(parents=True, exist_ok=True)
        zip_path = generate_evidence_package(
            gate_results=[{"gate_id": 1, "gate_name": "Test", "passed": True, "checks": {}, "description": "t"}],
            audit_trail=[{"artifact": "test.py", "type": "test"}],
        )
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
            self.assertIn("gate_results.json", names)
            self.assertIn("artifact_audit_trail.json", names)
            self.assertIn("signoff_template.md", names)

    def test_zip_is_valid(self):
        from certification import generate_evidence_package, CERTIFICATION_DIR
        CERTIFICATION_DIR.mkdir(parents=True, exist_ok=True)
        zip_path = generate_evidence_package(
            gate_results=[{"gate_id": 1, "gate_name": "T", "passed": False, "checks": {}, "description": "t"}],
            audit_trail=[],
        )
        self.assertTrue(zipfile.is_zipfile(zip_path))


class TestSignoffTemplate(unittest.TestCase):
    """Tests for generate_signoff_template()."""

    def test_contains_all_gates(self):
        from certification import generate_signoff_template
        gate_results = [
            {"gate_id": i, "gate_name": f"Gate{i}", "passed": i <= 3,
             "checks": {"check_a": True}, "description": f"desc {i}"}
            for i in range(1, 7)
        ]
        signoff = generate_signoff_template(gate_results)
        self.assertIn("Gate1", signoff)
        self.assertIn("Gate6", signoff)
        self.assertIn("Stakeholder Sign-Off", signoff)

    def test_stakeholder_table(self):
        from certification import generate_signoff_template
        signoff = generate_signoff_template([
            {"gate_id": 1, "gate_name": "Test", "passed": True, "checks": {}, "description": "t"},
        ])
        self.assertIn("Data Engineering Lead", signoff)
        self.assertIn("Security Officer", signoff)
        self.assertIn("Compliance Officer", signoff)
        self.assertIn("Project Manager", signoff)
        self.assertIn("Business Owner", signoff)

    def test_ready_for_production_yes(self):
        from certification import generate_signoff_template
        signoff = generate_signoff_template([
            {"gate_id": 1, "gate_name": "Only", "passed": True, "checks": {}, "description": "t"},
        ])
        self.assertIn("Ready for production:** Yes", signoff)

    def test_ready_for_production_no(self):
        from certification import generate_signoff_template
        signoff = generate_signoff_template([
            {"gate_id": 1, "gate_name": "Only", "passed": False, "checks": {}, "description": "t"},
        ])
        self.assertIn("address failing gates", signoff)

    def test_checkboxes(self):
        from certification import generate_signoff_template
        signoff = generate_signoff_template([
            {"gate_id": 1, "gate_name": "Pass", "passed": True, "checks": {"c1": True}, "description": "t"},
            {"gate_id": 2, "gate_name": "Fail", "passed": False, "checks": {"c2": False}, "description": "t"},
        ])
        self.assertIn("[x]", signoff)
        self.assertIn("[ ]", signoff)


class TestRunCertification(unittest.TestCase):
    """Tests for run_certification() end-to-end."""

    def test_run_returns_result(self):
        from certification import run_certification
        result = run_certification()
        self.assertIn("gates_passed", result)
        self.assertIn("gates_total", result)
        self.assertEqual(result["gates_total"], 6)
        self.assertIn("evidence_package", result)
        self.assertIn("ready_for_production", result)
        self.assertIsInstance(result["ready_for_production"], bool)

    def test_artifacts_tracked(self):
        from certification import run_certification
        result = run_certification()
        self.assertIn("artifacts_tracked", result)
        self.assertIsInstance(result["artifacts_tracked"], int)


class TestRetentionPolicyConfig(unittest.TestCase):
    """Tests for RETENTION_POLICIES configuration."""

    def test_all_sensitivity_levels_covered(self):
        from compliance import RETENTION_POLICIES
        self.assertIn("Highly Confidential", RETENTION_POLICIES)
        self.assertIn("Confidential", RETENTION_POLICIES)
        self.assertIn("Internal", RETENTION_POLICIES)
        self.assertIn("Public", RETENTION_POLICIES)

    def test_retention_days_ordering(self):
        from compliance import RETENTION_POLICIES
        hc = RETENTION_POLICIES["Highly Confidential"]["retention_days"]
        c = RETENTION_POLICIES["Confidential"]["retention_days"]
        i = RETENTION_POLICIES["Internal"]["retention_days"]
        p = RETENTION_POLICIES["Public"]["retention_days"]
        # Higher sensitivity = shorter retention (data minimization)
        self.assertLess(hc, c)
        self.assertLess(c, i)
        self.assertLess(i, p)


class TestCompliantRegions(unittest.TestCase):
    """Tests for COMPLIANT_REGIONS configuration."""

    def test_highly_confidential_restricted(self):
        from compliance import COMPLIANT_REGIONS
        regions = COMPLIANT_REGIONS["Highly Confidential"]
        self.assertIsNotNone(regions)
        self.assertIsInstance(regions, list)
        self.assertGreater(len(regions), 0)

    def test_internal_any_region(self):
        from compliance import COMPLIANT_REGIONS
        self.assertIsNone(COMPLIANT_REGIONS["Internal"])

    def test_public_any_region(self):
        from compliance import COMPLIANT_REGIONS
        self.assertIsNone(COMPLIANT_REGIONS["Public"])


if __name__ == "__main__":
    unittest.main()
