"""
Phase 10 — Sprints 77–79 Tests

Sprint 77: Statistical Validation & SCD Testing
Sprint 78: Referential Integrity & A/B Testing
Sprint 79: Data Catalog Integration (Purview / Unity Catalog)
"""

import json
import os
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ═══════════════════════════════════════════════
#  Sprint 77 — Statistical Validation
# ═══════════════════════════════════════════════

from run_validation import (
    generate_statistical_validation_cell,
    generate_scd2_validation_cell,
    generate_null_distribution_cell,
)


class TestStatisticalValidation:
    """Sprint 77: Distribution comparison cell generation."""

    def test_generates_cell_with_threshold(self):
        cell = generate_statistical_validation_cell("silver.orders", threshold=0.1)
        assert "Statistical Validation" in cell
        assert "threshold" in cell
        assert "0.1" in cell

    def test_default_threshold_005(self):
        cell = generate_statistical_validation_cell("silver.orders")
        assert "0.05" in cell

    def test_contains_mean_stddev(self):
        cell = generate_statistical_validation_cell("silver.orders")
        assert "mean" in cell
        assert "stddev" in cell

    def test_contains_level_6_result(self):
        cell = generate_statistical_validation_cell("silver.orders")
        assert "'level': 6" in cell or '"level": 6' in cell

    def test_handles_numeric_cols_limit(self):
        cell = generate_statistical_validation_cell("gold.kpi")
        assert "numeric_cols[:10]" in cell  # Limited to first 10


class TestSCD2Validation:
    """Sprint 77: SCD Type 2 validation cell generation."""

    def test_generates_scd2_cell(self):
        cell = generate_scd2_validation_cell("customer_id")
        assert "SCD Type 2" in cell or "SCD2" in cell

    def test_checks_current_flag(self):
        cell = generate_scd2_validation_cell()
        assert "current_flag" in cell

    def test_checks_effective_date(self):
        cell = generate_scd2_validation_cell()
        assert "effective_date" in cell

    def test_checks_multiple_current_records(self):
        cell = generate_scd2_validation_cell()
        assert "multi_current" in cell or "multiple current" in cell

    def test_level_7(self):
        cell = generate_scd2_validation_cell()
        assert "'level': 7" in cell or "level 7" in cell.lower()


class TestNullDistribution:
    """Sprint 77: Null percentage comparison."""

    def test_generates_null_check_cell(self):
        cell = generate_null_distribution_cell()
        assert "Null Distribution" in cell

    def test_checks_percentage_threshold(self):
        cell = generate_null_distribution_cell()
        assert "1.0" in cell or "1%" in cell

    def test_level_8(self):
        cell = generate_null_distribution_cell()
        assert "'level': 8" in cell

    def test_compares_source_and_target(self):
        cell = generate_null_distribution_cell()
        assert "df_source" in cell
        assert "df_target" in cell


# ═══════════════════════════════════════════════
#  Sprint 78 — Referential Integrity & A/B Testing
# ═══════════════════════════════════════════════

from run_validation import (
    extract_fk_relationships,
    generate_ri_validation_cell,
    generate_ab_test_cell,
    generate_business_rules_cell,
)


class TestFKRelationshipExtraction:
    """Sprint 78: FK relationship extraction from mapping metadata."""

    def test_extract_from_lookup_conditions(self):
        mapping = {
            "targets": ["DIM_CUSTOMER"],
            "transformations": ["SQ", "LKP"],
            "lookup_conditions": [
                {"lookup": "DIM_PRODUCT", "condition": "product_id = product_id"},
            ],
        }
        rels = extract_fk_relationships(mapping)
        assert len(rels) >= 1
        assert rels[0]["parent_table"] == "DIM_PRODUCT"
        assert rels[0]["source"] == "LKP"

    def test_extract_from_jnr(self):
        mapping = {
            "targets": ["FACT_SALES"],
            "transformations": ["SQ", "JNR"],
            "lookup_conditions": [],
            "sources": ["ORDERS", "CUSTOMERS"],
        }
        rels = extract_fk_relationships(mapping)
        assert len(rels) >= 1
        assert rels[0]["source"] == "JNR"

    def test_no_relationships(self):
        mapping = {
            "targets": ["TARGET"],
            "transformations": ["SQ", "EXP"],
            "lookup_conditions": [],
            "sources": ["SOURCE"],
        }
        rels = extract_fk_relationships(mapping)
        assert len(rels) == 0


class TestRIValidation:
    """Sprint 78: Referential integrity validation cell."""

    def test_with_relationships(self):
        rels = [
            {"parent_table": "DIM_PRODUCT", "child_table": "FACT_SALES",
             "condition": "product_id = product_id", "source": "LKP"},
        ]
        cell = generate_ri_validation_cell(rels)
        assert "Referential Integrity" in cell
        assert "dim_product" in cell

    def test_without_relationships(self):
        cell = generate_ri_validation_cell([])
        assert "SKIPPED" in cell
        assert "No FK relationships" in cell

    def test_level_9(self):
        rels = [{"parent_table": "A", "child_table": "B", "condition": "x=y", "source": "LKP"}]
        cell = generate_ri_validation_cell(rels)
        assert "'level': 9" in cell


class TestABTestHarness:
    """Sprint 78: A/B test harness generation."""

    def test_generates_ab_cell(self):
        cell = generate_ab_test_cell()
        assert "A/B Test" in cell

    def test_compares_sorted_dataframes(self):
        cell = generate_ab_test_cell()
        assert "orderBy" in cell

    def test_row_by_row_comparison(self):
        cell = generate_ab_test_cell()
        assert "mismatches" in cell

    def test_level_10(self):
        cell = generate_ab_test_cell()
        assert "'level': 10" in cell

    def test_limits_sample_size(self):
        cell = generate_ab_test_cell()
        assert "1000" in cell


class TestBusinessRules:
    """Sprint 78: Custom business rules validation."""

    def test_no_rules_skipped(self):
        cell = generate_business_rules_cell()
        assert "SKIPPED" in cell
        assert "No custom rules" in cell

    def test_empty_rules_skipped(self):
        cell = generate_business_rules_cell([])
        assert "SKIPPED" in cell

    def test_with_custom_rules(self):
        rules = [{"column": "revenue", "condition": "> 0"}]
        cell = generate_business_rules_cell(rules)
        assert "revenue" in cell
        assert "> 0" in cell

    def test_level_11(self):
        rules = [{"column": "status", "condition": "IS NOT NULL"}]
        cell = generate_business_rules_cell(rules)
        assert "'level': 11" in cell

    def test_multiple_rules(self):
        rules = [
            {"column": "amount", "condition": "> 0"},
            {"column": "date", "condition": "IS NOT NULL"},
        ]
        cell = generate_business_rules_cell(rules)
        assert "amount" in cell
        assert "date" in cell


# ═══════════════════════════════════════════════
#  Sprint 79 — Data Catalog Integration
# ═══════════════════════════════════════════════

from catalog_integration import (
    generate_purview_entities,
    generate_unity_catalog_lineage,
    export_column_lineage,
    generate_impact_analysis,
)


class TestPurviewEntityGeneration:
    """Sprint 79: Purview-compatible metadata JSON."""

    def _inventory(self):
        return {
            "mappings": [
                {
                    "name": "M_LOAD_CUSTOMERS",
                    "sources": ["Oracle.HR.CUSTOMERS"],
                    "targets": ["DIM_CUSTOMER"],
                    "field_lineage": [],
                    "complexity": "Medium",
                },
            ]
        }

    def test_generates_entities(self):
        result = generate_purview_entities(self._inventory())
        entities = result["entities"]["entities"]
        assert len(entities) >= 2  # source + target

    def test_generates_lineage_relationships(self):
        result = generate_purview_entities(self._inventory())
        assert len(result["relationships"]) >= 1

    def test_entity_has_qualified_name(self):
        result = generate_purview_entities(self._inventory())
        entities = result["entities"]["entities"]
        assert all("qualifiedName" in e["attributes"] for e in entities)

    def test_metadata_has_counts(self):
        result = generate_purview_entities(self._inventory())
        assert "entity_count" in result["metadata"]
        assert "lineage_count" in result["metadata"]

    def test_empty_inventory(self):
        result = generate_purview_entities({"mappings": []})
        assert result["metadata"]["entity_count"] == 0

    def test_lineage_label_includes_mapping_name(self):
        result = generate_purview_entities(self._inventory())
        labels = [r.get("label", "") for r in result["relationships"]]
        assert any("M_LOAD_CUSTOMERS" in l for l in labels)


class TestUnityCatalogLineage:
    """Sprint 79: Unity Catalog lineage SQL generation."""

    def _inventory(self):
        return {
            "mappings": [
                {
                    "name": "M_LOAD_ORDERS",
                    "sources": ["Oracle.SALES.ORDERS"],
                    "targets": ["FACT_ORDERS"],
                    "complexity": "Complex",
                },
            ]
        }

    def test_generates_alter_table_tags(self):
        stmts = generate_unity_catalog_lineage(self._inventory())
        assert any("SET TAGS" in s for s in stmts)

    def test_includes_migration_source_tag(self):
        stmts = generate_unity_catalog_lineage(self._inventory())
        tag_stmts = [s for s in stmts if "SET TAGS" in s]
        assert any("informatica" in s for s in tag_stmts)

    def test_includes_comment_on_table(self):
        stmts = generate_unity_catalog_lineage(self._inventory())
        assert any("COMMENT ON TABLE" in s for s in stmts)

    def test_uses_catalog_prefix(self):
        os.environ["INFORMATICA_DATABRICKS_CATALOG"] = "test_cat"
        try:
            stmts = generate_unity_catalog_lineage(self._inventory())
            assert any("test_cat." in s for s in stmts)
        finally:
            os.environ.pop("INFORMATICA_DATABRICKS_CATALOG", None)

    def test_empty_inventory(self):
        stmts = generate_unity_catalog_lineage({"mappings": []})
        assert len(stmts) == 0


class TestColumnLineageExport:
    """Sprint 79: Column-level lineage export."""

    def test_exports_field_lineage(self):
        inv = {
            "mappings": [
                {
                    "name": "M_LOAD_CUSTOMERS",
                    "field_lineage": [
                        {
                            "source_instance": "SQ_CUSTOMERS",
                            "source_field": "CUST_NAME",
                            "target_instance": "TGT_DIM_CUSTOMER",
                            "target_field": "customer_name",
                            "transforms": ["EXP"],
                        },
                    ],
                },
            ]
        }
        entries = export_column_lineage(inv)
        assert len(entries) == 1
        assert entries[0]["source_column"] == "CUST_NAME"
        assert entries[0]["target_column"] == "customer_name"

    def test_empty_lineage(self):
        inv = {"mappings": [{"name": "M_X", "field_lineage": []}]}
        entries = export_column_lineage(inv)
        assert len(entries) == 0


class TestImpactAnalysis:
    """Sprint 79: Impact analysis for source tables."""

    def _inventory(self):
        return {
            "mappings": [
                {
                    "name": "M_LOAD_CUSTOMERS",
                    "sources": ["Oracle.HR.CUSTOMERS"],
                    "targets": ["DIM_CUSTOMER"],
                },
                {
                    "name": "M_LOAD_ORDERS",
                    "sources": ["Oracle.SALES.ORDERS", "Oracle.HR.CUSTOMERS"],
                    "targets": ["FACT_ORDERS"],
                },
            ]
        }

    def test_finds_affected_mappings(self):
        report = generate_impact_analysis(self._inventory(), "CUSTOMERS")
        assert "M_LOAD_CUSTOMERS" in report
        assert "M_LOAD_ORDERS" in report

    def test_finds_downstream_targets(self):
        report = generate_impact_analysis(self._inventory(), "CUSTOMERS")
        assert "DIM_CUSTOMER" in report
        assert "FACT_ORDERS" in report

    def test_no_matches(self):
        report = generate_impact_analysis(self._inventory(), "NONEXISTENT_TABLE")
        assert "No mappings found" in report

    def test_case_insensitive(self):
        report = generate_impact_analysis(self._inventory(), "customers")
        assert "M_LOAD_CUSTOMERS" in report

    def test_has_summary_section(self):
        report = generate_impact_analysis(self._inventory(), "CUSTOMERS")
        assert "## Summary" in report
        assert "Affected mappings:" in report
