"""
Sprint 25 — Lineage & Conversion Scoring
Tests for: extract_field_lineage, calculate_conversion_score, estimate_manual_effort,
generate_lineage_mermaid, write_lineage_json, inventory enrichment.
"""

import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from run_assessment import (
    AUTO_CONVERTIBLE_TX,
    PLACEHOLDER_TX,
    calculate_conversion_score,
    estimate_manual_effort,
    extract_field_lineage,
    generate_lineage_mermaid,
    write_lineage_json,
    OUTPUT_DIR,
)


# ═══════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════

def _make_mapping_el(connectors):
    """Build an XML mapping element with given CONNECTOR defs."""
    root = ET.Element("MAPPING", NAME="TEST_MAP")
    for c in connectors:
        ET.SubElement(root, "CONNECTOR", **c)
    return root


def _make_mapping_dict(**overrides):
    """Build a mapping dict with defaults."""
    base = {
        "name": "M_TEST",
        "sources": ["SRC_TABLE"],
        "targets": ["TGT_TABLE"],
        "transformations": ["SQ", "EXP", "FIL"],
        "has_sql_override": False,
        "has_stored_proc": False,
        "complexity": "Simple",
        "sql_overrides": [],
        "lookup_conditions": [],
        "parameters": [],
        "target_load_order": [],
        "field_lineage": [],
    }
    base.update(overrides)
    return base


# ═══════════════════════════════════════════════
#  extract_field_lineage
# ═══════════════════════════════════════════════


class TestExtractFieldLineage:
    """Tests for extract_field_lineage()."""

    def test_simple_source_to_target(self):
        """SQ -> TGT direct connection traces one lineage path."""
        el = _make_mapping_el([
            {"FROMINSTANCE": "SQ_CUSTOMERS", "FROMFIELD": "CUST_ID",
             "TOINSTANCE": "TGT_CUSTOMERS", "TOFIELD": "CUSTOMER_ID",
             "FROMINSTANCETYPE": "Source Qualifier", "TOINSTANCETYPE": "Target Definition"},
        ])
        lineage = extract_field_lineage(el)
        assert len(lineage) == 1
        assert lineage[0]["source_field"] == "CUST_ID"
        assert lineage[0]["target_field"] == "CUSTOMER_ID"
        assert lineage[0]["source_instance"] == "SQ_CUSTOMERS"
        assert lineage[0]["target_instance"] == "TGT_CUSTOMERS"

    def test_through_expression(self):
        """SQ -> EXP -> TGT should trace through transformation."""
        el = _make_mapping_el([
            {"FROMINSTANCE": "SQ_SRC", "FROMFIELD": "ID",
             "TOINSTANCE": "EXP_CALC", "TOFIELD": "ID_IN",
             "FROMINSTANCETYPE": "Source Qualifier", "TOINSTANCETYPE": "Expression"},
            {"FROMINSTANCE": "EXP_CALC", "FROMFIELD": "ID_OUT",
             "TOINSTANCE": "TGT_DST", "TOFIELD": "CALC_ID",
             "FROMINSTANCETYPE": "Expression", "TOINSTANCETYPE": "Target Definition"},
        ])
        lineage = extract_field_lineage(el)
        assert len(lineage) == 1
        assert lineage[0]["source_field"] == "ID"
        assert lineage[0]["target_field"] == "CALC_ID"
        assert len(lineage[0]["transformations"]) == 1
        assert lineage[0]["transformations"][0]["instance"] == "EXP_CALC"

    def test_multi_hop_chain(self):
        """SQ -> EXP -> FIL -> TGT traces full chain."""
        el = _make_mapping_el([
            {"FROMINSTANCE": "SQ_SRC", "FROMFIELD": "AMT",
             "TOINSTANCE": "EXP_CONV", "TOFIELD": "AMT_IN",
             "FROMINSTANCETYPE": "Source Qualifier", "TOINSTANCETYPE": "Expression"},
            {"FROMINSTANCE": "EXP_CONV", "FROMFIELD": "AMT_OUT",
             "TOINSTANCE": "FIL_VALID", "TOFIELD": "AMT_CHK",
             "FROMINSTANCETYPE": "Expression", "TOINSTANCETYPE": "Filter"},
            {"FROMINSTANCE": "FIL_VALID", "FROMFIELD": "AMT_CHK",
             "TOINSTANCE": "TGT_OUT", "TOFIELD": "AMOUNT",
             "FROMINSTANCETYPE": "Filter", "TOINSTANCETYPE": "Target Definition"},
        ])
        lineage = extract_field_lineage(el)
        assert len(lineage) == 1
        assert lineage[0]["source_instance"] == "SQ_SRC"
        assert lineage[0]["target_instance"] == "TGT_OUT"
        tx_names = [t["instance"] for t in lineage[0]["transformations"]]
        assert "EXP_CONV" in tx_names
        assert "FIL_VALID" in tx_names

    def test_multiple_fields(self):
        """Two separate fields traced independently."""
        el = _make_mapping_el([
            {"FROMINSTANCE": "SQ_SRC", "FROMFIELD": "F1",
             "TOINSTANCE": "TGT_OUT", "TOFIELD": "F1_OUT",
             "FROMINSTANCETYPE": "Source Qualifier", "TOINSTANCETYPE": "Target Definition"},
            {"FROMINSTANCE": "SQ_SRC", "FROMFIELD": "F2",
             "TOINSTANCE": "TGT_OUT", "TOFIELD": "F2_OUT",
             "FROMINSTANCETYPE": "Source Qualifier", "TOINSTANCETYPE": "Target Definition"},
        ])
        lineage = extract_field_lineage(el)
        assert len(lineage) == 2
        fields = {e["source_field"] for e in lineage}
        assert fields == {"F1", "F2"}

    def test_no_connectors(self):
        """Empty mapping returns empty lineage."""
        el = ET.Element("MAPPING", NAME="EMPTY")
        lineage = extract_field_lineage(el)
        assert lineage == []

    def test_no_source_qualifier(self):
        """Connectors without Source Qualifier type produce no lineage."""
        el = _make_mapping_el([
            {"FROMINSTANCE": "EXP_X", "FROMFIELD": "A",
             "TOINSTANCE": "TGT_Y", "TOFIELD": "B",
             "FROMINSTANCETYPE": "Expression", "TOINSTANCETYPE": "Target Definition"},
        ])
        lineage = extract_field_lineage(el)
        assert lineage == []

    def test_branching_to_multiple_targets(self):
        """SQ -> two different targets."""
        el = _make_mapping_el([
            {"FROMINSTANCE": "SQ_SRC", "FROMFIELD": "ID",
             "TOINSTANCE": "TGT_A", "TOFIELD": "ID_A",
             "FROMINSTANCETYPE": "Source Qualifier", "TOINSTANCETYPE": "Target Definition"},
            {"FROMINSTANCE": "SQ_SRC", "FROMFIELD": "ID",
             "TOINSTANCE": "TGT_B", "TOFIELD": "ID_B",
             "FROMINSTANCETYPE": "Source Qualifier", "TOINSTANCETYPE": "Target Definition"},
        ])
        lineage = extract_field_lineage(el)
        assert len(lineage) == 2
        targets = {e["target_instance"] for e in lineage}
        assert targets == {"TGT_A", "TGT_B"}


# ═══════════════════════════════════════════════
#  calculate_conversion_score
# ═══════════════════════════════════════════════


class TestCalculateConversionScore:
    """Tests for calculate_conversion_score()."""

    def test_all_auto_convertible(self):
        """All auto-convertible TX => 100."""
        m = _make_mapping_dict(transformations=["SQ", "EXP", "FIL", "AGG"])
        assert calculate_conversion_score(m) == 100

    def test_all_placeholder(self):
        """All placeholder TX => low score."""
        m = _make_mapping_dict(transformations=["JTX", "CT"])
        score = calculate_conversion_score(m)
        assert score <= 30  # 0% auto (50w) + 100% sql (30w) + 0% gap (20w) = 30

    def test_mixed_tx(self):
        """Mix of auto + placeholder => partial score."""
        m = _make_mapping_dict(transformations=["SQ", "EXP", "JTX"])
        score = calculate_conversion_score(m)
        assert 30 < score < 90

    def test_no_transformations(self):
        """No transformations => 100 (nothing to convert)."""
        m = _make_mapping_dict(transformations=[])
        assert calculate_conversion_score(m) == 100

    def test_unconvertible_sql_overrides(self):
        """SQL overrides with CONNECT BY reduce score."""
        m = _make_mapping_dict(
            transformations=["SQ", "EXP"],
            sql_overrides=[{"type": "override", "value": "SELECT * FROM t CONNECT BY PRIOR id = parent_id"}],
        )
        score = calculate_conversion_score(m)
        assert score < 100

    def test_simple_sql_overrides(self):
        """SQL overrides without unsupported patterns => full SQL score."""
        m = _make_mapping_dict(
            transformations=["SQ", "EXP"],
            sql_overrides=[{"type": "override", "value": "SELECT id, name FROM customers WHERE active = 1"}],
        )
        assert calculate_conversion_score(m) == 100

    def test_score_range(self):
        """Score is always between 0 and 100."""
        for tx in [["SQ"], ["JTX"], ["SQ", "JTX", "CT", "HTTP"], []]:
            m = _make_mapping_dict(transformations=tx)
            score = calculate_conversion_score(m)
            assert 0 <= score <= 100

    def test_package_body_penalty(self):
        """PACKAGE BODY in SQL override reduces score."""
        m = _make_mapping_dict(
            transformations=["SQ"],
            sql_overrides=[{"type": "override", "value": "CREATE PACKAGE BODY pkg AS ..."}],
        )
        assert calculate_conversion_score(m) < 100

    def test_dbms_utl_penalty(self):
        """DBMS_ and UTL_ references reduce score."""
        m = _make_mapping_dict(
            transformations=["SQ", "EXP"],
            sql_overrides=[{"type": "override", "value": "DBMS_OUTPUT.PUT_LINE('test')"}],
        )
        assert calculate_conversion_score(m) < 100


# ═══════════════════════════════════════════════
#  estimate_manual_effort
# ═══════════════════════════════════════════════


class TestEstimateManualEffort:
    """Tests for estimate_manual_effort()."""

    def test_simple_mapping(self):
        """Simple mapping with auto TX => minimal effort."""
        m = _make_mapping_dict(complexity="Simple", transformations=["SQ", "EXP"])
        effort = estimate_manual_effort(m)
        assert effort == 0.5

    def test_custom_mapping(self):
        """Custom complexity => 8h base."""
        m = _make_mapping_dict(complexity="Custom", transformations=["SQ", "JTX"])
        effort = estimate_manual_effort(m)
        assert effort >= 8  # 8 base + 2 for JTX placeholder

    def test_placeholder_adds_hours(self):
        """Each placeholder adds 2 hours."""
        m = _make_mapping_dict(complexity="Simple", transformations=["SQ", "JTX", "CT"])
        effort = estimate_manual_effort(m)
        assert effort == 4.5  # 0.5 + 2*2

    def test_connect_by_sql_adds_hours(self):
        """CONNECT BY in SQL override adds 3 hours."""
        m = _make_mapping_dict(
            complexity="Simple",
            transformations=["SQ"],
            sql_overrides=[{"type": "override", "value": "SELECT * CONNECT BY PRIOR id = pid"}],
        )
        effort = estimate_manual_effort(m)
        assert effort == 3.5  # 0.5 + 3

    def test_merge_sql_adds_hours(self):
        """MERGE in SQL override adds 0.5 hours."""
        m = _make_mapping_dict(
            complexity="Simple",
            transformations=["SQ"],
            sql_overrides=[{"type": "override", "value": "MERGE INTO tgt USING src ON ..."}],
        )
        effort = estimate_manual_effort(m)
        assert effort == 1.0  # 0.5 + 0.5

    def test_medium_complexity_base(self):
        """Medium complexity has 2h base."""
        m = _make_mapping_dict(complexity="Medium")
        assert estimate_manual_effort(m) == 2.0

    def test_complex_complexity_base(self):
        """Complex complexity has 4h base."""
        m = _make_mapping_dict(complexity="Complex")
        assert estimate_manual_effort(m) == 4.0


# ═══════════════════════════════════════════════
#  generate_lineage_mermaid
# ═══════════════════════════════════════════════


class TestGenerateLineageMermaid:
    """Tests for generate_lineage_mermaid()."""

    def test_empty_lineage(self):
        """Empty lineage returns empty string."""
        assert generate_lineage_mermaid("TEST", []) == ""

    def test_produces_mermaid_block(self):
        """Non-empty lineage produces fenced mermaid block."""
        entries = [{
            "source_field": "ID",
            "source_instance": "SQ_SRC",
            "target_field": "ID_OUT",
            "target_instance": "TGT_DST",
            "transformations": [],
        }]
        result = generate_lineage_mermaid("M_TEST", entries)
        assert result.startswith("```mermaid")
        assert result.endswith("```")
        assert "flowchart LR" in result

    def test_contains_source_and_target_nodes(self):
        """Mermaid output has source and target node labels."""
        entries = [{
            "source_field": "A",
            "source_instance": "SQ_INPUT",
            "target_field": "B",
            "target_instance": "TGT_OUTPUT",
            "transformations": [{"instance": "EXP_CALC", "type": "Expression"}],
        }]
        result = generate_lineage_mermaid("M_TEST", entries)
        assert "SQ_INPUT" in result
        assert "TGT_OUTPUT" in result
        assert "EXP_CALC" in result

    def test_edge_between_nodes(self):
        """Arrow notation present in output."""
        entries = [{
            "source_field": "X",
            "source_instance": "SQ_A",
            "target_field": "Y",
            "target_instance": "TGT_B",
            "transformations": [],
        }]
        result = generate_lineage_mermaid("M_X", entries)
        assert "-->" in result

    def test_special_chars_sanitized(self):
        """Instance names with spaces/special chars get sanitized node IDs."""
        entries = [{
            "source_field": "X",
            "source_instance": "SQ Source 1",
            "target_field": "Y",
            "target_instance": "TGT-Dest.2",
            "transformations": [],
        }]
        result = generate_lineage_mermaid("M_SPECIAL", entries)
        # Node IDs should not contain spaces or special chars
        assert "SQ_Source_1" in result
        assert "TGT_Dest_2" in result


# ═══════════════════════════════════════════════
#  write_lineage_json
# ═══════════════════════════════════════════════


class TestWriteLineageJson:
    """Tests for write_lineage_json()."""

    def test_writes_file(self, tmp_path, monkeypatch):
        """write_lineage_json creates lineage.json."""
        monkeypatch.setattr("run_assessment.OUTPUT_DIR", tmp_path)
        mappings = [_make_mapping_dict(
            name="M_ALPHA",
            conversion_score=85,
            manual_effort_hours=1.5,
            field_lineage=[{
                "source_field": "A",
                "source_instance": "SQ_X",
                "target_field": "B",
                "target_instance": "TGT_Y",
                "transformations": [],
            }],
        )]
        result = write_lineage_json(mappings)
        out_file = tmp_path / "lineage.json"
        assert out_file.exists()
        data = json.loads(out_file.read_text(encoding="utf-8"))
        assert len(data) == 1
        assert data[0]["mapping"] == "M_ALPHA"
        assert data[0]["conversion_score"] == 85
        assert data[0]["manual_effort_hours"] == 1.5
        assert len(data[0]["field_lineage"]) == 1
        assert "mermaid_diagram" in data[0]

    def test_empty_mappings(self, tmp_path, monkeypatch):
        """Empty mappings list produces empty JSON array."""
        monkeypatch.setattr("run_assessment.OUTPUT_DIR", tmp_path)
        result = write_lineage_json([])
        data = json.loads((tmp_path / "lineage.json").read_text(encoding="utf-8"))
        assert data == []


# ═══════════════════════════════════════════════
#  Constants validation
# ═══════════════════════════════════════════════


class TestConstantSets:
    """Validate AUTO_CONVERTIBLE_TX and PLACEHOLDER_TX sets."""

    def test_no_overlap(self):
        """Auto and placeholder sets must not overlap."""
        assert AUTO_CONVERTIBLE_TX & PLACEHOLDER_TX == set()

    def test_auto_has_core_types(self):
        """Core transformation types present in AUTO_CONVERTIBLE_TX."""
        for tx in ["SQ", "EXP", "FIL", "AGG", "JNR", "LKP"]:
            assert tx in AUTO_CONVERTIBLE_TX

    def test_placeholder_has_java(self):
        """Java/Custom TX types in PLACEHOLDER_TX."""
        assert "JTX" in PLACEHOLDER_TX
        assert "CT" in PLACEHOLDER_TX


# ═══════════════════════════════════════════════
#  Integration: parse_mapping_xml enrichment
# ═══════════════════════════════════════════════


class TestMappingEnrichment:
    """Tests that parse_mapping_xml populates Sprint 25 fields."""

    def test_mapping_has_sprint25_fields(self, tmp_path):
        """Parsed mapping should have conversion_score, manual_effort_hours, lineage_summary."""
        xml = '''<?xml version="1.0" encoding="UTF-8"?>
<POWERMART>
  <REPOSITORY>
    <FOLDER NAME="TEST">
      <MAPPING NAME="M_ENRICHED" ISVALID="YES">
        <TRANSFORMATION NAME="SQ_SRC" TYPE="Source Qualifier"/>
        <TRANSFORMATION NAME="EXP_CALC" TYPE="Expression"/>
        <TRANSFORMATION NAME="TGT_OUT" TYPE="Target Definition"/>
        <CONNECTOR FROMINSTANCE="SQ_SRC" FROMFIELD="ID" TOINSTANCE="EXP_CALC" TOFIELD="ID_IN"
                   FROMINSTANCETYPE="Source Qualifier" TOINSTANCETYPE="Expression"/>
        <CONNECTOR FROMINSTANCE="EXP_CALC" FROMFIELD="ID_OUT" TOINSTANCE="TGT_OUT" TOFIELD="CUSTOMER_ID"
                   FROMINSTANCETYPE="Expression" TOINSTANCETYPE="Target Definition"/>
      </MAPPING>
    </FOLDER>
  </REPOSITORY>
</POWERMART>'''
        f = tmp_path / "enriched.xml"
        f.write_text(xml)
        from run_assessment import parse_mapping_xml
        mappings = parse_mapping_xml(f)
        assert len(mappings) == 1
        m = mappings[0]
        assert "conversion_score" in m
        assert "manual_effort_hours" in m
        assert "lineage_summary" in m
        assert "field_lineage" in m
        assert isinstance(m["conversion_score"], int)
        assert isinstance(m["manual_effort_hours"], (int, float))
        assert isinstance(m["field_lineage"], list)
        assert m["conversion_score"] > 0

    def test_mapping_lineage_traced(self, tmp_path):
        """Parsed mapping has field lineage tracing SQ -> EXP -> TGT."""
        xml = '''<?xml version="1.0" encoding="UTF-8"?>
<POWERMART>
  <REPOSITORY>
    <FOLDER NAME="TEST">
      <MAPPING NAME="M_TRACED" ISVALID="YES">
        <TRANSFORMATION NAME="SQ_SRC" TYPE="Source Qualifier"/>
        <TRANSFORMATION NAME="EXP_X" TYPE="Expression"/>
        <TRANSFORMATION NAME="TGT_DST" TYPE="Target Definition"/>
        <CONNECTOR FROMINSTANCE="SQ_SRC" FROMFIELD="AMT" TOINSTANCE="EXP_X" TOFIELD="AMT_IN"
                   FROMINSTANCETYPE="Source Qualifier" TOINSTANCETYPE="Expression"/>
        <CONNECTOR FROMINSTANCE="EXP_X" FROMFIELD="AMT_OUT" TOINSTANCE="TGT_DST" TOFIELD="AMOUNT"
                   FROMINSTANCETYPE="Expression" TOINSTANCETYPE="Target Definition"/>
      </MAPPING>
    </FOLDER>
  </REPOSITORY>
</POWERMART>'''
        f = tmp_path / "traced.xml"
        f.write_text(xml)
        from run_assessment import parse_mapping_xml
        mappings = parse_mapping_xml(f)
        m = mappings[0]
        assert len(m["field_lineage"]) >= 1
        entry = m["field_lineage"][0]
        assert entry["source_field"] == "AMT"
        assert entry["target_field"] == "AMOUNT"
