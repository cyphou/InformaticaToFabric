"""
Phase 11 — Streaming & Real-Time (Sprints 80–82)
Tests for streaming detection, CDC patterns, Structured Streaming templates,
MERGE INTO generation, Change Data Feed, Eventstream definitions,
watermark/late arrival handling, and CDC validation.
"""

import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class TestStreamingDetection(unittest.TestCase):
    """Sprint 80.1: Detect streaming/real-time indicators in mapping XML."""

    def test_detect_kafka_source_iics(self):
        """IICS mapping with Kafka reader objectType should be flagged as streaming."""
        import xml.etree.ElementTree as ET
        from run_assessment import detect_streaming_indicators

        xml = """
        <dTemplate name="m_kafka_test" objectType="com.infa.deployment.mapping">
          <field name="transformations">
            <dTemplate name="src_events" objectType="com.infa.adapter.kafka.reader">
              <field name="topic" value="order-events"/>
            </dTemplate>
          </field>
        </dTemplate>
        """
        el = ET.fromstring(xml)
        result = detect_streaming_indicators(el)
        self.assertTrue(result["is_streaming"])
        self.assertEqual(len(result["streaming_sources"]), 1)
        self.assertEqual(result["streaming_sources"][0]["topic"], "order-events")
        self.assertTrue(any("kafka" in ind for ind in result["indicators"]))

    def test_detect_eventhub_source(self):
        """IICS mapping with EventHub reader should be flagged as streaming."""
        import xml.etree.ElementTree as ET
        from run_assessment import detect_streaming_indicators

        xml = """
        <dTemplate name="m_eh_test" objectType="com.infa.deployment.mapping">
          <field name="transformations">
            <dTemplate name="src_eh" objectType="com.infa.adapter.eventhub.reader">
              <field name="topic" value="sensor-data"/>
            </dTemplate>
          </field>
        </dTemplate>
        """
        el = ET.fromstring(xml)
        result = detect_streaming_indicators(el)
        self.assertTrue(result["is_streaming"])
        self.assertEqual(len(result["streaming_sources"]), 1)

    def test_detect_jms_source_powercenter(self):
        """PowerCenter SOURCE with JMS DATABASETYPE should flag streaming."""
        import xml.etree.ElementTree as ET
        from run_assessment import detect_streaming_indicators

        xml = """
        <MAPPING NAME="m_jms_test">
          <SOURCE NAME="SRC_JMS_QUEUE" DATABASETYPE="JMS"/>
        </MAPPING>
        """
        el = ET.fromstring(xml)
        result = detect_streaming_indicators(el)
        self.assertTrue(result["is_streaming"])
        self.assertTrue(len(result["streaming_sources"]) >= 1)

    def test_batch_mapping_not_streaming(self):
        """A batch mapping with no streaming indicators should not be flagged."""
        import xml.etree.ElementTree as ET
        from run_assessment import detect_streaming_indicators

        xml = """
        <MAPPING NAME="m_batch_load">
          <SOURCE NAME="SRC_TABLE" DATABASETYPE="Oracle"/>
          <TARGET NAME="TGT_TABLE" DATABASETYPE="Oracle"/>
        </MAPPING>
        """
        el = ET.fromstring(xml)
        result = detect_streaming_indicators(el)
        self.assertFalse(result["is_streaming"])
        self.assertEqual(len(result["streaming_sources"]), 0)

    def test_detect_streaming_sink(self):
        """Kafka writer objectType should show up as streaming sink."""
        import xml.etree.ElementTree as ET
        from run_assessment import detect_streaming_indicators

        xml = """
        <dTemplate name="m_sink_test" objectType="com.infa.deployment.mapping">
          <field name="transformations">
            <dTemplate name="tgt_kafka" objectType="com.infa.adapter.kafka.writer">
              <field name="topic" value="output-events"/>
            </dTemplate>
          </field>
        </dTemplate>
        """
        el = ET.fromstring(xml)
        result = detect_streaming_indicators(el)
        self.assertTrue(result["is_streaming"])
        self.assertEqual(len(result["streaming_sinks"]), 1)
        self.assertEqual(result["streaming_sinks"][0]["topic"], "output-events")


class TestCDCDetection(unittest.TestCase):
    """Sprint 81.1: Detect CDC patterns in mapping XML."""

    def test_detect_cdc_source(self):
        """IICS source with cdcEnabled=true should be detected as CDC."""
        import xml.etree.ElementTree as ET
        from run_assessment import detect_cdc_indicators

        xml = """
        <dTemplate name="m_cdc_test" objectType="com.infa.deployment.mapping">
          <field name="transformations">
            <dTemplate name="src_orders" objectType="com.infa.adapter.oracle.reader">
              <field name="cdcEnabled" value="true"/>
              <field name="cdcColumn" value="CDC_OPERATION"/>
              <field name="extractType" value="incremental"/>
            </dTemplate>
          </field>
        </dTemplate>
        """
        el = ET.fromstring(xml)
        mapping_info = {"transformations": [], "name": "m_cdc_test"}
        result = detect_cdc_indicators(el, mapping_info)
        self.assertTrue(result["is_cdc"])
        self.assertGreaterEqual(len(result["cdc_sources"]), 1)
        # Find the source with actual CDC column
        cdc_src = [s for s in result["cdc_sources"] if s["cdc_column"] == "CDC_OPERATION"]
        self.assertTrue(len(cdc_src) >= 1)
        self.assertEqual(cdc_src[0]["extract_type"], "incremental")

    def test_detect_full_cdc_with_update_strategy(self):
        """Update Strategy with DD_INSERT/DD_UPDATE/DD_DELETE → full_cdc."""
        import xml.etree.ElementTree as ET
        from run_assessment import detect_cdc_indicators

        xml = """
        <dTemplate name="m_full_cdc" objectType="com.infa.deployment.mapping">
          <field name="transformations">
            <dTemplate name="upd_merge" objectType="com.infa.adapter.updatestrategy">
              <field name="updateStrategy" value="DD_INSERT WHEN CDC_OPERATION = 'I' ELSE DD_UPDATE WHEN CDC_OPERATION = 'U' ELSE DD_DELETE"/>
            </dTemplate>
          </field>
        </dTemplate>
        """
        el = ET.fromstring(xml)
        mapping_info = {"transformations": ["UPD"], "name": "m_full_cdc"}
        result = detect_cdc_indicators(el, mapping_info)
        self.assertTrue(result["is_cdc"])
        self.assertEqual(result["cdc_type"], "full_cdc")
        self.assertTrue(result["update_strategy"]["has_insert"])
        self.assertTrue(result["update_strategy"]["has_update"])
        self.assertTrue(result["update_strategy"]["has_delete"])

    def test_detect_upsert_only(self):
        """Update Strategy with DD_INSERT + DD_UPDATE only → upsert_only."""
        import xml.etree.ElementTree as ET
        from run_assessment import detect_cdc_indicators

        xml = """
        <dTemplate name="m_upsert" objectType="com.infa.deployment.mapping">
          <field name="transformations">
            <dTemplate name="upd_upsert" objectType="com.infa.adapter.updatestrategy">
              <field name="updateStrategy" value="DD_INSERT WHEN OP = 'I' ELSE DD_UPDATE"/>
            </dTemplate>
          </field>
        </dTemplate>
        """
        el = ET.fromstring(xml)
        mapping_info = {"transformations": ["UPD"], "name": "m_upsert"}
        result = detect_cdc_indicators(el, mapping_info)
        self.assertTrue(result["is_cdc"])
        self.assertEqual(result["cdc_type"], "upsert_only")

    def test_detect_merge_keys(self):
        """mergeKeys field in target → merge_keys populated."""
        import xml.etree.ElementTree as ET
        from run_assessment import detect_cdc_indicators

        xml = """
        <dTemplate name="m_merge" objectType="com.infa.deployment.mapping">
          <field name="transformations">
            <dTemplate name="tgt_silver" objectType="com.infa.adapter.lakehouse.writer">
              <field name="mergeKeys" value="ORDER_ID,LINE_ITEM_ID"/>
            </dTemplate>
          </field>
        </dTemplate>
        """
        el = ET.fromstring(xml)
        mapping_info = {"transformations": [], "name": "m_merge"}
        result = detect_cdc_indicators(el, mapping_info)
        self.assertEqual(result["merge_keys"], ["ORDER_ID", "LINE_ITEM_ID"])

    def test_upd_transformation_implies_cdc(self):
        """A mapping with UPD transformation but no explicit CDC fields → upsert_only."""
        import xml.etree.ElementTree as ET
        from run_assessment import detect_cdc_indicators

        xml = '<MAPPING NAME="m_upd_basic"><TRANSFORMATION TYPE="Expression" NAME="EXP_1"/></MAPPING>'
        el = ET.fromstring(xml)
        mapping_info = {"transformations": ["UPD"], "name": "m_upd_basic"}
        result = detect_cdc_indicators(el, mapping_info)
        self.assertTrue(result["is_cdc"])
        self.assertEqual(result["cdc_type"], "upsert_only")

    def test_no_cdc_for_batch(self):
        """A plain batch mapping without UPD or CDC indicators → not CDC."""
        import xml.etree.ElementTree as ET
        from run_assessment import detect_cdc_indicators

        xml = '<MAPPING NAME="m_simple"><SOURCE NAME="SRC" DATABASETYPE="Oracle"/></MAPPING>'
        el = ET.fromstring(xml)
        mapping_info = {"transformations": ["SQ", "EXP", "FIL"], "name": "m_simple"}
        result = detect_cdc_indicators(el, mapping_info)
        self.assertFalse(result["is_cdc"])
        self.assertEqual(result["cdc_type"], "none")


class TestStreamingTemplates(unittest.TestCase):
    """Sprint 80.2-80.4: Streaming templates generate valid code."""

    def test_kafka_template_exists(self):
        """Kafka streaming template file should exist."""
        template = Path(__file__).parent.parent / "templates" / "streaming_kafka.py"
        self.assertTrue(template.exists())
        content = template.read_text(encoding="utf-8")
        self.assertIn("readStream", content)
        self.assertIn("kafka", content.lower())
        self.assertIn("writeStream", content)

    def test_eventhub_template_exists(self):
        """Event Hub streaming template file should exist."""
        template = Path(__file__).parent.parent / "templates" / "streaming_eventhub.py"
        self.assertTrue(template.exists())
        content = template.read_text(encoding="utf-8")
        self.assertIn("readStream", content)
        self.assertIn("eventhubs", content)

    def test_autoloader_template_exists(self):
        """Auto Loader streaming template file should exist."""
        template = Path(__file__).parent.parent / "templates" / "streaming_autoloader.py"
        self.assertTrue(template.exists())
        content = template.read_text(encoding="utf-8")
        self.assertIn("cloudFiles", content)
        self.assertIn("schemaEvolutionMode", content)

    def test_kafka_template_has_checkpoint(self):
        """Kafka template must include checkpoint configuration."""
        template = Path(__file__).parent.parent / "templates" / "streaming_kafka.py"
        content = template.read_text(encoding="utf-8")
        self.assertIn("checkpointLocation", content)

    def test_eventhub_template_has_encryption(self):
        """Event Hub template should encrypt connection string."""
        template = Path(__file__).parent.parent / "templates" / "streaming_eventhub.py"
        content = template.read_text(encoding="utf-8")
        self.assertIn("encrypt", content)

    def test_autoloader_template_has_schema_evolution(self):
        """Auto Loader template should support schema evolution."""
        template = Path(__file__).parent.parent / "templates" / "streaming_autoloader.py"
        content = template.read_text(encoding="utf-8")
        self.assertIn("addNewColumns", content)


class TestCDCNotebookGeneration(unittest.TestCase):
    """Sprint 81.2-81.3: CDC-aware notebook generation."""

    def test_cdc_merge_cell_full_cdc(self):
        """Full CDC mapping should generate MERGE with delete handling."""
        from run_notebook_migration import _cdc_merge_cell

        mapping = {
            "name": "m_cdc_orders",
            "targets": ["tgt_silver_orders"],
            "cdc": {
                "is_cdc": True,
                "cdc_type": "full_cdc",
                "cdc_sources": [{"name": "src", "cdc_column": "CDC_OP", "extract_type": "incremental"}],
                "update_strategy": {"expression": "DD_INSERT/DD_UPDATE/DD_DELETE", "has_insert": True, "has_update": True, "has_delete": True},
                "merge_keys": ["ORDER_ID", "LINE_ITEM_ID"],
                "indicators": [],
            },
        }

        result = _cdc_merge_cell(mapping, 5)
        self.assertIn("CDC MERGE INTO", result)
        self.assertIn("full_cdc", result)
        self.assertIn("ORDER_ID", result)
        self.assertIn("LINE_ITEM_ID", result)
        self.assertIn("df_deletes", result)
        self.assertIn("whenMatchedDelete", result)
        self.assertIn("CDC_OP", result)

    def test_cdc_merge_cell_upsert_only(self):
        """Upsert-only CDC should generate MERGE without delete clause."""
        from run_notebook_migration import _cdc_merge_cell

        mapping = {
            "name": "m_upsert",
            "targets": ["tgt_dim_customer"],
            "cdc": {
                "is_cdc": True,
                "cdc_type": "upsert_only",
                "cdc_sources": [],
                "update_strategy": {"expression": "DD_UPDATE", "has_insert": True, "has_update": True, "has_delete": False},
                "merge_keys": ["CUSTOMER_ID"],
                "indicators": [],
            },
        }

        result = _cdc_merge_cell(mapping, 3)
        self.assertIn("upsert_only", result)
        self.assertIn("CUSTOMER_ID", result)
        self.assertNotIn("df_deletes", result)
        self.assertNotIn("whenMatchedDelete", result)

    def test_change_feed_reader_cell(self):
        """Change Data Feed reader cell should include readChangeFeed option."""
        from run_notebook_migration import _change_feed_reader_cell

        mapping = {
            "name": "m_cdf_test",
            "targets": ["silver_orders"],
        }
        result = _change_feed_reader_cell(mapping, 7)
        self.assertIn("readChangeFeed", result)
        self.assertIn("readStream", result)
        self.assertIn("_change_type", result)
        self.assertIn("update_postimage", result)

    def test_streaming_source_cell_kafka(self):
        """Streaming source cell for Kafka should include readStream + kafka format."""
        from run_notebook_migration import _streaming_source_cell

        mapping = {
            "name": "m_kafka_stream",
            "streaming": {
                "is_streaming": True,
                "streaming_sources": [
                    {"name": "src_events", "type": "com.infa.adapter.kafka.reader", "topic": "order-events"}
                ],
                "streaming_sinks": [],
                "indicators": [],
            },
        }
        result = _streaming_source_cell(mapping, 2)
        self.assertIsNotNone(result)
        self.assertIn("readStream", result)
        self.assertIn("kafka", result.lower())
        self.assertIn("order-events", result)
        self.assertIn("from_json", result)

    def test_streaming_source_cell_eventhub(self):
        """Streaming source cell for Event Hub should use eventhubs format."""
        from run_notebook_migration import _streaming_source_cell

        mapping = {
            "name": "m_eh_stream",
            "streaming": {
                "is_streaming": True,
                "streaming_sources": [
                    {"name": "src_eh", "type": "com.infa.adapter.eventhub.reader", "topic": "sensors"}
                ],
                "streaming_sinks": [],
                "indicators": [],
            },
        }
        result = _streaming_source_cell(mapping, 2)
        self.assertIsNotNone(result)
        self.assertIn("readStream", result)
        self.assertIn("eventhubs", result)

    def test_streaming_source_cell_none_for_batch(self):
        """Batch mapping should return None for streaming source cell."""
        from run_notebook_migration import _streaming_source_cell

        mapping = {
            "name": "m_batch",
            "streaming": {
                "is_streaming": False,
                "streaming_sources": [],
                "streaming_sinks": [],
                "indicators": [],
            },
        }
        result = _streaming_source_cell(mapping, 2)
        self.assertIsNone(result)

    def test_streaming_sink_cell_append(self):
        """Non-CDC streaming sink should use append mode."""
        from run_notebook_migration import _streaming_sink_cell

        mapping = {
            "name": "m_stream_sink",
            "targets": ["bronze_events"],
            "cdc": {"is_cdc": False},
        }
        result = _streaming_sink_cell(mapping, 5)
        self.assertIn("writeStream", result)
        self.assertIn("append", result)
        self.assertIn("checkpointLocation", result)

    def test_streaming_sink_cell_cdc_foreachbatch(self):
        """CDC streaming sink should use foreachBatch for MERGE pattern."""
        from run_notebook_migration import _streaming_sink_cell

        mapping = {
            "name": "m_cdc_stream",
            "targets": ["silver_orders"],
            "cdc": {"is_cdc": True, "cdc_type": "full_cdc"},
        }
        result = _streaming_sink_cell(mapping, 5)
        self.assertIn("foreachBatch", result)
        self.assertIn("merge", result.lower())

    @patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "fabric"})
    def test_generate_notebook_streaming_mapping(self):
        """End-to-end: streaming mapping should produce streaming source+sink cells."""
        from run_notebook_migration import generate_notebook

        mapping = {
            "name": "m_realtime_events",
            "sources": ["src_kafka"],
            "targets": ["tgt_bronze_events"],
            "transformations": ["EXP"],
            "complexity": "Medium",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
            "streaming": {
                "is_streaming": True,
                "streaming_sources": [
                    {"name": "src_kafka", "type": "com.infa.adapter.kafka.reader", "topic": "events"}
                ],
                "streaming_sinks": [],
                "indicators": ["streaming_source:kafka"],
            },
            "cdc": {"is_cdc": False, "cdc_type": "none", "cdc_sources": [], "update_strategy": None, "merge_keys": [], "indicators": []},
        }
        content = generate_notebook(mapping)
        self.assertIn("readStream", content)
        self.assertIn("writeStream", content)
        self.assertIn("checkpointLocation", content)

    @patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "fabric"})
    def test_generate_notebook_cdc_mapping(self):
        """End-to-end: CDC mapping should produce MERGE cell instead of generic UPD."""
        from run_notebook_migration import generate_notebook

        mapping = {
            "name": "m_cdc_orders",
            "sources": ["src_orders"],
            "targets": ["tgt_silver_orders"],
            "transformations": ["EXP", "UPD"],
            "complexity": "Complex",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
            "streaming": {"is_streaming": False, "streaming_sources": [], "streaming_sinks": [], "indicators": []},
            "cdc": {
                "is_cdc": True,
                "cdc_type": "full_cdc",
                "cdc_sources": [{"name": "src", "cdc_column": "CDC_OP", "extract_type": "incremental"}],
                "update_strategy": {"expression": "DD_INSERT/DD_UPDATE/DD_DELETE", "has_insert": True, "has_update": True, "has_delete": True},
                "merge_keys": ["ORDER_ID"],
                "indicators": [],
            },
        }
        content = generate_notebook(mapping)
        self.assertIn("CDC MERGE INTO", content)
        self.assertIn("full_cdc", content)
        self.assertIn("ORDER_ID", content)
        self.assertIn("df_deletes", content)


class TestEventstreamGeneration(unittest.TestCase):
    """Sprint 80: Eventstream definition generation."""

    def test_generate_eventstream_kafka(self):
        """Kafka streaming mapping → Eventstream with CustomApp input."""
        from run_pipeline_migration import generate_eventstream

        mapping = {
            "name": "m_kafka_events",
            "sources": ["src_events"],
            "targets": ["tgt_bronze_events", "tgt_silver_processed"],
            "streaming": {
                "is_streaming": True,
                "streaming_sources": [
                    {"name": "src_events", "type": "com.infa.adapter.kafka.reader", "topic": "order-events"}
                ],
                "streaming_sinks": [],
                "indicators": [],
            },
            "cdc": {"is_cdc": False, "cdc_type": "none"},
        }

        es = generate_eventstream(mapping)
        self.assertEqual(es["name"], "ES_m_kafka_events")
        self.assertEqual(es["type"], "Microsoft.Fabric.Eventstream")
        self.assertEqual(len(es["properties"]["inputSources"]), 1)
        self.assertEqual(es["properties"]["inputSources"][0]["type"], "CustomApp")
        self.assertEqual(len(es["properties"]["destinations"]), 2)
        self.assertIn("MigratedFromInformatica", es["properties"]["annotations"])

    def test_generate_eventstream_eventhub(self):
        """Event Hub streaming mapping → Eventstream with EventHub input."""
        from run_pipeline_migration import generate_eventstream

        mapping = {
            "name": "m_eh_events",
            "sources": ["src_eh"],
            "targets": ["tgt_bronze_raw"],
            "streaming": {
                "is_streaming": True,
                "streaming_sources": [
                    {"name": "src_eh", "type": "com.infa.adapter.eventhub.reader", "topic": "sensor-data"}
                ],
                "streaming_sinks": [],
                "indicators": [],
            },
            "cdc": {"is_cdc": False, "cdc_type": "none"},
        }

        es = generate_eventstream(mapping)
        self.assertEqual(es["properties"]["inputSources"][0]["type"], "EventHub")

    def test_eventstream_cdc_filter(self):
        """CDC mapping → Eventstream with CDC filter transformation."""
        from run_pipeline_migration import generate_eventstream

        mapping = {
            "name": "m_cdc_stream",
            "sources": ["src_cdc"],
            "targets": ["tgt_silver"],
            "streaming": {"is_streaming": True, "streaming_sources": [], "streaming_sinks": [], "indicators": []},
            "cdc": {
                "is_cdc": True,
                "cdc_type": "full_cdc",
                "cdc_sources": [{"name": "src", "cdc_column": "OP_TYPE", "extract_type": "incremental"}],
                "update_strategy": None,
                "merge_keys": [],
                "indicators": [],
            },
        }

        es = generate_eventstream(mapping)
        # Should have a filter transformation for CDC ops
        filters = [t for t in es["properties"]["transformations"] if t["type"] == "Filter"]
        self.assertEqual(len(filters), 1)
        self.assertIn("OP_TYPE", filters[0]["properties"]["expression"])

    def test_eventstream_destination_tiers(self):
        """Eventstream destinations should be classified by tier based on naming."""
        from run_pipeline_migration import generate_eventstream

        mapping = {
            "name": "m_multi_tier",
            "sources": [],
            "targets": ["tgt_bronze_raw", "tgt_silver_clean", "tgt_gold_agg"],
            "streaming": {"is_streaming": True, "streaming_sources": [], "streaming_sinks": [], "indicators": []},
            "cdc": {"is_cdc": False, "cdc_type": "none"},
        }

        es = generate_eventstream(mapping)
        dests = es["properties"]["destinations"]
        self.assertEqual(len(dests), 3)
        # Verify tier classification
        tier_map = {d["properties"]["tableName"].split(".")[0]: d["name"] for d in dests}
        self.assertIn("bronze", tier_map)
        self.assertIn("silver", tier_map)
        self.assertIn("gold", tier_map)


class TestCDCValidation(unittest.TestCase):
    """Sprint 81.5: CDC-specific validation generation."""

    def test_cdc_validation_cell_full_cdc(self):
        """Full CDC validation should include orphan delete check."""
        from run_validation import generate_cdc_validation_cell

        mapping = {
            "name": "m_cdc_orders",
            "cdc": {
                "is_cdc": True,
                "cdc_type": "full_cdc",
                "cdc_sources": [{"name": "src", "cdc_column": "OP_CODE", "extract_type": "incremental"}],
                "merge_keys": ["ORDER_ID"],
                "indicators": [],
            },
        }
        result = generate_cdc_validation_cell(mapping)
        self.assertIn("Orphan Delete Check", result)
        self.assertIn("OP_CODE", result)
        self.assertIn("ORDER_ID", result)
        self.assertIn("merge key uniqueness", result.lower())

    def test_cdc_validation_cell_upsert(self):
        """Upsert-only CDC should not include orphan delete check."""
        from run_validation import generate_cdc_validation_cell

        mapping = {
            "name": "m_upsert",
            "cdc": {
                "is_cdc": True,
                "cdc_type": "upsert_only",
                "cdc_sources": [],
                "merge_keys": ["CUSTOMER_ID"],
                "indicators": [],
            },
        }
        result = generate_cdc_validation_cell(mapping)
        self.assertNotIn("Orphan Delete", result)
        self.assertIn("CUSTOMER_ID", result)

    def test_cdc_validation_cell_not_cdc(self):
        """Non-CDC mapping should return empty string."""
        from run_validation import generate_cdc_validation_cell

        mapping = {"name": "m_batch", "cdc": {"is_cdc": False}}
        result = generate_cdc_validation_cell(mapping)
        self.assertEqual(result, "")

    def test_cdc_validation_cdc_coverage_check(self):
        """CDC validation should check for unrecognized operation types."""
        from run_validation import generate_cdc_validation_cell

        mapping = {
            "name": "m_cdc",
            "cdc": {
                "is_cdc": True,
                "cdc_type": "full_cdc",
                "cdc_sources": [{"name": "src", "cdc_column": "CDC_OP", "extract_type": ""}],
                "merge_keys": [],
                "indicators": [],
            },
        }
        result = generate_cdc_validation_cell(mapping)
        self.assertIn("CDC Coverage", result)
        self.assertIn("unrecognized", result.lower())


class TestInventoryStreamingCDC(unittest.TestCase):
    """Integration: streaming/CDC data flows through to inventory.json."""

    def test_inventory_includes_streaming_summary(self):
        """Inventory summary should include streaming and CDC counts."""
        from run_assessment import write_inventory_json

        mappings = [
            {
                "name": "m_stream",
                "sources": ["src"],
                "targets": ["tgt"],
                "transformations": ["EXP"],
                "has_sql_override": False,
                "has_stored_proc": False,
                "complexity": "Simple",
                "sql_overrides": [],
                "lookup_conditions": [],
                "parameters": [],
                "target_load_order": [],
                "conversion_score": 100,
                "manual_effort_hours": 0.5,
                "lineage_summary": "",
                "field_lineage": [],
                "streaming": {"is_streaming": True, "streaming_sources": [{"name": "kafka_src", "type": "kafka", "topic": "t"}], "streaming_sinks": [], "indicators": ["streaming_source:kafka"]},
                "cdc": {"is_cdc": False, "cdc_type": "none", "cdc_sources": [], "update_strategy": None, "merge_keys": [], "indicators": []},
            },
            {
                "name": "m_cdc",
                "sources": ["src"],
                "targets": ["tgt"],
                "transformations": ["UPD"],
                "has_sql_override": False,
                "has_stored_proc": False,
                "complexity": "Complex",
                "sql_overrides": [],
                "lookup_conditions": [],
                "parameters": [],
                "target_load_order": [],
                "conversion_score": 100,
                "manual_effort_hours": 4,
                "lineage_summary": "",
                "field_lineage": [],
                "streaming": {"is_streaming": False, "streaming_sources": [], "streaming_sinks": [], "indicators": []},
                "cdc": {"is_cdc": True, "cdc_type": "full_cdc", "cdc_sources": [], "update_strategy": None, "merge_keys": [], "indicators": []},
            },
        ]
        workflows = []
        inventory = write_inventory_json(mappings, workflows, [], [])
        self.assertEqual(inventory["summary"]["total_streaming_mappings"], 1)
        self.assertEqual(inventory["summary"]["total_cdc_mappings"], 1)

    def test_inventory_mapping_has_streaming_cdc_keys(self):
        """Each mapping in inventory should carry streaming and cdc keys."""
        from run_assessment import write_inventory_json

        mappings = [{
            "name": "m_test",
            "sources": [],
            "targets": [],
            "transformations": [],
            "has_sql_override": False,
            "has_stored_proc": False,
            "complexity": "Simple",
            "sql_overrides": [],
            "lookup_conditions": [],
            "parameters": [],
            "target_load_order": [],
            "conversion_score": 100,
            "manual_effort_hours": 0.5,
            "lineage_summary": "",
            "field_lineage": [],
            "streaming": {"is_streaming": False, "streaming_sources": [], "streaming_sinks": [], "indicators": []},
            "cdc": {"is_cdc": False, "cdc_type": "none", "cdc_sources": [], "update_strategy": None, "merge_keys": [], "indicators": []},
        }]
        inventory = write_inventory_json(mappings, [], [], [])
        m = inventory["mappings"][0]
        self.assertIn("streaming", m)
        self.assertIn("cdc", m)
        self.assertFalse(m["streaming"]["is_streaming"])
        self.assertFalse(m["cdc"]["is_cdc"])


class TestWatermarkAndIdempotency(unittest.TestCase):
    """Sprint 82: Watermark, late arrival, idempotency patterns."""

    def test_kafka_template_has_watermark_placeholder(self):
        """Kafka template should have watermark configuration."""
        template = Path(__file__).parent.parent / "templates" / "streaming_kafka.py"
        content = template.read_text(encoding="utf-8")
        self.assertIn("withWatermark", content)
        self.assertIn("watermark_delay", content)

    def test_eventhub_template_has_watermark(self):
        """Event Hub template should have watermark configuration."""
        template = Path(__file__).parent.parent / "templates" / "streaming_eventhub.py"
        content = template.read_text(encoding="utf-8")
        self.assertIn("withWatermark", content)

    def test_kafka_template_has_trigger_interval(self):
        """Kafka template should have configurable trigger interval."""
        template = Path(__file__).parent.parent / "templates" / "streaming_kafka.py"
        content = template.read_text(encoding="utf-8")
        self.assertIn("trigger", content)
        self.assertIn("processingTime", content)

    def test_autoloader_has_available_now_trigger(self):
        """Auto Loader template should use availableNow trigger for one-shot."""
        template = Path(__file__).parent.parent / "templates" / "streaming_autoloader.py"
        content = template.read_text(encoding="utf-8")
        self.assertIn("availableNow", content)

    def test_streaming_sink_has_checkpoint(self):
        """All streaming sink patterns should include checkpoint directory."""
        from run_notebook_migration import _streaming_sink_cell
        mapping = {
            "name": "m_test",
            "targets": ["tgt_events"],
            "cdc": {"is_cdc": False},
        }
        result = _streaming_sink_cell(mapping, 5)
        self.assertIn("checkpointLocation", result)

    def test_autoloader_has_merge_schema(self):
        """Auto Loader should support schema evolution via mergeSchema."""
        template = Path(__file__).parent.parent / "templates" / "streaming_autoloader.py"
        content = template.read_text(encoding="utf-8")
        self.assertIn("mergeSchema", content)


class TestEndToEndAssessment(unittest.TestCase):
    """Integration: run assessment on CDC/streaming XML files."""

    def test_iics_cdc_mapping_detected(self):
        """Parse the IICS CDC order pipeline XML and verify streaming+CDC detection."""
        from run_assessment import parse_iics_mapping

        iics_file = Path(__file__).parent.parent / "input" / "mappings" / "IICS_M_CDC_ORDER_PIPELINE.xml"
        if not iics_file.exists():
            self.skipTest("IICS CDC mapping file not available")

        mappings = parse_iics_mapping(iics_file)
        self.assertTrue(len(mappings) > 0)
        m = mappings[0]
        self.assertIn("streaming", m)
        self.assertIn("cdc", m)
        # This mapping has CDC indicators (Update Strategy, cdcEnabled)
        self.assertTrue(m["cdc"]["is_cdc"])

    def test_iics_realtime_mapping_detected(self):
        """Parse the IICS realtime inventory XML and verify streaming detection."""
        from run_assessment import parse_iics_mapping

        iics_file = Path(__file__).parent.parent / "input" / "mappings" / "IICS_M_REALTIME_INVENTORY.xml"
        if not iics_file.exists():
            self.skipTest("IICS realtime mapping file not available")

        mappings = parse_iics_mapping(iics_file)
        self.assertTrue(len(mappings) > 0)
        m = mappings[0]
        self.assertIn("streaming", m)
        # This mapping has a Kafka reader source
        self.assertTrue(m["streaming"]["is_streaming"])


if __name__ == "__main__":
    unittest.main()
