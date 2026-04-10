"""Tests for Azure Functions migration — detection, generation, and integration."""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from run_assessment import detect_functions_candidate
from run_functions_migration import (
    generate_service_bus_function,
    generate_event_hub_function,
    generate_sql_trigger_function,
    generate_cosmos_trigger_function,
    generate_http_function,
    generate_timer_function,
    generate_blob_trigger_function,
    generate_function,
    generate_host_json,
    generate_local_settings,
    generate_requirements_txt,
    generate_function_app_entry,
)
from run_pipeline_migration import _azure_function_activity


# =====================================================================
# Test: Functions Candidate Detection
# =====================================================================

class TestFunctionsCandidateDetection(unittest.TestCase):
    """Test detect_functions_candidate() in run_assessment.py."""

    def test_service_bus_source_detected(self):
        """Mapping with Service Bus streaming source → service_bus trigger."""
        mapping_info = {
            "name": "M_ESB_ORDERS",
            "sources": ["ORDER_QUEUE"],
            "targets": ["STG_ORDERS"],
            "transformations": ["SQ", "EXP"],
        }
        streaming_info = {
            "is_streaming": True,
            "streaming_sources": [
                {"name": "SB_READER", "type": "com.infa.adapter.servicebus.reader", "topic": "orders-queue"}
            ],
            "streaming_sinks": [],
            "indicators": ["streaming_source:servicebus"],
        }
        cdc_info = {"is_cdc": False, "cdc_type": "none"}

        result = detect_functions_candidate(mapping_info, streaming_info, cdc_info)
        self.assertTrue(result["is_candidate"])
        self.assertEqual(result["trigger_type"], "service_bus")
        self.assertIn("queue_source", result["indicators"][0])

    def test_jms_source_detected(self):
        """Mapping with JMS streaming source → service_bus trigger."""
        mapping_info = {
            "name": "M_JMS_EVENTS",
            "sources": ["JMS_TOPIC"],
            "targets": ["STG_EVENTS"],
            "transformations": ["SQ", "EXP"],
        }
        streaming_info = {
            "is_streaming": True,
            "streaming_sources": [
                {"name": "JMS_READER", "type": "jms", "topic": "events"}
            ],
            "streaming_sinks": [],
            "indicators": [],
        }
        cdc_info = {"is_cdc": False, "cdc_type": "none"}

        result = detect_functions_candidate(mapping_info, streaming_info, cdc_info)
        self.assertTrue(result["is_candidate"])
        self.assertEqual(result["trigger_type"], "service_bus")

    def test_rabbitmq_source_detected(self):
        """Mapping with RabbitMQ source → service_bus trigger."""
        mapping_info = {
            "name": "M_RABBIT_PROCESSOR",
            "sources": ["RABBIT_QUEUE"],
            "targets": ["TARGET"],
            "transformations": ["SQ"],
        }
        streaming_info = {
            "is_streaming": True,
            "streaming_sources": [
                {"name": "RABBIT", "type": "rabbitmq", "topic": ""}
            ],
            "streaming_sinks": [],
            "indicators": [],
        }
        cdc_info = {"is_cdc": False, "cdc_type": "none"}

        result = detect_functions_candidate(mapping_info, streaming_info, cdc_info)
        self.assertTrue(result["is_candidate"])
        self.assertEqual(result["trigger_type"], "service_bus")

    def test_simple_eventhub_detected(self):
        """Simple Event Hub consumer (few transforms) → event_hub trigger."""
        mapping_info = {
            "name": "M_EH_SIMPLE",
            "sources": ["EH_TOPIC"],
            "targets": ["STG_EVENTS"],
            "transformations": ["SQ", "EXP"],
        }
        streaming_info = {
            "is_streaming": True,
            "streaming_sources": [
                {"name": "EH_READER", "type": "eventhub", "topic": "iot-events"}
            ],
            "streaming_sinks": [],
            "indicators": [],
        }
        cdc_info = {"is_cdc": False, "cdc_type": "none"}

        result = detect_functions_candidate(mapping_info, streaming_info, cdc_info)
        self.assertTrue(result["is_candidate"])
        self.assertEqual(result["trigger_type"], "event_hub")

    def test_complex_eventhub_not_candidate(self):
        """Complex Event Hub consumer (many transforms) → NOT a Functions candidate."""
        mapping_info = {
            "name": "M_EH_COMPLEX",
            "sources": ["EH_TOPIC"],
            "targets": ["STG_EVENTS"],
            "transformations": ["SQ", "EXP", "LKP", "AGG", "FIL", "RTR"],
        }
        streaming_info = {
            "is_streaming": True,
            "streaming_sources": [
                {"name": "EH_READER", "type": "eventhub", "topic": "complex-events"}
            ],
            "streaming_sinks": [],
            "indicators": [],
        }
        cdc_info = {"is_cdc": False, "cdc_type": "none"}

        result = detect_functions_candidate(mapping_info, streaming_info, cdc_info)
        self.assertFalse(result["is_candidate"])

    def test_simple_cdc_sql_trigger(self):
        """Simple single-table CDC → sql_trigger."""
        mapping_info = {
            "name": "M_CDC_CUSTOMER",
            "sources": ["CUSTOMER"],
            "targets": ["STG_CUSTOMER"],
            "transformations": ["SQ", "UPD"],
        }
        streaming_info = {
            "is_streaming": False,
            "streaming_sources": [],
            "streaming_sinks": [],
            "indicators": [],
        }
        cdc_info = {
            "is_cdc": True,
            "cdc_type": "upsert_only",
            "cdc_sources": [{"name": "CUSTOMER", "cdc_column": "op_type"}],
            "merge_keys": ["customer_id"],
        }

        result = detect_functions_candidate(mapping_info, streaming_info, cdc_info)
        self.assertTrue(result["is_candidate"])
        self.assertEqual(result["trigger_type"], "sql_trigger")

    def test_cosmos_cdc_trigger(self):
        """Cosmos DB CDC → cosmos_trigger."""
        mapping_info = {
            "name": "M_CDC_COSMOS_ORDERS",
            "sources": ["cosmosdb_orders"],
            "targets": ["STG_ORDERS"],
            "transformations": ["SQ", "EXP"],
        }
        streaming_info = {
            "is_streaming": False, "streaming_sources": [], "streaming_sinks": [], "indicators": [],
        }
        cdc_info = {
            "is_cdc": True, "cdc_type": "upsert_only",
            "cdc_sources": [{"name": "cosmos_orders", "cdc_column": ""}],
            "merge_keys": ["order_id"],
        }

        result = detect_functions_candidate(mapping_info, streaming_info, cdc_info)
        self.assertTrue(result["is_candidate"])
        self.assertEqual(result["trigger_type"], "cosmos_trigger")

    def test_complex_cdc_not_candidate(self):
        """Complex CDC with many targets/transforms → NOT a Functions candidate."""
        mapping_info = {
            "name": "M_CDC_COMPLEX",
            "sources": ["TABLE_A"],
            "targets": ["TARGET_1", "TARGET_2", "TARGET_3"],
            "transformations": ["SQ", "EXP", "LKP", "AGG", "UPD"],
        }
        streaming_info = {
            "is_streaming": False, "streaming_sources": [], "streaming_sinks": [], "indicators": [],
        }
        cdc_info = {
            "is_cdc": True, "cdc_type": "full_cdc",
            "cdc_sources": [{"name": "TABLE_A"}], "merge_keys": ["id"],
        }

        result = detect_functions_candidate(mapping_info, streaming_info, cdc_info)
        self.assertFalse(result["is_candidate"])

    def test_esb_name_hint(self):
        """Mapping with ESB-like name → http trigger."""
        mapping_info = {
            "name": "M_ESB_ORDER_GATEWAY",
            "sources": ["ORDER_API"],
            "targets": ["STG_ORDERS"],
            "transformations": ["SQ", "EXP"],
        }
        streaming_info = {
            "is_streaming": False, "streaming_sources": [], "streaming_sinks": [], "indicators": [],
        }
        cdc_info = {"is_cdc": False, "cdc_type": "none"}

        result = detect_functions_candidate(mapping_info, streaming_info, cdc_info)
        self.assertTrue(result["is_candidate"])
        self.assertEqual(result["trigger_type"], "http")
        self.assertIn("esb", result["indicators"][0])

    def test_api_name_hint(self):
        """Mapping with API-like name → http trigger."""
        mapping_info = {
            "name": "M_API_CUSTOMER_LOOKUP",
            "sources": ["CUSTOMER"],
            "targets": ["API_RESPONSE"],
            "transformations": ["SQ"],
        }
        streaming_info = {
            "is_streaming": False, "streaming_sources": [], "streaming_sinks": [], "indicators": [],
        }
        cdc_info = {"is_cdc": False, "cdc_type": "none"}

        result = detect_functions_candidate(mapping_info, streaming_info, cdc_info)
        self.assertTrue(result["is_candidate"])
        self.assertEqual(result["trigger_type"], "http")

    def test_blob_source_detected(self):
        """Mapping with blob/file source and simple transforms → blob trigger."""
        mapping_info = {
            "name": "M_FILE_IMPORT",
            "sources": ["blob_landing_data"],
            "targets": ["STG_DATA"],
            "transformations": ["SQ", "EXP"],
        }
        streaming_info = {
            "is_streaming": False, "streaming_sources": [], "streaming_sinks": [], "indicators": [],
        }
        cdc_info = {"is_cdc": False, "cdc_type": "none"}

        result = detect_functions_candidate(mapping_info, streaming_info, cdc_info)
        self.assertTrue(result["is_candidate"])
        self.assertEqual(result["trigger_type"], "blob")

    def test_flat_file_blob_detected(self):
        """Mapping with flat_file source → blob trigger."""
        mapping_info = {
            "name": "M_FLAT_FILE_LOAD",
            "sources": ["flat_file_customers"],
            "targets": ["STG_CUSTOMERS"],
            "transformations": ["SQ"],
        }
        streaming_info = {
            "is_streaming": False, "streaming_sources": [], "streaming_sinks": [], "indicators": [],
        }
        cdc_info = {"is_cdc": False, "cdc_type": "none"}

        result = detect_functions_candidate(mapping_info, streaming_info, cdc_info)
        self.assertTrue(result["is_candidate"])
        self.assertEqual(result["trigger_type"], "blob")

    def test_normal_batch_not_candidate(self):
        """Standard batch ETL mapping → NOT a Functions candidate."""
        mapping_info = {
            "name": "M_LOAD_CUSTOMERS",
            "sources": ["CUSTOMER_TABLE"],
            "targets": ["DIM_CUSTOMER"],
            "transformations": ["SQ", "EXP", "LKP", "FIL"],
        }
        streaming_info = {
            "is_streaming": False, "streaming_sources": [], "streaming_sinks": [], "indicators": [],
        }
        cdc_info = {"is_cdc": False, "cdc_type": "none"}

        result = detect_functions_candidate(mapping_info, streaming_info, cdc_info)
        self.assertFalse(result["is_candidate"])
        self.assertEqual(result["trigger_type"], "none")

    def test_result_structure(self):
        """Verify result dict always has required keys."""
        mapping_info = {"name": "TEST", "sources": [], "targets": [], "transformations": []}
        streaming_info = {"is_streaming": False, "streaming_sources": [], "streaming_sinks": [], "indicators": []}
        cdc_info = {"is_cdc": False, "cdc_type": "none"}

        result = detect_functions_candidate(mapping_info, streaming_info, cdc_info)
        self.assertIn("is_candidate", result)
        self.assertIn("trigger_type", result)
        self.assertIn("reason", result)
        self.assertIn("indicators", result)


# =====================================================================
# Test: Function Code Generation
# =====================================================================

class TestServiceBusFunctionGeneration(unittest.TestCase):
    """Test Service Bus function generation."""

    def test_generates_function_code(self):
        mapping = {
            "name": "M_ORDERS_QUEUE",
            "sources": ["ORDER_QUEUE"],
            "targets": ["STG_ORDERS"],
            "transformations": ["SQ", "EXP"],
            "streaming": {
                "streaming_sources": [{"name": "SB", "type": "servicebus", "topic": "orders"}],
            },
            "functions_candidate": {"is_candidate": True, "trigger_type": "service_bus"},
        }
        result = generate_service_bus_function(mapping)
        self.assertEqual(result["name"], "FN_M_ORDERS_QUEUE")
        self.assertEqual(result["trigger_type"], "service_bus")
        self.assertIn("service_bus_queue_trigger", result["code"])
        self.assertIn("orders", result["code"])
        self.assertIn("FN_M_ORDERS_QUEUE", result["code"])

    def test_uses_topic_from_streaming_sources(self):
        mapping = {
            "name": "M_SB_TEST",
            "sources": ["Q"],
            "targets": ["T"],
            "transformations": [],
            "streaming": {
                "streaming_sources": [{"name": "SB", "type": "servicebus", "topic": "my-custom-queue"}],
            },
            "functions_candidate": {"is_candidate": True, "trigger_type": "service_bus"},
        }
        result = generate_service_bus_function(mapping)
        self.assertIn("my-custom-queue", result["code"])


class TestEventHubFunctionGeneration(unittest.TestCase):
    """Test Event Hub function generation."""

    def test_generates_event_hub_function(self):
        mapping = {
            "name": "M_EH_INGEST",
            "sources": ["EH_TOPIC"],
            "targets": ["RAW_EVENTS"],
            "transformations": ["SQ"],
            "streaming": {
                "streaming_sources": [{"name": "EH", "type": "eventhub", "topic": "iot-data"}],
            },
            "functions_candidate": {"is_candidate": True, "trigger_type": "event_hub"},
        }
        result = generate_event_hub_function(mapping)
        self.assertEqual(result["trigger_type"], "event_hub")
        self.assertIn("event_hub_message_trigger", result["code"])
        self.assertIn("iot-data", result["code"])
        self.assertIn("cardinality", result["code"])

    def test_batch_processing(self):
        """Event Hub function processes batches."""
        mapping = {
            "name": "M_EH_BATCH",
            "sources": ["S"],
            "targets": ["T"],
            "transformations": [],
            "streaming": {"streaming_sources": []},
            "functions_candidate": {"is_candidate": True, "trigger_type": "event_hub"},
        }
        result = generate_event_hub_function(mapping)
        self.assertIn("list[func.EventHubEvent]", result["code"])
        self.assertIn("_write_batch", result["code"])


class TestSqlTriggerFunctionGeneration(unittest.TestCase):
    """Test SQL trigger function generation."""

    def test_generates_sql_trigger_function(self):
        mapping = {
            "name": "M_CDC_CUSTOMER",
            "sources": ["CUSTOMER"],
            "targets": ["STG_CUSTOMER"],
            "transformations": ["SQ", "UPD"],
            "streaming": {"streaming_sources": []},
            "cdc": {
                "is_cdc": True,
                "cdc_type": "full_cdc",
                "cdc_sources": [],
                "merge_keys": ["customer_id"],
            },
            "functions_candidate": {"is_candidate": True, "trigger_type": "sql_trigger"},
        }
        result = generate_sql_trigger_function(mapping)
        self.assertEqual(result["trigger_type"], "sql_trigger")
        self.assertIn("sql_trigger", result["code"])
        self.assertIn("CUSTOMER", result["code"])
        self.assertIn("customer_id", result["code"])
        self.assertIn("full_cdc", result["code"])

    def test_handles_insert_update_delete(self):
        mapping = {
            "name": "M_CDC_FULL",
            "sources": ["TABLE_A"],
            "targets": ["TARGET_A"],
            "transformations": ["SQ", "UPD"],
            "streaming": {"streaming_sources": []},
            "cdc": {"is_cdc": True, "cdc_type": "full_cdc", "merge_keys": ["id"]},
            "functions_candidate": {"is_candidate": True, "trigger_type": "sql_trigger"},
        }
        result = generate_sql_trigger_function(mapping)
        self.assertIn("inserts", result["code"])
        self.assertIn("updates", result["code"])
        self.assertIn("deletes", result["code"])
        self.assertIn("_upsert_records", result["code"])
        self.assertIn("_delete_records", result["code"])


class TestCosmosTriggerFunctionGeneration(unittest.TestCase):
    """Test Cosmos DB trigger function generation."""

    def test_generates_cosmos_function(self):
        mapping = {
            "name": "M_COSMOS_ORDERS",
            "sources": ["cosmosdb_orders"],
            "targets": ["STG_ORDERS"],
            "transformations": ["SQ"],
            "streaming": {"streaming_sources": []},
            "cdc": {"is_cdc": True},
            "functions_candidate": {"is_candidate": True, "trigger_type": "cosmos_trigger"},
        }
        result = generate_cosmos_trigger_function(mapping)
        self.assertEqual(result["trigger_type"], "cosmos_trigger")
        self.assertIn("cosmos_db_trigger_v3", result["code"])
        self.assertIn("cosmosdb_orders", result["code"])
        self.assertIn("DocumentList", result["code"])


class TestHttpFunctionGeneration(unittest.TestCase):
    """Test HTTP trigger function generation."""

    def test_generates_http_function(self):
        mapping = {
            "name": "M_ESB_GATEWAY",
            "sources": ["API"],
            "targets": ["STG_API"],
            "transformations": ["SQ"],
            "functions_candidate": {"is_candidate": True, "trigger_type": "http"},
        }
        result = generate_http_function(mapping)
        self.assertEqual(result["trigger_type"], "http")
        self.assertIn("@app.route", result["code"])
        self.assertIn("HttpRequest", result["code"])
        self.assertIn("HttpResponse", result["code"])
        self.assertIn("m_esb_gateway", result["code"])  # lowercase route

    def test_returns_202_accepted(self):
        mapping = {
            "name": "M_API_TEST",
            "sources": [],
            "targets": ["T"],
            "transformations": [],
            "functions_candidate": {"is_candidate": True, "trigger_type": "http"},
        }
        result = generate_http_function(mapping)
        self.assertIn("202", result["code"])
        self.assertIn("accepted", result["code"])


class TestTimerFunctionGeneration(unittest.TestCase):
    """Test Timer trigger function generation."""

    def test_generates_timer_function(self):
        mapping = {
            "name": "M_PERIODIC_SYNC",
            "sources": ["SOURCE_TABLE"],
            "targets": ["TARGET_TABLE"],
            "transformations": ["SQ"],
            "functions_candidate": {"is_candidate": True, "trigger_type": "timer"},
        }
        result = generate_timer_function(mapping)
        self.assertEqual(result["trigger_type"], "timer")
        self.assertIn("timer_trigger", result["code"])
        self.assertIn("schedule", result["code"])
        self.assertIn("past_due", result["code"])


class TestBlobTriggerFunctionGeneration(unittest.TestCase):
    """Test Blob trigger function generation."""

    def test_generates_blob_function(self):
        mapping = {
            "name": "M_FILE_IMPORT",
            "sources": ["blob_container"],
            "targets": ["STG_DATA"],
            "transformations": ["SQ"],
            "functions_candidate": {"is_candidate": True, "trigger_type": "blob"},
        }
        result = generate_blob_trigger_function(mapping)
        self.assertEqual(result["trigger_type"], "blob")
        self.assertIn("blob_trigger", result["code"])
        self.assertIn("InputStream", result["code"])
        self.assertIn("_parse_file", result["code"])

    def test_path_uses_mapping_name(self):
        mapping = {
            "name": "M_IMPORT_SALES",
            "sources": ["s"],
            "targets": ["t"],
            "transformations": [],
            "functions_candidate": {"is_candidate": True, "trigger_type": "blob"},
        }
        result = generate_blob_trigger_function(mapping)
        self.assertIn("m_import_sales", result["code"])


# =====================================================================
# Test: Dispatch & project scaffolding
# =====================================================================

class TestFunctionDispatch(unittest.TestCase):
    """Test generate_function() dispatch logic."""

    def test_dispatches_service_bus(self):
        mapping = {
            "name": "M_SB",
            "sources": ["S"],
            "targets": ["T"],
            "transformations": [],
            "streaming": {"streaming_sources": []},
            "functions_candidate": {"is_candidate": True, "trigger_type": "service_bus"},
        }
        result = generate_function(mapping)
        self.assertEqual(result["trigger_type"], "service_bus")

    def test_dispatches_sql_trigger(self):
        mapping = {
            "name": "M_CDC",
            "sources": ["S"],
            "targets": ["T"],
            "transformations": [],
            "streaming": {"streaming_sources": []},
            "cdc": {"is_cdc": True, "cdc_type": "upsert_only", "merge_keys": ["id"]},
            "functions_candidate": {"is_candidate": True, "trigger_type": "sql_trigger"},
        }
        result = generate_function(mapping)
        self.assertEqual(result["trigger_type"], "sql_trigger")

    def test_dispatches_http_by_default(self):
        mapping = {
            "name": "M_UNKNOWN",
            "sources": [],
            "targets": [],
            "transformations": [],
            "functions_candidate": {"is_candidate": True, "trigger_type": "unknown_type"},
        }
        result = generate_function(mapping)
        self.assertEqual(result["trigger_type"], "http")

    def test_all_trigger_types_generate_valid_code(self):
        """Every trigger type produces non-empty code with function name."""
        for trigger in ["service_bus", "event_hub", "sql_trigger", "cosmos_trigger", "http", "timer", "blob"]:
            mapping = {
                "name": "M_TEST",
                "sources": ["S"],
                "targets": ["T"],
                "transformations": [],
                "streaming": {"streaming_sources": []},
                "cdc": {"is_cdc": True, "cdc_type": "upsert_only", "merge_keys": ["id"]},
                "functions_candidate": {"is_candidate": True, "trigger_type": trigger},
            }
            result = generate_function(mapping)
            self.assertTrue(len(result["code"]) > 100, f"Empty code for trigger {trigger}")
            self.assertIn("FN_M_TEST", result["code"])


class TestProjectScaffolding(unittest.TestCase):
    """Test project scaffold generators."""

    def test_host_json_structure(self):
        host = generate_host_json()
        self.assertEqual(host["version"], "2.0")
        self.assertIn("extensionBundle", host)
        self.assertIn("logging", host)
        self.assertIn("extensions", host)

    def test_local_settings_structure(self):
        settings = generate_local_settings()
        self.assertFalse(settings["IsEncrypted"])
        self.assertEqual(settings["Values"]["FUNCTIONS_WORKER_RUNTIME"], "python")
        self.assertIn("ServiceBusConnection__fullyQualifiedNamespace", settings["Values"])
        self.assertIn("SqlConnectionString", settings["Values"])

    def test_requirements_txt(self):
        req = generate_requirements_txt()
        self.assertIn("azure-functions", req)
        self.assertIn("azure-identity", req)
        self.assertIn("azure-servicebus", req)
        self.assertIn("pyodbc", req)

    def test_function_app_entry(self):
        functions = [
            {"name": "FN_A", "trigger_type": "service_bus", "code": "..."},
            {"name": "FN_B", "trigger_type": "http", "code": "..."},
        ]
        entry = generate_function_app_entry(functions)
        self.assertIn("FN_A", entry)
        self.assertIn("FN_B", entry)
        self.assertIn("service_bus", entry)
        self.assertIn("http", entry)
        self.assertIn("azure.functions", entry)


# =====================================================================
# Test: Pipeline Integration
# =====================================================================

class TestPipelineIntegration(unittest.TestCase):
    """Test AzureFunctionActivity in pipeline migration."""

    def test_azure_function_activity_structure(self):
        func_info = {
            "is_candidate": True,
            "trigger_type": "service_bus",
            "reason": "Queue source maps to Service Bus trigger",
        }
        activity = _azure_function_activity("S_ORDERS", "M_ORDERS", ["Start"], func_info)
        self.assertEqual(activity["name"], "FN_M_ORDERS")
        self.assertEqual(activity["type"], "AzureFunctionActivity")
        self.assertIn("functionName", activity["typeProperties"])
        self.assertIn("functionAppUrl", activity["typeProperties"])
        self.assertEqual(activity["dependsOn"], [])  # Start is filtered out

    def test_azure_function_activity_with_dependencies(self):
        func_info = {"is_candidate": True, "trigger_type": "http", "reason": "ESB"}
        activity = _azure_function_activity("S_API", "M_API", ["S_PREV", "S_OTHER"], func_info)
        self.assertEqual(len(activity["dependsOn"]), 2)
        dep_names = [d["activity"] for d in activity["dependsOn"]]
        self.assertIn("S_PREV", dep_names)
        self.assertIn("S_OTHER", dep_names)

    def test_activity_includes_reason(self):
        func_info = {
            "is_candidate": True,
            "trigger_type": "sql_trigger",
            "reason": "Simple CDC maps to SQL trigger",
        }
        activity = _azure_function_activity("S_CDC", "M_CDC", [], func_info)
        self.assertIn("Simple CDC maps to SQL trigger", activity["description"])

    def test_activity_policy(self):
        func_info = {"is_candidate": True, "trigger_type": "http", "reason": "test"}
        activity = _azure_function_activity("S_X", "M_X", [], func_info)
        self.assertEqual(activity["policy"]["retry"], 2)
        self.assertEqual(activity["policy"]["timeout"], "0.00:10:00")


# =====================================================================
# Test: End-to-end generation to temp dir
# =====================================================================

class TestEndToEndGeneration(unittest.TestCase):
    """Test full Functions migration flow writing to temp directory."""

    def test_generates_all_artifacts(self):
        """Generate Functions project artifacts for a candidate mapping."""
        with tempfile.TemporaryDirectory() as tmpdir:
            import run_functions_migration as rfm
            original_output = rfm.OUTPUT_DIR
            rfm.OUTPUT_DIR = Path(tmpdir)

            mapping = {
                "name": "M_SB_ORDERS",
                "sources": ["ORDERS_QUEUE"],
                "targets": ["STG_ORDERS"],
                "transformations": ["SQ", "EXP"],
                "streaming": {
                    "streaming_sources": [{"name": "SB", "type": "servicebus", "topic": "orders"}],
                },
                "cdc": {"is_cdc": False},
                "functions_candidate": {
                    "is_candidate": True,
                    "trigger_type": "service_bus",
                    "reason": "Queue source",
                    "indicators": ["queue_source:servicebus"],
                },
            }

            fn_result = rfm.generate_function(mapping)
            fn_path = Path(tmpdir) / f"{fn_result['name']}.py"
            fn_path.write_text(fn_result["code"], encoding="utf-8")

            host = rfm.generate_host_json()
            (Path(tmpdir) / "host.json").write_text(json.dumps(host), encoding="utf-8")

            self.assertTrue(fn_path.exists())
            self.assertTrue((Path(tmpdir) / "host.json").exists())

            content = fn_path.read_text(encoding="utf-8")
            self.assertIn("service_bus_queue_trigger", content)
            self.assertIn("orders", content)

            rfm.OUTPUT_DIR = original_output

    def test_inventory_functions_candidate_field(self):
        """Verify inventory output includes functions_candidate field."""
        from run_assessment import write_inventory_json

        test_mapping = {
            "name": "M_TEST",
            "sources": ["S"],
            "targets": ["T"],
            "transformations": ["SQ"],
            "has_sql_override": False,
            "has_stored_proc": False,
            "complexity": "Simple",
            "sql_overrides": [],
            "lookup_conditions": [],
            "parameters": [],
            "target_load_order": [],
            "conversion_score": 90,
            "manual_effort_hours": 1,
            "lineage_summary": "",
            "field_lineage": [],
            "streaming": {"is_streaming": False, "streaming_sources": [], "streaming_sinks": [], "indicators": []},
            "cdc": {"is_cdc": False, "cdc_type": "none", "cdc_sources": [], "update_strategy": None, "merge_keys": [], "indicators": []},
            "functions_candidate": {"is_candidate": True, "trigger_type": "http", "reason": "ESB", "indicators": ["esb"]},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            import run_assessment as ra
            original_output = ra.OUTPUT_DIR
            ra.OUTPUT_DIR = Path(tmpdir)

            inv = write_inventory_json([test_mapping], [], [], [])

            self.assertIn("total_functions_candidates", inv["summary"])
            self.assertEqual(inv["summary"]["total_functions_candidates"], 1)
            self.assertTrue(inv["mappings"][0]["functions_candidate"]["is_candidate"])

            ra.OUTPUT_DIR = original_output


# =====================================================================
# Test: Code quality checks on generated functions
# =====================================================================

class TestGeneratedCodeQuality(unittest.TestCase):
    """Verify generated function code follows best practices."""

    def _get_all_generated_codes(self):
        mapping = {
            "name": "M_QUALITY_TEST",
            "sources": ["SOURCE"],
            "targets": ["TARGET"],
            "transformations": ["SQ"],
            "streaming": {"streaming_sources": [{"name": "S", "type": "servicebus", "topic": "t"}]},
            "cdc": {"is_cdc": True, "cdc_type": "upsert_only", "merge_keys": ["id"]},
            "functions_candidate": {"is_candidate": True},
        }
        generators = [
            generate_service_bus_function,
            generate_event_hub_function,
            generate_sql_trigger_function,
            generate_cosmos_trigger_function,
            generate_http_function,
            generate_timer_function,
            generate_blob_trigger_function,
        ]
        return [g(mapping) for g in generators]

    def test_all_functions_have_logging(self):
        """All generated functions use logging."""
        for fn in self._get_all_generated_codes():
            self.assertIn("import logging", fn["code"], f"{fn['name']} missing logging")

    def test_all_functions_have_error_handling(self):
        """All generated functions have try/except."""
        for fn in self._get_all_generated_codes():
            self.assertIn("except", fn["code"], f"{fn['name']} missing error handling")

    def test_all_functions_have_transform_method(self):
        """All generated functions have a _transform() method."""
        for fn in self._get_all_generated_codes():
            self.assertIn("def _transform", fn["code"], f"{fn['name']} missing _transform()")

    def test_no_hardcoded_credentials(self):
        """No generated function contains hardcoded credentials."""
        for fn in self._get_all_generated_codes():
            code = fn["code"].lower()
            self.assertNotIn("password=", code, f"{fn['name']} has hardcoded password")
            self.assertNotIn("secret=", code, f"{fn['name']} has hardcoded secret")
            self.assertNotIn("accountkey=", code, f"{fn['name']} has hardcoded key")

    def test_function_names_prefixed(self):
        """All function names are prefixed with FN_."""
        for fn in self._get_all_generated_codes():
            self.assertTrue(fn["name"].startswith("FN_"), f"{fn['name']} missing FN_ prefix")


if __name__ == "__main__":
    unittest.main()
