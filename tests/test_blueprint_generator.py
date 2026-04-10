"""
Tests for CDC / Real-Time Deployment Blueprint Generator
Tests the Event Hub + APIM + Azure Functions blueprint generation.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path


class TestAnalyzeCandidates(unittest.TestCase):
    """Tests for analyze_candidates()."""

    def test_import(self):
        import run_blueprint_generator
        self.assertTrue(hasattr(run_blueprint_generator, "analyze_candidates"))
        self.assertTrue(hasattr(run_blueprint_generator, "generate_blueprint"))
        self.assertTrue(hasattr(run_blueprint_generator, "generate_bicep_blueprint"))
        self.assertTrue(hasattr(run_blueprint_generator, "generate_terraform_blueprint"))

    def test_no_inventory(self):
        from run_blueprint_generator import analyze_candidates
        result = analyze_candidates("/nonexistent.json")
        self.assertEqual(result["cdc"], [])
        self.assertEqual(result["realtime"], [])
        self.assertEqual(result["esb_api"], [])
        self.assertEqual(result["summary"]["total_candidates"], 0)

    def test_cdc_candidate_detected(self):
        from run_blueprint_generator import analyze_candidates
        inv = {
            "mappings": [{
                "name": "M_CDC_ORDERS",
                "sources": ["orders"],
                "targets": ["delta_orders"],
                "complexity": "Simple",
                "functions_candidate": {"is_candidate": True, "trigger_type": "sql_trigger"},
                "cdc": {"cdc_type": "full_cdc", "merge_keys": ["order_id"]},
            }],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = analyze_candidates(f.name)
        os.unlink(f.name)
        self.assertEqual(len(result["cdc"]), 1)
        self.assertEqual(result["cdc"][0]["name"], "M_CDC_ORDERS")
        self.assertTrue(result["summary"]["needs_event_hub"])

    def test_realtime_candidate_detected(self):
        from run_blueprint_generator import analyze_candidates
        inv = {
            "mappings": [{
                "name": "M_STREAM_EVENTS",
                "sources": ["event_source"],
                "targets": ["event_sink"],
                "complexity": "Simple",
                "functions_candidate": {"is_candidate": True, "trigger_type": "event_hub"},
                "streaming": {"is_streaming": True, "streaming_sources": [{"topic": "user-events"}]},
            }],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = analyze_candidates(f.name)
        os.unlink(f.name)
        self.assertEqual(len(result["realtime"]), 1)
        self.assertTrue(result["summary"]["needs_event_hub"])

    def test_esb_api_candidate_detected(self):
        from run_blueprint_generator import analyze_candidates
        inv = {
            "mappings": [{
                "name": "M_API_INGEST",
                "sources": ["api_source"],
                "targets": ["target"],
                "complexity": "Simple",
                "functions_candidate": {"is_candidate": True, "trigger_type": "http"},
            }],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = analyze_candidates(f.name)
        os.unlink(f.name)
        self.assertEqual(len(result["esb_api"]), 1)
        self.assertTrue(result["summary"]["needs_apim"])
        self.assertFalse(result["summary"]["needs_event_hub"])

    def test_mixed_candidates(self):
        from run_blueprint_generator import analyze_candidates
        inv = {
            "mappings": [
                {
                    "name": "M_CDC",
                    "sources": ["src"], "targets": ["tgt"], "complexity": "Simple",
                    "functions_candidate": {"is_candidate": True, "trigger_type": "sql_trigger"},
                    "cdc": {"cdc_type": "upsert_only", "merge_keys": ["id"]},
                },
                {
                    "name": "M_STREAM",
                    "sources": ["src"], "targets": ["tgt"], "complexity": "Simple",
                    "functions_candidate": {"is_candidate": True, "trigger_type": "event_hub"},
                    "streaming": {"is_streaming": True, "streaming_sources": []},
                },
                {
                    "name": "M_API",
                    "sources": ["src"], "targets": ["tgt"], "complexity": "Simple",
                    "functions_candidate": {"is_candidate": True, "trigger_type": "http"},
                },
            ],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = analyze_candidates(f.name)
        os.unlink(f.name)
        self.assertEqual(result["summary"]["total_candidates"], 3)
        self.assertTrue(result["summary"]["needs_event_hub"])
        self.assertTrue(result["summary"]["needs_apim"])
        self.assertTrue(result["summary"]["needs_functions"])

    def test_no_candidates_in_inventory(self):
        from run_blueprint_generator import analyze_candidates
        inv = {"mappings": [{"name": "M_BATCH", "sources": ["s"], "targets": ["t"], "complexity": "Simple"}]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = analyze_candidates(f.name)
        os.unlink(f.name)
        self.assertEqual(result["summary"]["total_candidates"], 0)
        self.assertFalse(result["summary"]["needs_event_hub"])
        self.assertFalse(result["summary"]["needs_apim"])


class TestArchitectureDoc(unittest.TestCase):
    """Tests for generate_architecture_doc()."""

    def test_empty_analysis(self):
        from run_blueprint_generator import generate_architecture_doc
        analysis = {"cdc": [], "realtime": [], "esb_api": [], "summary": {
            "cdc_count": 0, "realtime_count": 0, "esb_api_count": 0,
            "needs_event_hub": False, "needs_apim": False, "needs_functions": True,
            "total_candidates": 0,
        }}
        doc = generate_architecture_doc(analysis)
        self.assertIn("CDC / Real-Time Deployment Blueprint", doc)
        self.assertIn("Azure Functions", doc)

    def test_cdc_architecture(self):
        from run_blueprint_generator import generate_architecture_doc
        analysis = {
            "cdc": [{"name": "M_CDC", "sources": ["orders"], "targets": ["delta_orders"],
                     "trigger_type": "sql_trigger", "cdc_type": "full_cdc", "merge_keys": ["id"],
                     "complexity": "Simple"}],
            "realtime": [], "esb_api": [],
            "summary": {"cdc_count": 1, "realtime_count": 0, "esb_api_count": 0,
                        "needs_event_hub": True, "needs_apim": False, "needs_functions": True,
                        "total_candidates": 1},
        }
        doc = generate_architecture_doc(analysis, target="fabric")
        self.assertIn("Event Hub", doc)
        self.assertIn("CDC Pipeline", doc)
        self.assertIn("mermaid", doc)
        self.assertIn("M_CDC", doc)

    def test_apim_architecture(self):
        from run_blueprint_generator import generate_architecture_doc
        analysis = {
            "cdc": [], "realtime": [],
            "esb_api": [{"name": "M_API", "sources": ["s"], "targets": ["t"],
                        "trigger_type": "http", "complexity": "Simple"}],
            "summary": {"cdc_count": 0, "realtime_count": 0, "esb_api_count": 1,
                        "needs_event_hub": False, "needs_apim": True, "needs_functions": True,
                        "total_candidates": 1},
        }
        doc = generate_architecture_doc(analysis)
        self.assertIn("API Management", doc)
        self.assertIn("APIM", doc)
        self.assertIn("rate-limit", doc.lower().replace("_", "-").replace(" ", "-") or doc)

    def test_full_architecture(self):
        from run_blueprint_generator import generate_architecture_doc
        analysis = {
            "cdc": [{"name": "M_CDC", "sources": ["s"], "targets": ["t"],
                     "cdc_type": "upsert", "merge_keys": ["id"], "trigger_type": "sql_trigger",
                     "complexity": "Simple"}],
            "realtime": [{"name": "M_RT", "sources": ["s"], "targets": ["t"],
                         "event_hub_name": "events", "trigger_type": "event_hub",
                         "complexity": "Simple"}],
            "esb_api": [{"name": "M_API", "sources": ["s"], "targets": ["t"],
                        "trigger_type": "http", "complexity": "Simple"}],
            "summary": {"cdc_count": 1, "realtime_count": 1, "esb_api_count": 1,
                        "needs_event_hub": True, "needs_apim": True, "needs_functions": True,
                        "total_candidates": 3},
        }
        doc = generate_architecture_doc(analysis, target="databricks", region="westeurope")
        self.assertIn("Databricks", doc)
        self.assertIn("westeurope", doc)
        self.assertIn("CDC Mappings", doc)
        self.assertIn("Real-Time Mappings", doc)
        self.assertIn("API / ESB Mappings", doc)


class TestBicepBlueprint(unittest.TestCase):
    """Tests for generate_bicep_blueprint()."""

    def _analysis_with_all(self):
        return {
            "cdc": [{"name": "M_CDC"}],
            "realtime": [{"name": "M_RT"}],
            "esb_api": [{"name": "M_API"}],
            "summary": {"cdc_count": 1, "realtime_count": 1, "esb_api_count": 1,
                        "needs_event_hub": True, "needs_apim": True, "needs_functions": True,
                        "total_candidates": 3},
        }

    def test_bicep_has_function_app(self):
        from run_blueprint_generator import generate_bicep_blueprint
        bicep = generate_bicep_blueprint(self._analysis_with_all())
        self.assertIn("Microsoft.Web/sites", bicep)
        self.assertIn("functionapp", bicep)
        self.assertIn("FUNCTIONS_WORKER_RUNTIME", bicep)

    def test_bicep_has_event_hub(self):
        from run_blueprint_generator import generate_bicep_blueprint
        bicep = generate_bicep_blueprint(self._analysis_with_all())
        self.assertIn("Microsoft.EventHub/namespaces", bicep)
        self.assertIn("cdc-events", bicep)
        self.assertIn("realtime-events", bicep)
        self.assertIn("fabric-sink", bicep)

    def test_bicep_has_apim(self):
        from run_blueprint_generator import generate_bicep_blueprint
        bicep = generate_bicep_blueprint(self._analysis_with_all())
        self.assertIn("Microsoft.ApiManagement", bicep)
        self.assertIn("migration-api", bicep)
        self.assertIn("rate-limit", bicep)
        self.assertIn("validate-jwt", bicep)

    def test_bicep_has_managed_identity_rbac(self):
        from run_blueprint_generator import generate_bicep_blueprint
        bicep = generate_bicep_blueprint(self._analysis_with_all())
        self.assertIn("roleAssignments", bicep)
        self.assertIn("Event Hub Data Sender", bicep)
        self.assertIn("Event Hub Data Receiver", bicep)

    def test_bicep_no_event_hub_when_not_needed(self):
        from run_blueprint_generator import generate_bicep_blueprint
        analysis = {
            "cdc": [], "realtime": [],
            "esb_api": [{"name": "M_API"}],
            "summary": {"cdc_count": 0, "realtime_count": 0, "esb_api_count": 1,
                        "needs_event_hub": False, "needs_apim": True, "needs_functions": True,
                        "total_candidates": 1},
        }
        bicep = generate_bicep_blueprint(analysis)
        self.assertNotIn("Microsoft.EventHub", bicep)
        self.assertIn("Microsoft.ApiManagement", bicep)

    def test_bicep_no_apim_when_not_needed(self):
        from run_blueprint_generator import generate_bicep_blueprint
        analysis = {
            "cdc": [{"name": "M_CDC"}], "realtime": [], "esb_api": [],
            "summary": {"cdc_count": 1, "realtime_count": 0, "esb_api_count": 0,
                        "needs_event_hub": True, "needs_apim": False, "needs_functions": True,
                        "total_candidates": 1},
        }
        bicep = generate_bicep_blueprint(analysis)
        self.assertNotIn("Microsoft.ApiManagement", bicep)
        self.assertIn("Microsoft.EventHub", bicep)

    def test_bicep_has_app_insights(self):
        from run_blueprint_generator import generate_bicep_blueprint
        bicep = generate_bicep_blueprint(self._analysis_with_all())
        self.assertIn("Microsoft.Insights/components", bicep)
        self.assertIn("APPINSIGHTS_INSTRUMENTATIONKEY", bicep)

    def test_bicep_parameters(self):
        from run_blueprint_generator import generate_bicep_parameters
        params = generate_bicep_parameters(self._analysis_with_all(), region="westeurope", env="prod")
        self.assertEqual(params["parameters"]["location"]["value"], "westeurope")
        self.assertEqual(params["parameters"]["environment"]["value"], "prod")
        self.assertIn("eventHubNamespaceName", params["parameters"])
        self.assertIn("apimName", params["parameters"])

    def test_bicep_parameters_no_optional(self):
        from run_blueprint_generator import generate_bicep_parameters
        analysis = {"cdc": [], "realtime": [], "esb_api": [],
                    "summary": {"needs_event_hub": False, "needs_apim": False}}
        params = generate_bicep_parameters(analysis)
        self.assertNotIn("eventHubNamespaceName", params["parameters"])
        self.assertNotIn("apimName", params["parameters"])


class TestTerraformBlueprint(unittest.TestCase):
    """Tests for generate_terraform_blueprint()."""

    def _analysis_with_all(self):
        return {
            "cdc": [{"name": "M_CDC"}],
            "realtime": [{"name": "M_RT"}],
            "esb_api": [{"name": "M_API"}],
            "summary": {"cdc_count": 1, "realtime_count": 1, "esb_api_count": 1,
                        "needs_event_hub": True, "needs_apim": True, "needs_functions": True,
                        "total_candidates": 3},
        }

    def test_terraform_has_function_app(self):
        from run_blueprint_generator import generate_terraform_blueprint
        tf = generate_terraform_blueprint(self._analysis_with_all())
        self.assertIn("azurerm_linux_function_app", tf)
        self.assertIn("python_version", tf)

    def test_terraform_has_event_hub(self):
        from run_blueprint_generator import generate_terraform_blueprint
        tf = generate_terraform_blueprint(self._analysis_with_all())
        self.assertIn("azurerm_eventhub_namespace", tf)
        self.assertIn("cdc-events", tf)
        self.assertIn("realtime-events", tf)

    def test_terraform_has_apim(self):
        from run_blueprint_generator import generate_terraform_blueprint
        tf = generate_terraform_blueprint(self._analysis_with_all())
        self.assertIn("azurerm_api_management", tf)
        self.assertIn("migration-functions", tf)

    def test_terraform_has_rbac(self):
        from run_blueprint_generator import generate_terraform_blueprint
        tf = generate_terraform_blueprint(self._analysis_with_all())
        self.assertIn("azurerm_role_assignment", tf)
        self.assertIn("Event Hubs Data Sender", tf)
        self.assertIn("Event Hubs Data Receiver", tf)

    def test_terraform_variables(self):
        from run_blueprint_generator import generate_terraform_variables_blueprint
        analysis = self._analysis_with_all()
        tf_vars = generate_terraform_variables_blueprint(analysis, region="northeurope")
        self.assertIn('default = "northeurope"', tf_vars)
        self.assertIn("enable_event_hub", tf_vars)
        self.assertIn("enable_apim", tf_vars)
        self.assertIn("event_hub_partition_count", tf_vars)

    def test_terraform_no_optional_when_not_needed(self):
        from run_blueprint_generator import generate_terraform_blueprint
        analysis = {
            "cdc": [], "realtime": [], "esb_api": [],
            "summary": {"cdc_count": 0, "realtime_count": 0, "esb_api_count": 0,
                        "needs_event_hub": False, "needs_apim": False, "needs_functions": True,
                        "total_candidates": 0},
        }
        tf = generate_terraform_blueprint(analysis)
        self.assertNotIn("azurerm_eventhub", tf)
        self.assertNotIn("azurerm_api_management", tf)


class TestDeploymentScripts(unittest.TestCase):
    """Tests for deployment script generators."""

    def _analysis_full(self):
        return {
            "cdc": [{"name": "M_CDC"}], "realtime": [], "esb_api": [{"name": "M_API"}],
            "summary": {"cdc_count": 1, "realtime_count": 0, "esb_api_count": 1,
                        "needs_event_hub": True, "needs_apim": True, "needs_functions": True,
                        "total_candidates": 2},
        }

    def test_deploy_ps1(self):
        from run_blueprint_generator import generate_deploy_ps1
        ps1 = generate_deploy_ps1(self._analysis_full())
        self.assertIn("az group create", ps1)
        self.assertIn("az deployment group create", ps1)
        self.assertIn("func azure functionapp publish", ps1)
        self.assertIn("Validating", ps1)

    def test_deploy_sh(self):
        from run_blueprint_generator import generate_deploy_sh
        sh = generate_deploy_sh(self._analysis_full())
        self.assertIn("#!/bin/bash", sh)
        self.assertIn("az group create", sh)
        self.assertIn("func azure functionapp publish", sh)

    def test_validate_ps1(self):
        from run_blueprint_generator import generate_validate_ps1
        ps1 = generate_validate_ps1(self._analysis_full())
        self.assertIn("[PASS]", ps1)
        self.assertIn("[FAIL]", ps1)
        self.assertIn("Function App running", ps1)
        self.assertIn("Event Hub", ps1)
        self.assertIn("APIM", ps1)

    def test_deploy_custom_params(self):
        from run_blueprint_generator import generate_deploy_ps1
        ps1 = generate_deploy_ps1(self._analysis_full(), resource_prefix="prod", env="prod", region="westeurope")
        self.assertIn('"prod"', ps1)
        self.assertIn('"westeurope"', ps1)


class TestGenerateBlueprint(unittest.TestCase):
    """Tests for the full generate_blueprint() orchestrator."""

    def test_run_with_no_inventory(self):
        from run_blueprint_generator import generate_blueprint, OUTPUT_DIR
        result = generate_blueprint(inventory_path="/nonexistent.json")
        self.assertIn("analysis", result)
        self.assertIn("generated", result)
        self.assertIn("components", result)
        self.assertGreater(len(result["generated"]), 0)

    def test_run_generates_architecture_md(self):
        from run_blueprint_generator import generate_blueprint, OUTPUT_DIR
        result = generate_blueprint(inventory_path="/nonexistent.json")
        arch_path = OUTPUT_DIR / "architecture.md"
        self.assertTrue(arch_path.exists())

    def test_run_generates_bicep(self):
        from run_blueprint_generator import generate_blueprint, OUTPUT_DIR
        generate_blueprint(inventory_path="/nonexistent.json")
        self.assertTrue((OUTPUT_DIR / "bicep" / "main.bicep").exists())
        self.assertTrue((OUTPUT_DIR / "bicep" / "parameters.json").exists())

    def test_run_generates_terraform(self):
        from run_blueprint_generator import generate_blueprint, OUTPUT_DIR
        generate_blueprint(inventory_path="/nonexistent.json")
        self.assertTrue((OUTPUT_DIR / "terraform" / "main.tf").exists())
        self.assertTrue((OUTPUT_DIR / "terraform" / "variables.tf").exists())

    def test_run_generates_scripts(self):
        from run_blueprint_generator import generate_blueprint, OUTPUT_DIR
        generate_blueprint(inventory_path="/nonexistent.json")
        self.assertTrue((OUTPUT_DIR / "scripts" / "deploy.ps1").exists())
        self.assertTrue((OUTPUT_DIR / "scripts" / "deploy.sh").exists())
        self.assertTrue((OUTPUT_DIR / "scripts" / "validate.ps1").exists())

    def test_run_with_cdc_inventory(self):
        from run_blueprint_generator import generate_blueprint
        inv = {
            "mappings": [{
                "name": "M_CDC_TEST",
                "sources": ["src_table"], "targets": ["tgt_table"],
                "complexity": "Simple",
                "functions_candidate": {"is_candidate": True, "trigger_type": "sql_trigger"},
                "cdc": {"cdc_type": "upsert_only", "merge_keys": ["id"]},
            }],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = generate_blueprint(inventory_path=f.name)
        os.unlink(f.name)
        self.assertTrue(result["components"]["event_hub"])
        self.assertFalse(result["components"]["apim"])

    def test_components_flag_accuracy(self):
        from run_blueprint_generator import generate_blueprint
        inv = {
            "mappings": [{
                "name": "M_HTTP",
                "sources": ["s"], "targets": ["t"], "complexity": "Simple",
                "functions_candidate": {"is_candidate": True, "trigger_type": "http"},
            }],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = generate_blueprint(inventory_path=f.name)
        os.unlink(f.name)
        self.assertFalse(result["components"]["event_hub"])
        self.assertTrue(result["components"]["apim"])


class TestBlueprintSecurity(unittest.TestCase):
    """Security-focused tests for blueprint generation."""

    def test_bicep_uses_managed_identity(self):
        from run_blueprint_generator import generate_bicep_blueprint
        analysis = {
            "cdc": [{"name": "M_CDC"}], "realtime": [], "esb_api": [],
            "summary": {"cdc_count": 1, "realtime_count": 0, "esb_api_count": 0,
                        "needs_event_hub": True, "needs_apim": False, "needs_functions": True,
                        "total_candidates": 1},
        }
        bicep = generate_bicep_blueprint(analysis)
        self.assertIn("ManagedIdentity", bicep)

    def test_bicep_apim_requires_subscription(self):
        from run_blueprint_generator import generate_bicep_blueprint
        analysis = {
            "cdc": [], "realtime": [],
            "esb_api": [{"name": "M_API"}],
            "summary": {"cdc_count": 0, "realtime_count": 0, "esb_api_count": 1,
                        "needs_event_hub": False, "needs_apim": True, "needs_functions": True,
                        "total_candidates": 1},
        }
        bicep = generate_bicep_blueprint(analysis)
        self.assertIn("subscriptionRequired: true", bicep)

    def test_bicep_storage_https_only(self):
        from run_blueprint_generator import generate_bicep_blueprint
        analysis = {
            "cdc": [], "realtime": [], "esb_api": [],
            "summary": {"cdc_count": 0, "realtime_count": 0, "esb_api_count": 0,
                        "needs_event_hub": False, "needs_apim": False, "needs_functions": True,
                        "total_candidates": 0},
        }
        bicep = generate_bicep_blueprint(analysis)
        self.assertIn("supportsHttpsTrafficOnly: true", bicep)
        self.assertIn("TLS1_2", bicep)

    def test_terraform_storage_tls(self):
        from run_blueprint_generator import generate_terraform_blueprint
        analysis = {
            "cdc": [], "realtime": [], "esb_api": [],
            "summary": {"cdc_count": 0, "realtime_count": 0, "esb_api_count": 0,
                        "needs_event_hub": False, "needs_apim": False, "needs_functions": True,
                        "total_candidates": 0},
        }
        tf = generate_terraform_blueprint(analysis)
        self.assertIn("TLS1_2", tf)

    def test_function_app_https_only(self):
        from run_blueprint_generator import generate_bicep_blueprint
        analysis = {
            "cdc": [], "realtime": [], "esb_api": [],
            "summary": {"cdc_count": 0, "realtime_count": 0, "esb_api_count": 0,
                        "needs_event_hub": False, "needs_apim": False, "needs_functions": True,
                        "total_candidates": 0},
        }
        bicep = generate_bicep_blueprint(analysis)
        self.assertIn("httpsOnly: true", bicep)


if __name__ == "__main__":
    unittest.main()
