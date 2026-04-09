"""
Phase 15 Tests — Sprints 92–94
Sprint 92: Terraform & Bicep IaC generation
Sprint 93: Container & Kubernetes deployment
Sprint 94: CI/CD pipeline generation (GitHub Actions & Azure DevOps)
"""

import json
import os
import re
import sys
import textwrap
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ═══════════════════════════════════════════════
#  Sprint 92 — Terraform & Bicep IaC Generation
# ═══════════════════════════════════════════════

from iac_generator import (
    extract_variables,
    generate_terraform_variables,
    generate_terraform_fabric,
    generate_terraform_databricks,
    generate_bicep_parameters,
    generate_bicep_fabric,
    generate_bicep_databricks,
    generate_iac,
)


def _fabric_config():
    return {
        "target": "fabric",
        "fabric": {
            "workspace_name": "Test-Workspace",
            "workspace_id": "ws-123",
            "capacity_name": "test-cap",
            "capacity_sku": "F4",
            "admin_members": ["admin@test.com"],
        },
        "lakehouse": {"bronze": "bronze", "silver": "silver", "gold": "gold"},
        "azure": {"resource_group": "rg-test", "location": "westus2"},
    }


def _databricks_config():
    return {
        "target": "databricks",
        "databricks": {
            "workspace_name": "dbw-test",
            "workspace_url": "https://adb-xxx.azuredatabricks.net",
            "catalog": "test_catalog",
            "secret_scope": "test-scope",
            "sku": "premium",
            "node_type": "Standard_DS4_v2",
            "min_workers": 2,
            "max_workers": 8,
        },
        "azure": {"resource_group": "rg-dbtest", "location": "eastus"},
    }


class TestExtractVariables:
    """Sprint 92: Variable extraction from config."""

    def test_fabric_variables(self):
        v = extract_variables(_fabric_config())
        assert v["target"] == "fabric"
        assert v["resource_group_name"] == "rg-test"
        assert v["location"] == "westus2"
        assert "fabric" in v
        assert v["fabric"]["workspace_name"] == "Test-Workspace"
        assert v["fabric"]["capacity_sku"] == "F4"

    def test_databricks_variables(self):
        v = extract_variables(_databricks_config())
        assert v["target"] == "databricks"
        assert "databricks" in v
        assert v["databricks"]["catalog"] == "test_catalog"
        assert v["databricks"]["sku"] == "premium"
        assert v["databricks"]["min_workers"] == 2

    def test_keyvault_variables(self):
        v = extract_variables(_fabric_config())
        assert "keyvault" in v
        assert v["keyvault"]["sku"] == "standard"

    def test_storage_variables(self):
        v = extract_variables(_fabric_config())
        assert "storage" in v
        assert v["storage"]["sku"] == "Standard_LRS"

    def test_tags_present(self):
        v = extract_variables(_fabric_config())
        assert "tags" in v
        assert v["tags"]["project"] == "informatica-migration"

    def test_inventory_stats(self):
        inventory = {
            "mappings": [
                {"name": "M1", "complexity": "Simple"},
                {"name": "M2", "complexity": "Complex"},
                {"name": "M3", "complexity": "Complex"},
            ],
            "workflows": [{"name": "WF1"}],
        }
        v = extract_variables(_fabric_config(), inventory)
        assert v["inventory_stats"]["mapping_count"] == 3
        assert v["inventory_stats"]["complex_count"] == 2
        assert v["inventory_stats"]["workflow_count"] == 1

    def test_lakehouse_tiers(self):
        v = extract_variables(_fabric_config())
        assert v["lakehouse"]["bronze"] == "bronze"
        assert v["lakehouse"]["silver"] == "silver"
        assert v["lakehouse"]["gold"] == "gold"


class TestTerraformVariables:
    """Sprint 92: variables.tf generation."""

    def test_contains_resource_group(self):
        v = extract_variables(_fabric_config())
        content = generate_terraform_variables(v)
        assert 'variable "resource_group_name"' in content
        assert "rg-test" in content

    def test_contains_location(self):
        v = extract_variables(_fabric_config())
        content = generate_terraform_variables(v)
        assert 'variable "location"' in content
        assert "westus2" in content

    def test_contains_fabric_vars(self):
        v = extract_variables(_fabric_config())
        content = generate_terraform_variables(v)
        assert 'variable "fabric_workspace_name"' in content
        assert 'variable "fabric_capacity_sku"' in content

    def test_contains_databricks_vars(self):
        v = extract_variables(_databricks_config())
        content = generate_terraform_variables(v)
        assert 'variable "databricks_workspace_name"' in content
        assert 'variable "databricks_sku"' in content
        assert 'variable "databricks_catalog"' in content

    def test_contains_keyvault_var(self):
        v = extract_variables(_fabric_config())
        content = generate_terraform_variables(v)
        assert 'variable "keyvault_name"' in content

    def test_contains_storage_var(self):
        v = extract_variables(_fabric_config())
        content = generate_terraform_variables(v)
        assert 'variable "storage_account_name"' in content


class TestTerraformFabric:
    """Sprint 92: Terraform HCL for Fabric."""

    def test_has_required_providers(self):
        v = extract_variables(_fabric_config())
        hcl = generate_terraform_fabric(v)
        assert "hashicorp/azurerm" in hcl

    def test_has_resource_group(self):
        v = extract_variables(_fabric_config())
        hcl = generate_terraform_fabric(v)
        assert 'resource "azurerm_resource_group" "migration"' in hcl

    def test_has_keyvault(self):
        v = extract_variables(_fabric_config())
        hcl = generate_terraform_fabric(v)
        assert 'resource "azurerm_key_vault" "migration"' in hcl
        assert "purge_protection_enabled" in hcl

    def test_has_storage(self):
        v = extract_variables(_fabric_config())
        hcl = generate_terraform_fabric(v)
        assert 'resource "azurerm_storage_account" "migration"' in hcl

    def test_has_fabric_capacity(self):
        v = extract_variables(_fabric_config())
        hcl = generate_terraform_fabric(v)
        assert 'resource "azurerm_fabric_capacity" "migration"' in hcl

    def test_has_outputs(self):
        v = extract_variables(_fabric_config())
        hcl = generate_terraform_fabric(v)
        assert 'output "resource_group_name"' in hcl
        assert 'output "keyvault_uri"' in hcl
        assert 'output "fabric_capacity_id"' in hcl


class TestTerraformDatabricks:
    """Sprint 92: Terraform HCL for Databricks."""

    def test_has_databricks_provider(self):
        v = extract_variables(_databricks_config())
        hcl = generate_terraform_databricks(v)
        assert "databricks/databricks" in hcl

    def test_has_workspace_resource(self):
        v = extract_variables(_databricks_config())
        hcl = generate_terraform_databricks(v)
        assert 'resource "azurerm_databricks_workspace" "migration"' in hcl

    def test_has_unity_catalog(self):
        v = extract_variables(_databricks_config())
        hcl = generate_terraform_databricks(v)
        assert 'resource "databricks_catalog" "migration"' in hcl

    def test_has_schemas(self):
        v = extract_variables(_databricks_config())
        hcl = generate_terraform_databricks(v)
        assert 'resource "databricks_schema" "bronze"' in hcl
        assert 'resource "databricks_schema" "silver"' in hcl
        assert 'resource "databricks_schema" "gold"' in hcl

    def test_has_cluster(self):
        v = extract_variables(_databricks_config())
        hcl = generate_terraform_databricks(v)
        assert 'resource "databricks_cluster" "migration"' in hcl
        assert "Standard_DS4_v2" in hcl

    def test_has_outputs(self):
        v = extract_variables(_databricks_config())
        hcl = generate_terraform_databricks(v)
        assert 'output "databricks_workspace_url"' in hcl
        assert 'output "catalog_name"' in hcl


class TestBicepParameters:
    """Sprint 92: Bicep parameter generation."""

    def test_has_location_param(self):
        v = extract_variables(_fabric_config())
        content = generate_bicep_parameters(v)
        assert "param location string" in content

    def test_has_rg_param(self):
        v = extract_variables(_fabric_config())
        content = generate_bicep_parameters(v)
        assert "param resourceGroupName string" in content

    def test_has_fabric_params(self):
        v = extract_variables(_fabric_config())
        content = generate_bicep_parameters(v)
        assert "param fabricCapacityName string" in content
        assert "param fabricCapacitySku string" in content

    def test_has_databricks_params(self):
        v = extract_variables(_databricks_config())
        content = generate_bicep_parameters(v)
        assert "param databricksWorkspaceName string" in content
        assert "param databricksSku string" in content


class TestBicepFabric:
    """Sprint 92: Bicep templates for Fabric."""

    def test_has_target_scope(self):
        v = extract_variables(_fabric_config())
        content = generate_bicep_fabric(v)
        assert "targetScope = 'resourceGroup'" in content

    def test_has_keyvault_resource(self):
        v = extract_variables(_fabric_config())
        content = generate_bicep_fabric(v)
        assert "Microsoft.KeyVault/vaults" in content

    def test_has_storage_resource(self):
        v = extract_variables(_fabric_config())
        content = generate_bicep_fabric(v)
        assert "Microsoft.Storage/storageAccounts" in content

    def test_has_fabric_capacity_resource(self):
        v = extract_variables(_fabric_config())
        content = generate_bicep_fabric(v)
        assert "Microsoft.Fabric/capacities" in content

    def test_has_outputs(self):
        v = extract_variables(_fabric_config())
        content = generate_bicep_fabric(v)
        assert "output keyVaultUri" in content
        assert "output fabricCapacityId" in content


class TestBicepDatabricks:
    """Sprint 92: Bicep templates for Databricks."""

    def test_has_databricks_resource(self):
        v = extract_variables(_databricks_config())
        content = generate_bicep_databricks(v)
        assert "Microsoft.Databricks/workspaces" in content

    def test_has_adls_gen2(self):
        v = extract_variables(_databricks_config())
        content = generate_bicep_databricks(v)
        assert "isHnsEnabled: true" in content

    def test_no_public_ip(self):
        v = extract_variables(_databricks_config())
        content = generate_bicep_databricks(v)
        assert "enableNoPublicIp" in content

    def test_has_outputs(self):
        v = extract_variables(_databricks_config())
        content = generate_bicep_databricks(v)
        assert "output databricksWorkspaceUrl" in content
        assert "output databricksWorkspaceId" in content


class TestIaCOrchestrator:
    """Sprint 92: End-to-end IaC generation."""

    def test_generate_all_fabric(self, tmp_path):
        result = generate_iac(
            config=_fabric_config(), target="fabric", fmt="all",
            output_dir=str(tmp_path),
        )
        assert len(result["terraform"]) >= 2
        assert len(result["bicep"]) >= 2
        assert "variables" in result
        # Files actually written
        for f in result["terraform"] + result["bicep"]:
            assert Path(f).exists()

    def test_generate_all_databricks(self, tmp_path):
        result = generate_iac(
            config=_databricks_config(), target="databricks", fmt="all",
            output_dir=str(tmp_path),
        )
        assert len(result["terraform"]) >= 2
        assert len(result["bicep"]) >= 2

    def test_terraform_only(self, tmp_path):
        result = generate_iac(
            config=_fabric_config(), fmt="terraform",
            output_dir=str(tmp_path),
        )
        assert len(result["terraform"]) >= 2
        assert len(result["bicep"]) == 0

    def test_bicep_only(self, tmp_path):
        result = generate_iac(
            config=_fabric_config(), fmt="bicep",
            output_dir=str(tmp_path),
        )
        assert len(result["terraform"]) == 0
        assert len(result["bicep"]) >= 2

    def test_tfvars_json_written(self, tmp_path):
        generate_iac(
            config=_fabric_config(), target="fabric", fmt="terraform",
            output_dir=str(tmp_path),
        )
        tfvars = tmp_path / "terraform" / "terraform.tfvars.json"
        assert tfvars.exists()
        data = json.loads(tfvars.read_text(encoding="utf-8"))
        assert "resource_group_name" in data


# ═══════════════════════════════════════════════
#  Sprint 93 — Container & Kubernetes Deployment
# ═══════════════════════════════════════════════

class TestDockerfile:
    """Sprint 93: Dockerfile validation."""

    def test_dockerfile_exists(self):
        assert (PROJECT_ROOT / "Dockerfile").exists()

    def test_uses_python_312(self):
        content = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")
        assert "python:3.12" in content

    def test_non_root_user(self):
        content = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")
        assert "USER migrator" in content or "USER 1000" in content

    def test_healthcheck(self):
        content = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")
        assert "HEALTHCHECK" in content

    def test_exposes_port(self):
        content = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")
        assert "EXPOSE 8000" in content

    def test_no_hardcoded_secrets(self):
        content = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")
        assert "password" not in content.lower() or "password" in content.lower() and "key" not in content.lower()
        # More robust check: no ENV with secret values
        for line in content.split("\n"):
            if line.strip().startswith("ENV") and ("TOKEN" in line or "SECRET" in line):
                # Should use ARG or runtime env, not hardcoded values
                assert "=" not in line.split("#")[0] or '""' in line or "${" in line


class TestDockerCompose:
    """Sprint 93: Docker Compose validation."""

    def test_compose_file_exists(self):
        assert (PROJECT_ROOT / "docker-compose.yml").exists()

    def test_has_migration_service(self):
        content = (PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        assert "migration:" in content

    def test_has_web_service(self):
        content = (PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        assert "web:" in content

    def test_has_redis_service(self):
        content = (PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        assert "redis:" in content

    def test_has_healthcheck(self):
        content = (PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        assert "healthcheck:" in content

    def test_has_volume_mounts(self):
        content = (PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        assert "volumes:" in content


class TestKubernetesManifests:
    """Sprint 93: Kubernetes manifest validation."""

    def test_deployment_exists(self):
        assert (PROJECT_ROOT / "k8s" / "deployment.yaml").exists()

    def test_service_exists(self):
        assert (PROJECT_ROOT / "k8s" / "service.yaml").exists()

    def test_configmap_exists(self):
        assert (PROJECT_ROOT / "k8s" / "configmap.yaml").exists()

    def test_secret_exists(self):
        assert (PROJECT_ROOT / "k8s" / "secret.yaml").exists()

    def test_deployment_has_probes(self):
        content = (PROJECT_ROOT / "k8s" / "deployment.yaml").read_text(encoding="utf-8")
        assert "livenessProbe:" in content
        assert "readinessProbe:" in content

    def test_deployment_non_root(self):
        content = (PROJECT_ROOT / "k8s" / "deployment.yaml").read_text(encoding="utf-8")
        assert "runAsNonRoot: true" in content

    def test_deployment_resource_limits(self):
        content = (PROJECT_ROOT / "k8s" / "deployment.yaml").read_text(encoding="utf-8")
        assert "resources:" in content
        assert "limits:" in content
        assert "requests:" in content

    def test_service_clusterip(self):
        content = (PROJECT_ROOT / "k8s" / "service.yaml").read_text(encoding="utf-8")
        assert "ClusterIP" in content
        assert "8000" in content

    def test_secret_no_hardcoded_values(self):
        content = (PROJECT_ROOT / "k8s" / "secret.yaml").read_text(encoding="utf-8")
        # Secret template should have empty placeholder values, not actual secrets
        for line in content.split("\n"):
            if ":" in line and line.strip().startswith(("FABRIC_TOKEN", "DATABRICKS_TOKEN", "DD_API_KEY")):
                val = line.split(":", 1)[1].strip().strip('"')
                assert val == "", f"Secret {line.strip()} should be empty placeholder"


class TestHelmChart:
    """Sprint 93: Helm chart validation."""

    def test_chart_yaml_exists(self):
        assert (PROJECT_ROOT / "charts" / "informatica-migration" / "Chart.yaml").exists()

    def test_values_yaml_exists(self):
        assert (PROJECT_ROOT / "charts" / "informatica-migration" / "values.yaml").exists()

    def test_templates_exist(self):
        tmpl_dir = PROJECT_ROOT / "charts" / "informatica-migration" / "templates"
        assert tmpl_dir.exists()
        assert (tmpl_dir / "deployment.yaml").exists()

    def test_chart_name(self):
        content = (PROJECT_ROOT / "charts" / "informatica-migration" / "Chart.yaml").read_text(encoding="utf-8")
        assert "name: informatica-migration" in content

    def test_values_defaults(self):
        content = (PROJECT_ROOT / "charts" / "informatica-migration" / "values.yaml").read_text(encoding="utf-8")
        assert "replicaCount:" in content
        assert "image:" in content
        assert "resources:" in content


# ═══════════════════════════════════════════════
#  Sprint 94 — CI/CD Pipeline Generation
# ═══════════════════════════════════════════════

from cicd_generator import (
    extract_secrets,
    generate_github_actions,
    generate_azure_devops,
    validate_github_actions,
    validate_azure_devops,
    generate_cicd,
)


class TestSecretExtraction:
    """Sprint 94: Secret mapping."""

    def test_fabric_secrets(self):
        secrets = extract_secrets({"target": "fabric"})
        names = [s["name"] for s in secrets]
        assert "AZURE_CREDENTIALS" in names
        assert "FABRIC_WORKSPACE_ID" in names
        assert "FABRIC_TOKEN" in names

    def test_databricks_secrets(self):
        secrets = extract_secrets({"target": "databricks"})
        names = [s["name"] for s in secrets]
        assert "AZURE_CREDENTIALS" in names
        assert "DATABRICKS_HOST" in names
        assert "DATABRICKS_TOKEN" in names

    def test_required_flag(self):
        secrets = extract_secrets({"target": "fabric"})
        # AZURE_CREDENTIALS should be required
        azure_cred = next(s for s in secrets if s["name"] == "AZURE_CREDENTIALS")
        assert azure_cred["required"] is True

    def test_all_have_env_var(self):
        secrets = extract_secrets({"target": "fabric"})
        for s in secrets:
            assert "env_var" in s and s["env_var"]

    def test_no_duplicates(self):
        secrets = extract_secrets({"target": "fabric"})
        names = [s["name"] for s in secrets]
        assert len(names) == len(set(names))


class TestGitHubActions:
    """Sprint 94: GitHub Actions workflow generation."""

    def test_generates_yaml(self, tmp_path):
        content, path = generate_github_actions({"target": "fabric"}, output_dir=str(tmp_path))
        assert Path(path).exists()
        assert len(content) > 100

    def test_has_required_keys(self, tmp_path):
        content, _ = generate_github_actions({"target": "fabric"}, output_dir=str(tmp_path))
        assert "name:" in content
        assert "on:" in content
        assert "jobs:" in content

    def test_has_assessment_stage(self, tmp_path):
        content, _ = generate_github_actions({"target": "fabric"}, output_dir=str(tmp_path))
        assert "assess:" in content
        assert "run_assessment.py" in content

    def test_has_convert_stage(self, tmp_path):
        content, _ = generate_github_actions({"target": "fabric"}, output_dir=str(tmp_path))
        assert "convert:" in content
        assert "run_sql_migration.py" in content

    def test_has_validate_stage(self, tmp_path):
        content, _ = generate_github_actions({"target": "fabric"}, output_dir=str(tmp_path))
        assert "validate:" in content
        assert "pytest" in content

    def test_has_deploy_stage(self, tmp_path):
        content, _ = generate_github_actions({"target": "fabric"}, output_dir=str(tmp_path))
        assert "deploy:" in content

    def test_uses_secrets(self, tmp_path):
        content, _ = generate_github_actions({"target": "fabric"}, output_dir=str(tmp_path))
        assert "secrets." in content

    def test_no_hardcoded_secrets(self, tmp_path):
        content, _ = generate_github_actions({"target": "fabric"}, output_dir=str(tmp_path))
        # Should not have hardcoded token/password values
        valid, issues = validate_github_actions(content)
        secret_issues = [i for i in issues if "hardcoded" in i.lower()]
        assert len(secret_issues) == 0

    def test_fabric_deploy_step(self, tmp_path):
        content, _ = generate_github_actions({"target": "fabric"}, output_dir=str(tmp_path))
        assert "deploy_to_fabric" in content

    def test_databricks_deploy_step(self, tmp_path):
        content, _ = generate_github_actions({"target": "databricks"}, output_dir=str(tmp_path))
        assert "deploy_to_databricks" in content

    def test_has_workflow_dispatch(self, tmp_path):
        content, _ = generate_github_actions({"target": "fabric"}, output_dir=str(tmp_path))
        assert "workflow_dispatch:" in content

    def test_python_version(self, tmp_path):
        content, _ = generate_github_actions({"target": "fabric"}, output_dir=str(tmp_path))
        assert "3.12" in content


class TestAzureDevOps:
    """Sprint 94: Azure DevOps pipeline generation."""

    def test_generates_yaml(self, tmp_path):
        content, path = generate_azure_devops({"target": "fabric"}, output_dir=str(tmp_path))
        assert Path(path).exists()
        assert len(content) > 100

    def test_has_trigger(self, tmp_path):
        content, _ = generate_azure_devops({"target": "fabric"}, output_dir=str(tmp_path))
        assert "trigger:" in content

    def test_has_stages(self, tmp_path):
        content, _ = generate_azure_devops({"target": "fabric"}, output_dir=str(tmp_path))
        assert "stages:" in content
        assert "Assess" in content
        assert "Convert" in content
        assert "Validate" in content

    def test_has_environment_gates(self, tmp_path):
        content, _ = generate_azure_devops({"target": "fabric"}, output_dir=str(tmp_path))
        assert "environment: development" in content or "environment: production" in content

    def test_has_test_results_publishing(self, tmp_path):
        content, _ = generate_azure_devops({"target": "fabric"}, output_dir=str(tmp_path))
        assert "PublishTestResults" in content

    def test_dev_deploy_condition(self, tmp_path):
        content, _ = generate_azure_devops({"target": "fabric"}, output_dir=str(tmp_path))
        assert "develop" in content

    def test_prod_deploy_condition(self, tmp_path):
        content, _ = generate_azure_devops({"target": "fabric"}, output_dir=str(tmp_path))
        assert "refs/heads/main" in content

    def test_no_hardcoded_secrets(self, tmp_path):
        content, _ = generate_azure_devops({"target": "fabric"}, output_dir=str(tmp_path))
        valid, issues = validate_azure_devops(content)
        secret_issues = [i for i in issues if "hardcoded" in i.lower()]
        assert len(secret_issues) == 0


class TestPipelineValidation:
    """Sprint 94: Pipeline YAML validation."""

    def test_valid_github(self, tmp_path):
        content, _ = generate_github_actions({"target": "fabric"}, output_dir=str(tmp_path))
        valid, issues = validate_github_actions(content)
        assert valid, f"Validation failed: {issues}"

    def test_valid_azdo(self, tmp_path):
        content, _ = generate_azure_devops({"target": "fabric"}, output_dir=str(tmp_path))
        valid, issues = validate_azure_devops(content)
        assert valid, f"Validation failed: {issues}"

    def test_invalid_missing_name(self):
        valid, issues = validate_github_actions("on:\njobs:\n  build:\n")
        assert not valid
        assert any("name:" in i for i in issues)

    def test_invalid_missing_stages(self):
        valid, issues = validate_azure_devops("trigger:\n  branches:\n    include:\n      - main\n")
        assert not valid
        assert any("stages:" in i for i in issues)


class TestCICDOrchestrator:
    """Sprint 94: End-to-end CI/CD generation."""

    def test_generate_all(self, tmp_path):
        result = generate_cicd(
            config={"target": "fabric"}, fmt="all",
            output_dir=str(tmp_path),
        )
        assert result["github"] is not None
        assert result["azdo"] is not None
        assert len(result["secrets"]) > 0

    def test_github_only(self, tmp_path):
        result = generate_cicd(
            config={"target": "fabric"}, fmt="github",
            output_dir=str(tmp_path),
        )
        assert result["github"] is not None
        assert result["azdo"] is None

    def test_azdo_only(self, tmp_path):
        result = generate_cicd(
            config={"target": "databricks"}, fmt="azdo",
            output_dir=str(tmp_path),
        )
        assert result["azdo"] is not None
        assert result["github"] is None

    def test_validation_results(self, tmp_path):
        result = generate_cicd(
            config={"target": "fabric"}, fmt="all",
            output_dir=str(tmp_path),
        )
        assert "github" in result["validation"]
        assert "azdo" in result["validation"]
        assert result["validation"]["github"]["valid"] is True
        assert result["validation"]["azdo"]["valid"] is True
