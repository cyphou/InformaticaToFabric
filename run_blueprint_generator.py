"""
CDC / Real-Time Deployment Blueprint Generator

Generates infrastructure blueprints for CDC/real-time migration patterns that
combine Azure Functions + Event Hub + API Management (APIM).

Architecture patterns generated:
  1. CDC Pipeline:       Source DB → SQL Trigger/Change Feed → Function → Event Hub → Fabric/Databricks
  2. Real-Time Ingest:   Event Producer → Event Hub → Function → Delta Lake
  3. ESB/API Gateway:    Client → APIM → HTTP Function → Event Hub → Function → Target

Outputs:
  output/blueprints/
    architecture.md              — Architecture overview & diagram
    bicep/
      main.bicep                 — All resources (Event Hub + Functions + APIM + Storage)
      parameters.json            — Deployment parameters
    terraform/
      main.tf                    — All resources
      variables.tf               — Terraform variables
    scripts/
      deploy.ps1                 — PowerShell deployment script
      deploy.sh                  — Bash deployment script
      validate.ps1               — Post-deployment validation

Usage:
    python run_blueprint_generator.py
    python run_blueprint_generator.py --target fabric
    python run_blueprint_generator.py --target databricks --region westeurope
"""

import json
import os
import textwrap
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output" / "blueprints"
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"


from migration_utils import get_target as _get_target


# ─────────────────────────────────────────────
#  Inventory Analysis — detect CDC/RT patterns
# ─────────────────────────────────────────────

def analyze_candidates(inventory_path=None):
    """Analyze inventory for CDC, real-time, and ESB/API candidates.

    Returns dict with categorized candidates and recommended architecture.
    """
    inv_path = inventory_path or INVENTORY_PATH
    if not Path(inv_path).exists():
        return {"cdc": [], "realtime": [], "esb_api": [], "summary": {
            "total_candidates": 0, "cdc_count": 0, "realtime_count": 0,
            "esb_api_count": 0, "needs_event_hub": False, "needs_apim": False,
            "needs_functions": False,
        }}

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    cdc = []
    realtime = []
    esb_api = []

    for mapping in inventory.get("mappings", []):
        func_cand = mapping.get("functions_candidate", {})
        streaming = mapping.get("streaming", {})
        cdc_info = mapping.get("cdc", {})

        trigger_type = func_cand.get("trigger_type", "")
        is_candidate = func_cand.get("is_candidate", False)

        entry = {
            "name": mapping["name"],
            "trigger_type": trigger_type,
            "sources": mapping.get("sources", []),
            "targets": mapping.get("targets", []),
            "complexity": mapping.get("complexity", "Simple"),
        }

        if cdc_info or trigger_type in ("sql_trigger", "cosmos_trigger"):
            entry["cdc_type"] = cdc_info.get("cdc_type", "upsert_only")
            entry["merge_keys"] = cdc_info.get("merge_keys", [])
            cdc.append(entry)
        elif trigger_type == "event_hub" or streaming.get("is_streaming"):
            entry["event_hub_name"] = (
                streaming.get("streaming_sources", [{}])[0].get("topic", "events")
                if streaming.get("streaming_sources") else "events"
            )
            realtime.append(entry)
        elif trigger_type == "http" or is_candidate:
            esb_api.append(entry)

    # Determine which components are needed
    needs_event_hub = bool(cdc or realtime)
    needs_apim = bool(esb_api)
    needs_functions = bool(cdc or realtime or esb_api)

    return {
        "cdc": cdc,
        "realtime": realtime,
        "esb_api": esb_api,
        "summary": {
            "total_candidates": len(cdc) + len(realtime) + len(esb_api),
            "cdc_count": len(cdc),
            "realtime_count": len(realtime),
            "esb_api_count": len(esb_api),
            "needs_event_hub": needs_event_hub,
            "needs_apim": needs_apim,
            "needs_functions": needs_functions,
        },
    }


# ─────────────────────────────────────────────
#  Architecture Document
# ─────────────────────────────────────────────

def generate_architecture_doc(analysis, target="fabric", region="eastus"):
    """Generate architecture overview markdown with Mermaid diagrams."""
    s = analysis["summary"]
    cdc = analysis["cdc"]
    realtime = analysis["realtime"]
    esb_api = analysis["esb_api"]

    lines = [
        "# CDC / Real-Time Deployment Blueprint",
        "",
        f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"**Target Platform:** {target.title()}",
        f"**Region:** {region}",
        "",
        "## Summary",
        "",
        f"| Pattern | Count | Components |",
        f"|---------|-------|------------|",
        f"| CDC (Change Data Capture) | {s['cdc_count']} | SQL Trigger / Cosmos Change Feed → Function → Event Hub → {target.title()} |",
        f"| Real-Time Streaming | {s['realtime_count']} | Event Producer → Event Hub → Function → Delta Lake |",
        f"| ESB / API Gateway | {s['esb_api_count']} | Client → APIM → HTTP Function → Target |",
        "",
        "## Architecture",
        "",
        "```mermaid",
        "flowchart LR",
    ]

    # CDC flow
    if cdc:
        lines.extend([
            '    subgraph CDC["CDC Pipeline"]',
            '        DB[(Source DB)] -->|Change Tracking| FN_CDC["Azure Function\\nSQL Trigger"]',
            '        COSMOS[(Cosmos DB)] -->|Change Feed| FN_COSM["Azure Function\\nCosmos Trigger"]',
            '        FN_CDC --> EH["Event Hub\\n(ingestion)"]',
            '        FN_COSM --> EH',
            '    end',
        ])

    # Real-time flow
    if realtime:
        lines.extend([
            '    subgraph RT["Real-Time Ingest"]',
            '        PROD["Event Producers"] --> EH_RT["Event Hub\\n(streaming)"]',
            '        EH_RT --> FN_RT["Azure Function\\nEvent Hub Trigger"]',
            '    end',
        ])

    # ESB/API flow
    if esb_api:
        lines.extend([
            '    subgraph API["API Gateway"]',
            '        CLIENT["API Clients"] --> APIM["Azure API\\nManagement"]',
            '        APIM --> FN_HTTP["Azure Function\\nHTTP Trigger"]',
            '    end',
        ])

    # Target
    target_node = "FABRIC" if target == "fabric" else "DBX"
    target_label = "Fabric\\nLakehouse" if target == "fabric" else "Databricks\\nDelta Lake"
    lines.append(f'    {target_node}[("{target_label}")]')

    if cdc:
        lines.append(f'    EH --> FN_SINK["Azure Function\\nSink"] --> {target_node}')
    if realtime:
        lines.append(f'    FN_RT --> {target_node}')
    if esb_api:
        lines.append(f'    FN_HTTP --> {target_node}')

    lines.extend([
        '    MONITOR["Azure Monitor\\n+ App Insights"]',
        f'    FN_CDC -.-> MONITOR' if cdc else '',
        f'    FN_RT -.-> MONITOR' if realtime else '',
        f'    FN_HTTP -.-> MONITOR' if esb_api else '',
        "```",
        "",
        "## Components",
        "",
    ])

    # Remove empty lines from mermaid
    lines = [l for l in lines if l != '']

    # Component details
    if s["needs_event_hub"]:
        lines.extend([
            "### Azure Event Hub",
            "",
            f"- **Namespace SKU:** Standard (auto-inflate enabled)",
            f"- **Partitions:** 4 (scale up for >1000 events/sec)",
            f"- **Retention:** 7 days (24h for dev)",
            f"- **Consumer groups:** `$Default`, `fabric-sink`, `monitoring`",
            f"- **Event Hubs:** {len(set(r.get('event_hub_name', 'events') for r in realtime)) + (1 if cdc else 0)}",
            "",
        ])

    lines.extend([
        "### Azure Functions",
        "",
        f"- **Plan:** Consumption (scale to zero) or Premium (VNET, longer timeout)",
        f"- **Runtime:** Python 3.11+",
        f"- **Functions:** {s['total_candidates']}",
        f"- **App Insights:** Enabled (sampling: 5%)",
        "",
    ])

    if s["needs_apim"]:
        lines.extend([
            "### Azure API Management (APIM)",
            "",
            f"- **SKU:** Consumption (serverless) or Developer (for testing)",
            f"- **APIs:** {len(esb_api)} HTTP function backends",
            f"- **Policies:** Rate limiting (100 req/min), JWT validation, CORS",
            f"- **Products:** `migration-api` (subscription required)",
            "",
        ])

    # Mapping tables
    if cdc:
        lines.extend([
            "## CDC Mappings",
            "",
            "| Mapping | Source | Target | CDC Type | Merge Key |",
            "|---------|--------|--------|----------|-----------|",
        ])
        for c in cdc:
            src = c["sources"][0] if c["sources"] else "-"
            tgt = c["targets"][0] if c["targets"] else "-"
            lines.append(f"| {c['name']} | {src} | {tgt} | {c.get('cdc_type', 'upsert')} | {', '.join(c.get('merge_keys', ['-']))} |")
        lines.append("")

    if realtime:
        lines.extend([
            "## Real-Time Mappings",
            "",
            "| Mapping | Event Hub | Target | Complexity |",
            "|---------|-----------|--------|------------|",
        ])
        for r in realtime:
            tgt = r["targets"][0] if r["targets"] else "-"
            lines.append(f"| {r['name']} | {r.get('event_hub_name', 'events')} | {tgt} | {r['complexity']} |")
        lines.append("")

    if esb_api:
        lines.extend([
            "## API / ESB Mappings",
            "",
            "| Mapping | Route | Target | APIM Policy |",
            "|---------|-------|--------|-------------|",
        ])
        for a in esb_api:
            route = a["name"].lower()
            tgt = a["targets"][0] if a["targets"] else "-"
            lines.append(f"| {a['name']} | `/{route}` | {tgt} | rate-limit + JWT |")
        lines.append("")

    return "\n".join(lines)


# ─────────────────────────────────────────────
#  Bicep Template Generation
# ─────────────────────────────────────────────

def generate_bicep_blueprint(analysis, target="fabric", region="eastus",
                             resource_prefix="mig", env="dev"):
    """Generate Azure Bicep template for the full CDC/RT blueprint."""
    s = analysis["summary"]

    template = textwrap.dedent(f"""\
        // CDC / Real-Time Deployment Blueprint — Bicep
        // Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
        // Target: {target.title()} | Region: {region}

        targetScope = 'resourceGroup'

        // ── Parameters ──
        @description('Azure region')
        param location string = '{region}'

        @description('Environment (dev/test/prod)')
        @allowed(['dev', 'test', 'prod'])
        param environment string = '{env}'

        @description('Resource name prefix')
        param prefix string = '{resource_prefix}'

        @description('Function App name')
        param functionAppName string = '${{prefix}}-func-${{environment}}'

        @description('Storage account name (3-24 chars, lowercase)')
        param storageAccountName string = '${{prefix}}st${{environment}}'

        @description('App Insights name')
        param appInsightsName string = '${{prefix}}-insights-${{environment}}'
    """)

    # Event Hub params
    if s["needs_event_hub"]:
        template += textwrap.dedent(f"""\

            @description('Event Hub namespace name')
            param eventHubNamespaceName string = '${{prefix}}-ehns-${{environment}}'

            @description('Event Hub SKU')
            @allowed(['Basic', 'Standard', 'Premium'])
            param eventHubSku string = 'Standard'

            @description('Event Hub partition count')
            param eventHubPartitionCount int = 4

            @description('Event Hub message retention (days)')
            param eventHubRetentionDays int = environment == 'prod' ? 7 : 1
        """)

    # APIM params
    if s["needs_apim"]:
        template += textwrap.dedent(f"""\

            @description('API Management name')
            param apimName string = '${{prefix}}-apim-${{environment}}'

            @description('APIM SKU')
            @allowed(['Consumption', 'Developer', 'Basic', 'Standard', 'Premium'])
            param apimSku string = environment == 'prod' ? 'Standard' : 'Consumption'

            @description('APIM publisher email')
            param apimPublisherEmail string = 'admin@contoso.com'

            @description('APIM publisher name')
            param apimPublisherName string = 'Migration Team'
        """)

    # ── Resources ──
    template += textwrap.dedent("""\

        // ── Log Analytics Workspace ──
        resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
          name: '${prefix}-log-${environment}'
          location: location
          properties: {
            sku: { name: 'PerGB2018' }
            retentionInDays: 30
          }
        }

        // ── Application Insights ──
        resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
          name: appInsightsName
          location: location
          kind: 'web'
          properties: {
            Application_Type: 'web'
            WorkspaceResourceId: logAnalytics.id
          }
        }

        // ── Storage Account (Functions + checkpointing) ──
        resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
          name: storageAccountName
          location: location
          kind: 'StorageV2'
          sku: { name: 'Standard_LRS' }
          properties: {
            supportsHttpsTrafficOnly: true
            minimumTlsVersion: 'TLS1_2'
            allowBlobPublicAccess: false
          }
          tags: { environment: environment, project: 'informatica-migration' }
        }

        // ── App Service Plan (Consumption) ──
        resource hostingPlan 'Microsoft.Web/serverfarms@2023-01-01' = {
          name: '${prefix}-plan-${environment}'
          location: location
          sku: {
            name: 'Y1'
            tier: 'Dynamic'
          }
          kind: 'functionapp'
          properties: { reserved: true }  // Linux
        }

        // ── Function App ──
        resource functionApp 'Microsoft.Web/sites@2023-01-01' = {
          name: functionAppName
          location: location
          kind: 'functionapp,linux'
          properties: {
            serverFarmId: hostingPlan.id
            httpsOnly: true
            siteConfig: {
              pythonVersion: '3.11'
              linuxFxVersion: 'PYTHON|3.11'
              appSettings: [
                { name: 'AzureWebJobsStorage', value: 'DefaultEndpointsProtocol=https;AccountName=${storageAccount.name};AccountKey=${storageAccount.listKeys().keys[0].value}' }
                { name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }
                { name: 'FUNCTIONS_WORKER_RUNTIME', value: 'python' }
                { name: 'APPINSIGHTS_INSTRUMENTATIONKEY', value: appInsights.properties.InstrumentationKey }
                { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: appInsights.properties.ConnectionString }
    """)

    if s["needs_event_hub"]:
        template += textwrap.dedent("""\
                { name: 'EventHubConnection__fullyQualifiedNamespace', value: '${eventHubNamespace.name}.servicebus.windows.net' }
    """)

    template += textwrap.dedent("""\
              ]
            }
          }
          tags: { environment: environment, project: 'informatica-migration' }
        }
    """)

    # ── Event Hub ──
    if s["needs_event_hub"]:
        template += textwrap.dedent("""\

        // ── Event Hub Namespace ──
        resource eventHubNamespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
          name: eventHubNamespaceName
          location: location
          sku: {
            name: eventHubSku
            tier: eventHubSku
            capacity: 1
          }
          properties: {
            isAutoInflateEnabled: true
            maximumThroughputUnits: 10
          }
          tags: { environment: environment, project: 'informatica-migration' }
        }

        // ── Event Hub: CDC events ──
        resource eventHubCdc 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
          parent: eventHubNamespace
          name: 'cdc-events'
          properties: {
            partitionCount: eventHubPartitionCount
            messageRetentionInDays: eventHubRetentionDays
          }
        }

        // Consumer groups
        resource cgDefault 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2024-01-01' = {
          parent: eventHubCdc
          name: 'fabric-sink'
        }

        resource cgMonitor 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2024-01-01' = {
          parent: eventHubCdc
          name: 'monitoring'
        }

        // ── Event Hub: real-time events ──
        resource eventHubRealtime 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
          parent: eventHubNamespace
          name: 'realtime-events'
          properties: {
            partitionCount: eventHubPartitionCount
            messageRetentionInDays: eventHubRetentionDays
          }
        }

        // ── Managed Identity — Function → Event Hub (Azure RBAC) ──
        resource functionAppIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
          name: '${prefix}-func-identity-${environment}'
          location: location
        }

        // Event Hub Data Sender role for Function App
        resource ehSenderRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
          name: guid(eventHubNamespace.id, functionApp.id, 'sender')
          scope: eventHubNamespace
          properties: {
            roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2b629674-e913-4c01-ae53-ef4638d8f975') // Event Hub Data Sender
            principalId: functionApp.identity.principalId
            principalType: 'ServicePrincipal'
          }
        }

        // Event Hub Data Receiver role
        resource ehReceiverRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
          name: guid(eventHubNamespace.id, functionApp.id, 'receiver')
          scope: eventHubNamespace
          properties: {
            roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'a638d3c7-ab3a-418d-83e6-5f17a39d4fde') // Event Hub Data Receiver
            principalId: functionApp.identity.principalId
            principalType: 'ServicePrincipal'
          }
        }
    """)

    # ── APIM ──
    if s["needs_apim"]:
        api_count = len(analysis["esb_api"])
        template += textwrap.dedent(f"""\

        // ── API Management ──
        resource apim 'Microsoft.ApiManagement/service@2023-05-01-preview' = {{
          name: apimName
          location: location
          sku: {{
            name: apimSku
            capacity: apimSku == 'Consumption' ? 0 : 1
          }}
          properties: {{
            publisherEmail: apimPublisherEmail
            publisherName: apimPublisherName
          }}
          tags: {{ environment: environment, project: 'informatica-migration' }}
        }}

        // ── APIM Product ──
        resource apimProduct 'Microsoft.ApiManagement/service/products@2023-05-01-preview' = {{
          parent: apim
          name: 'migration-api'
          properties: {{
            displayName: 'Migration API'
            description: 'APIs migrated from Informatica ESB/HTTP patterns'
            subscriptionRequired: true
            approvalRequired: false
            state: 'published'
          }}
        }}

        // ── APIM Backend (Functions) ──
        resource apimBackend 'Microsoft.ApiManagement/service/backends@2023-05-01-preview' = {{
          parent: apim
          name: 'functions-backend'
          properties: {{
            url: 'https://${{functionApp.properties.defaultHostName}}/api'
            protocol: 'http'
            tls: {{
              validateCertificateChain: true
              validateCertificateName: true
            }}
          }}
        }}

        // ── APIM API (one per ESB mapping) ──
        resource apimApi 'Microsoft.ApiManagement/service/apis@2023-05-01-preview' = {{
          parent: apim
          name: 'migration-functions-api'
          properties: {{
            displayName: 'Migration Functions'
            path: 'migration'
            protocols: ['https']
            serviceUrl: 'https://${{functionApp.properties.defaultHostName}}/api'
            subscriptionRequired: true
          }}
        }}

        // ── APIM Policy (rate limiting + JWT) ──
        resource apimApiPolicy 'Microsoft.ApiManagement/service/apis/policies@2023-05-01-preview' = {{
          parent: apimApi
          name: 'policy'
          properties: {{
            value: '<policies><inbound><base /><rate-limit calls="100" renewal-period="60" /><validate-jwt header-name="Authorization" failed-validation-httpcode="401" /></inbound><backend><base /></backend><outbound><base /></outbound></policies>'
            format: 'rawxml'
          }}
        }}
    """)

    # ── Outputs ──
    template += textwrap.dedent("""\

        // ── Outputs ──
        output functionAppName string = functionApp.properties.defaultHostName
        output appInsightsKey string = appInsights.properties.InstrumentationKey
    """)
    if s["needs_event_hub"]:
        template += "output eventHubNamespace string = eventHubNamespace.name\n"
    if s["needs_apim"]:
        template += "output apimGatewayUrl string = apim.properties.gatewayUrl\n"

    return template


def generate_bicep_parameters(analysis, target="fabric", region="eastus",
                               resource_prefix="mig", env="dev"):
    """Generate parameters.json for the Bicep template."""
    s = analysis["summary"]
    params = {
        "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#",
        "contentVersion": "1.0.0.0",
        "parameters": {
            "location": {"value": region},
            "environment": {"value": env},
            "prefix": {"value": resource_prefix},
            "functionAppName": {"value": f"{resource_prefix}-func-{env}"},
            "storageAccountName": {"value": f"{resource_prefix}st{env}"},
            "appInsightsName": {"value": f"{resource_prefix}-insights-{env}"},
        },
    }
    if s["needs_event_hub"]:
        params["parameters"]["eventHubNamespaceName"] = {"value": f"{resource_prefix}-ehns-{env}"}
        params["parameters"]["eventHubSku"] = {"value": "Standard"}
        params["parameters"]["eventHubPartitionCount"] = {"value": 4}
        params["parameters"]["eventHubRetentionDays"] = {"value": 7 if env == "prod" else 1}
    if s["needs_apim"]:
        params["parameters"]["apimName"] = {"value": f"{resource_prefix}-apim-{env}"}
        params["parameters"]["apimSku"] = {"value": "Standard" if env == "prod" else "Consumption"}
        params["parameters"]["apimPublisherEmail"] = {"value": "admin@contoso.com"}
        params["parameters"]["apimPublisherName"] = {"value": "Migration Team"}

    return params


# ─────────────────────────────────────────────
#  Terraform Generation
# ─────────────────────────────────────────────

def generate_terraform_blueprint(analysis, target="fabric", region="eastus",
                                 resource_prefix="mig", env="dev"):
    """Generate Terraform HCL for the full CDC/RT blueprint."""
    s = analysis["summary"]

    tf = textwrap.dedent(f"""\
        # CDC / Real-Time Deployment Blueprint — Terraform
        # Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
        # Target: {target.title()} | Region: {region}

        terraform {{
          required_providers {{
            azurerm = {{
              source  = "hashicorp/azurerm"
              version = "~> 3.90"
            }}
          }}
        }}

        provider "azurerm" {{
          features {{}}
        }}

        locals {{
          prefix      = var.prefix
          environment = var.environment
          tags = {{
            environment = var.environment
            project     = "informatica-migration"
            managed_by  = "terraform"
          }}
        }}

        # ── Resource Group ──
        resource "azurerm_resource_group" "blueprint" {{
          name     = "${{local.prefix}}-rg-${{local.environment}}"
          location = var.location
          tags     = local.tags
        }}

        # ── Log Analytics ──
        resource "azurerm_log_analytics_workspace" "blueprint" {{
          name                = "${{local.prefix}}-log-${{local.environment}}"
          location            = azurerm_resource_group.blueprint.location
          resource_group_name = azurerm_resource_group.blueprint.name
          sku                 = "PerGB2018"
          retention_in_days   = 30
          tags                = local.tags
        }}

        # ── Application Insights ──
        resource "azurerm_application_insights" "blueprint" {{
          name                = "${{local.prefix}}-insights-${{local.environment}}"
          location            = azurerm_resource_group.blueprint.location
          resource_group_name = azurerm_resource_group.blueprint.name
          workspace_id        = azurerm_log_analytics_workspace.blueprint.id
          application_type    = "web"
          tags                = local.tags
        }}

        # ── Storage Account ──
        resource "azurerm_storage_account" "blueprint" {{
          name                     = "${{local.prefix}}st${{local.environment}}"
          resource_group_name      = azurerm_resource_group.blueprint.name
          location                 = azurerm_resource_group.blueprint.location
          account_tier             = "Standard"
          account_replication_type = "LRS"
          min_tls_version          = "TLS1_2"
          tags                     = local.tags
        }}

        # ── App Service Plan (Consumption) ──
        resource "azurerm_service_plan" "blueprint" {{
          name                = "${{local.prefix}}-plan-${{local.environment}}"
          resource_group_name = azurerm_resource_group.blueprint.name
          location            = azurerm_resource_group.blueprint.location
          os_type             = "Linux"
          sku_name            = "Y1"
          tags                = local.tags
        }}

        # ── Function App ──
        resource "azurerm_linux_function_app" "blueprint" {{
          name                = "${{local.prefix}}-func-${{local.environment}}"
          resource_group_name = azurerm_resource_group.blueprint.name
          location            = azurerm_resource_group.blueprint.location
          service_plan_id     = azurerm_service_plan.blueprint.id
          storage_account_name       = azurerm_storage_account.blueprint.name
          storage_account_access_key = azurerm_storage_account.blueprint.primary_access_key
          tags                = local.tags

          identity {{
            type = "SystemAssigned"
          }}

          site_config {{
            application_stack {{
              python_version = "3.11"
            }}
            application_insights_connection_string = azurerm_application_insights.blueprint.connection_string
          }}

          app_settings = {{
            "FUNCTIONS_WORKER_RUNTIME" = "python"
    """)

    if s["needs_event_hub"]:
        tf += '        "EventHubConnection__fullyQualifiedNamespace" = "${azurerm_eventhub_namespace.blueprint[0].name}.servicebus.windows.net"\n'

    tf += textwrap.dedent("""\
          }
        }
    """)

    # ── Event Hub ──
    if s["needs_event_hub"]:
        tf += textwrap.dedent(f"""\

        # ── Event Hub Namespace ──
        resource "azurerm_eventhub_namespace" "blueprint" {{
          count               = var.enable_event_hub ? 1 : 0
          name                = "${{local.prefix}}-ehns-${{local.environment}}"
          location            = azurerm_resource_group.blueprint.location
          resource_group_name = azurerm_resource_group.blueprint.name
          sku                 = "Standard"
          capacity            = 1
          auto_inflate_enabled   = true
          maximum_throughput_units = 10
          tags                = local.tags
        }}

        resource "azurerm_eventhub" "cdc" {{
          count               = var.enable_event_hub ? 1 : 0
          name                = "cdc-events"
          namespace_name      = azurerm_eventhub_namespace.blueprint[0].name
          resource_group_name = azurerm_resource_group.blueprint.name
          partition_count     = var.event_hub_partition_count
          message_retention   = var.event_hub_retention_days
        }}

        resource "azurerm_eventhub" "realtime" {{
          count               = var.enable_event_hub ? 1 : 0
          name                = "realtime-events"
          namespace_name      = azurerm_eventhub_namespace.blueprint[0].name
          resource_group_name = azurerm_resource_group.blueprint.name
          partition_count     = var.event_hub_partition_count
          message_retention   = var.event_hub_retention_days
        }}

        resource "azurerm_eventhub_consumer_group" "fabric_sink" {{
          count               = var.enable_event_hub ? 1 : 0
          name                = "fabric-sink"
          namespace_name      = azurerm_eventhub_namespace.blueprint[0].name
          eventhub_name       = azurerm_eventhub.cdc[0].name
          resource_group_name = azurerm_resource_group.blueprint.name
        }}

        # ── RBAC: Function → Event Hub Data Sender ──
        resource "azurerm_role_assignment" "func_eh_sender" {{
          count                = var.enable_event_hub ? 1 : 0
          scope                = azurerm_eventhub_namespace.blueprint[0].id
          role_definition_name = "Azure Event Hubs Data Sender"
          principal_id         = azurerm_linux_function_app.blueprint.identity[0].principal_id
        }}

        resource "azurerm_role_assignment" "func_eh_receiver" {{
          count                = var.enable_event_hub ? 1 : 0
          scope                = azurerm_eventhub_namespace.blueprint[0].id
          role_definition_name = "Azure Event Hubs Data Receiver"
          principal_id         = azurerm_linux_function_app.blueprint.identity[0].principal_id
        }}
    """)

    # ── APIM ──
    if s["needs_apim"]:
        tf += textwrap.dedent(f"""\

        # ── API Management ──
        resource "azurerm_api_management" "blueprint" {{
          count               = var.enable_apim ? 1 : 0
          name                = "${{local.prefix}}-apim-${{local.environment}}"
          location            = azurerm_resource_group.blueprint.location
          resource_group_name = azurerm_resource_group.blueprint.name
          publisher_name      = "Migration Team"
          publisher_email     = "admin@contoso.com"
          sku_name            = var.apim_sku
          tags                = local.tags
        }}

        resource "azurerm_api_management_api" "functions" {{
          count               = var.enable_apim ? 1 : 0
          name                = "migration-functions"
          resource_group_name = azurerm_resource_group.blueprint.name
          api_management_name = azurerm_api_management.blueprint[0].name
          revision            = "1"
          display_name        = "Migration Functions API"
          path                = "migration"
          protocols           = ["https"]
          service_url         = "https://${{azurerm_linux_function_app.blueprint.default_hostname}}/api"
          subscription_required = true
        }}

        resource "azurerm_api_management_product" "migration" {{
          count               = var.enable_apim ? 1 : 0
          product_id          = "migration-api"
          api_management_name = azurerm_api_management.blueprint[0].name
          resource_group_name = azurerm_resource_group.blueprint.name
          display_name        = "Migration API"
          subscription_required = true
          published           = true
        }}
    """)

    # ── Outputs ──
    tf += textwrap.dedent("""\

        # ── Outputs ──
        output "function_app_hostname" {
          value = azurerm_linux_function_app.blueprint.default_hostname
        }

        output "app_insights_key" {
          value     = azurerm_application_insights.blueprint.instrumentation_key
          sensitive = true
        }
    """)
    if s["needs_event_hub"]:
        tf += textwrap.dedent("""\
        output "event_hub_namespace" {
          value = var.enable_event_hub ? azurerm_eventhub_namespace.blueprint[0].name : ""
        }
    """)
    if s["needs_apim"]:
        tf += textwrap.dedent("""\
        output "apim_gateway_url" {
          value = var.enable_apim ? azurerm_api_management.blueprint[0].gateway_url : ""
        }
    """)

    return tf


def generate_terraform_variables_blueprint(analysis, region="eastus",
                                            resource_prefix="mig", env="dev"):
    """Generate variables.tf for the blueprint."""
    s = analysis["summary"]
    lines = [
        '# CDC / Real-Time Blueprint — Variables',
        '',
        'variable "location" {',
        '  type    = string',
        f'  default = "{region}"',
        '}',
        '',
        'variable "environment" {',
        '  type    = string',
        f'  default = "{env}"',
        '}',
        '',
        'variable "prefix" {',
        '  type    = string',
        f'  default = "{resource_prefix}"',
        '}',
    ]

    if s["needs_event_hub"]:
        lines.extend([
            '',
            'variable "enable_event_hub" {',
            '  type    = bool',
            '  default = true',
            '}',
            '',
            'variable "event_hub_partition_count" {',
            '  type    = number',
            '  default = 4',
            '}',
            '',
            'variable "event_hub_retention_days" {',
            '  type    = number',
            f'  default = {7 if env == "prod" else 1}',
            '}',
        ])

    if s["needs_apim"]:
        lines.extend([
            '',
            'variable "enable_apim" {',
            '  type    = bool',
            '  default = true',
            '}',
            '',
            'variable "apim_sku" {',
            '  type    = string',
            f'  default = "{("Standard_1" if env == "prod" else "Consumption_0")}"',
            '}',
        ])

    return "\n".join(lines)


# ─────────────────────────────────────────────
#  Deployment Scripts
# ─────────────────────────────────────────────

def generate_deploy_ps1(analysis, resource_prefix="mig", env="dev", region="eastus"):
    """Generate PowerShell deployment script."""
    s = analysis["summary"]
    return textwrap.dedent(f"""\
        # CDC / Real-Time Blueprint — Deployment Script (PowerShell)
        # Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}

        param(
            [string]$Environment = "{env}",
            [string]$Location = "{region}",
            [string]$Prefix = "{resource_prefix}",
            [string]$ResourceGroup = "$Prefix-rg-$Environment"
        )

        $ErrorActionPreference = "Stop"
        Write-Host "=== CDC/RT Blueprint Deployment ===" -ForegroundColor Cyan

        # 1. Create Resource Group
        Write-Host "`n[1/5] Creating resource group: $ResourceGroup" -ForegroundColor Yellow
        az group create --name $ResourceGroup --location $Location --tags environment=$Environment project=informatica-migration | Out-Null

        # 2. Deploy Bicep template
        Write-Host "[2/5] Deploying Bicep template..." -ForegroundColor Yellow
        $deployment = az deployment group create `
            --resource-group $ResourceGroup `
            --template-file ./bicep/main.bicep `
            --parameters ./bicep/parameters.json `
            --parameters environment=$Environment `
            --name "blueprint-$(Get-Date -Format 'yyyyMMdd-HHmmss')" | ConvertFrom-Json

        if ($LASTEXITCODE -ne 0) {{
            Write-Error "Deployment failed!"
            exit 1
        }}

        $funcHostname = $deployment.properties.outputs.functionAppName.value
        Write-Host "  Function App: https://$funcHostname" -ForegroundColor Green
        {"$ehNamespace = $deployment.properties.outputs.eventHubNamespace.value" if s["needs_event_hub"] else "# Event Hub not needed"}
        {"$apimUrl = $deployment.properties.outputs.apimGatewayUrl.value" if s["needs_apim"] else "# APIM not needed"}

        # 3. Deploy Function App code
        Write-Host "[3/5] Deploying function code..." -ForegroundColor Yellow
        $funcAppName = "$Prefix-func-$Environment"
        Push-Location ../functions
        func azure functionapp publish $funcAppName --python
        Pop-Location

        # 4. Configure Function App settings
        Write-Host "[4/5] Configuring app settings..." -ForegroundColor Yellow
        az functionapp config appsettings set `
            --name $funcAppName `
            --resource-group $ResourceGroup `
            --settings "MIGRATION_TARGET={_get_target()}" | Out-Null

        # 5. Validate deployment
        Write-Host "[5/5] Validating deployment..." -ForegroundColor Yellow
        $funcStatus = az functionapp show --name $funcAppName --resource-group $ResourceGroup --query "state" -o tsv
        Write-Host "  Function App status: $funcStatus" -ForegroundColor Green
        {"" if not s['needs_event_hub'] else '$ehStatus = az eventhubs namespace show --name $ehNamespace --resource-group $ResourceGroup --query "status" -o tsv\\nWrite-Host "  Event Hub status: $ehStatus" -ForegroundColor Green'}
        {"" if not s['needs_apim'] else 'Write-Host "  APIM Gateway: $apimUrl" -ForegroundColor Green'}

        Write-Host "`n=== Deployment Complete ===" -ForegroundColor Cyan
    """)


def generate_deploy_sh(analysis, resource_prefix="mig", env="dev", region="eastus"):
    """Generate Bash deployment script."""
    s = analysis["summary"]
    return textwrap.dedent(f"""\
        #!/bin/bash
        # CDC / Real-Time Blueprint — Deployment Script (Bash)
        # Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
        set -euo pipefail

        ENVIRONMENT="${{1:-{env}}}"
        LOCATION="${{2:-{region}}}"
        PREFIX="${{3:-{resource_prefix}}}"
        RG="${{PREFIX}}-rg-${{ENVIRONMENT}}"

        echo "=== CDC/RT Blueprint Deployment ==="

        # 1. Create Resource Group
        echo "[1/5] Creating resource group: $RG"
        az group create --name "$RG" --location "$LOCATION" \\
            --tags environment="$ENVIRONMENT" project=informatica-migration >/dev/null

        # 2. Deploy Bicep
        echo "[2/5] Deploying Bicep template..."
        az deployment group create \\
            --resource-group "$RG" \\
            --template-file ./bicep/main.bicep \\
            --parameters ./bicep/parameters.json \\
            --parameters environment="$ENVIRONMENT" \\
            --name "blueprint-$(date +%Y%m%d-%H%M%S)"

        # 3. Deploy function code
        echo "[3/5] Deploying function code..."
        FUNC_APP="${{PREFIX}}-func-${{ENVIRONMENT}}"
        pushd ../functions >/dev/null
        func azure functionapp publish "$FUNC_APP" --python
        popd >/dev/null

        # 4. Configure
        echo "[4/5] Configuring app settings..."
        az functionapp config appsettings set \\
            --name "$FUNC_APP" --resource-group "$RG" \\
            --settings "MIGRATION_TARGET={_get_target()}" >/dev/null

        # 5. Validate
        echo "[5/5] Validating..."
        STATUS=$(az functionapp show --name "$FUNC_APP" --resource-group "$RG" --query "state" -o tsv)
        echo "  Function App: $STATUS"

        echo "=== Deployment Complete ==="
    """)


def generate_validate_ps1(analysis, resource_prefix="mig", env="dev"):
    """Generate post-deployment validation script."""
    s = analysis["summary"]
    return textwrap.dedent(f"""\
        # CDC / Real-Time Blueprint — Post-Deployment Validation
        # Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}

        param(
            [string]$Environment = "{env}",
            [string]$Prefix = "{resource_prefix}",
            [string]$ResourceGroup = "$Prefix-rg-$Environment"
        )

        $ErrorActionPreference = "Continue"
        $passed = 0
        $failed = 0

        function Test-Check($name, $result) {{
            if ($result) {{
                Write-Host "  [PASS] $name" -ForegroundColor Green
                $script:passed++
            }} else {{
                Write-Host "  [FAIL] $name" -ForegroundColor Red
                $script:failed++
            }}
        }}

        Write-Host "=== Post-Deployment Validation ===" -ForegroundColor Cyan

        # Function App
        $funcApp = "$Prefix-func-$Environment"
        $funcState = az functionapp show --name $funcApp --resource-group $ResourceGroup --query "state" -o tsv 2>$null
        Test-Check "Function App running" ($funcState -eq "Running")

        $funcFunctions = az functionapp function list --name $funcApp --resource-group $ResourceGroup 2>$null | ConvertFrom-Json
        Test-Check "Function App has functions" ($funcFunctions.Count -gt 0)

        {"# Event Hub" if s["needs_event_hub"] else "# Event Hub (skipped)"}
        {"$ehNs = az eventhubs namespace show --name $Prefix-ehns-$Environment --resource-group $ResourceGroup --query 'status' -o tsv 2>`$null`nTest-Check 'Event Hub Namespace active' (`$ehNs -eq 'Active')" if s["needs_event_hub"] else ""}

        {"# APIM" if s["needs_apim"] else "# APIM (skipped)"}
        {"$apimState = az apim show --name $Prefix-apim-$Environment --resource-group $ResourceGroup --query 'provisioningState' -o tsv 2>`$null`nTest-Check 'APIM provisioned' (`$apimState -eq 'Succeeded')" if s["needs_apim"] else ""}

        # App Insights
        $aiKey = az monitor app-insights component show --app "$Prefix-insights-$Environment" --resource-group $ResourceGroup --query "instrumentationKey" -o tsv 2>$null
        Test-Check "App Insights configured" ($aiKey -ne $null -and $aiKey -ne "")

        Write-Host "`n=== Results: $passed passed, $failed failed ===" -ForegroundColor $(if ($failed -eq 0) {{ "Green" }} else {{ "Red" }})
    """)


# ─────────────────────────────────────────────
#  Orchestrator
# ─────────────────────────────────────────────

def generate_blueprint(inventory_path=None, target=None, region="eastus",
                       resource_prefix="mig", env="dev"):
    """Generate the full CDC/RT deployment blueprint.

    Returns dict with generated file paths.
    """
    target = target or _get_target()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("  CDC / Real-Time Deployment Blueprint Generator")
    print("=" * 60)

    # 1. Analyze inventory
    print(f"\n📋 Analyzing inventory for CDC/RT candidates...")
    analysis = analyze_candidates(inventory_path)
    s = analysis["summary"]
    print(f"   CDC mappings: {s['cdc_count']}")
    print(f"   Real-time mappings: {s['realtime_count']}")
    print(f"   ESB/API mappings: {s['esb_api_count']}")
    print(f"   Needs Event Hub: {'Yes' if s['needs_event_hub'] else 'No'}")
    print(f"   Needs APIM: {'Yes' if s['needs_apim'] else 'No'}")

    generated = []

    # 2. Architecture doc
    print(f"\n📐 Generating architecture document...")
    arch_doc = generate_architecture_doc(analysis, target=target, region=region)
    arch_path = OUTPUT_DIR / "architecture.md"
    arch_path.write_text(arch_doc, encoding="utf-8")
    generated.append(str(arch_path))

    # 3. Bicep
    print(f"\n🏗️  Generating Bicep templates...")
    bicep_dir = OUTPUT_DIR / "bicep"
    bicep_dir.mkdir(parents=True, exist_ok=True)

    bicep = generate_bicep_blueprint(analysis, target=target, region=region,
                                     resource_prefix=resource_prefix, env=env)
    bicep_path = bicep_dir / "main.bicep"
    bicep_path.write_text(bicep, encoding="utf-8")
    generated.append(str(bicep_path))

    params = generate_bicep_parameters(analysis, target=target, region=region,
                                        resource_prefix=resource_prefix, env=env)
    params_path = bicep_dir / "parameters.json"
    params_path.write_text(json.dumps(params, indent=2), encoding="utf-8")
    generated.append(str(params_path))

    # 4. Terraform
    print(f"\n🏗️  Generating Terraform templates...")
    tf_dir = OUTPUT_DIR / "terraform"
    tf_dir.mkdir(parents=True, exist_ok=True)

    tf = generate_terraform_blueprint(analysis, target=target, region=region,
                                       resource_prefix=resource_prefix, env=env)
    tf_path = tf_dir / "main.tf"
    tf_path.write_text(tf, encoding="utf-8")
    generated.append(str(tf_path))

    tf_vars = generate_terraform_variables_blueprint(analysis, region=region,
                                                      resource_prefix=resource_prefix, env=env)
    tf_vars_path = tf_dir / "variables.tf"
    tf_vars_path.write_text(tf_vars, encoding="utf-8")
    generated.append(str(tf_vars_path))

    # 5. Deployment scripts
    print(f"\n📜 Generating deployment scripts...")
    scripts_dir = OUTPUT_DIR / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    ps1 = generate_deploy_ps1(analysis, resource_prefix=resource_prefix, env=env, region=region)
    ps1_path = scripts_dir / "deploy.ps1"
    ps1_path.write_text(ps1, encoding="utf-8")
    generated.append(str(ps1_path))

    sh = generate_deploy_sh(analysis, resource_prefix=resource_prefix, env=env, region=region)
    sh_path = scripts_dir / "deploy.sh"
    sh_path.write_text(sh, encoding="utf-8")
    generated.append(str(sh_path))

    validate = generate_validate_ps1(analysis, resource_prefix=resource_prefix, env=env)
    validate_path = scripts_dir / "validate.ps1"
    validate_path.write_text(validate, encoding="utf-8")
    generated.append(str(validate_path))

    # 6. Save analysis
    analysis_path = OUTPUT_DIR / "analysis.json"
    analysis_path.write_text(json.dumps(analysis, indent=2), encoding="utf-8")
    generated.append(str(analysis_path))

    print(f"\n✅ Blueprint generated — {len(generated)} files in {OUTPUT_DIR}")
    for f in generated:
        print(f"   → {Path(f).relative_to(WORKSPACE)}")

    return {
        "analysis": analysis,
        "generated": generated,
        "components": {
            "event_hub": s["needs_event_hub"],
            "apim": s["needs_apim"],
            "functions": s["needs_functions"],
        },
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="CDC/RT Deployment Blueprint Generator")
    parser.add_argument("--target", choices=["fabric", "databricks"], default=None)
    parser.add_argument("--region", default="eastus")
    parser.add_argument("--prefix", default="mig", help="Resource name prefix")
    parser.add_argument("--env", default="dev", choices=["dev", "test", "prod"])
    parser.add_argument("--inventory", default=None, help="Path to inventory.json")
    args = parser.parse_args()

    generate_blueprint(
        inventory_path=args.inventory,
        target=args.target,
        region=args.region,
        resource_prefix=args.prefix,
        env=args.env,
    )


if __name__ == "__main__":
    main()
