# CDC / Real-Time Deployment Blueprint — Terraform
# Generated: 2026-04-10 07:23:51 UTC
# Target: Fabric | Region: eastus

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
  }
}

provider "azurerm" {
  features {}
}

locals {
  prefix      = var.prefix
  environment = var.environment
  tags = {
    environment = var.environment
    project     = "informatica-migration"
    managed_by  = "terraform"
  }
}

# ── Resource Group ──
resource "azurerm_resource_group" "blueprint" {
  name     = "${local.prefix}-rg-${local.environment}"
  location = var.location
  tags     = local.tags
}

# ── Log Analytics ──
resource "azurerm_log_analytics_workspace" "blueprint" {
  name                = "${local.prefix}-log-${local.environment}"
  location            = azurerm_resource_group.blueprint.location
  resource_group_name = azurerm_resource_group.blueprint.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.tags
}

# ── Application Insights ──
resource "azurerm_application_insights" "blueprint" {
  name                = "${local.prefix}-insights-${local.environment}"
  location            = azurerm_resource_group.blueprint.location
  resource_group_name = azurerm_resource_group.blueprint.name
  workspace_id        = azurerm_log_analytics_workspace.blueprint.id
  application_type    = "web"
  tags                = local.tags
}

# ── Storage Account ──
resource "azurerm_storage_account" "blueprint" {
  name                     = "${local.prefix}st${local.environment}"
  resource_group_name      = azurerm_resource_group.blueprint.name
  location                 = azurerm_resource_group.blueprint.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"
  tags                     = local.tags
}

# ── App Service Plan (Consumption) ──
resource "azurerm_service_plan" "blueprint" {
  name                = "${local.prefix}-plan-${local.environment}"
  resource_group_name = azurerm_resource_group.blueprint.name
  location            = azurerm_resource_group.blueprint.location
  os_type             = "Linux"
  sku_name            = "Y1"
  tags                = local.tags
}

# ── Function App ──
resource "azurerm_linux_function_app" "blueprint" {
  name                = "${local.prefix}-func-${local.environment}"
  resource_group_name = azurerm_resource_group.blueprint.name
  location            = azurerm_resource_group.blueprint.location
  service_plan_id     = azurerm_service_plan.blueprint.id
  storage_account_name       = azurerm_storage_account.blueprint.name
  storage_account_access_key = azurerm_storage_account.blueprint.primary_access_key
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_stack {
      python_version = "3.11"
    }
    application_insights_connection_string = azurerm_application_insights.blueprint.connection_string
  }

  app_settings = {
    "FUNCTIONS_WORKER_RUNTIME" = "python"
  }
}

# ── Outputs ──
output "function_app_hostname" {
  value = azurerm_linux_function_app.blueprint.default_hostname
}

output "app_insights_key" {
  value     = azurerm_application_insights.blueprint.instrumentation_key
  sensitive = true
}
