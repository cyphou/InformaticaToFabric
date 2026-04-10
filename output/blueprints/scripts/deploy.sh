#!/bin/bash
# CDC / Real-Time Blueprint — Deployment Script (Bash)
# Generated: 2026-04-10 14:15:27 UTC
set -euo pipefail

ENVIRONMENT="${1:-dev}"
LOCATION="${2:-eastus}"
PREFIX="${3:-mig}"
RG="${PREFIX}-rg-${ENVIRONMENT}"

echo "=== CDC/RT Blueprint Deployment ==="

# 1. Create Resource Group
echo "[1/5] Creating resource group: $RG"
az group create --name "$RG" --location "$LOCATION" \
    --tags environment="$ENVIRONMENT" project=informatica-migration >/dev/null

# 2. Deploy Bicep
echo "[2/5] Deploying Bicep template..."
az deployment group create \
    --resource-group "$RG" \
    --template-file ./bicep/main.bicep \
    --parameters ./bicep/parameters.json \
    --parameters environment="$ENVIRONMENT" \
    --name "blueprint-$(date +%Y%m%d-%H%M%S)"

# 3. Deploy function code
echo "[3/5] Deploying function code..."
FUNC_APP="${PREFIX}-func-${ENVIRONMENT}"
pushd ../functions >/dev/null
func azure functionapp publish "$FUNC_APP" --python
popd >/dev/null

# 4. Configure
echo "[4/5] Configuring app settings..."
az functionapp config appsettings set \
    --name "$FUNC_APP" --resource-group "$RG" \
    --settings "MIGRATION_TARGET=fabric" >/dev/null

# 5. Validate
echo "[5/5] Validating..."
STATUS=$(az functionapp show --name "$FUNC_APP" --resource-group "$RG" --query "state" -o tsv)
echo "  Function App: $STATUS"

echo "=== Deployment Complete ==="
