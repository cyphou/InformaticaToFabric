# CDC / Real-Time Blueprint — Deployment Script (PowerShell)
# Generated: 2026-04-10 07:23:51 UTC

param(
    [string]$Environment = "dev",
    [string]$Location = "eastus",
    [string]$Prefix = "mig",
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

if ($LASTEXITCODE -ne 0) {
    Write-Error "Deployment failed!"
    exit 1
}

$funcHostname = $deployment.properties.outputs.functionAppName.value
Write-Host "  Function App: https://$funcHostname" -ForegroundColor Green
# Event Hub not needed
# APIM not needed

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
    --settings "MIGRATION_TARGET=fabric" | Out-Null

# 5. Validate deployment
Write-Host "[5/5] Validating deployment..." -ForegroundColor Yellow
$funcStatus = az functionapp show --name $funcAppName --resource-group $ResourceGroup --query "state" -o tsv
Write-Host "  Function App status: $funcStatus" -ForegroundColor Green



Write-Host "`n=== Deployment Complete ===" -ForegroundColor Cyan
