# CDC / Real-Time Blueprint — Post-Deployment Validation
# Generated: 2026-04-10 14:15:27 UTC

param(
    [string]$Environment = "dev",
    [string]$Prefix = "mig",
    [string]$ResourceGroup = "$Prefix-rg-$Environment"
)

$ErrorActionPreference = "Continue"
$passed = 0
$failed = 0

function Test-Check($name, $result) {
    if ($result) {
        Write-Host "  [PASS] $name" -ForegroundColor Green
        $script:passed++
    } else {
        Write-Host "  [FAIL] $name" -ForegroundColor Red
        $script:failed++
    }
}

Write-Host "=== Post-Deployment Validation ===" -ForegroundColor Cyan

# Function App
$funcApp = "$Prefix-func-$Environment"
$funcState = az functionapp show --name $funcApp --resource-group $ResourceGroup --query "state" -o tsv 2>$null
Test-Check "Function App running" ($funcState -eq "Running")

$funcFunctions = az functionapp function list --name $funcApp --resource-group $ResourceGroup 2>$null | ConvertFrom-Json
Test-Check "Function App has functions" ($funcFunctions.Count -gt 0)

# Event Hub (skipped)


# APIM (skipped)


# App Insights
$aiKey = az monitor app-insights component show --app "$Prefix-insights-$Environment" --resource-group $ResourceGroup --query "instrumentationKey" -o tsv 2>$null
Test-Check "App Insights configured" ($aiKey -ne $null -and $aiKey -ne "")

Write-Host "`n=== Results: $passed passed, $failed failed ===" -ForegroundColor $(if ($failed -eq 0) { "Green" } else { "Red" })
