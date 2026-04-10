// CDC / Real-Time Deployment Blueprint — Bicep
// Generated: 2026-04-10 14:15:27 UTC
// Target: Fabric | Region: eastus

targetScope = 'resourceGroup'

// ── Parameters ──
@description('Azure region')
param location string = 'eastus'

@description('Environment (dev/test/prod)')
@allowed(['dev', 'test', 'prod'])
param environment string = 'dev'

@description('Resource name prefix')
param prefix string = 'mig'

@description('Function App name')
param functionAppName string = '${prefix}-func-${environment}'

@description('Storage account name (3-24 chars, lowercase)')
param storageAccountName string = '${prefix}st${environment}'

@description('App Insights name')
param appInsightsName string = '${prefix}-insights-${environment}'

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
      ]
    }
  }
  tags: { environment: environment, project: 'informatica-migration' }
}

// ── Outputs ──
output functionAppName string = functionApp.properties.defaultHostName
output appInsightsKey string = appInsights.properties.InstrumentationKey
