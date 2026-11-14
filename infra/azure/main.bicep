// Budget-gated learning infrastructure; NO live deployment is triggered by GitHub.
targetScope = 'resourceGroup'

@description('Use lowercase alphanumeric prefix to stay within Azure name limits.')
@minLength(3)
@maxLength(10)
param prefix string = 'portde01'
param location string = resourceGroup().location
@description('Always false unless manually approved after price/credit review.')
param deployDatabricks bool = false

var suffix = uniqueString(resourceGroup().id)
var storageName = toLower(concat('st', prefix, suffix))
var factoryName = concat('adf-', prefix, '-', suffix)
var vaultName = concat('kv-', prefix, '-', suffix)
var dbName = concat('dbw-', prefix, '-', suffix)

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  tags: { environment: 'learning', dataClassification: 'synthetic' }
  properties: {
    isHnsEnabled: true
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
  }
}

resource blob 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  name: 'default'
  parent: storage
}
resource raw 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  name: 'raw'
  parent: blob
  properties: { publicAccess: 'None' }
}
resource quarantine 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  name: 'quarantine'
  parent: blob
  properties: { publicAccess: 'None' }
}
resource curated 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  name: 'curated'
  parent: blob
  properties: { publicAccess: 'None' }
}
resource vault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: vaultName
  location: location
  properties: {
    tenantId: subscription().tenantId
    sku: { family: 'A', name: 'standard' }
    enableRbacAuthorization: true
    accessPolicies: []
    softDeleteRetentionInDays: 7
  }
}
resource factory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: factoryName
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {}
}
resource databricks 'Microsoft.Databricks/workspaces@2023-02-01' = if (deployDatabricks) {
  name: dbName
  location: location
  sku: { name: 'premium' }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', concat('rg-', resourceGroup().name, '-db-managed'))
  }
}
output storageName string = storage.name
output dataFactoryName string = factory.name
output keyVaultName string = vault.name
output workspaceName string = deployDatabricks ? databricks.name : 'not-created'
