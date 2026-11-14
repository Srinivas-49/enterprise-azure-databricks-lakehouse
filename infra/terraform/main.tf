terraform {
  required_version = ">= 1.6.0"
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}
provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}
variable "subscription_id" { type = string }
variable "resource_group_name" { type = string }
variable "storage_account_name" {
  type = string
  validation {
    condition = can(regex("^[a-z0-9]{3,24}$", var.storage_account_name))
    error_message = "Use 3-24 lower-case letters or digits."
  }
}
variable "location" {
  type = string
  default = "uksouth"
}
resource "azurerm_resource_group" "portfolio" {
  name = var.resource_group_name
  location = var.location
}
resource "azurerm_storage_account" "lake" {
  name = var.storage_account_name
  resource_group_name = azurerm_resource_group.portfolio.name
  location = azurerm_resource_group.portfolio.location
  account_tier = "Standard"
  account_replication_type = "LRS"
  account_kind = "StorageV2"
  is_hns_enabled = true
  min_tls_version = "TLS1_2"
  https_traffic_only_enabled = true
  allow_nested_items_to_be_public = false
}
resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name = "bronze"
  storage_account_id = azurerm_storage_account.lake.id
}
resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name = "silver"
  storage_account_id = azurerm_storage_account.lake.id
}
resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name = "gold"
  storage_account_id = azurerm_storage_account.lake.id
}
output "storage_account_name" { value = azurerm_storage_account.lake.name }
