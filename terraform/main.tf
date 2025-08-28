# Spotify Pipeline - Azure Infrastructure
terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
  
  # Remote state storage (uncomment and configure)
  # backend "azurerm" {
  #   resource_group_name  = "terraform-state-rg"
  #   storage_account_name = "terraformstatesa"
  #   container_name       = "tfstate"
  #   key                  = "spotify-pipeline.terraform.tfstate"
  # }
}

# Configure Azure Provider
provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

# Variables
variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "spotify-pipeline"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "East US"
}

variable "databricks_sku" {
  description = "Databricks SKU"
  type        = string
  default     = "standard"
}

# Local values for tagging and naming
locals {
  common_tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "terraform"
  }
  
  resource_prefix = "${var.project_name}-${var.environment}"
}

# Resource Group
resource "azurerm_resource_group" "main" {
  name     = "${local.resource_prefix}-rg"
  location = var.location
  tags     = local.common_tags
}

# Data Lake Storage Gen2
resource "azurerm_storage_account" "adls" {
  name                     = "${replace(local.resource_prefix, "-", "")}adls"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind            = "StorageV2"
  is_hns_enabled          = true  # Hierarchical namespace for Data Lake

  blob_properties {
    delete_retention_policy {
      days = 7
    }
  }

  tags = local.common_tags
}

# Storage containers for different data layers
resource "azurerm_storage_data_lake_gen2_filesystem" "data_layers" {
  for_each           = toset(["raw", "bronze", "silver", "gold", "tmp", "staging"])
  name               = each.key
  storage_account_id = azurerm_storage_account.adls.id
}

# PostgreSQL Server (for operational data)
resource "azurerm_postgresql_flexible_server" "postgres" {
  name                   = "${local.resource_prefix}-postgres"
  resource_group_name    = azurerm_resource_group.main.name
  location              = azurerm_resource_group.main.location
  version               = "14"
  administrator_login    = "spotify_admin"
  administrator_password = "P@ssw0rd123!" # Use Azure Key Vault in production
  
  storage_mb            = 32768
  sku_name             = "B_Standard_B1ms"
  backup_retention_days = 7
  
  tags = local.common_tags
}

# PostgreSQL Database
resource "azurerm_postgresql_flexible_server_database" "spotify_db" {
  name      = "spotify_data"
  server_id = azurerm_postgresql_flexible_server.postgres.id
  collation = "en_US.utf8"
  charset   = "utf8"
}

# Event Hubs Namespace (for Kafka-compatible streaming)
resource "azurerm_eventhub_namespace" "kafka" {
  name                = "${local.resource_prefix}-eventhubs"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Standard"
  capacity            = 1

  tags = local.common_tags
}

# Event Hub for streaming events
resource "azurerm_eventhub" "streaming_events" {
  name                = "streaming-events"
  namespace_name      = azurerm_eventhub_namespace.kafka.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 2
  message_retention   = 1
}

# Databricks Workspace
resource "azurerm_databricks_workspace" "main" {
  name                = "${local.resource_prefix}-databricks"
  resource_group_name = azurerm_resource_group.main.name
  location           = azurerm_resource_group.main.location
  sku                = var.databricks_sku

  tags = local.common_tags
}

# Container Registry for custom images
resource "azurerm_container_registry" "acr" {
  name                = "${replace(local.resource_prefix, "-", "")}acr"
  resource_group_name = azurerm_resource_group.main.name
  location           = azurerm_resource_group.main.location
  sku                = "Basic"
  admin_enabled      = true

  tags = local.common_tags
}

# Log Analytics Workspace for monitoring
resource "azurerm_log_analytics_workspace" "main" {
  name                = "${local.resource_prefix}-logs"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = 30

  tags = local.common_tags
}