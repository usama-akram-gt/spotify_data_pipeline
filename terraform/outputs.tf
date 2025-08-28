# Terraform Outputs

output "resource_group_name" {
  description = "Name of the resource group"
  value       = azurerm_resource_group.main.name
}

output "adls_account_name" {
  description = "Name of the Data Lake Storage account"
  value       = azurerm_storage_account.adls.name
}

output "adls_primary_access_key" {
  description = "Primary access key for Data Lake Storage"
  value       = azurerm_storage_account.adls.primary_access_key
  sensitive   = true
}

output "adls_connection_string" {
  description = "Connection string for Data Lake Storage"
  value       = azurerm_storage_account.adls.primary_connection_string
  sensitive   = true
}

output "postgres_server_name" {
  description = "PostgreSQL server name"
  value       = azurerm_postgresql_flexible_server.postgres.name
}

output "postgres_fqdn" {
  description = "PostgreSQL server FQDN"
  value       = azurerm_postgresql_flexible_server.postgres.fqdn
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = "https://${azurerm_databricks_workspace.main.workspace_url}"
}

output "databricks_workspace_id" {
  description = "Databricks workspace ID"
  value       = azurerm_databricks_workspace.main.workspace_id
}

output "eventhub_namespace_name" {
  description = "Event Hubs namespace name"
  value       = azurerm_eventhub_namespace.kafka.name
}

output "eventhub_connection_string" {
  description = "Event Hubs connection string"
  value       = azurerm_eventhub_namespace.kafka.default_primary_connection_string
  sensitive   = true
}

output "container_registry_name" {
  description = "Container Registry name"
  value       = azurerm_container_registry.acr.name
}

output "container_registry_login_server" {
  description = "Container Registry login server"
  value       = azurerm_container_registry.acr.login_server
}

output "log_analytics_workspace_id" {
  description = "Log Analytics workspace ID"
  value       = azurerm_log_analytics_workspace.main.id
}

# Environment variables template
output "env_variables_template" {
  description = "Template for environment variables"
  value = <<-EOT
# Azure Environment Variables - Copy to .env file
ADLS_ACCOUNT_NAME=${azurerm_storage_account.adls.name}
ADLS_ACCOUNT_KEY=${azurerm_storage_account.adls.primary_access_key}
ADLS_CONTAINER_NAME=gold

DB_HOST=${azurerm_postgresql_flexible_server.postgres.fqdn}
DB_PORT=5432
DB_NAME=spotify_data
DB_USER=spotify_admin
DB_PASSWORD=P@ssw0rd123!

DATABRICKS_WORKSPACE_URL=${azurerm_databricks_workspace.main.workspace_url}
# Set DATABRICKS_ACCESS_TOKEN after creating personal access token in workspace

AZURE_EVENTHUBS_NAMESPACE=${azurerm_eventhub_namespace.kafka.name}
AZURE_EVENTHUBS_CONNECTION_STRING="${azurerm_eventhub_namespace.kafka.default_primary_connection_string}"

ACR_LOGIN_SERVER=${azurerm_container_registry.acr.login_server}
EOT
  sensitive = true
}