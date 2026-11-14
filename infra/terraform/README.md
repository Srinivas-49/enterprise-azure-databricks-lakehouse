# ADLS Gen2 Terraform reference
Creates an Azure resource group, HNS storage account, and Bronze/Silver/Gold filesystems. It has not been applied to an Azure subscription.
Run terraform init, terraform fmt -check, terraform validate, then terraform plan with your subscription_id, resource_group_name, and globally unique storage_account_name.
Review cloud pricing and Azure RBAC before any apply. Never commit credentials or state files. Use OIDC and remote state for production deployments.
