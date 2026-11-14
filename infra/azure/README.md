# Shared Azure learning foundation — PREVIEW ONLY

This source declares ADLS Gen2 (raw/quarantine/curated), Azure Data Factory with managed identity, Key Vault with RBAC and an OPTIONAL Azure Databricks Premium workspace. No paid resources have been deployed by this project. Databricks is OFF by default and this template creates no clusters, jobs or automatic schedules.

For a self-reported $150 Azure balance, first check the exact subscription, expiry date, regional prices and quotas in Azure Portal. Reserve $30; set budget notifications at $40, $80 and $120. Budget notifications are NOT spending caps. Do not deploy three Databricks workspaces; use one shared environment for the three demos, and remove it once finished.

Human-operated steps on your own computer after checking billing:
1. az login
2. az group create --name rg-portfolio-learning --location uksouth
3. az deployment group what-if --resource-group rg-portfolio-learning --template-file infra/azure/main.bicep --parameters prefix=portde01 deployDatabricks=false
4. Review changes and current prices in Azure Pricing Calculator.
5. Only with explicit approval: az deployment group create --resource-group rg-portfolio-learning --template-file infra/azure/main.bicep --parameters prefix=portde01 deployDatabricks=false
6. Apply minimum necessary Azure RBAC to the Data Factory managed identity before copying any files.
7. After use, delete the dedicated resource group and confirm active charges have ended. Soft-deleted Key Vault names can remain reserved.

Use synthetic data only. This learning baseline does not implement private endpoints, firewall restrictions, workload managed identities for Databricks, logging, recovery or compliance requirements. Validate the template with Azure Bicep tooling and subscription policy before actual creation. GitHub CI does not authenticate to Azure.

Local proof of data-to-output:
python -m scripts.generate_demo_data --out demo-data --orders 1000
python -m scripts.validate_demo_pipeline --data demo-data --output demo-output
python -m pytest -q
