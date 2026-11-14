# Docker and Azure DevOps CI artifacts

- `Dockerfile` creates a lightweight Python 3.11 test container. `docker build -t azure-lakehouse-ci . && docker run --rm azure-lakehouse-ci` runs pytest.
- `azure-pipelines.yml` specifies a Microsoft-hosted Linux pipeline for Python compilation, unit tests, and Docker build/test. It must be linked to an Azure DevOps project to execute.
- `.dockerignore` excludes generated databases, caches, credentials and Terraform state.

## Verification boundaries

GitHub contains the CI definitions; **no Azure DevOps build has yet been run** and no Docker image has been built in this environment (Docker daemon unavailable). The local Python tests are run independently. Do not claim a deployed container, service SLA or production engineering outcome from these files.
