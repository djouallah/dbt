# dbt on Microsoft Fabric — CI/CD with Fabric CLI

A CI/CD pipeline that deploys a dbt project to Microsoft Fabric using the [Fabric CLI](https://microsoft.github.io/fabric-cli/) (`fab`). The notebook runs dbt inside Fabric and exports results to Delta Lake tables, which are served via a Direct Lake semantic model in Power BI.

## How it works

1. **CI** — GitHub Actions runs `dbt run` + `dbt test` against a local DuckDB instance (Azurite for blob storage)
2. **Deploy** — `python deploy.py` orchestrates the full deployment:
   - Creates the lakehouse (idempotent)
   - Deploys notebook + lakehouse via `fab deploy`
   - Copies dbt project files to OneLake
   - Runs the notebook synchronously (`fab job run`) — executes dbt, creates Delta tables
   - Deploys the semantic model
   - Refreshes the semantic model
   - Deploys the pipeline and sets the cron schedule

## Configuration

All deployment targets are in `deploy_config.yml` — no hardcoded values in the scripts.

`parameter.yml` handles environment-specific value substitution (workspace ID, lakehouse ID) during `fab deploy`.

## Requirements

- [Microsoft Fabric CLI](https://microsoft.github.io/fabric-cli/) (`pip install ms-fabric-cli`)
- Azure AD app registration with Fabric API permissions

## CI/CD Setup (GitHub Actions)

Set as GitHub secrets:
- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`

Push to `main` — runs CI tests then deploys to Fabric and publishes dbt docs to GitHub Pages.

## Manual deploy

```bash
az login
python deploy.py
```
