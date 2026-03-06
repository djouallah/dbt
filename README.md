# AEMO Electricity — dbt + DuckLake

Downloads Australian electricity market data (AEMO NEM), transforms with dbt-duckdb + DuckLake, and exports to Delta Lake on Microsoft Fabric.

## Deployment to Microsoft Fabric

### Option 1: CI/CD with GitHub Actions (recommended)

Requires an Azure AD app registration with federated credentials for GitHub Actions OIDC. Set `AZURE_CLIENT_ID` and `AZURE_TENANT_ID` as GitHub secrets, and update `WORKSPACE_ID` / `TENANT_ID` in `deploy_to_fabric.py`.

- Push to `main` — runs dbt CI (Azurite + `dbt run` + `dbt test`)
- Push to `production` — deploys everything to Fabric + docs to GitHub Pages
- First deploy creates the lakehouse and seeds data; subsequent deploys are safe to re-run

### Option 2: Manual from laptop

No app registration needed — uses `az login`.

```bash
az login --tenant YOUR_TENANT_ID
python deploy_to_fabric.py                   # deploy everything
python deploy_to_fabric.py semantic_model    # just one step
```

### Option 3: Upload notebook

Upload `dbt.ipynb` to a Fabric workspace, attach a lakehouse named `raw`, and run it.

---

See the [blog post](https://datamonkeysite.com/2026/03/05/building-a-data-pipeline-using-vscode-and-claude-out-of-thin-air/) for a full walkthrough.

## Environment Variables

| Variable               | Default                                  | Purpose                          |
|------------------------|------------------------------------------|----------------------------------|
| `ROOT_PATH`            | `abfss://{WORKSPACE}@onelake.../{LAKEHOUSE}` | OneLake storage root        |
| `METADATA_LOCAL_PATH`  | `/lakehouse/default/Files/metadata.db`   | DuckLake SQLite metadata DB      |
| `DBT_SCHEMA`           | `aemo`                                   | Target schema                    |
| `download_limit`       | `2`                                      | Max files to download per source |
| `process_limit`        | `500`                                    | Max files to process per model   |
| `daily_source`         | `aemo`                                   | `aemo` (live) or `github` (backfill) |
