---
name: deploy-fabric-assets
description: Deploy dbt project assets to Microsoft Fabric via REST API. Trigger when user asks about deploying to Fabric, Fabric REST API, deploying lakehouse/notebook/pipeline/semantic model, or deploy_to_fabric.py.
---

# Deploy Fabric Assets via REST API

## Script: deploy_to_fabric.py

CLI with `--env` and step selection:
```
python deploy_to_fabric.py                           # deploy everything (production)
python deploy_to_fabric.py --env test                # deploy everything (test)
python deploy_to_fabric.py --env test notebook       # deploy notebook to test
python deploy_to_fabric.py semantic_model            # deploy semantic model (production)
```

Steps (in order): `lakehouse`, `files`, `initial_load`, `notebook`, `semantic_model`, `pipeline`, `schedule`
Environments: `test`, `production` (default: `production`)

## Configuration: deploy_config.json

Single config file with both environments:
```json
{
  "tenant_id": "your-tenant-id",
  "environments": {
    "production": {
      "workspace_id": "prod-workspace-id",
      "schedule_interval_minutes": 30
    },
    "test": {
      "workspace_id": "test-workspace-id",
      "schedule_interval_minutes": 0
    }
  },
  "lakehouse_name": "data",
  "notebook_name": "run",
  "pipeline_name": "run_pipeline",
  "pipeline_timeout": "0.01:00:00",
  "metadata_local_path": "/lakehouse/default/Files/metadata.db",
  "deploy_branch": "production",
  "semantic_model_name": "aemo_electricity",
  "download_limit": 100,
  "process_limit": 1000
}
```

Per-environment keys override top-level defaults (e.g. `workspace_id`, `schedule_interval_minutes`). Any key can be overridden per environment.

## Auth
- `AzureCliCredential` with tenant enforcement (`az login` required)
- Token scope: `https://api.fabric.microsoft.com/.default`
- Same token works for both Fabric API and Power BI API
- In CI: uses federated identity (`azure/login@v2` with `client-id` + `tenant-id`)

## API Endpoints
- Fabric: `https://api.fabric.microsoft.com/v1`
- Power BI refresh: `https://api.powerbi.com/v1.0/myorg/datasets/{id}/refreshes`

## Deploy Pattern (all items)
1. Check if item exists (GET list endpoint)
2. If exists: POST `updateDefinition` with base64 parts
3. If new: POST create endpoint with `displayName` + definition
4. Handle 202 async: poll `GET /operations/{id}` until Succeeded/Failed

## Deploy Steps

### lakehouse
```python
POST /v1/workspaces/{id}/lakehouses
{"displayName": "name", "creationPayload": {"enableSchemas": True}}
```
- `enableSchemas: True` creates schema-enabled lakehouse with `Tables/{schema}/` folder structure
- Never recreated if already exists
- Outputs `first_deploy` flag for gating `initial_load`

### files
- `DataLakeServiceClient("https://onelake.dfs.fabric.microsoft.com")`
- Path: `{LAKEHOUSE_ID}/Files/dbt/{relative_path}`
- Excludes: `.git`, `target`, `logs`, `dbt_packages`, `__pycache__`, `.github`, `.claude`, `metadata.db`, `.user.yml`, `deploy_config.json`

### initial_load
- Only runs when `first_deploy == true` (new lakehouse)
- Deploys notebook with reduced limits (`download_limit=2, process_limit=2`)
- Runs the notebook and waits for completion (polling with timeout)
- Then re-deploys notebook with normal limits from config
- Purpose: bootstrap initial data before enabling the full pipeline

### notebook
- Format: `ipynb`, part path: `notebook-content.ipynb`
- Generated as 2 cells: pip install + dbt run/test
- Env vars baked into the notebook JSON by the script

### semantic_model
- **Must use TMSL (model.bim)** — TMDL create is NOT supported by Fabric REST API
- Parts: `model.bim` (base64) + `definition.pbism` (base64 `{"version":"1.0"}`)
- `{{ONELAKE_URL}}` placeholder in model.bim, substituted at deploy time
- Refresh after deploy: clearValues (wait 15s) then full refresh

### pipeline
- Type: `DataPipeline`, activity type: `TridentNotebook`
- Single activity: "Run Notebook"
- Concurrency: 1 (enforced for DuckLake single-writer)
- Timeout from config (`pipeline_timeout`)

### schedule
- Cron type on pipeline job, PATCH to update, POST to create
- `SCHEDULE_INTERVAL_MINUTES=0` creates a disabled schedule
- Timezone: "AUS Eastern Standard Time"
- Idempotent: skips update if interval and enabled flag match

## Production Branch Pattern
- Script clones `production` branch (configurable via `deploy_branch`) into temp dir
- All assets (dbt files, model.bim) read from clone
- `deploy_to_fabric.py` itself runs from the current branch, NOT from production
- `generate_schema_name.sql` macro must be on production for correct schema routing
- Production branch contains only what's needed to run: dbt project files, model.bim, tests

## CI/CD Pattern (deploy.yml)

A single GitHub Actions workflow (`deploy.yml`) handles both testing and deployment:

### Trigger
```yaml
on:
  push:
    branches: [main, production]
  workflow_dispatch:
    inputs:
      environment:
        type: choice
        options: [test, production]
        default: production
```

### Environment selection
```yaml
env:
  DEPLOY_ENV: ${{ github.event_name == 'workflow_dispatch' && inputs.environment || (github.ref_name == 'main' && 'test' || 'production') }}
```
- Push to `main` → deploys to **test** workspace (schedule disabled)
- Push to `production` → deploys to **production** workspace (30-min schedule)
- Manual trigger → choose environment
- All workspace/schedule config comes from `deploy_config.json`, not workflow env vars

### Job DAG (8 jobs)

```
test → lakehouse → files → initial_load* → notebook → semantic_model → pipeline → schedule → deploy-docs
                                 (* only if first_deploy)
```

1. **test** — Azurite emulator + `dbt run --target ci` + `dbt test --target ci` + docs artifact
2. **lakehouse** — Create lakehouse, output `first_deploy` flag
3. **files** — Upload project files to OneLake
4. **initial_load** — Bootstrap data (only if `first_deploy == true`, 30min timeout)
5. **notebook** — Deploy notebook to Fabric
6. **semantic_model** — Deploy model.bim with Direct Lake config
7. **pipeline** — Create/update data pipeline
8. **schedule** — Set up recurring schedule
9. **deploy-docs** — Publish dbt docs to GitHub Pages

Steps 4+ use `if: ${{ !failure() && !cancelled() }}` to continue even when `initial_load` is skipped.

### CI Testing with Azurite

Azurite emulates Azure Blob Storage locally, allowing full dbt pipeline testing without Azure:

```yaml
- name: Start Azurite
  run: |
    npm install -g azurite
    azurite-blob --silent --skipApiVersionCheck --blobPort 10000 &
    for i in $(seq 1 10); do nc -z 127.0.0.1 10000 && break || sleep 1; done
    python -c "
    from azure.storage.blob import BlobServiceClient
    c = BlobServiceClient.from_connection_string('...')
    c.create_container('dbt')
    "
```

The CI profile target uses `az://dbt` as `ROOT_PATH` and includes Azurite's well-known connection string in both `settings` and `secrets`.

### Auth in CI
```yaml
- uses: azure/login@v2
  with:
    client-id: ${{ secrets.AZURE_CLIENT_ID }}
    tenant-id: ${{ secrets.AZURE_TENANT_ID }}
    allow-no-subscriptions: true
```
Uses workload identity federation (OIDC) — no client secret needed. Requires `id-token: write` permission.

## CLI argparse
Use manual validation instead of argparse `choices` parameter — `nargs="*"` with `choices` breaks on empty args:
```python
parser.add_argument("steps", nargs="*", default=None, help="...")
args = parser.parse_args()
if not args.steps:
    STEPS = set(ALL_STEPS)
else:
    invalid = set(args.steps) - set(ALL_STEPS)
    if invalid:
        parser.error(f"invalid step(s): ...")
    STEPS = set(args.steps)
```
