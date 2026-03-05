---
name: deploy-fabric-assets
description: Deploy dbt project assets to Microsoft Fabric via REST API. Trigger when user asks about deploying to Fabric, Fabric REST API, deploying lakehouse/notebook/pipeline/semantic model, or deploy_to_fabric.py.
---

# Deploy Fabric Assets via REST API

## Script: deploy_to_fabric.py

CLI with step selection:
```
python deploy_to_fabric.py                  # deploy everything
python deploy_to_fabric.py semantic_model   # deploy semantic model only
python deploy_to_fabric.py files notebook   # deploy files + notebook only
```

Steps: `lakehouse`, `files`, `notebook`, `pipeline`, `schedule`, `semantic_model`

## Auth
- `AzureCliCredential` with tenant enforcement (`az login` required)
- Token scope: `https://api.fabric.microsoft.com/.default`
- Same token works for both Fabric API and Power BI API

## API Endpoints
- Fabric: `https://api.fabric.microsoft.com/v1`
- Power BI refresh: `https://api.powerbi.com/v1.0/myorg/datasets/{id}/refreshes`

## Deploy Pattern (all items)
1. Check if item exists (GET list endpoint)
2. If exists: POST `updateDefinition` with base64 parts
3. If new: POST create endpoint with `displayName` + definition
4. Handle 202 async: poll `GET /operations/{id}` until Succeeded/Failed

## Lakehouse Creation
```python
POST /v1/workspaces/{id}/lakehouses
{"displayName": "raw", "creationPayload": {"enableSchemas": True}}
```
`enableSchemas: True` creates schema-enabled lakehouse with `Tables/{schema}/` folder structure.

## File Upload to OneLake
- `DataLakeServiceClient("https://onelake.dfs.fabric.microsoft.com")`
- Path: `{LAKEHOUSE_ID}/Files/dbt/{relative_path}`

## Notebook Deploy
- Format: `ipynb`, part path: `notebook-content.ipynb`

## Pipeline Deploy
- Type: `DataPipeline`, activity type: `TridentNotebook`
- Part path: `pipeline-content.json`

## Schedule
- Cron type on pipeline job, PATCH to update, POST to create

## Semantic Model Deploy
- **Must use TMSL (model.bim)** — TMDL create is NOT supported by Fabric REST API
- Parts: `model.bim` (base64) + `definition.pbism` (base64 `{"version":"1.0"}`)
- `{{ONELAKE_URL}}` placeholder in model.bim, substituted at deploy time
- POST `/v1/workspaces/{id}/semanticModels` for create
- POST `.../{id}/updateDefinition` for update
- Refresh after deploy: clearValues (wait 15s) then full refresh

## Production Branch Pattern
- Script clones `production` branch into temp dir
- All assets (dbt files, model.bim) read from clone
- `deploy_to_fabric.py` itself is NOT on production branch
- `generate_schema_name.sql` macro must be on production for correct schema routing

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
