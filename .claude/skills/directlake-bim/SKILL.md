---
name: directlake-bim
description: Build model.bim for Direct Lake on OneLake (pure, no SQL endpoint). Trigger when user asks about Direct Lake, model.bim, semantic model definition, Power BI on OneLake, or sourceLineageTag.
---

# Build model.bim for Direct Lake on OneLake (Pure, No SQL Endpoint)

## Critical: This is NOT standard Direct Lake
Standard Direct Lake goes through the SQL analytics endpoint.
Pure Direct Lake on OneLake reads Delta tables directly from OneLake storage — NO SQL endpoint fallback.

## Reference: github.com/djouallah/fabric_demo/semantic_model

## The Key Annotation
```json
{
  "name": "PBI_ProTooling",
  "value": "[\"RemoteModeling\", \"DirectLakeOnOneLakeCreatedInDesktop\"]"
}
```
Without `DirectLakeOnOneLakeCreatedInDesktop`, Power BI falls back to SQL endpoint.

## Expression (M Query)
```json
{
  "name": "DirectLake",
  "kind": "m",
  "expression": [
    "let",
    "    Source = AzureStorage.DataLake(\"{{ONELAKE_URL}}\", [HierarchicalNavigation=true])",
    "in",
    "    Source"
  ]
}
```
URL format: `https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{LAKEHOUSE_ID}`

## Partition Source (every table)
```json
{
  "name": "table_name",
  "mode": "directLake",
  "source": {
    "type": "entity",
    "entityName": "table_name",
    "expressionSource": "DirectLake",
    "schemaName": "mart"
  }
}
```
- `schemaName` maps to folder under `Tables/` (e.g., `Tables/mart/dim_calendar/`)
- `entityName` is the Delta table folder name
- `expressionSource` references the M expression name

## Required on Tables
- `sourceLineageTag`: `[schema].[table_name]` (e.g., `[mart].[fct_summary]`)

## Required on Columns
- `sourceLineageTag`: column name (e.g., `"date"`, `"DUID"`)

## Required on Relationships
- `relyOnReferentialIntegrity: true` — mandatory for Direct Lake
- Default single direction (dim filters fact) — do NOT use `bothDirections`

## PBI_RemovedChildren (on expression)
Lists tables present in lakehouse but excluded from semantic model.
Schema prefix must match actual schema where tables live:
```json
{
  "name": "PBI_RemovedChildren",
  "value": "[{\"sourceLineageTag\":\"[landing].[fct_scada]\"},{\"sourceLineageTag\":\"[landing].[fct_scada_today]\"},{\"sourceLineageTag\":\"[landing].[fct_price]\"},{\"sourceLineageTag\":\"[landing].[fct_price_today]\"},{\"sourceLineageTag\":\"[landing].[stg_csv_archive_log]\"}]"
}
```

## model.bim Structure
```
compatibilityLevel: 1604
model:
  culture: en-US
  defaultPowerBIDataSourceVersion: powerBI_V3
  annotations: [PBI_ProTooling with DirectLakeOnOneLakeCreatedInDesktop]
  expressions: [AzureStorage.DataLake M expression + PBI_RemovedChildren]
  tables: [columns with sourceLineageTag + measures + directLake partitions]
  relationships: [with relyOnReferentialIntegrity]
```

## Refresh Pattern
1. POST clearValues refresh (wait 15s for 202)
2. POST full refresh
Both via Power BI API: `https://api.powerbi.com/v1.0/myorg/datasets/{id}/refreshes`

## TMDL vs TMSL (model.bim)
- **Fabric REST API create does NOT support TMDL** — always requires `model.bim` (TMSL)
- `updateDefinition` may support TMDL with `"format": "TMDL"` but unreliable
- **Use model.bim (TMSL) for deployment** — it works reliably for both create and update
- TMDL is only useful for Git integration / Fabric workspace source control

## Common Pitfalls
- Missing `DirectLakeOnOneLakeCreatedInDesktop` -> falls back to SQL endpoint
- Wrong `schemaName` -> "source tables do not exist" error
- Missing `sourceLineageTag` on tables/columns -> table resolution fails
- `crossFilteringBehavior: bothDirections` -> wrong for star schema
- Refresh race condition -> need 15s wait between clearValues and full
- TMDL on create API -> "Missing required artifact model.bim" error
- Descriptions on tables/columns/measures help Fabric AI generate reports
