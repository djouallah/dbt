{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['[file]', '[REGIONID]', '[SETTLEMENTDATE]', '[INTERVENTION]'],
    schema='landing'
) }}

{#-- Reads the new AEMO daily files, filtering to the DREGION price records. The file set comes
     from the archive log (new_source_files) and is passed to OPENROWSET as an EXPLICIT BULK (...)
     list — NOT a folder glob, which would re-read the whole archive every run.

     merge, not append. The file list already excludes anything in {{ this }}, but it is computed
     BEFORE the write, so two overlapping runs both see a file as new and both append it — silent
     duplicates. The key match is the write-time guard; the file list stays as the thing that
     keeps the merge source small.

     Unlike spark, this cannot be insert-only: dbt-fabric merge is default__get_merge_sql, which
     always emits WHEN MATCHED THEN UPDATE SET <every column> (merge_update_columns=[] is falsy
     and falls through to all columns). For append-only data that branch is a semantic no-op — a
     matched row is rewritten with its own identical values — so this is correct, just not free.
     If the leg gets slow, the fallback is delete+insert on unique_key=['[file]'], the strategy
     fct_summary already uses here.

     Key columns are bracketed: dbt interpolates them raw into the ON clause and `file` is a
     reserved word. No partition_by — Fabric Warehouse has no table partitioning; month_key is
     kept as a plain column. --#}

{%- set read_cols = [
  'I','UNIT','XX','VERSION','SETTLEMENTDATE','RUNNO','REGIONID','INTERVENTION','RRP','EEP',
  'ROP','APCFLAG','MARKETSUSPENDEDFLAG','TOTALDEMAND','DEMANDFORECAST','DISPATCHABLEGENERATION',
  'DISPATCHABLELOAD','NETINTERCHANGE','EXCESSGENERATION','LOWER5MINDISPATCH','LOWER5MINIMPORT',
  'LOWER5MINLOCALDISPATCH','LOWER5MINLOCALPRICE','LOWER5MINLOCALREQ','LOWER5MINPRICE','LOWER5MINREQ',
  'LOWER5MINSUPPLYPRICE','LOWER60SECDISPATCH','LOWER60SECIMPORT','LOWER60SECLOCALDISPATCH',
  'LOWER60SECLOCALPRICE','LOWER60SECLOCALREQ','LOWER60SECPRICE','LOWER60SECREQ','LOWER60SECSUPPLYPRICE',
  'LOWER6SECDISPATCH','LOWER6SECIMPORT','LOWER6SECLOCALDISPATCH','LOWER6SECLOCALPRICE','LOWER6SECLOCALREQ',
  'LOWER6SECPRICE','LOWER6SECREQ','LOWER6SECSUPPLYPRICE','RAISE5MINDISPATCH','RAISE5MINIMPORT',
  'RAISE5MINLOCALDISPATCH','RAISE5MINLOCALPRICE','RAISE5MINLOCALREQ','RAISE5MINPRICE','RAISE5MINREQ',
  'RAISE5MINSUPPLYPRICE','RAISE60SECDISPATCH','RAISE60SECIMPORT','RAISE60SECLOCALDISPATCH',
  'RAISE60SECLOCALPRICE','RAISE60SECLOCALREQ','RAISE60SECPRICE','RAISE60SECREQ','RAISE60SECSUPPLYPRICE',
  'RAISE6SECDISPATCH','RAISE6SECIMPORT','RAISE6SECLOCALDISPATCH','RAISE6SECLOCALPRICE','RAISE6SECLOCALREQ',
  'RAISE6SECPRICE','RAISE6SECREQ','RAISE6SECSUPPLYPRICE','AGGREGATEDISPATCHERROR','AVAILABLEGENERATION',
  'AVAILABLELOAD','INITIALSUPPLY','CLEAREDSUPPLY','LOWERREGIMPORT','LOWERREGLOCALDISPATCH',
  'LOWERREGLOCALREQ','LOWERREGREQ','RAISEREGIMPORT','RAISEREGLOCALDISPATCH','RAISEREGLOCALREQ',
  'RAISEREGREQ','RAISE5MINLOCALVIOLATION','RAISEREGLOCALVIOLATION','RAISE60SECLOCALVIOLATION',
  'RAISE6SECLOCALVIOLATION','LOWER5MINLOCALVIOLATION','LOWERREGLOCALVIOLATION','LOWER60SECLOCALVIOLATION',
  'LOWER6SECLOCALVIOLATION','RAISE5MINVIOLATION','RAISEREGVIOLATION','RAISE60SECVIOLATION',
  'RAISE6SECVIOLATION','LOWER5MINVIOLATION','LOWERREGVIOLATION','LOWER60SECVIOLATION','LOWER6SECVIOLATION',
  'RAISE6SECRRP','RAISE6SECROP','RAISE6SECAPCFLAG','RAISE60SECRRP','RAISE60SECROP','RAISE60SECAPCFLAG',
  'RAISE5MINRRP','RAISE5MINROP','RAISE5MINAPCFLAG','RAISEREGRRP','RAISEREGROP','RAISEREGAPCFLAG',
  'LOWER6SECRRP','LOWER6SECROP','LOWER6SECAPCFLAG','LOWER60SECRRP','LOWER60SECROP','LOWER60SECAPCFLAG',
  'LOWER5MINRRP','LOWER5MINROP','LOWER5MINAPCFLAG','LOWERREGRRP','LOWERREGROP','LOWERREGAPCFLAG',
  'RAISE6SECACTUALAVAILABILITY','RAISE60SECACTUALAVAILABILITY','RAISE5MINACTUALAVAILABILITY',
  'RAISEREGACTUALAVAILABILITY','LOWER6SECACTUALAVAILABILITY','LOWER60SECACTUALAVAILABILITY',
  'LOWER5MINACTUALAVAILABILITY','LOWERREGACTUALAVAILABILITY','LORSURPLUS','LRCSURPLUS'
] -%}
{%- set num_cols = read_cols | reject('in', ['I','UNIT','XX','REGIONID','SETTLEMENTDATE']) | list -%}

{%- set new_files = new_source_files('daily', this if is_incremental() else none) -%}
{%- if is_incremental() and new_files | length == 0 -%}
{#-- No new daily files this run: compile to a zero-row no-op (the merge source is empty). --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{%- else -%}
SELECT
  [UNIT],
  [REGIONID],
  {{ cast_floats(num_cols) }}
  {{ parse_filename('src.filepath()') }} AS [file],
  TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6)) AS [SETTLEMENTDATE],
  TRY_CAST([SETTLEMENTDATE] AS DATE) AS [DATE],
  YEAR(TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6))) AS [YEAR],
  -- Monthly partition key (YYYYMM): low-cardinality column kept for downstream pruning
  -- (Fabric has no native partitioning, but the column is still useful as a filter).
  YEAR(TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6))) * 100
    + MONTH(TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6))) AS [month_key]
FROM {{ openrowset_csv_files(new_files, read_cols) }} AS src
WHERE [I] = 'D' AND [UNIT] = 'DREGION' AND [VERSION] = '3'
{#-- Dedup guard, rendered ONLY on the wildcard fallback (pending > 1024): T-SQL rejects
     filepath(N) when the BULK path has no wildcard to index, so it must not appear when the
     file list is explicit — and the explicit list already excludes ingested files anyway. --#}
{%- if is_incremental() and new_files | length == 1 and '*' in new_files[0] %}
  AND {{ parse_filename('src.filepath(1)') }} NOT IN (SELECT [file] FROM {{ this }})
{%- endif %}
{%- endif %}
