{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['[file]', '[DUID]', '[SETTLEMENTDATE]', '[INTERVENTION]'],
    schema='landing'
) }}

{#-- Reads the new AEMO daily files, filtering to the DUNIT SCADA records. The set of files is
     resolved from the archive log (new_source_files) and passed to OPENROWSET as an EXPLICIT
     BULK (...) list — NOT a folder glob, which would re-read the whole archive every run. That
     list still does the selection, but it is computed before the write, so merge on the natural
     key is what actually guards against two overlapping runs both inserting the same file. See
     the fct_price.sql header for why this merge cannot be insert-only on dbt-fabric, and for the
     delete+insert fallback. No partition_by in Fabric; month_key kept as a plain column. --#}

{%- set read_cols = [
  'I','UNIT','XX','VERSION','SETTLEMENTDATE','RUNNO','DUID','INTERVENTION','DISPATCHMODE','AGCSTATUS',
  'INITIALMW','TOTALCLEARED','RAMPDOWNRATE','RAMPUPRATE','LOWER5MIN','LOWER60SEC','LOWER6SEC','RAISE5MIN',
  'RAISE60SEC','RAISE6SEC','MARGINAL5MINVALUE','MARGINAL60SECVALUE','MARGINAL6SECVALUE','MARGINALVALUE',
  'VIOLATION5MINDEGREE','VIOLATION60SECDEGREE','VIOLATION6SECDEGREE','VIOLATIONDEGREE','LOWERREG','RAISEREG',
  'AVAILABILITY','RAISE6SECFLAGS','RAISE60SECFLAGS','RAISE5MINFLAGS','RAISEREGFLAGS','LOWER6SECFLAGS',
  'LOWER60SECFLAGS','LOWER5MINFLAGS','LOWERREGFLAGS','RAISEREGAVAILABILITY','RAISEREGENABLEMENTMAX',
  'RAISEREGENABLEMENTMIN','LOWERREGAVAILABILITY','LOWERREGENABLEMENTMAX','LOWERREGENABLEMENTMIN',
  'RAISE6SECACTUALAVAILABILITY','RAISE60SECACTUALAVAILABILITY','RAISE5MINACTUALAVAILABILITY',
  'RAISEREGACTUALAVAILABILITY','LOWER6SECACTUALAVAILABILITY','LOWER60SECACTUALAVAILABILITY',
  'LOWER5MINACTUALAVAILABILITY','LOWERREGACTUALAVAILABILITY'
] -%}
{%- set num_cols = read_cols | reject('in', ['I','UNIT','XX','DUID','SETTLEMENTDATE']) | list -%}

{%- set new_files = new_source_files('daily', this if is_incremental() else none) -%}
{%- if is_incremental() and new_files | length == 0 -%}
{#-- No new daily files this run: compile to a zero-row no-op (the merge source is empty). --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{%- else -%}
SELECT
  [UNIT],
  [DUID],
  {{ cast_floats(num_cols) }}
  {{ parse_filename('src.filepath()') }} AS [file],
  TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6)) AS [SETTLEMENTDATE],
  TRY_CAST([SETTLEMENTDATE] AS DATE) AS [DATE],
  YEAR(TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6))) AS [YEAR],
  YEAR(TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6))) * 100
    + MONTH(TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6))) AS [month_key]
FROM {{ openrowset_csv_files(new_files, read_cols) }} AS src
WHERE [I] = 'D' AND [UNIT] = 'DUNIT' AND [VERSION] = '3'
{#-- Dedup guard, rendered ONLY on the wildcard fallback (pending > 1024): T-SQL rejects
     filepath(N) when the BULK path has no wildcard to index, so it must not appear when the
     file list is explicit — and the explicit list already excludes ingested files anyway. --#}
{%- if is_incremental() and new_files | length == 1 and '*' in new_files[0] %}
  AND {{ parse_filename('src.filepath(1)') }} NOT IN (SELECT [file] FROM {{ this }})
{%- endif %}
{%- endif %}
