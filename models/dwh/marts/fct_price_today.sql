{{ config(
    materialized='incremental',
    incremental_strategy='append',
    schema='landing'
) }}

{#-- Intraday price — reads the new price_today files. The file set comes from the archive log
     (new_source_files) and is passed to OPENROWSET as an EXPLICIT BULK (...) list, not a folder
     glob (which would re-read the whole archive each run). The list already excludes files in
     {{ this }}, so the append stays idempotent at file grain. --#}

{%- set read_cols = [
  'I','DISPATCH','PRICE','xx','SETTLEMENTDATE','RUNNO','REGIONID','DISPATCHINTERVAL','INTERVENTION','RRP',
  'EEP','ROP','APCFLAG','MARKETSUSPENDEDFLAG','LASTCHANGED','RAISE6SECRRP','RAISE6SECROP','RAISE6SECAPCFLAG',
  'RAISE60SECRRP','RAISE60SECROP','RAISE60SECAPCFLAG','RAISE5MINRRP','RAISE5MINROP','RAISE5MINAPCFLAG',
  'RAISEREGRRP','RAISEREGROP','RAISEREGAPCFLAG','LOWER6SECRRP','LOWER6SECROP','LOWER6SECAPCFLAG',
  'LOWER60SECRRP','LOWER60SECROP','LOWER60SECAPCFLAG','LOWER5MINRRP','LOWER5MINROP','LOWER5MINAPCFLAG',
  'LOWERREGRRP','LOWERREGROP','LOWERREGAPCFLAG','PRICE_STATUS','PRE_AP_ENERGY_PRICE','PRE_AP_RAISE6_PRICE',
  'PRE_AP_RAISE60_PRICE','PRE_AP_RAISE5MIN_PRICE','PRE_AP_RAISEREG_PRICE','PRE_AP_LOWER6_PRICE',
  'PRE_AP_LOWER60_PRICE','PRE_AP_LOWER5MIN_PRICE','PRE_AP_LOWERREG_PRICE','RAISE1SECRRP','RAISE1SECROP',
  'RAISE1SECAPCFLAG','LOWER1SECRRP','LOWER1SECROP','LOWER1SECAPCFLAG','PRE_AP_RAISE1_PRICE','PRE_AP_LOWER1_PRICE',
  'CUMUL_PRE_AP_ENERGY_PRICE','CUMUL_PRE_AP_RAISE6_PRICE','CUMUL_PRE_AP_RAISE60_PRICE',
  'CUMUL_PRE_AP_RAISE5MIN_PRICE','CUMUL_PRE_AP_RAISEREG_PRICE','CUMUL_PRE_AP_LOWER6_PRICE',
  'CUMUL_PRE_AP_LOWER60_PRICE','CUMUL_PRE_AP_LOWER5MIN_PRICE','CUMUL_PRE_AP_LOWERREG_PRICE',
  'CUMUL_PRE_AP_RAISE1_PRICE','CUMUL_PRE_AP_LOWER1_PRICE','OCD_STATUS','MII_STATUS'
] -%}
{%- set skip = ['I','DISPATCH','PRICE','xx','SETTLEMENTDATE','REGIONID','LASTCHANGED','PRICE_STATUS','OCD_STATUS','MII_STATUS'] -%}
{%- set num_cols = read_cols | reject('in', skip) | list -%}

{%- set new_files = new_source_files('price_today', this if is_incremental() else none) -%}
{%- if is_incremental() and new_files | length == 0 -%}
{#-- No new price_today files this run: compile to a zero-row no-op. --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{%- else -%}
SELECT
  [REGIONID],
  {{ cast_floats(num_cols) }}
  TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6)) AS [SETTLEMENTDATE],
  TRY_CAST([SETTLEMENTDATE] AS DATE) AS [DATE],
  {{ parse_filename('src.filepath()') }} AS [file],
  YEAR(TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6))) AS [YEAR]
FROM {{ openrowset_csv_files(new_files, read_cols) }} AS src
WHERE [I] = 'D' AND [PRICE] = 'PRICE'
{%- endif %}
