-- Insert-only: a matched row is left alone, so re-processing a file dedupes on the unique_key.
-- Never plain 'append' -- the pre_hook file list is computed before the write, so two
-- overlapping runs would both append the same file. Rationale for every config choice here:
-- CLAUDE.md, "Incremental write strategies are per engine".
-- The two strategy spellings are temporary; they collapse to one when duckrun accepts
-- merge_clauses do_nothing (duckrun#20).
-- The pending-file probe must run BEFORE config(): it feeds the has_files no-op gate and
-- incremental_predicates, and config() needs the latter.
{%- set pending_files_query -%}
SELECT csv_filename FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = 'daily'
{%- if is_incremental() %}
AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this }})
{%- endif -%}
{%- endset -%}

{%- if execute and flags.WHICH in ('run', 'build', 'retry') -%}
  {%- set files_result = run_query(pending_files_query) -%}
  {%- set pending_files = files_result.columns[0].values() | list if files_result else [] -%}
{%- else -%}
  {#-- Parse time: unknowable. none means "do not narrow the merge". --#}
  {%- set pending_files = none -%}
{%- endif -%}
{%- set has_files = pending_files is none or pending_files | length > 0 -%}

{#-- Literal file names, one spelling for both adapters: only literals prune target files, and
    duckrun rewrites DBT_INTERNAL_DEST itself. See macros/pending_file_predicate.sql. --#}
{%- set file_predicate = pending_file_predicate(pending_files) -%}

{{ config(
    materialized='incremental',
    incremental_strategy='insert' if target.name == 'duckrun' else 'merge',
    merge_clauses=none if target.name == 'duckrun' else {'when_matched': [{'action': 'do_nothing'}]},
    unique_key=['file', 'REGIONID', 'SETTLEMENTDATE','INTERVENTION'],
    partition_by=['month_key'] if target.name == 'duckrun' else none,
    incremental_predicates=file_predicate,
    pre_hook="SET VARIABLE price_daily_paths = (SELECT COALESCE(NULLIF(list('{{ get_csv_archive_path() }}' || archive_path), []), ['']) FROM (SELECT archive_path FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = 'daily'{% if is_incremental() %} AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this }}){% endif %} ORDER BY archive_path))"
) }}

{% if has_files %}
{# The CSV layout in file order — single source of truth: the read_csv
   columns spec and the CAST select are both generated from this list. #}
{%- set csv_cols = [
    'I', 'UNIT', 'XX', 'VERSION',
    'SETTLEMENTDATE', 'RUNNO', 'REGIONID', 'INTERVENTION',
    'RRP', 'EEP', 'ROP', 'APCFLAG',
    'MARKETSUSPENDEDFLAG', 'TOTALDEMAND', 'DEMANDFORECAST', 'DISPATCHABLEGENERATION',
    'DISPATCHABLELOAD', 'NETINTERCHANGE', 'EXCESSGENERATION', 'LOWER5MINDISPATCH',
    'LOWER5MINIMPORT', 'LOWER5MINLOCALDISPATCH', 'LOWER5MINLOCALPRICE', 'LOWER5MINLOCALREQ',
    'LOWER5MINPRICE', 'LOWER5MINREQ', 'LOWER5MINSUPPLYPRICE', 'LOWER60SECDISPATCH',
    'LOWER60SECIMPORT', 'LOWER60SECLOCALDISPATCH', 'LOWER60SECLOCALPRICE', 'LOWER60SECLOCALREQ',
    'LOWER60SECPRICE', 'LOWER60SECREQ', 'LOWER60SECSUPPLYPRICE', 'LOWER6SECDISPATCH',
    'LOWER6SECIMPORT', 'LOWER6SECLOCALDISPATCH', 'LOWER6SECLOCALPRICE', 'LOWER6SECLOCALREQ',
    'LOWER6SECPRICE', 'LOWER6SECREQ', 'LOWER6SECSUPPLYPRICE', 'RAISE5MINDISPATCH',
    'RAISE5MINIMPORT', 'RAISE5MINLOCALDISPATCH', 'RAISE5MINLOCALPRICE', 'RAISE5MINLOCALREQ',
    'RAISE5MINPRICE', 'RAISE5MINREQ', 'RAISE5MINSUPPLYPRICE', 'RAISE60SECDISPATCH',
    'RAISE60SECIMPORT', 'RAISE60SECLOCALDISPATCH', 'RAISE60SECLOCALPRICE', 'RAISE60SECLOCALREQ',
    'RAISE60SECPRICE', 'RAISE60SECREQ', 'RAISE60SECSUPPLYPRICE', 'RAISE6SECDISPATCH',
    'RAISE6SECIMPORT', 'RAISE6SECLOCALDISPATCH', 'RAISE6SECLOCALPRICE', 'RAISE6SECLOCALREQ',
    'RAISE6SECPRICE', 'RAISE6SECREQ', 'RAISE6SECSUPPLYPRICE', 'AGGREGATEDISPATCHERROR',
    'AVAILABLEGENERATION', 'AVAILABLELOAD', 'INITIALSUPPLY', 'CLEAREDSUPPLY',
    'LOWERREGIMPORT', 'LOWERREGLOCALDISPATCH', 'LOWERREGLOCALREQ', 'LOWERREGREQ',
    'RAISEREGIMPORT', 'RAISEREGLOCALDISPATCH', 'RAISEREGLOCALREQ', 'RAISEREGREQ',
    'RAISE5MINLOCALVIOLATION', 'RAISEREGLOCALVIOLATION', 'RAISE60SECLOCALVIOLATION', 'RAISE6SECLOCALVIOLATION',
    'LOWER5MINLOCALVIOLATION', 'LOWERREGLOCALVIOLATION', 'LOWER60SECLOCALVIOLATION', 'LOWER6SECLOCALVIOLATION',
    'RAISE5MINVIOLATION', 'RAISEREGVIOLATION', 'RAISE60SECVIOLATION', 'RAISE6SECVIOLATION',
    'LOWER5MINVIOLATION', 'LOWERREGVIOLATION', 'LOWER60SECVIOLATION', 'LOWER6SECVIOLATION',
    'RAISE6SECRRP', 'RAISE6SECROP', 'RAISE6SECAPCFLAG', 'RAISE60SECRRP',
    'RAISE60SECROP', 'RAISE60SECAPCFLAG', 'RAISE5MINRRP', 'RAISE5MINROP',
    'RAISE5MINAPCFLAG', 'RAISEREGRRP', 'RAISEREGROP', 'RAISEREGAPCFLAG',
    'LOWER6SECRRP', 'LOWER6SECROP', 'LOWER6SECAPCFLAG', 'LOWER60SECRRP',
    'LOWER60SECROP', 'LOWER60SECAPCFLAG', 'LOWER5MINRRP', 'LOWER5MINROP',
    'LOWER5MINAPCFLAG', 'LOWERREGRRP', 'LOWERREGROP', 'LOWERREGAPCFLAG',
    'RAISE6SECACTUALAVAILABILITY', 'RAISE60SECACTUALAVAILABILITY', 'RAISE5MINACTUALAVAILABILITY', 'RAISEREGACTUALAVAILABILITY',
    'LOWER6SECACTUALAVAILABILITY', 'LOWER60SECACTUALAVAILABILITY', 'LOWER5MINACTUALAVAILABILITY', 'LOWERREGACTUALAVAILABILITY',
    'LORSURPLUS', 'LRCSURPLUS'
] -%}
{# Kept raw or handled in the tail instead of CAST(... AS DOUBLE) #}
{%- set not_double = ['I', 'UNIT', 'XX', 'SETTLEMENTDATE', 'REGIONID'] -%}
WITH price_staging AS (
  SELECT *
  FROM read_csv(
    getvariable('price_daily_paths'),
    skip = 1,
    header = 0,
    all_varchar = 1,
    columns = {
      {%- for name in csv_cols %}
      '{{ name }}': 'VARCHAR'{{ "," if not loop.last }}
      {%- endfor %}
    },
    filename = 1,
    null_padding = true,
    ignore_errors = 1,
    auto_detect = false,
    hive_partitioning = false
  )
  WHERE I = 'D' AND UNIT = 'DREGION' AND VERSION = '3'
)

SELECT
  UNIT,
  REGIONID,
  {%- for name in csv_cols if name not in not_double %}
  CAST({{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('filename') }} AS file,
  CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS YEAR{% if target.name == 'duckrun' %},
  -- Monthly partition key (YYYYMM), the Delta partition column -- same expression as the duckrun
  -- AEMO reference model. duckrun only; see fct_scada.sql for why iceberg does not get it.
  CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) * 100
    + CAST(MONTH(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS month_key
{% endif %}
FROM price_staging
{% else %}
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
