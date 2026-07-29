-- Insert-only on both targets: matched rows are left alone, so every commit stays a
-- single append snapshot -- the OneLake catalog rejects multi-snapshot commits (see the
-- fct_summary.sql header) -- while a re-processed file dedupes on the unique_key instead
-- of double-inserting.
--
-- Two spellings for the same semantics, because the adapters expose it differently:
--   duckrun -> incremental_strategy='insert'. delta-rs insert-only merge, and the commit
--     is OCC-fenced on the version the model read, so a concurrent writer loses the race
--     with CommitFailedError instead of appending a duplicate. NOT merge_clauses
--     do_nothing: duckrun's clause translator accepts only 'update'/'delete' for
--     when_matched and raises on 'do_nothing' (delta_plugin.py, _specs_from_merge_clauses).
--   iceberg -> merge + when_matched do_nothing, which dbt-duckdb does support. Omitting
--     when_matched would default to update-by-name and draw the REST catalog's 400.
--
-- This is NOT append. The pre_hook file list already excludes ingested files, but it is
-- computed before the write, so two overlapping runs both see a file as new and both
-- append it. The key match is the guard underneath that; the file list stays as the thing
-- that keeps the merge source small.
--
-- The pending-file probe below runs BEFORE config() on purpose: it feeds both the has_files
-- no-op gate and incremental_predicates, and config() needs the latter. It is the same single
-- query that used to return only COUNT(*), so this costs no extra read of {{ this }}.
-- See macros/pending_file_predicate.sql for why the predicate has to carry literal file names
-- rather than a target.x = source.x comparison -- without it this merge scans the whole
-- 143M-row table on every run, which is what failed the duckrun leg.
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

{#-- duckrun gets BOTH predicates. The literal file predicate is the only thing that prunes
    target FILES: a column-to-column predicate scans 60/60 even when the table is partitioned
    (measured -- see macros/pending_file_predicate.sql), so month_key alone "reads like the lever
    but is not the thing doing the work". Shipping it alone is what got fct_scada OOM-killed on a
    122 GiB notebook. month_key stays because it selects the partitions the write lands in.
    Safe to AND them: every key match shares SETTLEMENTDATE, hence month_key, and the file name
    leads the unique_key -- so both are implied by the ON clause and remove no match it would make.
    The macro emits DBT_INTERNAL_DEST and duckrun rewrites that to `target` itself, so one
    spelling serves both adapters. --#}
{%- set file_predicate = pending_file_predicate(pending_files) -%}
{%- set duckrun_predicates = (file_predicate if file_predicate else []) + ['target.month_key = source.month_key'] %}

{#-- OCC FENCE -- DO NOT DELETE. duckrun writes this model with `append`, which is only
     fenced when the adapter sees the relation name in the RENDERED MODEL SQL:
       reads_self = dbt_believes_exists and ... and (this | string) in model_sql
     (_delta_core.sql). When true it commits via append_if_unchanged(read_version=vB), so a
     concurrent writer fails loudly with CommitFailedError instead of appending a duplicate;
     when false it degrades SILENTLY to an unfenced last-writer-wins append. The token below
     is what makes that true, on purpose -- it was previously true only by accident, via a
     passing mention of {{ this }} in a prose comment. A comment counts: dbt renders it. --#}

{{ config(
    materialized='incremental',
    incremental_strategy='append' if target.name == 'duckrun' else 'merge',
    merge_clauses=none if target.name == 'duckrun' else {'when_matched': [{'action': 'do_nothing'}]},
    unique_key=['file', 'REGIONID', 'SETTLEMENTDATE','INTERVENTION'],
    partition_by=['month_key'] if target.name == 'duckrun' else none,
    incremental_predicates=(none if target.name == 'duckrun' else file_predicate),
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
