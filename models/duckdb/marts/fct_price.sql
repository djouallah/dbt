-- Insert-only on both targets: matched rows are left alone, so every commit stays a
-- single append snapshot -- the OneLake catalog rejects multi-snapshot commits (see the
-- fct_summary.sql header) -- while a re-processed file dedupes on the unique_key instead
-- of double-inserting.
--
-- Two spellings for the same semantics, because the adapters expose it differently:
--   duckrun -> incremental_strategy='insert'. Since 0.4.34 that is NOT a delta-rs merge: the
--     adapter anti-joins the batch against the target's KEY columns in DuckDB and commits a
--     plain append (add actions only, no file rewritten), so cost tracks the batch instead of
--     the target's partition span, and the append is ALWAYS fenced to the version the anti-join
--     read -- a concurrent writer loses with CommitFailedError instead of duplicating. The
--     delta-rs insert-only merge it replaced measured 6.7s/+8.4GB RSS against 0.9s/+84MB on a
--     20M-row table; the memory is why it OOM-killed fct_scada (369M rows) outright, and why
--     this file briefly carried 'append' plus a reads_self fence instead. Both are gone.
--     NOT merge_clauses do_nothing: duckrun's clause translator accepts only 'update'/'delete'
--     for when_matched and raises on 'do_nothing' (delta_plugin.py, _specs_from_merge_clauses),
--     which is the one thing keeping the two targets from sharing a single config.
--   iceberg -> merge + when_matched do_nothing, which dbt-duckdb does support (it has no
--     'insert' strategy at all). Omitting when_matched would default to update-by-name and
--     draw the REST catalog's 400.
--
-- Neither is plain 'append'. The pre_hook file list already excludes ingested files, but it is
-- computed before the write, so two overlapping runs both see a file as new and both append it.
-- The key match is the guard underneath that; the file list stays as the thing that keeps the
-- source small.
--
-- The pending-file probe below runs BEFORE config() on purpose: it feeds both the has_files
-- no-op gate and iceberg's incremental_predicates, and config() needs the latter. It is the same
-- single query that used to return only COUNT(*), so this costs no extra read of this table.
-- See macros/pending_file_predicate.sql for why an ICEBERG predicate has to carry literal file
-- names rather than a target.x = source.x comparison -- without it that merge scans the whole
-- 143M-row table every run. duckrun builds the equivalent itself, from the batch.
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

{#-- ICEBERG ONLY: the literal file predicate. Its merge prunes target FILES only from literal
    values -- a column-to-column predicate scans 60/60 even on a partitioned table (measured --
    see macros/pending_file_predicate.sql). duckrun needs none of it: engine.probe_filters builds
    the equivalent from the batch itself (an exact `IN` list for the declared partition equality,
    min/max bounds for every other join key, so `file` gets its range for free). Both predicates
    are implied by the ON clause -- `file` leads the unique_key and every key match shares
    SETTLEMENTDATE, hence month_key -- so neither removes a match the key would have made. --#}
{%- set file_predicate = pending_file_predicate(pending_files) -%}

{{ config(
    materialized='incremental',
    incremental_strategy='insert' if target.name == 'duckrun' else 'merge',
    merge_clauses=none if target.name == 'duckrun' else {'when_matched': [{'action': 'do_nothing'}]},
    unique_key=['file', 'REGIONID', 'SETTLEMENTDATE','INTERVENTION'],
    partition_by=['month_key'] if target.name == 'duckrun' else none,
    incremental_predicates=(['target.month_key = source.month_key']
                            if target.name == 'duckrun' else file_predicate),
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
