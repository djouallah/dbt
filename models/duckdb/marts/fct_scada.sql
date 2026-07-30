-- Insert-only on both targets: a matched row is left alone, so a re-processed file dedupes on
-- the unique_key instead of double-inserting. See the fct_price.sql header for why the two
-- adapters cannot spell that the same way, and for why neither is plain 'append'.
--
-- fct_scada is the big one (369M rows). On duckrun 'insert' is a DuckDB anti-join over the
-- target's KEY columns committed as an add-only append, so its cost tracks the BATCH, not the
-- table; partition_by + the declared month_key equality are what let the probe emit a literal
-- `"month_key" IN (...)` and skip whole partition directories.
-- The pending-file probe runs BEFORE config() on purpose: it feeds both the has_files no-op
-- gate and iceberg's incremental_predicates, and config() needs the latter. Same single query
-- that used to return only COUNT(*), so it costs no extra read of this table. See
-- macros/pending_file_predicate.sql -- it is the ICEBERG predicate only; duckrun derives its
-- own literal filters from the batch.
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

{#-- ICEBERG ONLY: the literal file predicate. delta-rs/DuckDB prune target FILES only from
    literal values -- a column-to-column predicate scans 60/60 even on a partitioned table
    (measured -- see macros/pending_file_predicate.sql). duckrun needs none of it: its insert
    anti-join derives its own literal filters from the batch (engine.probe_filters -- an exact
    `IN` list for the declared partition equality, min/max bounds for every other join key, so
    `file` gets its range for free). --#}
{%- set file_predicate = pending_file_predicate(pending_files) -%}

{{ config(
    materialized='incremental',
    incremental_strategy='insert' if target.name == 'duckrun' else 'merge',
    merge_clauses=none if target.name == 'duckrun' else {'when_matched': [{'action': 'do_nothing'}]},
    unique_key=['file', 'DUID', 'SETTLEMENTDATE','INTERVENTION'],
    partition_by=['month_key'] if target.name == 'duckrun' else none,
    incremental_predicates=(['target.month_key = source.month_key']
                            if target.name == 'duckrun' else file_predicate),
    pre_hook="SET VARIABLE scada_daily_paths = (SELECT COALESCE(NULLIF(list('{{ get_csv_archive_path() }}' || archive_path), []), ['']) FROM (SELECT archive_path FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = 'daily'{% if is_incremental() %} AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this }}){% endif %} ORDER BY archive_path))"
) }}

{% if has_files %}
{# The CSV layout in file order — single source of truth: the read_csv
   columns spec and the CAST select are both generated from this list. #}
{%- set csv_cols = [
    'I', 'UNIT', 'XX', 'VERSION',
    'SETTLEMENTDATE', 'RUNNO', 'DUID', 'INTERVENTION',
    'DISPATCHMODE', 'AGCSTATUS', 'INITIALMW', 'TOTALCLEARED',
    'RAMPDOWNRATE', 'RAMPUPRATE', 'LOWER5MIN', 'LOWER60SEC',
    'LOWER6SEC', 'RAISE5MIN', 'RAISE60SEC', 'RAISE6SEC',
    'MARGINAL5MINVALUE', 'MARGINAL60SECVALUE', 'MARGINAL6SECVALUE', 'MARGINALVALUE',
    'VIOLATION5MINDEGREE', 'VIOLATION60SECDEGREE', 'VIOLATION6SECDEGREE', 'VIOLATIONDEGREE',
    'LOWERREG', 'RAISEREG', 'AVAILABILITY', 'RAISE6SECFLAGS',
    'RAISE60SECFLAGS', 'RAISE5MINFLAGS', 'RAISEREGFLAGS', 'LOWER6SECFLAGS',
    'LOWER60SECFLAGS', 'LOWER5MINFLAGS', 'LOWERREGFLAGS', 'RAISEREGAVAILABILITY',
    'RAISEREGENABLEMENTMAX', 'RAISEREGENABLEMENTMIN', 'LOWERREGAVAILABILITY', 'LOWERREGENABLEMENTMAX',
    'LOWERREGENABLEMENTMIN', 'RAISE6SECACTUALAVAILABILITY', 'RAISE60SECACTUALAVAILABILITY', 'RAISE5MINACTUALAVAILABILITY',
    'RAISEREGACTUALAVAILABILITY', 'LOWER6SECACTUALAVAILABILITY', 'LOWER60SECACTUALAVAILABILITY', 'LOWER5MINACTUALAVAILABILITY',
    'LOWERREGACTUALAVAILABILITY'
] -%}
{# Kept raw or handled in the tail instead of CAST(... AS DOUBLE) #}
{%- set not_double = ['I', 'UNIT', 'XX', 'SETTLEMENTDATE', 'DUID'] -%}
WITH scada_staging AS (
  SELECT *
  FROM read_csv(
    getvariable('scada_daily_paths'),
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
  WHERE I = 'D' AND UNIT = 'DUNIT' AND VERSION = '3'
)

SELECT
  UNIT,
  DUID,
  {%- for name in csv_cols if name not in not_double %}
  CAST({{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('filename') }} AS file,
  CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS YEAR{% if target.name == 'duckrun' %},
  -- Monthly partition key (YYYYMM), the Delta partition column -- same expression as the duckrun
  -- AEMO reference model, and the same key dwh already carries. duckrun only: the iceberg table
  -- was not dropped, so adding a column there would have to schema-evolve through the REST
  -- catalog, which is where this project has repeatedly hit 400s.
  CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) * 100
    + CAST(MONTH(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS month_key
{% endif %}
FROM scada_staging
{% else %}
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
