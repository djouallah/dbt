-- Insert-only on both targets: matched rows are left alone, so every commit stays a single
-- append snapshot, while a re-processed file dedupes on the unique_key instead of
-- double-inserting. duckrun spells that 'insert', iceberg spells it merge + when_matched
-- do_nothing -- see the fct_price.sql header for why the two cannot be written the same way,
-- and for why neither is 'append'.
-- The pending-file probe runs BEFORE config() on purpose: it feeds both the has_files
-- no-op gate and incremental_predicates, and config() needs the latter. Same single query
-- that used to return only COUNT(*), so it costs no extra read of {{ this }}. See
-- macros/pending_file_predicate.sql for why the predicate must carry literal file names.
{%- set pending_files_query -%}
SELECT csv_filename FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = 'price_today'
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

{{ config(
    materialized='incremental',
    incremental_strategy='insert' if target.name == 'duckrun' else 'merge',
    merge_clauses=none if target.name == 'duckrun' else {'when_matched': [{'action': 'do_nothing'}]},
    unique_key=['file', 'REGIONID', 'SETTLEMENTDATE','INTERVENTION'],
    partition_by=['month_key'] if target.name == 'duckrun' else none,
    incremental_predicates=(['target.month_key = source.month_key']
                            if target.name == 'duckrun'
                            else pending_file_predicate(pending_files)),
    pre_hook="SET VARIABLE price_today_paths = (SELECT COALESCE(NULLIF(list('{{ get_csv_archive_path() }}' || archive_path), []), ['']) FROM (SELECT archive_path FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = 'price_today'{% if is_incremental() %} AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this }}){% endif %} ORDER BY archive_path))"
) }}

{% set csv_archive_path = get_csv_archive_path() %}

{% if has_files %}
{# The CSV layout in file order — single source of truth: the read_csv
   columns spec and the CAST select are both generated from this list. #}
{%- set csv_cols = [
    ('I', 'VARCHAR'), ('DISPATCH', 'VARCHAR'),
    ('PRICE', 'VARCHAR'), ('xx', 'VARCHAR'),
    ('SETTLEMENTDATE', 'timestamp'), ('RUNNO', 'VARCHAR'),
    ('REGIONID', 'VARCHAR'), ('DISPATCHINTERVAL', 'VARCHAR'),
    ('INTERVENTION', 'VARCHAR'), ('RRP', 'VARCHAR'),
    ('EEP', 'VARCHAR'), ('ROP', 'VARCHAR'),
    ('APCFLAG', 'VARCHAR'), ('MARKETSUSPENDEDFLAG', 'VARCHAR'),
    ('LASTCHANGED', 'VARCHAR'), ('RAISE6SECRRP', 'VARCHAR'),
    ('RAISE6SECROP', 'VARCHAR'), ('RAISE6SECAPCFLAG', 'VARCHAR'),
    ('RAISE60SECRRP', 'VARCHAR'), ('RAISE60SECROP', 'VARCHAR'),
    ('RAISE60SECAPCFLAG', 'VARCHAR'), ('RAISE5MINRRP', 'VARCHAR'),
    ('RAISE5MINROP', 'VARCHAR'), ('RAISE5MINAPCFLAG', 'VARCHAR'),
    ('RAISEREGRRP', 'VARCHAR'), ('RAISEREGROP', 'VARCHAR'),
    ('RAISEREGAPCFLAG', 'VARCHAR'), ('LOWER6SECRRP', 'VARCHAR'),
    ('LOWER6SECROP', 'VARCHAR'), ('LOWER6SECAPCFLAG', 'VARCHAR'),
    ('LOWER60SECRRP', 'VARCHAR'), ('LOWER60SECROP', 'VARCHAR'),
    ('LOWER60SECAPCFLAG', 'VARCHAR'), ('LOWER5MINRRP', 'VARCHAR'),
    ('LOWER5MINROP', 'VARCHAR'), ('LOWER5MINAPCFLAG', 'VARCHAR'),
    ('LOWERREGRRP', 'VARCHAR'), ('LOWERREGROP', 'VARCHAR'),
    ('LOWERREGAPCFLAG', 'VARCHAR'), ('PRICE_STATUS', 'VARCHAR'),
    ('PRE_AP_ENERGY_PRICE', 'VARCHAR'), ('PRE_AP_RAISE6_PRICE', 'VARCHAR'),
    ('PRE_AP_RAISE60_PRICE', 'VARCHAR'), ('PRE_AP_RAISE5MIN_PRICE', 'VARCHAR'),
    ('PRE_AP_RAISEREG_PRICE', 'VARCHAR'), ('PRE_AP_LOWER6_PRICE', 'VARCHAR'),
    ('PRE_AP_LOWER60_PRICE', 'VARCHAR'), ('PRE_AP_LOWER5MIN_PRICE', 'VARCHAR'),
    ('PRE_AP_LOWERREG_PRICE', 'VARCHAR'), ('RAISE1SECRRP', 'VARCHAR'),
    ('RAISE1SECROP', 'VARCHAR'), ('RAISE1SECAPCFLAG', 'VARCHAR'),
    ('LOWER1SECRRP', 'VARCHAR'), ('LOWER1SECROP', 'VARCHAR'),
    ('LOWER1SECAPCFLAG', 'VARCHAR'), ('PRE_AP_RAISE1_PRICE', 'VARCHAR'),
    ('PRE_AP_LOWER1_PRICE', 'VARCHAR'), ('CUMUL_PRE_AP_ENERGY_PRICE', 'VARCHAR'),
    ('CUMUL_PRE_AP_RAISE6_PRICE', 'VARCHAR'), ('CUMUL_PRE_AP_RAISE60_PRICE', 'VARCHAR'),
    ('CUMUL_PRE_AP_RAISE5MIN_PRICE', 'VARCHAR'), ('CUMUL_PRE_AP_RAISEREG_PRICE', 'VARCHAR'),
    ('CUMUL_PRE_AP_LOWER6_PRICE', 'VARCHAR'), ('CUMUL_PRE_AP_LOWER60_PRICE', 'VARCHAR'),
    ('CUMUL_PRE_AP_LOWER5MIN_PRICE', 'VARCHAR'), ('CUMUL_PRE_AP_LOWERREG_PRICE', 'VARCHAR'),
    ('CUMUL_PRE_AP_RAISE1_PRICE', 'VARCHAR'), ('CUMUL_PRE_AP_LOWER1_PRICE', 'VARCHAR'),
    ('OCD_STATUS', 'VARCHAR'), ('MII_STATUS', 'VARCHAR')
] -%}
{# Kept raw or handled in the tail instead of CAST(... AS DOUBLE) #}
{%- set not_double = ['I', 'DISPATCH', 'PRICE', 'xx', 'SETTLEMENTDATE', 'REGIONID', 'LASTCHANGED', 'PRICE_STATUS', 'OCD_STATUS', 'MII_STATUS'] -%}
WITH price_staging AS (
  SELECT *
  FROM read_csv(
    getvariable('price_today_paths'),
    skip = 1,
    header = 0,
    all_varchar = 1,
    columns = {
      {%- for name, type in csv_cols %}
      '{{ name }}': '{{ type }}'{{ "," if not loop.last }}
      {%- endfor %}
    },
    filename = 1,
    null_padding = true,
    ignore_errors = 1,
    auto_detect = false,
    hive_partitioning = false
  )
  WHERE I = 'D' AND PRICE = 'PRICE'
)

SELECT
  REGIONID,
  {%- for name, type in csv_cols if name not in not_double %}
  CAST({{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  {{ parse_filename('filename') }} AS file,
  CAST(YEAR(SETTLEMENTDATE) AS INT) AS YEAR{% if target.name == 'duckrun' %},
  -- Monthly partition key (YYYYMM), the Delta partition column -- same expression as the duckrun
  -- AEMO reference model. duckrun only; see fct_scada.sql for why iceberg does not get it.
  CAST(YEAR(SETTLEMENTDATE) AS INT) * 100
    + CAST(MONTH(SETTLEMENTDATE) AS INT) AS month_key
{% endif %}
FROM price_staging
{% else %}
-- No unprocessed files: empty result keeps existing data untouched
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
