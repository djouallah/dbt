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
