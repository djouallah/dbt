-- Intraday SCADA (Spark). Fabric Spark 3.5 has no read_files(), so the model parses the landed
-- CSV folder inline with from_csv and an EXPLICIT schema (handles the ragged AEMO rows), and
-- selects by column name. _metadata.file_name drives file-level incremental.
{%- set cols = ['I','DISPATCH','UNIT_SCADA','xx','SETTLEMENTDATE','DUID','SCADAVALUE','LASTCHANGED'] -%}
{%- set view_schema %}{% for c in cols %}`{{ c }}` STRING{{ ', ' if not loop.last }}{% endfor %}{% endset %}
{#-- No pre-created raw object at all — see the note in fct_scada.sql. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

-- depends_on: {{ ref('stg_csv_archive_log') }}

{% set new_files = spark_new_files('scada_today', this) if is_incremental() else [] %}
{#-- Plain (non-trimming) tags: {%- -%} here would eat the newline that ends the depends_on
     comment above and glue `WITH raw AS (` onto it, commenting out the CTE header. --#}
{% if is_incremental() and new_files | length == 0 %}
{#-- No new intraday files this run: compile to a zero-row no-op (append inserts nothing). --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{% else %}
WITH raw AS (
  SELECT
    from_csv(value, '{{ view_schema }}', map('mode', 'PERMISSIVE')) AS r,
    _metadata.file_name AS _fname
  FROM text.`{{ get_csv_archive_path() }}/scada_today{{ ('/{' ~ new_files | join(',') ~ '}') if is_incremental() else '' }}`
)
SELECT
  r.DUID,
  CAST(r.SCADAVALUE AS DOUBLE) AS INITIALMW,
  {{ parse_filename('_fname') }} AS file,
  -- AEMO ships SETTLEMENTDATE as 'yyyy/MM/dd HH:mm:ss'. Spark's CAST(string AS TIMESTAMP)
  -- accepts only yyyy-MM-dd and returns NULL for slashes instead of erroring (non-ANSI mode),
  -- which silently nulled the whole column here. DuckDB and T-SQL both parse slashes, so only
  -- this leg was affected. Parse the format explicitly.
  to_timestamp(r.SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS SETTLEMENTDATE,
  to_timestamp(r.LASTCHANGED, 'yyyy/MM/dd HH:mm:ss') AS LASTCHANGED,
  to_date(r.SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS DATE,
  CAST(YEAR(to_timestamp(r.SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss')) AS INT) AS YEAR
FROM raw
WHERE r.I = 'D' AND CAST(r.SCADAVALUE AS DOUBLE) <> 0
{% endif %}
