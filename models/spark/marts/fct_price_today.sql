-- Intraday regional prices (Spark). from_csv with explicit schema over the landed CSV folder.
{%- set csv_cols = [
    'I','DISPATCH','PRICE','xx','SETTLEMENTDATE','RUNNO','REGIONID','DISPATCHINTERVAL',
    'INTERVENTION','RRP','EEP','ROP','APCFLAG','MARKETSUSPENDEDFLAG','LASTCHANGED',
    'RAISE6SECRRP','RAISE6SECROP','RAISE6SECAPCFLAG','RAISE60SECRRP','RAISE60SECROP',
    'RAISE60SECAPCFLAG','RAISE5MINRRP','RAISE5MINROP','RAISE5MINAPCFLAG','RAISEREGRRP',
    'RAISEREGROP','RAISEREGAPCFLAG','LOWER6SECRRP','LOWER6SECROP','LOWER6SECAPCFLAG',
    'LOWER60SECRRP','LOWER60SECROP','LOWER60SECAPCFLAG','LOWER5MINRRP','LOWER5MINROP',
    'LOWER5MINAPCFLAG','LOWERREGRRP','LOWERREGROP','LOWERREGAPCFLAG','PRICE_STATUS',
    'PRE_AP_ENERGY_PRICE','PRE_AP_RAISE6_PRICE','PRE_AP_RAISE60_PRICE','PRE_AP_RAISE5MIN_PRICE',
    'PRE_AP_RAISEREG_PRICE','PRE_AP_LOWER6_PRICE','PRE_AP_LOWER60_PRICE','PRE_AP_LOWER5MIN_PRICE',
    'PRE_AP_LOWERREG_PRICE','RAISE1SECRRP','RAISE1SECROP','RAISE1SECAPCFLAG','LOWER1SECRRP',
    'LOWER1SECROP','LOWER1SECAPCFLAG','PRE_AP_RAISE1_PRICE','PRE_AP_LOWER1_PRICE',
    'CUMUL_PRE_AP_ENERGY_PRICE','CUMUL_PRE_AP_RAISE6_PRICE','CUMUL_PRE_AP_RAISE60_PRICE',
    'CUMUL_PRE_AP_RAISE5MIN_PRICE','CUMUL_PRE_AP_RAISEREG_PRICE','CUMUL_PRE_AP_LOWER6_PRICE',
    'CUMUL_PRE_AP_LOWER60_PRICE','CUMUL_PRE_AP_LOWER5MIN_PRICE','CUMUL_PRE_AP_LOWERREG_PRICE',
    'CUMUL_PRE_AP_RAISE1_PRICE','CUMUL_PRE_AP_LOWER1_PRICE','OCD_STATUS','MII_STATUS'
] -%}
{%- set not_double = ['I','DISPATCH','PRICE','xx','SETTLEMENTDATE','REGIONID','LASTCHANGED','PRICE_STATUS','OCD_STATUS','MII_STATUS'] -%}
{%- set view_schema %}{% for c in csv_cols %}`{{ c }}` STRING{{ ', ' if not loop.last }}{% endfor %}{% endset %}
{#-- No pre-created raw object at all — see the note in fct_scada.sql. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

-- depends_on: {{ ref('stg_csv_archive_log') }}

{% set new_files = spark_new_files('price_today', this if is_incremental() else none) %}
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
  FROM text.`{{ get_csv_archive_path() }}/price_today/{{ '{' ~ new_files | join(',') ~ '}' }}`
)
SELECT
  r.REGIONID,
  {%- for name in csv_cols if name not in not_double %}
  CAST(r.{{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  -- AEMO ships SETTLEMENTDATE as 'yyyy/MM/dd HH:mm:ss'. Spark's CAST(string AS TIMESTAMP)
  -- accepts only yyyy-MM-dd and returns NULL for slashes instead of erroring (non-ANSI mode),
  -- which silently nulled the whole column here. DuckDB and T-SQL both parse slashes, so only
  -- this leg was affected. Parse the format explicitly.
  to_timestamp(r.SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS SETTLEMENTDATE,
  to_date(r.SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS DATE,
  {{ parse_filename('_fname') }} AS file,
  CAST(YEAR(to_timestamp(r.SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss')) AS INT) AS YEAR
FROM raw
WHERE r.I = 'D' AND r.PRICE = 'PRICE'
{% endif %}
