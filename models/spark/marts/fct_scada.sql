-- Generation SCADA from the daily AEMO files (Spark). from_csv over the landed CSV folder
-- with an explicit schema; select by column name.
{%- set csv_cols = [
    'I','UNIT','XX','VERSION','SETTLEMENTDATE','RUNNO','DUID','INTERVENTION',
    'DISPATCHMODE','AGCSTATUS','INITIALMW','TOTALCLEARED','RAMPDOWNRATE','RAMPUPRATE',
    'LOWER5MIN','LOWER60SEC','LOWER6SEC','RAISE5MIN','RAISE60SEC','RAISE6SEC',
    'MARGINAL5MINVALUE','MARGINAL60SECVALUE','MARGINAL6SECVALUE','MARGINALVALUE',
    'VIOLATION5MINDEGREE','VIOLATION60SECDEGREE','VIOLATION6SECDEGREE','VIOLATIONDEGREE',
    'LOWERREG','RAISEREG','AVAILABILITY','RAISE6SECFLAGS','RAISE60SECFLAGS','RAISE5MINFLAGS',
    'RAISEREGFLAGS','LOWER6SECFLAGS','LOWER60SECFLAGS','LOWER5MINFLAGS','LOWERREGFLAGS',
    'RAISEREGAVAILABILITY','RAISEREGENABLEMENTMAX','RAISEREGENABLEMENTMIN','LOWERREGAVAILABILITY',
    'LOWERREGENABLEMENTMAX','LOWERREGENABLEMENTMIN','RAISE6SECACTUALAVAILABILITY',
    'RAISE60SECACTUALAVAILABILITY','RAISE5MINACTUALAVAILABILITY','RAISEREGACTUALAVAILABILITY',
    'LOWER6SECACTUALAVAILABILITY','LOWER60SECACTUALAVAILABILITY','LOWER5MINACTUALAVAILABILITY',
    'LOWERREGACTUALAVAILABILITY'
] -%}
{%- set not_double = ['I','UNIT','XX','SETTLEMENTDATE','DUID'] -%}
{%- set view_schema %}{% for c in csv_cols %}`{{ c }}` STRING{{ ', ' if not loop.last }}{% endfor %}{% endset %}
{#-- The raw CSVs are read inline via from_csv over the text.`path` datasource — no pre-created
     raw object AT ALL. This is NOT because Spark has no CSV reader: the obvious fix, and the
     one a notebook uses, is `spark.read.format("csv").schema(user_schema).load(paths)`, which
     is vectorized and prunes columns the way DuckDB's read_csv does. dbt cannot reach it.
     Both catalog-object approaches are illegal on a schema-enabled lakehouse:
     dbt-fabricspark builds its <model>__dbt_tmp intermediate as a PERSISTENT view, which cannot
     reference a TEMPORARY VIEW (INVALID_TEMP_OBJ_REFERENCE), and Fabric Spark rejects an external
     CSV table with an explicit schema ("External tables with partition columns or schema or
     properties are not supported"). A path scan is not a catalog object, so the persistent tmp
     view may reference it, and from_csv carries the explicit schema the ragged AEMO rows need.
     On incremental runs the path is an explicit brace glob of THIS RUN's new files (see
     spark_new_files) — a bare folder scan re-read the whole archive every run and took 30+
     minutes. A first/full-refresh build DOES read the bare folder: everything is new then. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

-- depends_on: {{ ref('stg_csv_archive_log') }}

{% set new_files = spark_new_files('daily', this) if is_incremental() else [] %}
{#-- Plain (non-trimming) tags: {%- -%} here would eat the newline that ends the depends_on
     comment above and glue `WITH raw AS (` onto it, commenting out the CTE header. --#}
{% if is_incremental() and new_files | length == 0 %}
{#-- No new daily files this run: compile to a zero-row no-op (append inserts nothing). --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{% else %}
WITH raw AS (
  SELECT
    from_csv(value, '{{ view_schema }}', map('mode', 'PERMISSIVE')) AS r,
    _metadata.file_name AS _fname
  FROM text.`{{ get_csv_archive_path() }}/daily{{ ('/{' ~ new_files | join(',') ~ '}') if is_incremental() else '' }}`
  {# Non-trimming comment tags on purpose; the trimming form glues WHERE onto the backtick
     path line. See the longer note on the same guard in fct_price.sql.
     Discard non-DUNIT lines BEFORE from_csv.
     DUNIT is a much larger share of a PUBLIC_DAILY file than DREGION is, so the win here is
     smaller, but it still skips a 53-column parse per rejected row. #}
  WHERE value LIKE 'D,DUNIT,%'
)
SELECT
  r.UNIT,
  r.DUID,
  {%- for name in csv_cols if name not in not_double %}
  CAST(r.{{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('_fname') }} AS file,
  -- AEMO ships SETTLEMENTDATE as 'yyyy/MM/dd HH:mm:ss'. Spark's CAST(string AS TIMESTAMP)
  -- accepts only yyyy-MM-dd and returns NULL for slashes instead of erroring (non-ANSI mode),
  -- which silently nulled the whole column here. DuckDB and T-SQL both parse slashes, so only
  -- this leg was affected. Parse the format explicitly.
  to_timestamp(r.SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS SETTLEMENTDATE,
  to_date(r.SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS DATE,
  CAST(YEAR(to_timestamp(r.SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss')) AS INT) AS YEAR
FROM raw
WHERE r.I = 'D' AND r.UNIT = 'DUNIT' AND r.VERSION = '3'
{% endif %}
