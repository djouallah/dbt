-- Regional spot prices from the daily AEMO files (Spark). from_csv over the landed CSV folder
-- with an explicit schema; select by column name. The reason it is from_csv and not the CSV
-- datasource is NOT that Spark lacks a reader -- `spark.read.format("csv").schema(...)` works
-- fine and is what a notebook would use. It is that neither catalog-object form survives here;
-- see the note in fct_scada.sql. from_csv is the only shape that carries an explicit schema
-- without being a catalog object, so the cost is paid per row and the WHERE below is what
-- keeps that cost off the rows we do not want.
{%- set csv_cols = [
    'I','UNIT','XX','VERSION','SETTLEMENTDATE','RUNNO','REGIONID','INTERVENTION',
    'RRP','EEP','ROP','APCFLAG','MARKETSUSPENDEDFLAG','TOTALDEMAND','DEMANDFORECAST',
    'DISPATCHABLEGENERATION','DISPATCHABLELOAD','NETINTERCHANGE','EXCESSGENERATION',
    'LOWER5MINDISPATCH','LOWER5MINIMPORT','LOWER5MINLOCALDISPATCH','LOWER5MINLOCALPRICE',
    'LOWER5MINLOCALREQ','LOWER5MINPRICE','LOWER5MINREQ','LOWER5MINSUPPLYPRICE','LOWER60SECDISPATCH',
    'LOWER60SECIMPORT','LOWER60SECLOCALDISPATCH','LOWER60SECLOCALPRICE','LOWER60SECLOCALREQ',
    'LOWER60SECPRICE','LOWER60SECREQ','LOWER60SECSUPPLYPRICE','LOWER6SECDISPATCH','LOWER6SECIMPORT',
    'LOWER6SECLOCALDISPATCH','LOWER6SECLOCALPRICE','LOWER6SECLOCALREQ','LOWER6SECPRICE','LOWER6SECREQ',
    'LOWER6SECSUPPLYPRICE','RAISE5MINDISPATCH','RAISE5MINIMPORT','RAISE5MINLOCALDISPATCH',
    'RAISE5MINLOCALPRICE','RAISE5MINLOCALREQ','RAISE5MINPRICE','RAISE5MINREQ','RAISE5MINSUPPLYPRICE',
    'RAISE60SECDISPATCH','RAISE60SECIMPORT','RAISE60SECLOCALDISPATCH','RAISE60SECLOCALPRICE',
    'RAISE60SECLOCALREQ','RAISE60SECPRICE','RAISE60SECREQ','RAISE60SECSUPPLYPRICE','RAISE6SECDISPATCH',
    'RAISE6SECIMPORT','RAISE6SECLOCALDISPATCH','RAISE6SECLOCALPRICE','RAISE6SECLOCALREQ','RAISE6SECPRICE',
    'RAISE6SECREQ','RAISE6SECSUPPLYPRICE','AGGREGATEDISPATCHERROR','AVAILABLEGENERATION','AVAILABLELOAD',
    'INITIALSUPPLY','CLEAREDSUPPLY','LOWERREGIMPORT','LOWERREGLOCALDISPATCH','LOWERREGLOCALREQ',
    'LOWERREGREQ','RAISEREGIMPORT','RAISEREGLOCALDISPATCH','RAISEREGLOCALREQ','RAISEREGREQ',
    'RAISE5MINLOCALVIOLATION','RAISEREGLOCALVIOLATION','RAISE60SECLOCALVIOLATION','RAISE6SECLOCALVIOLATION',
    'LOWER5MINLOCALVIOLATION','LOWERREGLOCALVIOLATION','LOWER60SECLOCALVIOLATION','LOWER6SECLOCALVIOLATION',
    'RAISE5MINVIOLATION','RAISEREGVIOLATION','RAISE60SECVIOLATION','RAISE6SECVIOLATION','LOWER5MINVIOLATION',
    'LOWERREGVIOLATION','LOWER60SECVIOLATION','LOWER6SECVIOLATION','RAISE6SECRRP','RAISE6SECROP',
    'RAISE6SECAPCFLAG','RAISE60SECRRP','RAISE60SECROP','RAISE60SECAPCFLAG','RAISE5MINRRP','RAISE5MINROP',
    'RAISE5MINAPCFLAG','RAISEREGRRP','RAISEREGROP','RAISEREGAPCFLAG','LOWER6SECRRP','LOWER6SECROP',
    'LOWER6SECAPCFLAG','LOWER60SECRRP','LOWER60SECROP','LOWER60SECAPCFLAG','LOWER5MINRRP','LOWER5MINROP',
    'LOWER5MINAPCFLAG','LOWERREGRRP','LOWERREGROP','LOWERREGAPCFLAG','RAISE6SECACTUALAVAILABILITY',
    'RAISE60SECACTUALAVAILABILITY','RAISE5MINACTUALAVAILABILITY','RAISEREGACTUALAVAILABILITY',
    'LOWER6SECACTUALAVAILABILITY','LOWER60SECACTUALAVAILABILITY','LOWER5MINACTUALAVAILABILITY',
    'LOWERREGACTUALAVAILABILITY','LORSURPLUS','LRCSURPLUS'
] -%}
{%- set not_double = ['I','UNIT','XX','SETTLEMENTDATE','REGIONID'] -%}
{%- set view_schema %}{% for c in csv_cols %}`{{ c }}` STRING{{ ', ' if not loop.last }}{% endfor %}{% endset %}
{#-- No pre-created raw object at all — see the note in fct_scada.sql. --#}
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
  {# Non-trimming comment tags on purpose. The trimming form used elsewhere in this file eats
     the newline after the backtick path and renders the WHERE glued onto it, which is the same
     family of bug as the depends_on trap. Do not "tidy" this into the trimming form, and do not
     write the trimming tokens inside a comment either -- Jinja comments do not nest.

     Discard non-DREGION lines BEFORE from_csv, not after. WHERE is evaluated ahead of the
     SELECT list, so the plan is Scan -> Filter -> Project and from_csv never runs on a row
     this rejects. Same predicate as the tail WHERE, expressed against the raw line: a
     PUBLIC_DAILY file holds every report type AEMO ships that day and DREGION is a sliver of
     it (5 regions x 288 intervals), so without this the model fully parses 130 columns of
     every DISPATCH/TRADING/etc. row and then throws it away. That is what made a full rebuild
     1,773 tasks at ~150s each. The tail WHERE stays -- it is the correctness check and still
     enforces VERSION; this is only the cheap pre-pass. #}
  WHERE value LIKE 'D,DREGION,%'
)
SELECT
  r.UNIT,
  r.REGIONID,
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
WHERE r.I = 'D' AND r.UNIT = 'DREGION' AND r.VERSION = '3'
{% endif %}
