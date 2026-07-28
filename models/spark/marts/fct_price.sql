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
{#-- Two different reads, picked by is_incremental() — see the note in fct_scada.sql for why
     neither one can serve both.

     NOT incremental (first build / --full-refresh): the materialization runs a bare
     CREATE TABLE AS SELECT with no __dbt_tmp at all, and a CTAS executes immediately, so it
     MAY reference a TEMPORARY VIEW. That buys the real CSV datasource with an explicit
     schema — vectorized, column-pruning, the same read the notebook version of this pipeline
     used. This is the path that matters: it folds the whole ~3,000-file archive.

     Incremental: the materialization builds __dbt_tmp as a PERSISTENT view, whose stored
     definition cannot reference a temp view, so this path keeps from_csv. It only ever reads
     THIS run's new files, so the per-row parse is noise.

     USING csv is only legal on a TEMPORARY view — persistent CREATE VIEW is `AS query` only
     and cannot carry USING/OPTIONS, and CREATE TABLE ... USING csv with a schema is rejected
     by Fabric. So temp is forced, which is exactly why it only works on the CTAS path. --#}
{%- set daily_root = get_csv_archive_path() ~ '/daily' -%}
{%- set raw_view = 'raw_daily_price' -%}
{#-- Insert-only merge, not append. skip_matched_step drops the WHEN MATCHED branch entirely
     (dbt-fabricspark honours it in fabricspark__get_merge_sql), so this is MERGE ... WHEN NOT
     MATCHED THEN INSERT and nothing else -- the right shape for append-only data, and it
     cannot hit a multiple-source-row match error because there is no matched clause.

     Why not append: spark_new_files() below excludes files already in {{ this }}, but that
     list is computed BEFORE the write. Two overlapping runs both see a file as new and both
     append it. The key match is the write-time guard underneath the file list.

     merge and append take the identical path in this materialization -- the compiled SQL
     becomes a persistent __dbt_tmp view either way, and only the DML that follows differs --
     so the CSV read below is untouched by this. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='delta',
    unique_key=['file', 'REGIONID', 'SETTLEMENTDATE', 'INTERVENTION'],
    skip_matched_step=true,
    pre_hook=(none if is_incremental() else
      "CREATE OR REPLACE TEMPORARY VIEW " ~ raw_view ~ " (" ~ view_schema ~ ")"
      ~ " USING csv OPTIONS (path '" ~ daily_root ~ "', header 'true', mode 'PERMISSIVE')")
) }}

-- depends_on: {{ ref('stg_csv_archive_log') }}

{% set new_files = spark_new_files('daily', this) if is_incremental() else [] %}
{#-- Plain (non-trimming) tags: {%- -%} here would eat the newline that ends the depends_on
     comment above and glue `WITH raw AS (` onto it, commenting out the CTE header. --#}
{% if is_incremental() and new_files | length == 0 %}
{#-- No new daily files this run: compile to a zero-row no-op (the merge source is empty). --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{% elif not is_incremental() %}
{#-- CTAS path: read the temp view the pre_hook built. Real CSV datasource, so the columns
     arrive already split — no `r.` struct prefix, and the file name comes from
     input_file_name() rather than _metadata (a view with a declared column list does not
     expose _metadata). parse_filename handles a full abfss path either way. --#}
SELECT
  UNIT,
  REGIONID,
  {%- for name in csv_cols if name not in not_double %}
  CAST({{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('input_file_name()') }} AS file,
  -- See the SETTLEMENTDATE note in the incremental branch below.
  to_timestamp(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS SETTLEMENTDATE,
  to_date(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS DATE,
  CAST(YEAR(to_timestamp(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss')) AS INT) AS YEAR
FROM {{ raw_view }}
WHERE I = 'D' AND UNIT = 'DREGION' AND VERSION = '3'
{% else %}
WITH raw AS (
  SELECT
    from_csv(value, '{{ view_schema }}', map('mode', 'PERMISSIVE')) AS r,
    _metadata.file_name AS _fname
  FROM text.`{{ get_csv_archive_path() }}/daily/{{ '{' ~ new_files | join(',') ~ '}' }}`
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
