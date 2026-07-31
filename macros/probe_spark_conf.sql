{#-- TEMPORARY DIAGNOSTIC — delete once it has reported. See the investigation note below.

     Question: does `spark_config.conf` in profiles.yml actually reach the Spark session?
     The Spark UI Environment tab says no. That tab is NOT a valid instrument here: it
     renders the SparkContext conf captured at application launch, and never shows a
     `spark.sql.*` value applied afterwards to a SparkSession. So it cannot distinguish
     "the conf was dropped" from "the conf is live but invisible there".

     This reads the EFFECTIVE SQLConf from inside the REPL dbt is actually using, which is
     the only authoritative answer. `SET <key>` with no `=` is a read on Spark SQL and
     returns one (key, value) row.

     Why it is called from two places. dbt-fabricspark defaults to high_concurrency, and a
     `dbt build` issues threads+1 acquires under one sessionTag (4 workers + dbt's master
     connection; microsoft/dbt-fabricspark#242). Fabric packs REPLs 2..N into the Spark
     application the FIRST acquire created — and a packed acquire's `conf` is unappliable
     by construction, because spark-submit already happened. So the master REPL and a
     worker REPL can legitimately disagree, and that disagreement IS the finding:

       master=true  worker=true   -> conf works; the Environment tab was a false negative
       master=true  worker=false  -> packing drops it; only the creating acquire configures
       master=false worker=false  -> Fabric discards `conf` on this endpoint outright
--#}
{% macro probe_spark_conf(label) %}
  {%- if execute and target.type == 'fabricspark' -%}
    {%- for key in ['spark.sql.parquet.vorder.default', 'spark.fabric.resourceProfile'] -%}
      {#-- Log the whole row rather than indexing a column. Spark returns (key, value) for a
           SET read, but this is a throwaway diagnostic and an IndexError here would abort the
           entire spark leg — a wasted paid dispatch to learn nothing. --#}
      {%- set result = run_query('SET ' ~ key) -%}
      {%- set row = result.rows[0] | list if result and result.rows else '<no row>' -%}
      {%- do log('SPARK_CONF_PROBE [' ~ label ~ '] ' ~ key ~ ' -> ' ~ row, info=True) -%}
    {%- endfor -%}
  {%- endif -%}
{% endmacro %}
