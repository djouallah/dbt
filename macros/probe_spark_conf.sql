{#-- TEMPORARY DIAGNOSTIC — one question left, then delete this file and its two call sites
     (dbt_project.yml on-run-start, models/spark/dimensions/dim_duid.sql pre_hook).

     WHAT IT ALREADY ESTABLISHED (run 30599066885, 2026-07-31). `spark_config.conf` in
     profiles.yml has no effect. Read from inside the REPLs dbt was actually using:

       [master] spark.sql.parquet.vorder.default -> false      (profile asks for "true")
       [worker] spark.sql.parquet.vorder.default -> false
       [master] spark.fabric.resourceProfile     -> writeHeavy
       [worker] spark.fabric.resourceProfile     -> writeHeavy

     The adapter is NOT what drops it: concurrent_livy.py:195-228 copies `conf` verbatim into
     the POST .../highConcurrencySessions body, and `conf` is a documented field of
     HighConcurrencySessionRequest. Nor is it REPL packing — the master connection is the
     acquire that CREATED the session, and it reads false too.

     Do not re-check this in the Spark UI Environment tab. That tab renders the SparkContext
     conf captured at application launch and never shows a `spark.sql.*` value applied later
     to a SparkSession, so it cannot tell "dropped" from "live but invisible". The in-session
     `SET <key>` read below is the only authoritative instrument. `SET <key>` with no `=` is a
     read, never an assignment, so it cannot fail the way setting a static conf can.

     THE ONE QUESTION LEFT. Two mechanisms both produce `false` and are not yet separated:

       (a) Fabric ignores `conf` on the HC acquire outright; or
       (b) Fabric applies it, then the writeHeavy resource profile overwrites it.

     CANARY_KEY settles it at zero extra capacity — it rides the next dispatch, whenever one
     happens. It is a made-up key no resource profile defines, set alongside V-Order in
     profiles.yml, so nothing can overwrite it:

       canary present -> (b): conf IS delivered; the resource profile is what wins. The fix is
                              then to set spark.fabric.resourceProfile in the same conf block.
       canary absent  -> (a): Fabric discards the conf wholesale. Only a per-REPL `SET`, a
                              tblproperty, or OPTIMIZE ... VORDER can help.
--#}
{% macro probe_spark_conf(label) %}
  {%- if execute and target.type == 'fabricspark' -%}
    {%- set keys = [
        'spark.sql.parquet.vorder.default',
        'spark.fabric.resourceProfile',
        'spark.dbt.probe.canary',
    ] -%}
    {%- for key in keys -%}
      {#-- Log the whole row rather than indexing a column. Spark returns (key, value) for a
           SET read, but this is a throwaway diagnostic and an IndexError here would abort the
           entire spark leg — a wasted paid dispatch to learn nothing. --#}
      {%- set result = run_query('SET ' ~ key) -%}
      {%- set row = result.rows[0] | list if result and result.rows else '<no row>' -%}
      {%- do log('SPARK_CONF_PROBE [' ~ label ~ '] ' ~ key ~ ' -> ' ~ row, info=True) -%}
    {%- endfor -%}
  {%- endif -%}
{% endmacro %}
