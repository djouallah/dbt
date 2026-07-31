{#-- TEMPORARY DIAGNOSTIC — one question left, then delete this file and its two call sites
     (dbt_project.yml on-run-start, models/spark/dimensions/dim_duid.sql pre_hook).

     WHAT IT ESTABLISHED (run 30599860363, 2026-07-31). `spark_config.conf` IS delivered;
     the V-Order key is overwritten by the resource profile. Master and worker identical:

       spark.sql.parquet.vorder.default -> false        (profile asks for "true")
       spark.fabric.resourceProfile     -> writeHeavy
       spark.dbt.probe.canary           -> alive        (made-up key, arrived intact)

     The canary is the finding: an arbitrary key survives to both REPLs, so delivery works
     end to end. `writeHeavy` DEFINES vorder.default=false and is applied AFTER the session
     conf, so it clobbers that key and leaves keys it does not define alone. Precedence.

     Two hypotheses this killed, so they do not get retried. The adapter is not dropping it
     (concurrent_livy.py:195-228 copies `conf` verbatim into the POST body, and `conf` is a
     documented field of HighConcurrencySessionRequest). REPL packing is not dropping it
     either — the canary reads `alive` on the worker, which is a packed acquire.

     Do not re-check this in the Spark UI Environment tab. That tab renders the SparkContext
     conf captured at application launch and never shows a `spark.sql.*` value applied later
     to a SparkSession, so it cannot tell "dropped" from "live but invisible". The in-session
     `SET <key>` read below is the only authoritative instrument. `SET <key>` with no `=` is a
     read, never an assignment, so it cannot fail the way setting a static conf can.

     WHY IT IS STILL HERE. The headline question is answered; keeping it costs three `SET`
     reads per REPL and buys the next experiment for free. The open one is whether the
     profile itself can be set from the conf block — add
     `spark.fabric.resourceProfile: readHeavyForPBI` to profiles.yml and read this probe:
     if resourceProfile flips and vorder.default follows it to `true`, that is the fix and
     the probe has earned its keep. If resourceProfile still reads `writeHeavy`, the profile
     is not settable per-session and the remaining levers are a tblproperty, OPTIMIZE ...
     VORDER, or a per-REPL `SET` issued after session start (which runs after the profile is
     applied, and would therefore win).

     Delete this file and its two call sites — dbt_project.yml on-run-start, and
     models/spark/dimensions/dim_duid.sql pre_hook — once that is settled. Drop the canary
     from profiles.yml at the same time.
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
