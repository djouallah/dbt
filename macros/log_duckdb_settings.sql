{#--
  Print the DuckDB settings that are actually IN FORCE, from the connection dbt is about to run
  the models on. Called from `on-run-start` for the duckrun and iceberg targets alike.

  WHY. The hooks above it in dbt_project.yml -- `SET GLOBAL temp_directory`,
  `preserve_insertion_order = false` -- say what was ASKED FOR. Nothing said what stuck, and that
  gap is what run 30867258967 could not close: a `cores: 4` dispatch died on fct_summary with
  `Out of Memory Error: failed to pin block of size 256.0 KiB (24.6 GiB/24.6 GiB used)` while the
  same build at `cores: 8` passes, and the leg log could not distinguish a DuckDB that was
  misconfigured from a node that simply has less RAM.

  WHAT TO READ IN THE OUTPUT:
    memory_limit              duckrun pins this itself (85% of node RAM) and announces it; here it
                              is confirmed from the session rather than from the adapter's banner.
    temp_directory            should be $TMPDIR/duckdb_spill, i.e. the notebook's big work disk --
                              fabric_build.py sets DUCKDB_TEMP_DIR there. An empty value means
                              spilling is OFF and the memory limit is a hard wall.
    max_temp_directory_size   the one nobody thinks about: it defaults to 90% of FREE DISK under
                              temp_directory, so a small work disk is a small spill budget and
                              nothing announces it. fabric_build.py prints the free bytes next to
                              this so the two can be read together.
    preserve_insertion_order  false on both targets. Left true, a large ORDER BY holds the whole
                              result in memory.
    threads                   4 on both, from profiles.yml, not from the node's core count.

  Read alongside fabric_build.py's `[fabric_build] t=+Ns ... spill=...` sampler: settings say what
  DuckDB was allowed to do, the sampler says what it did. Spill stuck at 0B while RSS climbs to
  memory_limit means the memory in use was never evictable, and the answer is more RAM.

  BOTH DUCKDB TARGETS, deliberately: CLAUDE.md's rule is that duckrun and iceberg run on identical
  DuckDB settings -- that is the whole point of the pair -- so a diagnostic given to one is given
  to the other. It renders to whitespace on dwh and spark, which have no duckdb_settings().

  It is a SELECT and a log(), so it writes nothing and costs microseconds; leaving it in place
  costs nothing and means the next surprise is already instrumented.
--#}
{% macro log_duckdb_settings() %}
  {%- if execute -%}
    {%- set rows = run_query(
          "SELECT name, value FROM duckdb_settings() WHERE name IN ("
          "'memory_limit', 'threads', 'temp_directory', 'max_temp_directory_size', "
          "'preserve_insertion_order') ORDER BY name") -%}
    {%- for row in rows -%}
      {{ log("duckdb setting: " ~ row[0] ~ " = " ~ row[1], info=True) }}
    {%- endfor -%}
  {%- endif -%}
{% endmacro %}
