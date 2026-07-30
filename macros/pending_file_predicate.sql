{#--
  Build the merge-pruning predicate for a fact model, from the list of source files this run
  is about to load. ICEBERG ONLY -- see WHO USES THIS below.

  WHY THIS EXISTS. Writing the facts with a keyed merge makes every run read the target to look
  for key collisions. On a 143M-row table on OneLake that is a full-table parquet scan every run,
  and it is what killed the duckrun leg back when duckrun also merged: a single GET sat for 212s
  and failed, three attempts running.

  Measured against delta-rs, on a 60-file table merging one new file:

    | predicate                                | target files scanned |
    |------------------------------------------|----------------------|
    | key only                                 | 60 / 60              |
    | key + target.DATE = source.DATE          | 60 / 60              |
    | key + target.month_key = source.month_key| 60 / 60              |
    | ... same, table partitioned by month_key | 60 / 60              |
    | key + a LITERAL filter                   |  0 / 60              |

  So for a MERGE a column-to-column predicate prunes NOTHING — delta-rs cannot know the source's
  range when it picks target files, and partitioning does not rescue it. Only literal values prune.

  WHO USES THIS: the iceberg target only. duckrun's facts write with `incremental_strategy='insert'`,
  which since 0.4.34 is a DuckDB anti-join over the key columns rather than a merge, and it builds
  its own literal filters from the batch (`engine.probe_filters`): an exact `"month_key" IN (...)`
  for the declared partition equality, min/max bounds for every other join key — so `file` gets its
  range for free and this macro would add nothing. `target.month_key = source.month_key` therefore
  DOES prune on that path, which is the opposite of the table above; the difference is who computes
  the literals, not the predicate text.

  WHY `file`. The pending file names are known at compile time, `file` is already the leading
  component of every fact's unique_key, and the predicate is therefore *implied* by the merge ON
  clause — it removes no match that the key would have made, so it is a pure pruning hint with no
  semantic effect. A colliding row (the race this merge exists to catch) carries one of these very
  file names, so it is still scanned: measured 1/60 files scanned and 0 rows inserted on a
  deliberate duplicate.

  Two shapes, because an IN list of a few thousand names bloats the plan for no benefit:
    <= 200 files  ->  target.file IN ('a','b',...)   exact, prunes to zero
    >  200 files  ->  target.file BETWEEN 'min' AND 'max'
  The range form is still sound: every colliding row's name is in the set, hence inside the
  range. It is looser, but a run that large is a backlog drain where most of the table is being
  touched anyway.

  ALIAS. The predicate is written with dbt's standard `DBT_INTERNAL_DEST`, because dbt-duckdb
  AND-s incremental_predicates into an ON clause built with DBT_INTERNAL_SOURCE/DBT_INTERNAL_DEST
  and knows no `target` alias — writing `target.file` here fails the iceberg leg outright. (The
  reverse holds on the duckrun side: its own aliases ARE `target`/`source`, which is why the
  month_key equality in those models is written that way and stays duckrun-only.)

  Returns dbt's `incremental_predicates` shape (a one-element list), or none when there is
  nothing to prune on — an empty file list means the model compiles to its zero-row no-op, and
  none at parse time means "unknown", where a wrong guess must not narrow the merge.
--#}

{% macro pending_file_predicate(pending_files) %}
  {%- if pending_files is none or pending_files | length == 0 -%}
    {{ return(none) }}
  {%- endif -%}
  {%- set names = pending_files | map('string') | sort -%}
  {%- if names | length <= 200 -%}
    {%- set quoted = [] -%}
    {%- for n in names -%}
      {%- do quoted.append("'" ~ n | replace("'", "''") ~ "'") -%}
    {%- endfor -%}
    {{ return(["DBT_INTERNAL_DEST.file IN (" ~ quoted | join(', ') ~ ")"]) }}
  {%- else -%}
    {%- set lo = (names | first) | replace("'", "''") -%}
    {%- set hi = (names | last) | replace("'", "''") -%}
    {{ return(["DBT_INTERNAL_DEST.file BETWEEN '" ~ lo ~ "' AND '" ~ hi ~ "'"]) }}
  {%- endif -%}
{% endmacro %}
