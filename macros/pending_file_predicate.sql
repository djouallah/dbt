{#--
  Build the merge-pruning predicate for a fact model, from the list of source files this run
  is about to load.

  WHY THIS EXISTS. Moving the facts off `append` onto a keyed merge made every run read the
  target to look for key collisions. On a 143M-row table on OneLake that is a full-table
  parquet scan every run, and it is what killed the duckrun leg: a single GET sat for 212s and
  failed, three attempts running.

  Measured against delta-rs, on a 60-file table merging one new file:

    | predicate                                | target files scanned |
    |------------------------------------------|----------------------|
    | key only                                 | 60 / 60              |
    | key + target.DATE = source.DATE          | 60 / 60              |
    | key + target.month_key = source.month_key| 60 / 60              |
    | ... same, table partitioned by month_key | 60 / 60              |
    | key + a LITERAL filter                   |  0 / 60              |

  So a column-to-column predicate prunes NOTHING — delta-rs cannot know the source's range when
  it picks target files, and partitioning does not rescue it. Only literal values prune. That is
  the difference between this and the duckrun integration-test model, whose
  `incremental_predicates=['target.month_key = source.month_key']` reads like the lever but is
  not the thing doing the work.

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

  ALIAS. The predicate is written with dbt's standard `DBT_INTERNAL_DEST`, which is the only
  spelling portable across the two adapters that render these models:
    * dbt-duckdb (iceberg) AND-s incremental_predicates into an ON clause built with
      DBT_INTERNAL_SOURCE/DBT_INTERNAL_DEST and knows no `target` alias — writing `target.file`
      here fails the iceberg leg outright;
    * duckrun rewrites DBT_INTERNAL_DEST -> target itself (_rewrite_merge_aliases) before handing
      the predicate to delta-rs.
  So write dbt's alias and let each adapter translate.

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
