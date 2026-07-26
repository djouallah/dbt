{#-- Extract the file "stem": the last path segment, then everything before the first '.'.
    e.g. '.../csv_raw/daily/PUBLIC_DAILY_20240101.CSV' -> 'PUBLIC_DAILY_20240101'.
    One macro, one branch per SQL dialect (keyed on target.type) so every model —
    DuckDB, Fabric Warehouse T-SQL, or Fabric Spark — calls parse_filename() the same way.
    Callers pass the dialect's file-path expression:
      DuckDB  -> the read_csv `filename` column
      Fabric  -> OPENROWSET <alias>.filepath(1)
      Spark   -> _metadata.file_name (already just the name; the split is a harmless no-op)   #}
{% macro parse_filename(filepath) %}
  {%- if target.type == 'fabric' -%}
    {#-- T-SQL: no split_part; walk the string with RIGHT/CHARINDEX/REVERSE. filepath(1) is
         NVARCHAR (not a storable Warehouse column type), so cast the short stem to VARCHAR. --#}
    {%- set fn -%}RIGHT({{ filepath }}, CHARINDEX('/', REVERSE({{ filepath }}) + '/') - 1){%- endset -%}
    CAST(LEFT({{ fn }}, CHARINDEX('.', {{ fn }} + '.') - 1) AS VARCHAR(256))
  {%- elif target.type == 'fabricspark' -%}
    substring_index(element_at(split({{ filepath }}, '/'), -1), '.', 1)
  {%- else -%}
    {#-- DuckDB (duckrun + iceberg) --#}
    split_part(split_part({{ filepath }}, '/', -1), '.', 1)
  {%- endif -%}
{% endmacro %}
