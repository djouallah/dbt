{% macro export_metadata() %}

{# Only run during execution, not during parsing #}
{% if execute %}

{% set metadata_path = get_metadata_path() %}
{% do log("[METADATA] Export path: " ~ metadata_path, info=True) %}

{# Discover DuckLake internal metadata tables #}
{% set tables_query %}
  SELECT database_name, schema_name, table_name
  FROM duckdb_tables()
  WHERE database_name LIKE '__ducklake_metadata_%'
  ORDER BY table_name
{% endset %}
{% set tables = run_query(tables_query) %}

{% if tables | length == 0 %}
  {% do log("[METADATA] No DuckLake metadata catalog found, skipping export", info=True) %}
{% else %}

  {% for row in tables.rows %}
    {% set db_name = row[0] %}
    {% set schema_name = row[1] %}
    {% set table_name = row[2] %}
    {% set fqn = db_name ~ '."' ~ schema_name ~ '".' ~ table_name %}
    {% set parquet_file = metadata_path ~ '/' ~ table_name ~ '.parquet' %}

    {% call statement('export_' ~ table_name, fetch_result=False) %}
      COPY {{ fqn }} TO '{{ parquet_file }}' (FORMAT PARQUET)
    {% endcall %}

    {% do log("[METADATA] Exported " ~ table_name, info=True) %}
  {% endfor %}

  {% do log("[METADATA] Export complete", info=True) %}

{% endif %}

{% endif %}

{% endmacro %}
