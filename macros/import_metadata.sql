{% macro import_metadata() %}

{# Only run during execution, not during parsing #}
{% if execute %}

{% set metadata_path = get_metadata_path() %}
{% do log("[METADATA] Import path: " ~ metadata_path, info=True) %}

{# Ensure metadata directory exists so glob() doesn't 404 on abfss:// #}
{% call statement('init_metadata_dir', fetch_result=False) %}
  COPY (SELECT 1 AS init) TO '{{ metadata_path }}/_init.csv' (FORMAT csv)
{% endcall %}

{# Discover DuckLake internal metadata tables #}
{% set tables_query %}
  SELECT database_name, schema_name, table_name
  FROM duckdb_tables()
  WHERE database_name LIKE '__ducklake_metadata_%'
  ORDER BY table_name
{% endset %}
{% set tables = run_query(tables_query) %}

{% if tables | length == 0 %}
  {% do log("[METADATA] No DuckLake metadata catalog found, skipping import", info=True) %}
{% else %}

  {# Check if any metadata parquet files exist on abfss #}
  {% set check_query %}
    SELECT count(*) as cnt FROM glob('{{ metadata_path }}/*.parquet')
  {% endset %}
  {% set check_result = run_query(check_query) %}
  {% set file_count = check_result.columns[0].values()[0] %}

  {% if file_count == 0 %}
    {% do log("[METADATA] No parquet files at " ~ metadata_path ~ ", starting fresh", info=True) %}
  {% else %}
    {% do log("[METADATA] Found " ~ file_count ~ " parquet files, importing...", info=True) %}

    {% for row in tables.rows %}
      {% set db_name = row[0] %}
      {% set schema_name = row[1] %}
      {% set table_name = row[2] %}
      {% set fqn = db_name ~ '."' ~ schema_name ~ '".' ~ table_name %}
      {% set parquet_file = metadata_path ~ '/' ~ table_name ~ '.parquet' %}

      {# Check if this specific parquet file exists #}
      {% set file_check %}
        SELECT count(*) FROM glob('{{ parquet_file }}')
      {% endset %}
      {% set file_exists = run_query(file_check).columns[0].values()[0] %}

      {% if file_exists > 0 %}
        {% call statement('delete_' ~ table_name, fetch_result=False) %}
          DELETE FROM {{ fqn }}
        {% endcall %}

        {% call statement('import_' ~ table_name, fetch_result=False) %}
          INSERT INTO {{ fqn }} SELECT * FROM read_parquet('{{ parquet_file }}')
        {% endcall %}

        {% do log("[METADATA] Imported " ~ table_name, info=True) %}
      {% endif %}
    {% endfor %}

    {% do log("[METADATA] Import complete", info=True) %}
  {% endif %}

{% endif %}

{% endif %}

{% endmacro %}
