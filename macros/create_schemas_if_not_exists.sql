{#-- on-run-start hook: Fabric Warehouse does not auto-create schemas, and dbt-fabric's
     create-schema runs per-model anyway, but the landing/mart schemas must exist before
     the FIRST model (and before the downloader inserts into landing.csv_archive_log).
     Idempotent: only creates each schema when missing. CREATE SCHEMA must be the only
     statement in its batch, hence the EXEC. --#}
{% macro create_schemas_if_not_exists(schemas) %}
  {%- if execute -%}
    {%- for s in schemas -%}
      {% set sql %}
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{{ s }}')
          EXEC('CREATE SCHEMA [{{ s }}]');
      {% endset %}
      {%- do run_query(sql) -%}
    {%- endfor -%}
  {%- endif -%}
{% endmacro %}
