{# Adapter overrides needed by the OneLake Iceberg REST catalog (the `iceberg` target).
   These dispatch on adapter type `duckdb`, so they apply only to the Iceberg target
   (type: duckdb); the Delta target is a different adapter (type: duckrun) and is
   unaffected. Even if it were reached, both overrides are benign for plain Delta. #}

{# get_columns_in_relation: Iceberg catalogs don't populate information_schema.columns,
   so use DESCRIBE and drop the Iceberg hidden "__" column. #}
{% macro duckdb__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          column_type as data_type,
          null as character_maximum_length,
          null as numeric_precision,
          null as numeric_scale
      from (describe {{ relation }})
      where column_name != '__'
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{# drop_relation: the DuckDB Iceberg extension does not support DROP TABLE ... CASCADE. #}
{% macro duckdb__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}
