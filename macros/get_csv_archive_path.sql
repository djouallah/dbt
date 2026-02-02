{% macro get_csv_archive_path() %}
{# Derive csv_archive_path from FABRIC_WORKSPACE/LAKEHOUSE or use direct override #}
{% set fabric_workspace = env_var('FABRIC_WORKSPACE', 'duckrun') %}
{% set fabric_lakehouse = env_var('FABRIC_LAKEHOUSE', 'dbt') %}
{% set default_path = 'abfss://' ~ fabric_workspace ~ '@onelake.dfs.fabric.microsoft.com/' ~ fabric_lakehouse ~ '.Lakehouse/Files/csv' %}
{{- env_var('csv_archive_path', default_path) -}}
{% endmacro %}
