{%- macro get_csv_archive_path() -%}
{%- set fabric_workspace = env_var('FABRIC_WORKSPACE', 'duckrun') | trim -%}
{%- set fabric_lakehouse = env_var('FABRIC_LAKEHOUSE', 'dbt') | trim -%}
{%- set default_path = 'abfss://' ~ fabric_workspace ~ '@onelake.dfs.fabric.microsoft.com/' ~ fabric_lakehouse ~ '.Lakehouse/Files/csv' -%}
{{ env_var('csv_archive_path', default_path) | trim }}
{%- endmacro -%}
