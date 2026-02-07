{%- macro get_root_path() -%}
{{ env_var('ROOT_PATH', 'abfss://duckrun@onelake.dfs.fabric.microsoft.com/dbt.Lakehouse') | trim }}
{%- endmacro -%}
