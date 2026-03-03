{%- macro get_root_path() -%}
{{ env_var('ROOT_PATH') | trim }}
{%- endmacro -%}
