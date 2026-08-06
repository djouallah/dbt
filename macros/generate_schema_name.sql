{#-- The model's +schema (landing / mart) used to be returned VERBATIM, which made
     target.schema (DBT_SCHEMA) dead config: no profile or env var could redirect a run
     away from the production schemas. That bit for real when another repo ran this
     project against a catalog that already held the full datasets — the "test" run
     merged into production landing/mart because nothing could point it elsewhere.

     Now target.schema is the redirect lever it should have been:
       - DBT_SCHEMA unset/default ('mart')  -> unchanged: +schema verbatim (landing, mart)
       - DBT_SCHEMA=anything_else           -> '<DBT_SCHEMA>_<+schema>' (dbt_landing, dbt_mart)
     Every engine goes through this macro, so one env var isolates a whole run. --#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.schema == 'mart' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema ~ '_' ~ (custom_schema_name | trim) }}
    {%- endif -%}
{%- endmacro %}
