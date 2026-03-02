{% macro run_delta_export() %}

{% set local_path = env_var('METADATA_LOCAL_PATH', '/tmp/ducklake_metadata.db') %}
{% set data_path = get_root_path() ~ '/Tables' %}

{# Release dbt's lock on the metadata file #}
{% call statement('use_memory', fetch_result=False) %}
  USE memory
{% endcall %}

{% call statement('detach_ducklake', fetch_result=False) %}
  DETACH ducklake
{% endcall %}

{# Reattach fresh without dbt's transaction #}
{% call statement('attach_ducklake', fetch_result=False) %}
  ATTACH 'ducklake:sqlite:{{ local_path }}' AS ducklake (DATA_PATH '{{ data_path }}')
{% endcall %}

{% call statement('use_ducklake', fetch_result=False) %}
  USE ducklake
{% endcall %}

{% call statement('install_delta_export', fetch_result=False) %}
  INSTALL delta_export FROM 'https://djouallah.github.io/delta_export'
{% endcall %}

{% call statement('load_delta_export', fetch_result=False) %}
  LOAD delta_export
{% endcall %}

{% call statement('run_delta_export', fetch_result=False) %}
  CALL delta_export()
{% endcall %}

{% do log("[DELTA] Export complete", info=True) %}

{% endmacro %}
