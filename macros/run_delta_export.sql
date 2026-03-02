{% macro run_delta_export() %}

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
