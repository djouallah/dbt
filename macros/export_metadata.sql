{% macro export_metadata() %}

{# Only run during execution, not during parsing #}
{% if execute %}

{% set local_path = env_var('METADATA_LOCAL_PATH', '/tmp/ducklake_metadata.db') %}
{% set remote_path = env_var('METADATA_REMOTE_BLOB', get_metadata_path() ~ '/data_0.db') %}
{% set data_path = get_root_path() ~ '/Tables' %}

{# Switch away from ducklake — can't detach the active database #}
{% call statement('use_memory', fetch_result=False) %}
  USE memory
{% endcall %}

{# Detach DuckLake to force WAL checkpoint and release lock #}
{% call statement('detach_ducklake', fetch_result=False) %}
  DETACH ducklake
{% endcall %}

{# Reattach fresh (no dbt transaction) and run delta export #}
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

{% do log("[METADATA] Delta export complete", info=True) %}

{# Detach again and upload metadata blob #}
{% call statement('use_memory_2', fetch_result=False) %}
  USE memory
{% endcall %}

{% call statement('detach_ducklake_2', fetch_result=False) %}
  DETACH ducklake
{% endcall %}

{% call statement('upload_metadata_blob', fetch_result=False) %}
  COPY (SELECT content FROM read_blob('{{ local_path }}')) TO '{{ remote_path }}' (FORMAT BLOB)
{% endcall %}

{% do log("[METADATA] Metadata DB uploaded to remote storage", info=True) %}

{% endif %}

{% endmacro %}
