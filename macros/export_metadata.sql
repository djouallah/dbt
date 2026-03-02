{% macro export_metadata() %}

{# Only run during execution, not during parsing #}
{% if execute %}

{% set local_path = env_var('METADATA_LOCAL_PATH', '/tmp/ducklake_metadata.db') %}
{% set remote_path = env_var('METADATA_REMOTE_BLOB', get_metadata_path() ~ '/data_0.db') %}

{# Run delta export while ducklake is still attached #}
{% call statement('run_delta_export', fetch_result=False) %}
  CALL delta_export()
{% endcall %}

{% do log("[METADATA] Delta export complete, uploading metadata DB...", info=True) %}

{# Switch away from ducklake — can't detach the active database #}
{% call statement('use_memory', fetch_result=False) %}
  USE memory
{% endcall %}

{# Detach DuckLake to force WAL checkpoint and flush all writes to SQLite file #}
{% call statement('detach_ducklake', fetch_result=False) %}
  DETACH ducklake
{% endcall %}

{# Upload the SQLite metadata DB as a blob to remote storage #}
{% call statement('upload_metadata_blob', fetch_result=False) %}
  COPY (SELECT content FROM read_blob('{{ local_path }}')) TO '{{ remote_path }}' (FORMAT BLOB)
{% endcall %}

{% do log("[METADATA] Export complete — metadata DB uploaded to remote storage", info=True) %}

{% endif %}

{% endmacro %}
