{% macro get_archive_paths(source_type) %}
  {% if execute %}
    {% set query %}
      SELECT source_filename FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = '{{ source_type }}'
    {% endset %}
    {% set results = run_query(query) %}
    {% set csv_archive_path = get_csv_archive_path() %}
    {% set paths = [] %}
    {% for row in results %}
      {% if source_type == 'daily' %}
        {% do paths.append("'zip://" ~ csv_archive_path ~ "/daily/year=" ~ row[0][13:17] ~ "/source_file=" ~ row[0] ~ "/data_0.zip/*.CSV'") %}
      {% elif source_type == 'scada_today' %}
        {% do paths.append("'zip://" ~ csv_archive_path ~ "/scada_today/day=" ~ row[0][12:20] ~ "/source_file=" ~ row[0] ~ "/data_0.zip/*.CSV'") %}
      {% elif source_type == 'price_today' %}
        {% do paths.append("'zip://" ~ csv_archive_path ~ "/price_today/day=" ~ row[0][17:25] ~ "/source_file=" ~ row[0] ~ "/data_0.zip/*.CSV'") %}
      {% endif %}
    {% endfor %}
    {{ return(paths) }}
  {% else %}
    {{ return([]) }}
  {% endif %}
{% endmacro %}
