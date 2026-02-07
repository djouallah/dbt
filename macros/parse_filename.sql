{% macro parse_filename(filepath) %}
    split_part(split_part({{ filepath }}, '/source_file=', 2), '/', 1)
{% endmacro %}
