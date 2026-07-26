{#-- Emit `TRY_CAST([col] AS FLOAT) AS [col],` for each name — the T-SQL equivalent of
     DuckDB's CAST(col AS DOUBLE) over the all-varchar OPENROWSET read. TRY_CAST yields
     NULL instead of failing on the odd malformed/empty field (ignore_errors=1). --#}
{% macro cast_floats(cols) %}
{%- for c in cols %}
  TRY_CAST([{{ c }}] AS FLOAT) AS [{{ c }}],
{%- endfor %}
{% endmacro %}
