{#-- Spark counterpart of new_source_files(): the bounded set of csv FILENAMES (with extension)
     a fact model may ingest THIS RUN, resolved from the archive log AT COMPILE TIME via
     run_query and inlined into the model as an explicit Hadoop brace glob
     (text.`<root>/<source_type>/{A.CSV,B.CSV,...}`).

     Explicit list, NOT a folder scan: the first from_csv version read the WHOLE csv_raw/<type>/
     folder per model per run and filtered on _metadata.file_name afterwards — a full text scan
     of years of archive (twice for /daily: fct_price + fct_scada) that put the spark leg at 30+
     minutes while the notebook reference (aemo_fabric) reads only the new files via
     spark.read.csv([paths]). This macro is the SQL-only equivalent of that driver-side set
     difference, and matches how the other dialects already resolve their file lists.

     The selection rule stays IDENTICAL to the other dialects — oldest first (ORDER BY
     archive_path), capped at process_limit, minus whatever {{ this }} already holds — so every
     engine folds the SAME files in the SAME order. Pass this_relation=none on a first/
     full-refresh build (no {{ this }} yet). Returns [] while parsing (execute=false). --#}
{% macro spark_new_files(source_type, this_relation) %}
  {%- if not execute -%}{{ return([]) }}{%- endif -%}
  {%- set q -%}
    SELECT archive_path
    FROM {{ ref('stg_csv_archive_log') }}
    WHERE source_type = '{{ source_type }}'
    {%- if this_relation is not none %}
      AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this_relation }})
    {%- endif %}
    ORDER BY archive_path
    LIMIT {{ env_var('process_limit', '1024') | int }}
  {%- endset -%}
  {%- set names = [] -%}
  {#-- archive_path is '/<subfolder>/<name>.CSV'; the glob needs just the real filename. --#}
  {%- for ap in run_query(q).columns[0].values() %}{% do names.append(ap.split('/')[-1]) %}{% endfor -%}
  {{ return(names) }}
{% endmacro %}
