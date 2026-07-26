{#-- Spark counterpart of new_source_files(): the csv FILENAMES (with extension) a fact model
     may ingest THIS RUN, resolved from the archive log AT COMPILE TIME via run_query and
     inlined into the model as an explicit Hadoop brace glob
     (text.`<root>/<source_type>/{A.CSV,B.CSV,...}`). NO per-run cap — every engine folds
     everything pending each run (the old process_limit cap existed to pace DuckDB; gone).

     Only called on INCREMENTAL runs: a first/full-refresh build reads the bare source folder
     (everything is new, so a folder scan IS the explicit list). Explicit list on incremental,
     NOT a folder scan: reading the whole csv_raw/<type>/ folder and filtering on
     _metadata.file_name afterwards re-read years of archive per model per run (30+ minutes,
     twice for /daily), while the notebook reference (aemo_fabric) reads only the new files
     via spark.read.csv([paths]). This macro is the SQL-only equivalent of that driver-side
     set difference.

     The selection rule stays IDENTICAL to the other dialects — files minus whatever
     {{ this }} already holds, oldest first — so every engine folds the SAME files.
     Returns [] while parsing (execute=false). --#}
{% macro spark_new_files(source_type, this_relation) %}
  {%- if not execute -%}{{ return([]) }}{%- endif -%}
  {%- set q -%}
    SELECT archive_path
    FROM {{ ref('stg_csv_archive_log') }}
    WHERE source_type = '{{ source_type }}'
      AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this_relation }})
    ORDER BY archive_path
  {%- endset -%}
  {%- set names = [] -%}
  {#-- archive_path is '/<subfolder>/<name>.CSV'; the glob needs just the real filename. --#}
  {%- for ap in run_query(q).columns[0].values() %}{% do names.append(ap.split('/')[-1]) %}{% endfor -%}
  {{ return(names) }}
{% endmacro %}
