{#-- The OneLake file paths a fact model reads THIS RUN, resolved from the archive log the
     downloader writes (csv_raw_archive_log.parquet). NO per-run cap: every engine folds
     EVERYTHING pending each run, so the engines compete directly on the same workload.
     (The old process_limit cap existed to pace DuckDB; it's gone.)

     - first run / --full-refresh (this_relation none): the WHOLE source folder as one
       wildcard path. Fabric OPENROWSET rejects >1024 explicit BULK paths per statement
       (chunking via UNION ALL doesn't help — the limit is per statement), so enumerating a
       full archive is impossible; the wildcard reads it all in one statement instead.
     - incremental: the EXPLICIT list of files NOT already ingested into {{ this }}. Steady
       state that is just the newest downloads, well under the 1024-path statement limit.

     The explicit path is built from the log's `archive_path` ('/<subfolder>/<name>.CSV'),
     which carries the real on-disk filename WITH extension — prefix it with the csv_raw root.
     The NOT IN dedup is on `csv_filename` (extension-stripped), which is exactly what the
     models store as [file] (parse_filename takes everything before the first '.'). Both
     verified live.

     `source_type` is also the subfolder under csv_raw/ ('daily', 'scada_today',
     'price_today'). Returns a list of full abfss paths; an empty list is valid (model
     compiles to a no-op). --#}
{% macro new_source_files(source_type, this_relation) %}
  {%- if not execute -%}{{ return([]) }}{%- endif -%}
  {%- set root = get_csv_archive_path() -%}
  {%- if this_relation is none -%}
    {{ return([root ~ '/' ~ source_type ~ '/*.CSV']) }}
  {%- endif -%}
  {%- set log_path = get_root_path() ~ '/csv_raw_archive_log.parquet' -%}
  {%- set q -%}
    SELECT l.archive_path
    FROM OPENROWSET(BULK '{{ log_path }}', FORMAT = 'PARQUET') AS l
    WHERE l.source_type = '{{ source_type }}'
      AND l.csv_filename NOT IN (SELECT DISTINCT [file] FROM {{ this_relation }})
    ORDER BY l.archive_path
  {%- endset -%}
  {%- set archive_paths = run_query(q).columns[0].values() -%}
  {%- set paths = [] -%}
  {%- for ap in archive_paths %}{% do paths.append(root ~ ap) %}{% endfor -%}
  {#-- More pending files than one OPENROWSET statement can take as an explicit list (hard
       Fabric limit, 1024 paths/statement): fall back to the folder wildcard. The models'
       incremental NOT IN [file] guard dedups the re-read rows, so this is slower, never
       wrong. Only happens while a large backlog drains. --#}
  {%- if paths | length > 1024 -%}
    {{ return([root ~ '/' ~ source_type ~ '/*.CSV']) }}
  {%- endif -%}
  {{ return(paths) }}
{% endmacro %}
