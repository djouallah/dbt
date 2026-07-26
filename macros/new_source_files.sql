{#-- Returns the EXPLICIT list of OneLake file paths a fact model should read this run, taken
     from the archive log the downloader writes (csv_raw_archive_log.parquet). This replaces the
     old "glob the whole folder + filter with filepath() NOT IN {{ this }}" pattern, which
     re-read the entire archive every run (years of files) just to keep the newest. Fabric
     Warehouse OPENROWSET accepts an explicit BULK ('f1','f2',...) list (verified live), so we
     hand it only the files it actually needs.

     - first run / --full-refresh (not is_incremental): ALL files of this source_type.
     - incremental: only files NOT already ingested into {{ this }}.

     The path is built from the log's `archive_path` ('/<subfolder>/<name>.CSV'), which carries
     the real on-disk filename WITH extension — prefix it with the csv_raw root. The NOT IN
     dedup is on `csv_filename` (extension-stripped), which is exactly what the models store as
     [file] (parse_filename takes everything before the first '.'). Both verified live.

     `source_type` is also the subfolder under csv_raw/ ('daily', 'scada_today', 'price_today').
     Returns a list of full abfss paths; an empty list is valid (model compiles to a no-op). --#}
{% macro new_source_files(source_type, this_relation) %}
  {%- if not execute -%}{{ return([]) }}{%- endif -%}
  {%- set root = get_csv_archive_path() -%}
  {%- set log_path = get_root_path() ~ '/csv_raw_archive_log.parquet' -%}
  {#-- Fabric OPENROWSET allows at most 1024 explicit BULK file paths PER STATEMENT (chunking via
       UNION ALL doesn't help — the limit is per statement). So cap each run to process_limit files,
       OLDEST first: a from-scratch backfill (e.g. fresh warehouse, ~3000 files in the log) then
       converges over a few runs instead of blowing the limit. Steady-state new files (<= the daily
       download limit) are well under the cap.

       1024 is also the project-wide default (see CI_PROCESS_LIMIT): it is the lowest ceiling of
       the four engines, so every leg folds the same number of files per run and they stay in step.
       The clamp below therefore only bites if someone dispatches a LARGER process_limit by hand —
       which would silently desync this leg from the others rather than erroring, so keep it. --#}
  {%- set process_limit = [env_var('process_limit', '1024') | int, 1024] | min -%}
  {%- if this_relation is not none -%}
    {%- set q -%}
      SELECT TOP {{ process_limit }} l.archive_path
      FROM OPENROWSET(BULK '{{ log_path }}', FORMAT = 'PARQUET') AS l
      WHERE l.source_type = '{{ source_type }}'
        AND l.csv_filename NOT IN (SELECT DISTINCT [file] FROM {{ this_relation }})
      ORDER BY l.archive_path
    {%- endset -%}
  {%- else -%}
    {%- set q -%}
      SELECT TOP {{ process_limit }} l.archive_path
      FROM OPENROWSET(BULK '{{ log_path }}', FORMAT = 'PARQUET') AS l
      WHERE l.source_type = '{{ source_type }}'
      ORDER BY l.archive_path
    {%- endset -%}
  {%- endif -%}
  {%- set archive_paths = run_query(q).columns[0].values() -%}
  {%- set paths = [] -%}
  {%- for ap in archive_paths %}{% do paths.append(root ~ ap) %}{% endfor -%}
  {{ return(paths) }}
{% endmacro %}
