{% macro download() %}

{# Only run during execution, not during parsing. Skip during 'dbt test' since models won't process files #}
{% if execute and flags.WHICH != 'test' %}

{% set root_path = get_root_path() %}
{% set csv_archive_path = root_path ~ '/Files/csv' %}
{% set csv_log_path = root_path ~ '/Files/csv_archive_log.parquet' %}
{% set download_limit = env_var('download_limit', '2') | int %}

{% do log("[DOWNLOAD] Starting download with PATH_ROOT=" ~ root_path ~ ", csv_archive_path=" ~ csv_archive_path ~ ", download_limit=" ~ download_limit, info=True) %}

{# Set up variables for the download script - each statement separately #}
{% call statement('set_path_root', fetch_result=False) %}
SET VARIABLE PATH_ROOT = '{{ root_path }}'
{% endcall %}

{% call statement('set_download_limit', fetch_result=False) %}
SET VARIABLE download_limit = {{ download_limit }}
{% endcall %}

{% call statement('set_csv_archive_path', fetch_result=False) %}
SET VARIABLE csv_archive_path = '{{ csv_archive_path }}'
{% endcall %}


{# Create temp table for the combined archive log (existing + new entries) #}
{% call statement('create_log', fetch_result=False) %}
CREATE OR REPLACE TEMP TABLE _csv_archive_log (
  source_type VARCHAR,
  source_filename VARCHAR,
  archive_path VARCHAR,
  archived_at TIMESTAMP,
  row_count BIGINT,
  source_url VARCHAR,
  etag VARCHAR
)
{% endcall %}

{# Set log path variable for SQL use #}
{% call statement('set_csv_log_path', fetch_result=False) %}
SET VARIABLE csv_log_path = '{{ csv_log_path }}'
{% endcall %}

{# Load existing log from parquet if it exists #}
{% set log_check %}
  SELECT count(*) FROM glob('{{ csv_log_path }}')
{% endset %}
{% set log_exists = run_query(log_check).columns[0].values()[0] %}

{% if log_exists > 0 %}
  {% call statement('load_existing_log', fetch_result=False) %}
  INSERT INTO _csv_archive_log SELECT * FROM read_parquet('{{ csv_log_path }}')
  {% endcall %}
  {% do log("[DOWNLOAD] Loaded existing log from parquet (" ~ log_exists ~ " file)", info=True) %}
{% else %}
  {% do log("[DOWNLOAD] No existing log file, starting fresh", info=True) %}
{% endif %}

{# Create dedup table from existing log #}
{% call statement('create_existing_log', fetch_result=False) %}
CREATE OR REPLACE TEMP TABLE _existing_archive_log AS
SELECT source_type, source_filename FROM _csv_archive_log
{% endcall %}

{# ============================================================================ #}
{# DAILY REPORTS (SCADA + PRICE) #}
{# ============================================================================ #}

{# daily_source: 'github' (historical archive) or 'aemo' (current, no rate limits) #}
{% set daily_source = var('daily_source', 'github') %}
{% do log("[DOWNLOAD] Daily source: " ~ daily_source, info=True) %}

{% if daily_source == 'github' %}
{% call statement('daily_files_web', fetch_result=False) %}
CREATE OR REPLACE TEMP TABLE daily_files_web AS
WITH
  api_responses AS (
    SELECT 2018 AS year, content AS json_content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2018')
    UNION ALL SELECT 2019, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2019')
    UNION ALL SELECT 2020, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2020')
    UNION ALL SELECT 2021, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2021')
    UNION ALL SELECT 2022, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2022')
    UNION ALL SELECT 2023, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2023')
    UNION ALL SELECT 2024, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2024')
    UNION ALL SELECT 2025, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2025')
    UNION ALL SELECT 2026, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2026')
  ),
  parsed_files AS (
    SELECT year, unnest(from_json(json_content, '["json"]')) AS file_info
    FROM api_responses
  )
SELECT
  json_extract_string(file_info, '$.download_url') AS full_url,
  split_part(json_extract_string(file_info, '$.name'), '.', 1) AS filename
FROM parsed_files
WHERE json_extract_string(file_info, '$.name') LIKE 'PUBLIC_DAILY%.zip'
ORDER BY full_url DESC
{% endcall %}
{% else %}
{# AEMO fallback - current files only, no rate limits #}
{% call statement('daily_files_web', fetch_result=False) %}
CREATE OR REPLACE TEMP TABLE daily_files_web AS
WITH
  html_data AS (
    SELECT content AS html FROM read_text('https://nemweb.com.au/Reports/Current/Daily_Reports/')
  ),
  lines AS (
    SELECT unnest(string_split(html, '<br>')) AS line FROM html_data
  )
SELECT
  'https://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
  split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\.zip)"', 1), '.', 1) AS filename
FROM lines
WHERE line LIKE '%PUBLIC_DAILY%.zip%'
ORDER BY full_url DESC
{% endcall %}
{% endif %}

{% call statement('daily_to_archive', fetch_result=False) %}
CREATE OR REPLACE TEMP TABLE daily_to_archive AS
SELECT full_url, filename
FROM daily_files_web
WHERE filename NOT IN (SELECT source_filename FROM _existing_archive_log WHERE source_type = 'daily')
LIMIT getvariable('download_limit')
{% endcall %}

{% call statement('set_has_daily', fetch_result=False) %}
SET VARIABLE has_daily_to_archive = (SELECT count(*) > 0 FROM daily_to_archive)
{% endcall %}

{% call statement('set_daily_urls', fetch_result=False) %}
SET VARIABLE daily_urls = (SELECT COALESCE(NULLIF(list(full_url), []), list_value('')) FROM daily_to_archive)
{% endcall %}

{% call statement('set_daily_expected', fetch_result=False) %}
SET VARIABLE daily_expected = (SELECT count(*) FROM daily_to_archive)
{% endcall %}

{% call statement('set_daily_paths', fetch_result=False) %}
SET VARIABLE daily_paths = (SELECT COALESCE(NULLIF(list(getvariable('csv_archive_path') || '/daily/year=' || CAST(substring(split_part(filename, '_', 3), 1, 4) AS INT) || '/source_file=' || filename || '/data_0.zip'), []), list_value('')) FROM daily_to_archive)
{% endcall %}

{% do log("[DOWNLOAD] Processing daily files...", info=True) %}

{% call statement('archive_daily', fetch_result=False) %}
COPY (
  SELECT
    content,
    split_part(split_part(filename, '/', -1), '.', 1) AS source_file,
    CAST(substring(split_part(split_part(filename, '/', -1), '_', 3), 1, 4) AS INT) AS year
  FROM read_blob(getvariable('daily_urls'))
  WHERE getvariable('has_daily_to_archive') AND filename != ''
) TO (getvariable('csv_archive_path') || '/daily')
(FORMAT BLOB, PARTITION_BY (year, source_file), FILE_EXTENSION 'zip', OVERWRITE_OR_IGNORE)
{% endcall %}

{% call statement('log_daily', fetch_result=False) %}
INSERT INTO _csv_archive_log BY NAME
SELECT
  'daily' AS source_type,
  filename AS source_filename,
  '/daily/year=' || CAST(substring(split_part(filename, '_', 3), 1, 4) AS INT) || '/source_file=' || filename || '/data_0.zip' AS archive_path,
  now() AS archived_at
FROM daily_to_archive
WHERE getvariable('daily_expected') > 0
  AND (SELECT count(*) FROM glob(getvariable('daily_paths'))) = getvariable('daily_expected')
{% endcall %}

{# ============================================================================ #}
{# INTRADAY SCADA (5-minute dispatch) #}
{# ============================================================================ #}

{% call statement('intraday_scada_web', fetch_result=False) %}
CREATE OR REPLACE TEMP TABLE intraday_scada_web AS
WITH
  html_data AS (
    SELECT content AS html FROM read_text('http://nemweb.com.au/Reports/Current/Dispatch_SCADA/')
  ),
  lines AS (
    SELECT unnest(string_split(html, '<br>')) AS line FROM html_data
  )
SELECT
  'http://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
  split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\.zip)"', 1), '.', 1) AS filename
FROM lines
WHERE line LIKE '%PUBLIC_DISPATCHSCADA%'
ORDER BY full_url DESC
LIMIT 500
{% endcall %}

{% call statement('scada_today_to_archive', fetch_result=False) %}
CREATE OR REPLACE TEMP TABLE scada_today_to_archive AS
SELECT full_url, filename
FROM intraday_scada_web
WHERE filename NOT IN (SELECT source_filename FROM _existing_archive_log WHERE source_type = 'scada_today')
LIMIT getvariable('download_limit')
{% endcall %}

{% call statement('set_has_scada', fetch_result=False) %}
SET VARIABLE has_scada_today_to_archive = (SELECT count(*) > 0 FROM scada_today_to_archive)
{% endcall %}

{% call statement('set_scada_urls', fetch_result=False) %}
SET VARIABLE scada_urls = (SELECT COALESCE(NULLIF(list(full_url), []), list_value('')) FROM scada_today_to_archive)
{% endcall %}

{% call statement('set_scada_expected', fetch_result=False) %}
SET VARIABLE scada_expected = (SELECT count(*) FROM scada_today_to_archive)
{% endcall %}

{% call statement('set_scada_paths', fetch_result=False) %}
SET VARIABLE scada_paths = (SELECT COALESCE(NULLIF(list(getvariable('csv_archive_path') || '/scada_today/day=' || substring(split_part(filename, '_', 3), 1, 8) || '/source_file=' || filename || '/data_0.zip'), []), list_value('')) FROM scada_today_to_archive)
{% endcall %}

{% do log("[DOWNLOAD] Processing intraday SCADA files...", info=True) %}

{% call statement('archive_scada', fetch_result=False) %}
COPY (
  SELECT
    content,
    split_part(split_part(filename, '/', -1), '.', 1) AS source_file,
    substring(split_part(split_part(filename, '/', -1), '_', 3), 1, 8) AS day
  FROM read_blob(getvariable('scada_urls'))
  WHERE getvariable('has_scada_today_to_archive') AND filename != ''
) TO (getvariable('csv_archive_path') || '/scada_today')
(FORMAT BLOB, PARTITION_BY (day, source_file), FILE_EXTENSION 'zip', OVERWRITE_OR_IGNORE)
{% endcall %}

{% call statement('log_scada', fetch_result=False) %}
INSERT INTO _csv_archive_log BY NAME
SELECT
  'scada_today' AS source_type,
  filename AS source_filename,
  '/scada_today/day=' || substring(split_part(filename, '_', 3), 1, 8) || '/source_file=' || filename || '/data_0.zip' AS archive_path,
  now() AS archived_at
FROM scada_today_to_archive
WHERE getvariable('scada_expected') > 0
  AND (SELECT count(*) FROM glob(getvariable('scada_paths'))) = getvariable('scada_expected')
{% endcall %}

{# ============================================================================ #}
{# INTRADAY PRICE (5-minute dispatch) #}
{# ============================================================================ #}

{% call statement('intraday_price_web', fetch_result=False) %}
CREATE OR REPLACE TEMP TABLE intraday_price_web AS
WITH
  html_data AS (
    SELECT content AS html FROM read_text('http://nemweb.com.au/Reports/Current/DispatchIS_Reports/')
  ),
  lines AS (
    SELECT unnest(string_split(html, '<br>')) AS line FROM html_data
  )
SELECT
  'http://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
  split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\.zip)"', 1), '.', 1) AS filename
FROM lines
WHERE line LIKE '%PUBLIC_DISPATCHIS_%.zip%'
ORDER BY full_url DESC
LIMIT 500
{% endcall %}

{% call statement('price_today_to_archive', fetch_result=False) %}
CREATE OR REPLACE TEMP TABLE price_today_to_archive AS
SELECT full_url, filename
FROM intraday_price_web
WHERE filename NOT IN (SELECT source_filename FROM _existing_archive_log WHERE source_type = 'price_today')
LIMIT getvariable('download_limit')
{% endcall %}

{% call statement('set_has_price', fetch_result=False) %}
SET VARIABLE has_price_today_to_archive = (SELECT count(*) > 0 FROM price_today_to_archive)
{% endcall %}

{% call statement('set_price_urls', fetch_result=False) %}
SET VARIABLE price_urls = (SELECT COALESCE(NULLIF(list(full_url), []), list_value('')) FROM price_today_to_archive)
{% endcall %}

{% call statement('set_price_expected', fetch_result=False) %}
SET VARIABLE price_expected = (SELECT count(*) FROM price_today_to_archive)
{% endcall %}

{% call statement('set_price_paths', fetch_result=False) %}
SET VARIABLE price_paths = (SELECT COALESCE(NULLIF(list(getvariable('csv_archive_path') || '/price_today/day=' || substring(split_part(filename, '_', 3), 1, 8) || '/source_file=' || filename || '/data_0.zip'), []), list_value('')) FROM price_today_to_archive)
{% endcall %}

{% do log("[DOWNLOAD] Processing intraday PRICE files...", info=True) %}

{% call statement('archive_price', fetch_result=False) %}
COPY (
  SELECT
    content,
    split_part(split_part(filename, '/', -1), '.', 1) AS source_file,
    substring(split_part(split_part(filename, '/', -1), '_', 3), 1, 8) AS day
  FROM read_blob(getvariable('price_urls'))
  WHERE getvariable('has_price_today_to_archive') AND filename != ''
) TO (getvariable('csv_archive_path') || '/price_today')
(FORMAT BLOB, PARTITION_BY (day, source_file), FILE_EXTENSION 'zip', OVERWRITE_OR_IGNORE)
{% endcall %}

{% call statement('log_price', fetch_result=False) %}
INSERT INTO _csv_archive_log BY NAME
SELECT
  'price_today' AS source_type,
  filename AS source_filename,
  '/price_today/day=' || substring(split_part(filename, '_', 3), 1, 8) || '/source_file=' || filename || '/data_0.zip' AS archive_path,
  now() AS archived_at
FROM price_today_to_archive
WHERE getvariable('price_expected') > 0
  AND (SELECT count(*) FROM glob(getvariable('price_paths'))) = getvariable('price_expected')
{% endcall %}

{# ============================================================================ #}
{# DUID REFERENCE DATA (from GitHub and WA AEMO) - always refresh #}
{# ============================================================================ #}

{% do log("[DOWNLOAD] Refreshing 4 DUID reference files...", info=True) %}

{% call statement('init_duid_dir', fetch_result=False) %}
COPY (SELECT 1 AS id, 'init' AS dummy) TO (getvariable('csv_archive_path') || '/duid')
(FORMAT csv, PARTITION_BY (dummy), FILE_EXTENSION 'csv', OVERWRITE_OR_IGNORE)
{% endcall %}

{% call statement('copy_duid_data', fetch_result=False) %}
COPY (
  SELECT * FROM read_csv_auto('https://raw.githubusercontent.com/djouallah/aemo_fabric/refs/heads/djouallah-patch-1/duid_data.csv', null_padding=true, ignore_errors=true)
) TO (getvariable('csv_archive_path') || '/duid/duid_data.csv') (FORMAT CSV, HEADER)
{% endcall %}

{% call statement('copy_facilities', fetch_result=False) %}
COPY (
  SELECT * FROM read_csv_auto('https://data.wa.aemo.com.au/datafiles/post-facilities/facilities.csv', null_padding=true, ignore_errors=true)
) TO (getvariable('csv_archive_path') || '/duid/facilities.csv') (FORMAT CSV, HEADER)
{% endcall %}

{% call statement('copy_wa_energy', fetch_result=False) %}
COPY (
  SELECT * FROM read_csv_auto('https://raw.githubusercontent.com/djouallah/aemo_fabric/refs/heads/main/WA_ENERGY.csv', header=true, null_padding=true, ignore_errors=true)
) TO (getvariable('csv_archive_path') || '/duid/WA_ENERGY.csv') (FORMAT CSV, HEADER)
{% endcall %}

{% call statement('copy_geo_data', fetch_result=False) %}
COPY (
  SELECT * FROM read_csv_auto('https://raw.githubusercontent.com/djouallah/aemo_fabric/refs/heads/main/geo_data.csv', null_padding=true, ignore_errors=true)
) TO (getvariable('csv_archive_path') || '/duid/geo_data.csv') (FORMAT CSV, HEADER)
{% endcall %}

{% call statement('delete_old_duid_logs', fetch_result=False) %}
DELETE FROM _csv_archive_log WHERE source_type LIKE 'duid_%'
{% endcall %}

{% call statement('log_duid', fetch_result=False) %}
INSERT INTO _csv_archive_log BY NAME
SELECT * FROM (VALUES
  ('duid_data', 'duid_data', '/duid/duid_data.csv', now()),
  ('duid_facilities', 'facilities', '/duid/facilities.csv', now()),
  ('duid_wa_energy', 'WA_ENERGY', '/duid/WA_ENERGY.csv', now()),
  ('duid_geo_data', 'geo_data', '/duid/geo_data.csv', now())
) AS t(source_type, source_filename, archive_path, archived_at)
{% endcall %}

{% do log("[DOWNLOAD] DUID files written: 4", info=True) %}

{# Write the combined log to parquet on abfss:// #}
{% call statement('save_log', fetch_result=False) %}
COPY _csv_archive_log TO '{{ csv_log_path }}' (FORMAT PARQUET)
{% endcall %}
{% do log("[DOWNLOAD] Log saved to " ~ csv_log_path, info=True) %}

{% do log("[DOWNLOAD COMPLETE]", info=True) %}

{% endif %}

{% endmacro %}
