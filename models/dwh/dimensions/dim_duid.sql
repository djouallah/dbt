{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='DUID',
    on_schema_change='sync_all_columns'
) }}

{#-- NOTE: do not add a leading `-- {{ ref(...) }}` dependency comment here. dbt-fabric wraps
     the model in EXEC('create view ... as <sql>'); the newline after such a line collapses and
     comments out the SELECT, producing "Incorrect syntax near 'a'". The downloader runs as a
     separate step before dbt anyway, so no ref dependency is needed. --#}

{#-- Reads the four DUID reference CSVs the downloader lands under Files/csv_raw/duid/. They keep
     their headers, so OPENROWSET(HEADER_ROW=TRUE) exposes the columns by name (no WITH /
     ordinal mapping). merge on DUID keeps the dimension current as attributes change.
     The duckrun version had a parse-time run_query "new DUIDs?" probe to skip rebuilds;
     dropped here — merge is naturally idempotent, so always rebuild + upsert. --#}

{%- set duid_path = get_csv_archive_path() ~ '/duid' -%}

{#-- Same logic as the duckrun version, but with the CTEs inlined as derived tables: dbt-fabric
     wraps a `merge` model in `MERGE ... USING (<model sql>)`, and a leading top-level WITH is
     invalid inside that parenthesised source. Nested derived tables are equivalent. --#}

SELECT
  a.DUID,
  MAX(a.Region) AS Region,
  MAX(
    UPPER(LEFT(TRIM(a.FuelSourceDescriptor), 1))
    + LOWER(SUBSTRING(TRIM(a.FuelSourceDescriptor), 2, LEN(TRIM(a.FuelSourceDescriptor))))
  ) AS FuelSourceDescriptor,
  MAX(a.Participant) AS Participant,
  MAX(states.State) AS State,
  MAX(geo.latitude) AS latitude,
  MAX(geo.longitude) AS longitude
FROM (
  SELECT DUID, Region, FuelSourceDescriptor, Participant
  FROM (
    SELECT
      [DUID] AS DUID,
      MAX([Region]) AS Region,
      MAX([Fuel Source - Descriptor]) AS FuelSourceDescriptor,
      MAX([Participant]) AS Participant
    FROM OPENROWSET(BULK '{{ duid_path }}/duid_data.csv', FORMAT = 'CSV',
                    PARSER_VERSION = '2.0', HEADER_ROW = TRUE) AS d
    WHERE LEN([DUID]) > 2
    GROUP BY [DUID]
  ) duid_aemo

  UNION ALL

  SELECT
    wa_facilities.DUID,
    wa_facilities.Region,
    wa_energy.Technology AS FuelSourceDescriptor,
    wa_facilities.Participant
  FROM (
    SELECT
      'WA1' AS Region,
      [Facility Code] AS DUID,
      [Participant Name] AS Participant
    FROM OPENROWSET(BULK '{{ duid_path }}/facilities.csv', FORMAT = 'CSV',
                    PARSER_VERSION = '2.0', HEADER_ROW = TRUE) AS f
  ) wa_facilities
  LEFT JOIN (
    SELECT [DUID] AS DUID, [Technology] AS Technology
    FROM OPENROWSET(BULK '{{ duid_path }}/WA_ENERGY.csv', FORMAT = 'CSV',
                    PARSER_VERSION = '2.0', HEADER_ROW = TRUE) AS w
  ) wa_energy ON wa_facilities.DUID = wa_energy.DUID
) a
JOIN (
  SELECT 'WA1' AS RegionID, 'Western Australia' AS State
  UNION ALL SELECT 'QLD1', 'Queensland'
  UNION ALL SELECT 'NSW1', 'New South Wales'
  UNION ALL SELECT 'TAS1', 'Tasmania'
  UNION ALL SELECT 'SA1', 'South Australia'
  UNION ALL SELECT 'VIC1', 'Victoria'
) states ON a.Region = states.RegionID
LEFT JOIN (
  {#-- geo_data.csv's key column is DUID (uppercase); Fabric HEADER_ROW binding is
       case-sensitive, so [duid] would fail with "Invalid column name 'duid'". --#}
  SELECT
    [DUID] AS duid,
    MAX(TRY_CAST([latitude] AS FLOAT)) AS latitude,
    MAX(TRY_CAST([longitude] AS FLOAT)) AS longitude
  FROM OPENROWSET(BULK '{{ duid_path }}/geo_data.csv', FORMAT = 'CSV',
                  PARSER_VERSION = '2.0', HEADER_ROW = TRUE) AS g
  WHERE [latitude] IS NOT NULL
  GROUP BY [DUID]
) geo ON a.DUID = geo.duid
GROUP BY a.DUID
