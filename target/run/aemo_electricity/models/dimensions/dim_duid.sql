
  
    
    

    create  table
      "ducklake"."aemo"."dim_duid__dbt_tmp"
  
    as (
      

WITH
  states AS (
    SELECT 'WA1' AS RegionID, 'Western Australia' AS State
    UNION ALL SELECT 'QLD1', 'Queensland'
    UNION ALL SELECT 'NSW1', 'New South Wales'
    UNION ALL SELECT 'TAS1', 'Tasmania'
    UNION ALL SELECT 'SA1', 'South Australia'
    UNION ALL SELECT 'VIC1', 'Victoria'
  ),

  duid_aemo AS (
    SELECT
      DUID AS DUID,
      first(Region) AS Region,
      first("Fuel Source - Descriptor") AS FuelSourceDescriptor,
      first(Participant) AS Participant
    FROM
      read_csv('/lakehouse/default/Files/csv/duid/duid_data.csv')
    WHERE
      length(DUID) > 2
    GROUP BY
      DUID
  ),

  wa_facilities AS (
    SELECT
      'WA1' AS Region,
      "Facility Code" AS DUID,
      "Participant Name" AS Participant
    FROM
      read_csv_auto('/lakehouse/default/Files/csv/duid/facilities.csv')
  ),

  wa_energy AS (
    SELECT *
    FROM read_csv_auto('/lakehouse/default/Files/csv/duid/WA_ENERGY.csv', header = 1)
  ),

  duid_wa AS (
    SELECT
      wa_facilities.DUID,
      wa_facilities.Region,
      wa_energy.Technology AS FuelSourceDescriptor,
      wa_facilities.Participant
    FROM wa_facilities
    LEFT JOIN wa_energy ON wa_facilities.DUID = wa_energy.DUID
  ),

  duid_all AS (
    SELECT * FROM duid_aemo
    UNION ALL
    SELECT * FROM duid_wa
  ),

  geo AS (
    SELECT
      duid,
      max(latitude) as latitude,
      max(longitude) as longitude
    FROM read_csv('/lakehouse/default/Files/csv/duid/geo_data.csv')
    WHERE latitude IS NOT NULL
    GROUP BY duid
  )

SELECT
  a.DUID,
  a.Region,
  UPPER(LEFT(TRIM(FuelSourceDescriptor), 1)) || LOWER(SUBSTR(TRIM(FuelSourceDescriptor), 2)) AS FuelSourceDescriptor,
  a.Participant,
  states.State,
  geo.latitude,
  geo.longitude
FROM duid_all a
JOIN states ON a.Region = states.RegionID
LEFT JOIN geo ON a.duid = geo.duid
    );
  
  