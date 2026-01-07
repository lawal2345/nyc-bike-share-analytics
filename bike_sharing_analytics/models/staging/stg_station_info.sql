-- models/staging/stg_station_info.sql
-- This model unpacks station information (names, locations, capacity)

WITH source AS (
    SELECT * FROM {{ source('bike_sharing_data', 'raw_station_info') }}
),

unnested AS (
    SELECT
        JSON_EXTRACT_SCALAR(station, '$.station_id') AS station_id,
        JSON_EXTRACT_SCALAR(station, '$.name') AS station_name,
        CAST(JSON_EXTRACT_SCALAR(station, '$.lat') AS FLOAT64) AS latitude,
        CAST(JSON_EXTRACT_SCALAR(station, '$.lon') AS FLOAT64) AS longitude,
        CAST(JSON_EXTRACT_SCALAR(station, '$.capacity') AS INT64) AS capacity
    FROM source,
    UNNEST(JSON_EXTRACT_ARRAY(stations)) AS station
)

SELECT * FROM unnested