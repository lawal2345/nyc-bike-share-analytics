-- models/staging/stg_station_status.sql
-- This model unpacks the JSON station data into rows

WITH source AS (
    SELECT * FROM {{ source('bike_sharing_data', 'raw_station_status') }}
),

unnested AS (
    SELECT
        data_fetched_at,
        date_partition,
        JSON_EXTRACT_SCALAR(station, '$.station_id') AS station_id,
        CAST(JSON_EXTRACT_SCALAR(station, '$.num_bikes_available') AS INT64) AS bikes_available,
        CAST(JSON_EXTRACT_SCALAR(station, '$.num_docks_available') AS INT64) AS docks_available,
        CAST(JSON_EXTRACT_SCALAR(station, '$.num_ebikes_available') AS INT64) AS ebikes_available,
        CAST(JSON_EXTRACT_SCALAR(station, '$.is_renting') AS INT64) AS is_renting,
        CAST(JSON_EXTRACT_SCALAR(station, '$.is_returning') AS INT64) AS is_returning
    FROM source,
    UNNEST(JSON_EXTRACT_ARRAY(stations)) AS station
)

SELECT * FROM unnested