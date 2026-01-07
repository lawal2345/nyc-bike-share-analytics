-- models/marts/dim_stations.sql
-- Dimension table: Station reference data

WITH station_info AS (
    SELECT * FROM {{ ref('stg_station_info') }}
),

final AS (
    SELECT
        station_id,
        station_name,
        latitude,
        longitude,
        capacity,
        
        -- Geographic grouping (simple neighborhood detection)
        CASE
            WHEN latitude >= 40.7 AND longitude >= -74.0 THEN 'Manhattan/Brooklyn Border'
            WHEN latitude < 40.7 AND longitude >= -74.0 THEN 'South Brooklyn'
            WHEN latitude >= 40.7 AND longitude < -74.0 THEN 'Jersey City'
            ELSE 'Other'
        END AS area
        
    FROM station_info
)

SELECT * FROM final