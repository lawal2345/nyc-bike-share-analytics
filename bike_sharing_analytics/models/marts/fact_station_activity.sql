-- models/marts/fact_station_activity.sql
-- Fact table: Station activity metrics over time

WITH station_status AS (
    SELECT * FROM {{ ref('stg_station_status') }}
),

station_info AS (
    SELECT * FROM {{ ref('stg_station_info') }}
),

final AS (
    SELECT
        -- Surrogate key (unique ID for each row)
        {{ dbt_utils.generate_surrogate_key(['status.data_fetched_at', 'status.station_id']) }} AS activity_id,
        
        -- Time dimensions
        status.data_fetched_at AS snapshot_timestamp,
        status.date_partition AS snapshot_date,
        EXTRACT(HOUR FROM status.data_fetched_at) AS hour_of_day,
        EXTRACT(DAYOFWEEK FROM status.data_fetched_at) AS day_of_week,
        
        -- Station reference
        status.station_id,
        
        -- Metrics
        status.bikes_available,
        status.docks_available,
        status.ebikes_available,
        status.is_renting,
        status.is_returning,
        
        -- Calculated metrics
        info.capacity,
        ROUND((status.bikes_available / info.capacity) * 100, 2) AS utilization_pct,
        (status.bikes_available + status.docks_available) AS total_capacity_check
        
    FROM station_status AS status
    LEFT JOIN station_info AS info
        ON status.station_id = info.station_id
    
    -- Filter out bad data
    WHERE status.bikes_available IS NOT NULL
        AND info.capacity > 0
)

SELECT * FROM final