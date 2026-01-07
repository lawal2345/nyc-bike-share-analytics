CREATE OR REPLACE TABLE `bike_sharing_data.raw_station_info`
(
    last_updated INT64,
    ttl INT64,
    version STRING,
    data_fetched_at TIMESTAMP,
    stations STRING
);