CREATE OR REPLACE TABLE `bike_sharing_data.raw_station_status`
(
  last_updated INT64,
  ttl INT64,
  version STRING,
  data_fetched_at TIMESTAMP,
  date_partition DATE,
  stations STRING
)
PARTITION BY date_partition;