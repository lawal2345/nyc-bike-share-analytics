# NYC Bike Share Analytics Pipeline

> Real-time data engineering project analyzing Citi Bike availability patterns across New York City

*** [Live Dashboard](https://lookerstudio.google.com/s/nMIHVZed3rM)**

---

## Project Overview

This project implements an end-to-end data pipeline that:
- Collects real-time bike-sharing data every 15 minutes from GBFS
- Stores raw data in Google Cloud Storage (Data Lake)
- Loads data into BigQuery (Data Warehouse)
- Transforms data using dbt (dimensional modeling, Kimball)
- Visualizes in Looker Studio

---

## Architecture

GBFS JSON
    ↓ (every 15 min)
Cloud Function
    ↓
Cloud Storage (Data Lake)
    ↓ (hourly via Prefect)
BigQuery (Raw Tables)
    ↓
dbt Transformations (Staging → Marts)
    ↓
Looker Studio Dashboard

**Technologies Used:**
- **Cloud:** Google Cloud Platform (Cloud Functions, Cloud Storage, BigQuery)
- **Orchestration:** Prefect
- **Transformation:** dbt (data build tool)
- **Visualization:** Looker Studio
- **Languages:** Python, SQL

---

## Project Structure

urban-mobility-project/
├── bigquery/                    # BigQuery table schemas
│   ├── create_raw_station_status.sql
│   └── create_raw_station_info.sql
├── data_loading/                # ETL scripts
│   ├── load_station_status.py
│   ├── load_station_info.py
│   └── prefect_load_station_status.py
├── bike_sharing_analytics/      # dbt project
│   ├── models/
│   │   ├── staging/            # Raw → Clean
│   │   │   ├── stg_station_status.sql
│   │   │   └── stg_station_info.sql
│   │   └── marts/              # Analytics tables
│   │       ├── fact_station_activity.sql
│   │       └── dim_stations.sql
│   └── dbt_project.yml
└── README.md


---

## Key Features

### 1. Automated Data Collection
- Cloud Function triggers every 15 minutes via Cloud Scheduler
- Fetches data from GBFS
- Saves timestamped JSON files to Cloud Storage

### 2. Incremental Loading with Duplicate Prevention
- Prefect orchestrates hourly loads to BigQuery
- Checks existing timestamps before loading
- Only processes new files (no duplicates)
- Automatic retry logic for failed operations

### 3. Dimensional Data Modeling (Kimball)
- **Staging Layer:** Unpacks JSON into structured rows
- **Fact Table:** Time-series metrics (bikes available, utilization %)
- **Dimension Table:** Station reference data (names, locations, capacity)
- Star schema optimized for analytics queries

### 4. Interactive Dashboard
- Real-time visualization of bike availability patterns
- Time-series analysis (hourly trends)
- Geographic distribution (area-based comparisons)
- Public shareable link

---

## Data Model

### Fact Table: `fact_station_activity`
Granularity: One row per station per snapshot (15-min intervals)

| Column | Type | Description |
|--------|------|-------------|
| activity_id | STRING | Unique identifier (surrogate key) |
| snapshot_timestamp | TIMESTAMP | When data was collected |
| hour_of_day | INTEGER | Hour (0-23) for time-based analysis |
| station_id | STRING | Foreign key to dim_stations |
| bikes_available | INTEGER | Bikes ready to rent |
| utilization_pct | FLOAT | (bikes/capacity) × 100 |

### Dimension Table: `dim_stations`
Granularity: One row per station

| Column | Type | Description |
|--------|------|-------------|
| station_id | STRING | Primary key |
| station_name | STRING | Human-readable name |
| latitude | FLOAT | GPS coordinate |
| longitude | FLOAT | GPS coordinate |
| capacity | INTEGER | Total docks at station |
| area | STRING | Geographic grouping |

---



### Top 10 Most Utilized Stations
```sql
SELECT 
    d.station_name,
    d.area,
    ROUND(AVG(f.utilization_pct), 1) as avg_utilization
FROM bike_sharing_data.fact_station_activity f
JOIN bike_sharing_data.dim_stations d
    ON f.station_id = d.station_id
GROUP BY d.station_name, d.area
ORDER BY avg_utilization DESC
LIMIT 10
```

---

## 🔮 Future Enhancements

- [ ] Add weather data (correlate bike usage with weather)
- [ ] Predictive modeling (forecast demand)
- [ ] Anomaly detection (identify unusual patterns)
- [ ] Real-time alerting (Slack notifications for system issues)
- [ ] Cost optimization (query optimization, partitioning strategies)

---

Data source: [Citi Bike GBFS API](https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json)

---

## Author

**Jesutofunmi Lawal**
- [LinkedIn](https://www.linkedin.com/in/jesutofunmi-lawal-51683218b/)

