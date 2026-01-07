"""
Load station information data to BigQuery. This fetches station info once from the API and loads it.
Station info rarely changes, so we only need one snapshot.
"""

import json
import requests
from datetime import datetime, timezone
from google.cloud import bigquery

# Configuration
station_info_url = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"
dataset_id = "bike_sharing_data"
table_id = "raw_station_info"

def fetch_station_info():
    """Fetch station information data from GBFS API"""
    print("Fetching station information from API...")
    response = requests.get(station_info_url, timeout=60)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()

def transform_to_bigquery(json_data):
    """Load row into BigQuery"""
    
    row = {
        "last_updated": json_data.get("last_updated"),
        "ttl": json_data.get("ttl"),
        "version": json_data.get("version"),
        "data_fetched_at": datetime.now(timezone.utc).isoformat(),
        "stations": json.dumps(json_data.get("data", {}).get("stations", []))
    }
    return row

def load_to_bigquery(row, dataset_id, table_id):
    """Load row into BigQuery"""
    bq_client = bigquery.Client()
    table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

    print(f"Loading station information into BigQuery table {table_ref}...")

    # Load data
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE # Replace existing data
    )

    job = bq_client.load_table_from_json(
        [row], # Single row as a list
        table_ref,
        job_config=job_config
    )

    # Wait for the job to complete
    job.result()
    print(f"Loaded station information into {table_ref}.")


def main():
    """Main execution function."""
    print("Starting station information load to BigQuery")

    # Step 1: Fetch station information data
    json_data = fetch_station_info()

    # Step 2: Transform to BigQuery row
    row = transform_to_bigquery(json_data)

    # Step 3: Load into BigQuery
    load_to_bigquery(row, dataset_id, table_id)

    print("Station information load completed.")

if __name__ == "__main__":
    main()