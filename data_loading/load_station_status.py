"""
Load station status data from cloud storage into BigQuery
This script:
1. Lists all JSON files in the cloud storage bucket
2. Reads each JSON file and extracts key fields
3. Loads the extracted data into BigQuery's raw_station_status table
"""

import json
from datetime import datetime
from google.cloud import storage, bigquery

# Configuration
bucket_name = "urban-mobility-data-lake-jol-dec-2025"
dataset_id = "bike_sharing_data"
table_id = "raw_station_status"
prefix = "raw/station_status/" # where our files are stored in the bucket

def list_json_files(bucket_name, prefix):
    """List all JSON files in the specified cloud storage bucket with the given prefix."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    # filter for .json files
    json_files = [blob.name for blob in blobs if blob.name.endswith('.json')]
    print(f"Found {len(json_files)} JSON files in bucket {bucket_name} with prefix {prefix}")
    return json_files

def read_json_from_gcs(bucket_name, blob_name):
    """Read JSON content from a GCS blob."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Download as string and parse JSON
    json_string = blob.download_as_string()
    return json.loads(json_string)

def transform_to_bigquery_row(json_data, blob_name):
    """Transform JSON data to match our BigQuery table schema.
     Our schema expects:
    - last_updated, ttl, version (from top level)
    - data_fetched_at, date_partition (we extract from filename)
    - stations (the entire stations array as a JSON string)
    """
    # Extract metadata from filename
    # Example: "raw/station_status/date=2026-01-02/status_20260102_143000.json" We want: "20260102_143000"

    filename = blob_name.split('/')[-1] # get the file name
    timestamp_str = filename.replace('status_', '').replace('.json', '') # "20260102_143000"
    
    # Parse timestamp: "20260102_143000" -> datetime object
    timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")

    # Extract date for partitioning: "2026-01-02"
    date_partition = timestamp.date()

    # Build the row (the schema contains columns. We are telling BigQuery the rows to insert)
    row = {
        "last_updated": json_data.get("last_updated"),
        "ttl": json_data.get("ttl"),
        "version": json_data.get("version"),
        "data_fetched_at": timestamp.isoformat(), # convert to ISO format string
        "date_partition": date_partition.isoformat(),
        "stations": json.dumps(json_data.get("data", {}).get("stations", []))
    }
    return row

def load_to_bigquery(rows, dataset_id, table_id):
    """Load rows into BigQuery."""
    bq_client = bigquery.Client()
    table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

    print(f"Loading {len(rows)} rows into BigQuery table {table_ref}...")

    # Load data
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    job = bq_client.load_table_from_json(
        rows,
        table_ref,
        job_config=job_config
    )

    # Wait for the job to complete
    job.result()
    print(f"Loaded {len(rows)} rows into {table_ref}.")


def main():
    """Main execution function."""

    print("Loading station status data to BigQuery")

    # Step 1: List all JSON files in the bucket
    json_files = list_json_files(bucket_name, prefix)

    if not json_files:
        print("No JSON files found. Make sure cloud function has run at least once.")
        return
    
    # Step 2: Read each JSON file and transform to BigQuery rows
    all_rows = []

    for i, blob_name in enumerate(json_files, 1):
        print(f"\nProcessing file {i}/len(json_files): {blob_name}")

        try:
            # Read JSON
            json_data = read_json_from_gcs(bucket_name, blob_name)

            # Transform to BigQuery row
            row = transform_to_bigquery_row(json_data, blob_name)

            # Append to all rows
            all_rows.append(row)

            print(f" Processed - {len(json_data.get('data', {}).get('stations', []))} stations")

        except Exception as e:
            print(f" Error: {e}")
            continue
    
    # Step 3: Load all rows into BigQuery
    if all_rows:
        load_to_bigquery(all_rows, dataset_id, table_id)
        print(f"\nSuccessfully loaded {len(all_rows)} rows into BigQuery.")
    else:
        print("No rows to load into BigQuery.")

if __name__ == "__main__":
    main()