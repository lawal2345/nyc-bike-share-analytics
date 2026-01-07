"""Prefect flow for loading station status data from Cloud Storage to BigQuery.

This is the orchestrated version of load_station_status.py.
Each function becomes a Prefect task for better monitoring and retry logic."""

import json
from datetime import datetime, timezone
from google.cloud import storage, bigquery
from prefect import flow, task
from prefect.logging import get_run_logger # Prefect logger, better than print()

# Configuration
bucket_name = "urban-mobility-data-lake-jol-dec-2025"
dataset_id = "bike_sharing_data"
table_id = "raw_station_status"
prefix = "raw/station_status/"


@task(retries=2, retry_delay_seconds=10)
def get_already_loaded_timestamps(dataset_id: str, table_id: str) -> set:
    """
    Query BigQuery to get timestamps that have already been loaded.
    
    Returns a set of timestamp strings like: '20260102_143000'
    This lets us skip files we've already processed.
    """

    logger = get_run_logger()

    bq_client = bigquery.Client()
    table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

    # Query to get distinct data_fetched_at timestamps
    query = f"""
    SELECT DISTINCT data_fetched_at
    FROM `{table_ref}`
    """

    try:
        query_job = bq_client.query(query)
        results = query_job.result()

        # Extract timestamps and convert to the filename format
        # BigQuery stores: '2026-01-02T14:30:00'
        # Filenames are: 'status_20260102_143000.json'
        # We need to convert for comparison

        loaded_timestamps = set()
        for row in results:
            timestamp_str = row.data_fetched_at.strftime("%Y%m%d_%H%M%S")
            loaded_timestamps.add(timestamp_str)
        
        logger.info(f"Found {len(loaded_timestamps)} already loaded timestamps in BigQuery.")
        return loaded_timestamps
    except Exception as e:
        # if table is empty or doesn't exist yet, we assume nothing is loaded
        logger.warning(f"Could not query BigQuery table {table_ref}: {e}")
        return set()
    

@task
def filter_new_files(all_files: list, loaded_timestamps: set) -> list:
    """
    Given a list of all files and a set of already loaded timestamps,
    return only files that have not been loaded yet.
    """

    logger = get_run_logger()

    new_files = []

    for blob_name in all_files:
        filename = blob_name.split('/')[-1]  # 'status_20260102_143000.json'
        timestamp_str = filename.replace('status_', '').replace('.json', '')  # '20260102_143000'
        
        # Check if this timestamp is already loaded
        if timestamp_str not in loaded_timestamps:
            new_files.append(blob_name)
            logger.info(f"New file to load: {filename}")
        else:
            logger.info(f"Skipping already loaded file: {filename}")
    
    logger.info(f"Found {len(new_files)} new files out of {len(all_files)} total files")
    return new_files


@task(retries=2, retry_delay_seconds=10)
def list_json_files(bucket_name: str, prefix: str) -> list:
    """ Decorated with @task for:
    - Automatic retries if Cloud Storage is temporarily unavailable
    - Logging of file count
    - Tracking in Prefect UI"""

    logger = get_run_logger()

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    json_files = [blob.name for blob in blobs if blob.name.endswith('.json')]
    logger.info(f"Found {len(json_files)} JSON files in bucket '{bucket_name}' with prefix '{prefix}'.")
    return json_files


@task(retries=3, retry_delay_seconds=5)
def read_json_from_gcs(bucket_name: str, blob_name: str) -> dict:
    """ 
     Read a single JSON file from Cloud Storage.
    
    Retries 3 times if network issues occur.
    """

    logger = get_run_logger()

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    json_string = blob.download_as_string()
    logger.info(f"Downloaded {blob_name} ({len(json_string)} bytes) from GCS.")

    return json.loads(json_string)  # Parse JSON string to Python object

@task
def transform_to_bigquery_row(json_data: dict, blob_name: str) -> dict:
    """
    Transform JSON data to match BigQuery schema. No retries needed - pure transformation logic.
    """

    logger = get_run_logger()

    # Extract timestamp from filename
    filename = blob_name.split('/')[-1]
    timestamp_str = filename.replace('status_', '').replace('.json', '')
    timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
    date_partition = timestamp.date()

    row = {
        "last_updated": json_data.get("last_updated"),
        "ttl": json_data.get("ttl"),
        "version": json_data.get("version"),
        "data_fetched_at": timestamp.isoformat(),
        "date_partition": date_partition.isoformat(),
        "stations": json.dumps(json_data.get("data", {}).get("stations", []))
    }

    logger.info(f"Transformed data for {timestamp}")
    return row

@task(retries=2, retry_delay_seconds=30)
def load_to_bigquery(rows: list, dataset_id: str, table_id: str):
    """
    Load rows to BigQuery.
    
    Retries with 30-second delay if BigQuery is temporarily unavailable.
    """

    logger = get_run_logger()

    bq_client = bigquery.Client()
    table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

    logger.info(f"Loading {len(rows)} rows to {table_ref}")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    job = bq_client.load_table_from_json(
        rows,
        table_ref,
        job_config=job_config
    )

    job.result()  # Wait for the job to complete
    logger.info(f"Successfully loaded {len(rows)} rows into {table_ref}.")


@flow(name="Load Station Status to BigQuery")
def load_station_status_flow():
    """
    Main Prefect flow for loading bike station status data.
    
    This orchestrates the entire pipeline:
    1. Check what's already in BigQuery
    2. List files in Cloud Storage
    3. Filter to only NEW files
    4. Read and transform each new file
    5. Batch load to BigQuery
    """
    logger = get_run_logger()
    
    logger.info("Starting station status data load pipeline")
    
    # NEW TASK 1: Check what's already loaded
    loaded_timestamps = get_already_loaded_timestamps(dataset_id, table_id)
    
    # Task 2: List all files in Cloud Storage
    all_files = list_json_files(bucket_name, prefix)
    
    if not all_files:
        logger.warning("No JSON files found in Cloud Storage - pipeline complete")
        return
    
    # NEW TASK 3: Filter to only new files
    new_files = filter_new_files(all_files, loaded_timestamps)
    
    if not new_files:
        logger.info("No new files to load - all files already in BigQuery!")
        return
    
    # Task 4: Process each NEW file
    all_rows = []
    
    for blob_name in new_files:
        try:
            # Read file
            json_data = read_json_from_gcs(bucket_name, blob_name)
            
            # Transform
            row = transform_to_bigquery_row(json_data, blob_name)
            
            # Add to batch
            all_rows.append(row)
            
        except Exception as e:
            logger.error(f"Failed to process {blob_name}: {e}")
            # Continue with other files even if one fails
            continue
    
    # Task 5: Load all rows
    if all_rows:
        load_to_bigquery(all_rows, dataset_id, table_id)
        logger.info(f"Pipeline complete - loaded {len(all_rows)} NEW snapshots")
    else:
        logger.warning("No rows to load (all files failed processing)")

if __name__ == "__main__":
    load_station_status_flow.serve(
        name="hourly-station-status-load",
        interval=3600 # Run every hour (3600 seconds)
    )