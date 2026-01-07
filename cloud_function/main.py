import json
from datetime import datetime, timezone
from google.cloud import storage
import requests
import functions_framework

# Configuration
bucket_name = "urban-mobility-data-lake-jol-dec-2025"
station_info_url = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"
station_status_url = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json"

def fetch_station_data(url):
    """Fetch data from GBFS API."""
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()  # Raise an error for bad responses
        return response.json()
    except Exception as e:
        print(f"Error fetching data from {url}: {e}")
        return None

def upload_to_gcs(bucket_name, data, blob_name):
    """Upload JSON data to Google Cloud Storage"""
    try:
        storage_client = storage.Client() # Creates connection to GCS
        bucket = storage_client.bucket(bucket_name) # Selects the bucket
        blob = bucket.blob(blob_name) # Creates a blob object
        blob.upload_from_string(
            data=json.dumps(data),
            content_type='application/json'
        )
        print(f"Uploaded data to {blob_name} in bucket {bucket_name}")
        return True
    except Exception as e:
        print(f"Error uploading data to GCS: {e}")
        return False
    

@functions_framework.http
def collect_bike_data(request):
    """Cloud Function to collect bike station data and upload to GCS."""
    print("Starting bike data collection...")

    # Generate timestamp for filenames
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    date_partition = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Fetch station status (real-time data)
    print("Fetching station status...")
    status_data = fetch_station_data(station_status_url)
    
    if status_data:
        # Create file path with date partitioning
        blob_name = f"raw/station_status/date={date_partition}/status_{timestamp}.json"
        
        # Upload to Cloud Storage
        success = upload_to_gcs(bucket_name, status_data, blob_name)
        
        if success:
            num_stations = len(status_data.get('data', {}).get('stations', []))
            message = f"Successfully collected data for {num_stations} stations at {timestamp}"
            print(message)
            return {"status": "success", "message": message, "timestamp": timestamp}
        else:
            return {"status": "error", "message": "Failed to upload to Cloud Storage"}, 500
    else:
        return {"status": "error", "message": "Failed to fetch data from API"}, 500