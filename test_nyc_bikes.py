import requests
import json

station_info_url = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"
station_status_url = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json"

# Get station information
info_response = requests.get(station_info_url) # gets the json format from the url
info_data = info_response.json() # it converts the JSON (info_response) text into a Python dictionary.

# How many stations
num_stations = len(info_data['data']['stations'])
print(f"Number of stations: {num_stations}")

# Show the first station's information
first_station_info = info_data['data']['stations'][0]
print("First station information:")
print(f"     - Name: {first_station_info['name']}")
print("      - Capacity:", first_station_info['capacity'])
print(f"     - Location: ({first_station_info['lat']}, {first_station_info['lon']})")

# Get real-time station status
status_response = requests.get(station_status_url)
status_data = status_response.json()

# Show first station's status
first_station_status = status_data['data']['stations'][0]
print("First station status:")
print(f"     - Station ID: {first_station_status['station_id']}")
print(f"     - Num Bikes Available: {first_station_status['num_bikes_available']}")
print(f"     - Num Docks Available: {first_station_status['num_docks_available']}")
print(f"     - Empty Docks: {first_station_status['num_docks_available']}")