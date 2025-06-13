import requests
import json
import os
from datetime import datetime, timedelta

# API base URL
API_BASE_URL = "https://environment.data.gov.uk/flood-monitoring"

# Directory to save output files
OUTPUT_DIR = "test_api_data"

def fetch_api_data(endpoint, params=None):
    """Fetch data from the API and return as JSON"""
    try:
        url = f"{API_BASE_URL}/{endpoint}"
        print(f"Fetching data from: {url}")
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise exception for 4XX/5XX responses
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return None

def save_to_file(data, filename):
    """Save JSON data to a file"""
    # Create output directory if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {OUTPUT_DIR}")
    
    # Save data to file
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    
    print(f"Data saved to {filepath}")
    return filepath

def get_latest_readings():
    """Get the latest readings for all stations - useful for current status"""
    print("\n--- Fetching latest readings for all stations ---")
    data = fetch_api_data("data/readings", {"latest": True})
    
    if data and "items" in data:
        count = len(data["items"])
        print(f"Retrieved {count} latest readings")
        filepath = save_to_file(data, "latest_readings.json")
        print(f"Latest readings saved to {filepath}")
        return data
    
    print("Failed to retrieve latest readings")
    return None

def get_stations_with_type(station_type=None, limit=500):
    """Get stations with geographic data - useful for spatial analysis"""
    print(f"\n--- Fetching stations data (type: {station_type if station_type else 'all'}) ---")
    
    params = {"_limit": limit, "_view": "full"}  # full view includes scale information
    if station_type:
        # Tham số cần dùng là 'type', với các giá trị hợp lệ: 'SingleLevel', 'MultiTraceLevel', 
        # 'Coastal', 'Groundwater' hoặc 'Meteorological'
        params["type"] = station_type
    
    data = fetch_api_data("id/stations", params)
    
    if data and "items" in data:
        count = len(data["items"])
        print(f"Retrieved {count} stations")
        
        type_suffix = f"_{station_type}" if station_type else ""
        filepath = save_to_file(data, f"stations{type_suffix}.json")
        print(f"Station data saved to {filepath}")
        return data
    elif data:
        # Log the response body for debugging
        print(f"API response without 'items': {json.dumps(data, indent=2)[:200]}...")
    
    print("Failed to retrieve stations data")
    return None

def get_active_floods():
    """Get active flood warnings - useful for classification and risk analysis"""
    print("\n--- Fetching active flood warnings ---")
    data = fetch_api_data("id/floods")
    
    if data and "items" in data:
        count = len(data["items"])
        print(f"Retrieved {count} flood warnings")
        filepath = save_to_file(data, "active_floods.json")
        print(f"Flood warnings saved to {filepath}")
        return data
    
    print("Failed to retrieve flood warnings")
    return None

def get_historical_readings(station_id, days=7):
    """Get historical readings for a station - useful for time series analysis"""
    print(f"\n--- Fetching {days}-day historical readings for station {station_id} ---")
    
    # Calculate date parameters
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    params = {
        "startdate": start_date.strftime("%Y-%m-%d"),
        "enddate": end_date.strftime("%Y-%m-%d"),
        "_sorted": True
    }
    
    data = fetch_api_data(f"id/stations/{station_id}/readings", params)
    
    if data and "items" in data:
        count = len(data["items"])
        print(f"Retrieved {count} historical readings")
        filepath = save_to_file(data, f"historical_{station_id}_{days}d.json")
        print(f"Historical readings saved to {filepath}")
        return data
    
    print(f"Failed to retrieve historical readings for station {station_id}")
    return None

def get_flood_areas_with_risk():
    """Get flood areas with risk level - useful for risk modeling"""
    print("\n--- Fetching flood areas ---")
    data = fetch_api_data("id/floodAreas")
    
    if data and "items" in data:
        count = len(data["items"])
        print(f"Retrieved {count} flood areas")
        filepath = save_to_file(data, "flood_areas.json")
        print(f"Flood areas saved to {filepath}")
        return data
    
    print("Failed to retrieve flood areas")
    return None

def main():
    print("Extracting ML-Ready Data from Flood Monitoring API")
    print("================================================")
    
    # 1. Get stations with geo data (useful for visualization)
    stations = get_stations_with_type()
    
    # 2. Get river level stations separately (most useful for ML)
    river_stations = get_stations_with_type("SingleLevel")
    
    # 3. Get latest readings for all stations
    latest_readings = get_latest_readings()
    
    # 4. Get active flood warnings (for classification/ML)
    flood_warnings = get_active_floods()
    
    # 5. Get flood areas (for risk analysis)
    flood_areas = get_flood_areas_with_risk()
    
    # 6. Get historical readings for selected stations
    if river_stations and "items" in river_stations:
        # Select up to 5 river monitoring stations for historical data
        samples = min(5, len(river_stations["items"]))
        print(f"\nSelecting {samples} sample stations for historical data...")
        
        for i, station in enumerate(river_stations["items"][:samples]):
            station_id = station.get('@id', '').split('/')[-1]
            if station_id:
                get_historical_readings(station_id, days=30)  # Get 30 days of data

    print("\nData extraction complete. All files saved to the ML_API_DATA directory.")
    print("This data is structured for:")
    print("1. Geospatial analysis and visualization of stations and flood areas")
    print("2. Time series analysis and prediction of water levels")
    print("3. Classification of flood risks and warnings")
    print("4. Correlation analysis between river levels and flood occurrences")

if __name__ == "__main__":
    main() 