import pandas as pd
import requests
from locations import get_hydropower_locations

def get_precipitation(lat, lon, start_date="2024-01-01", end_date="2024-12-31"):
    """
    Get precipitation data using Open-Meteo API
    """
    url = "https://archive-api.open-meteo.com/v1/era5"
    
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "precipitation_sum",
        "timezone": "UTC"
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if "daily" in data and "precipitation_sum" in data["daily"]:
            precip_values = data["daily"]["precipitation_sum"]
            # Filter out None values and calculate mean
            valid_precip = [p for p in precip_values if p is not None]
            
            if valid_precip:
                return sum(valid_precip) / len(valid_precip)  # Mean daily precipitation
        
        return None
        
    except Exception as e:
        print(f"Open-Meteo API error: {e}")
        return None

def main():
	powerplant_locations = get_hydropower_locations()
	
	# Loop over plants and fetch NDVI
	results = []
	for _, row in powerplant_locations.head(20).iterrows():
		print(f"Processing {row['name']} at ({row['latitude']}, {row['longitude']})")
		precip_val = get_precipitation(row['latitude'], row['longitude'])
		results.append({
			"name": row['name'],
			"latitude": row['latitude'],
			"longitude": row['longitude'],
			"precipitation": precip_val
		})

	precip_df = pd.DataFrame(results)
	print(precip_df)
	precip_df.to_csv("data/results/hydropower_precipitation.csv", index=False)
	
if __name__ == "__main__":
	main()
