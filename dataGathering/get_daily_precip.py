from time import sleep
import pandas as pd
import requests
from locations import get_hydropower_locations, get_locations
from tqdm import tqdm

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
	locations = get_locations()

	#load intermediate results if available
	try:
		inter_precip_df = pd.read_csv("data/intermediary/precipitation_intermediate.csv")
		print("Loaded intermediate precipitation results.")
	except FileNotFoundError:
		inter_precip_df = pd.DataFrame(columns=["name", "latitude", "longitude", "precipitation"])
		print("No intermediate precipitation results found. Starting fresh.")

	# Loop over plants and fetch precipitation
	results = []
	for _, row in tqdm(locations.iterrows(), total=len(locations), desc="Fetching precipitation"):
		mask = (
			(inter_precip_df['longitude'] == row['longitude']) &
			(inter_precip_df['latitude'] == row['latitude'])
		)
		if mask.any():
			print(f"Skipping {row['name']} as it already exists in intermediate results.")
			results.append({
				"name": row['name'],
				"latitude": row['latitude'],
				"longitude": row['longitude'],
				"precipitation": inter_precip_df.loc[(inter_precip_df['longitude'] == row['longitude'])
													& (inter_precip_df['latitude'] == row['latitude'])
													, 'precipitation'].values[0]
			})
			continue

		print(f"Processing {row['name']} at ({row['latitude']}, {row['longitude']})")
		precip_val = get_precipitation(row['latitude'], row['longitude'])

		if precip_val is None:
			print(f"Failed to fetch precipitation data for {row['name']}. Retrying in 10 seconds...")
			precip_val = get_precipitation(row['latitude'], row['longitude'])

		sleep(1.2)
		
		results.append({
			"name": row['name'],
			"latitude": row['latitude'],
			"longitude": row['longitude'],
			"precipitation": precip_val
		})

		if len(results) % 200 == 0 and len(results) > 0:
			inter_precip_df = pd.DataFrame(results)
			inter_precip_df.to_csv("data/intermediary/precipitation_intermediate.csv", index=False)

	precip_df = pd.DataFrame(results)
	print(inter_precip_df)
	precip_df.to_csv("data/results/hydropower_precipitation.csv", index=False)
	
if __name__ == "__main__":
	main()
