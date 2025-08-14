import json
import pandas as pd
import requests
import numpy as np
import rasterio
import tempfile
from shapely.geometry import Point
import geopandas as gpd
from powerplant_data import get_hydropower_data

AUTH_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

with open("client_info.json", "r") as f:
	AUTH_DATA = json.load(f)

print("Authenticating with Copernicus Data Space...")
print(f"AuthData: {AUTH_DATA}")

token_response = requests.post(AUTH_URL, data=AUTH_DATA)
print(f"Token response: {token_response.status_code} {token_response.text}")
ACCESS_TOKEN = token_response.json()["access_token"]

HEADERS = {
	"Authorization": f"Bearer {ACCESS_TOKEN}"
}


# NDVI extraction function (Processing API)
def get_ndvi(lat, lon, start_date="2024-04-01", end_date="2024-09-30"):
	# Create small bbox around the plant (~100m x 100m)
	buffer_deg = 0.0009  # ~100 m at equator
	bbox = [lon - buffer_deg, lat - buffer_deg, lon + buffer_deg, lat + buffer_deg]

	evalscript = """
	//VERSION=3
	function setup() {
	  return {
		input: [{
		  bands: ["B04", "B08", "SCL"],
		  units: "DN"
		}],
		output: [
		  {
			id: "ndvi",
			bands: 1,
			sampleType: "FLOAT32"
		  },
		  {
			id: "dataMask", 
			bands: 1
		  }
		],
		mosaicking: "ORBIT"
	  };
	}
	
	function evaluatePixel(samples) {
	  var validSamples = [];
	  
	  // Collect all valid samples across time
	  for (var i = 0; i < samples.length; i++) {
		var sample = samples[i];
		// SCL filtering: Exclude water (6) and invalid pixels
		if (sample.SCL == 6 || sample.SCL == 3 || sample.SCL == 8 ||
			sample.SCL == 9 || sample.SCL == 10 || sample.SCL == 11) {
			continue; // Skip this sample, continue to next
		}
		
		var ndvi = (sample.B08 - sample.B04) / (sample.B08 + sample.B04);
		
		// Filter out invalid NDVI values
		if (!isNaN(ndvi) && isFinite(ndvi)) {
		  validSamples.push(ndvi);
		}
	  }
	  
	  if (validSamples.length === 0) {
		return {
		  ndvi: [NaN],
		  dataMask: [0]
		};
	  }
	  
	  // Calculate temporal mean
	  var sum = 0;
	  for (var j = 0; j < validSamples.length; j++) {
		sum += validSamples[j];
	  }
	  var meanNdvi = sum / validSamples.length;
	  
	  return {
		ndvi: [meanNdvi],
		dataMask: [1]
	  };
	}
	"""

	payload = {
		"evalscript": evalscript,
		"input": {
			"bounds": {
				"bbox": bbox
			},
			"data": [{
				"type": "sentinel-2-l2a",
				"dataFilter": {
					"timeRange": {
						"from": f"{start_date}T00:00:00Z",
						"to": f"{end_date}T23:59:59Z"
					},
					"maxCloudCoverPercentage": 10
				},
				"processing": {
					"atmosphericCorrection": "NONE"
				}
			}]
		},
		"output": {
			"width": 50,
			"height": 50,
			"responses": [{
				"identifier": "ndvi",
				"format": {
					"type": "image/tiff"
				}
			}]
		}
	}

	resp = requests.post(
		f"https://sh.dataspace.copernicus.eu/api/v1/process",
		headers=HEADERS,
		json=payload
	)

	if resp.status_code != 200:
		print(f"Error {resp.status_code}: {resp.text}")
		return None

	# Save TIFF to temp file and compute mean NDVI
	with tempfile.NamedTemporaryFile(suffix=".tiff") as tmpfile:
		tmpfile.write(resp.content)
		tmpfile.flush()
		with rasterio.open(tmpfile.name) as src:
			arr = src.read(1)
			arr = arr.astype(np.float32)
			arr[arr == src.nodata] = np.nan
			return np.nanmean(arr)

def main():
	powerplant_locations = get_hydropower_data()
	
	# Loop over plants and fetch NDVI
	results = []
	for _, row in powerplant_locations.head(20).iterrows():
		print(f"Processing {row['name']} at ({row['latitude']}, {row['longitude']})")
		ndvi_val = get_ndvi(row['latitude'], row['longitude'])
		results.append({
			"name": row['name'],
			"latitude": row['latitude'],
			"longitude": row['longitude'],
			"ndvi": ndvi_val
		})

	ndvi_df = pd.DataFrame(results)
	print(ndvi_df)
	ndvi_df.to_csv("data/results/hydropower_ndvi.csv", index=False)
	
if __name__ == "__main__":
	main()
