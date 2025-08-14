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

def get_slope(lat, lon):
    """
    Simplified version using only DEM data without water masking
    Use this if the water masking version has issues
    """
    # Create bbox around the plant (~300m x 300m)
    buffer_deg = 0.0045  # ~500 m at equator
    bbox = [lon - buffer_deg, lat - buffer_deg, lon + buffer_deg, lat + buffer_deg]
    
    evalscript = """
    //VERSION=3
    function setup() {
        return {
            input: ["DEM"],
            output: {
                bands: 1,
                sampleType: "FLOAT32"
            }
        };
    }
    
    function evaluatePixel(sample) {
        return [sample.DEM];
    }
    """
    
    payload = {
        "evalscript": evalscript,
        "input": {
            "bounds": {"bbox": bbox},
            "data": [{
                "type": "dem",
                "dataFilter": {
                    "demInstance": "COPERNICUS_30"
                }
            }]
        },
        "output": {
            "width": 50,
            "height": 50,
            "responses": [{
                "identifier": "default",
                "format": {"type": "image/tiff"}
            }]
        }
    }
    
    resp = requests.post(
        "https://sh.dataspace.copernicus.eu/api/v1/process",
        headers=HEADERS,
        json=payload
    )
    
    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text}")
        return None
    
    # Calculate slope from DEM
    with tempfile.NamedTemporaryFile(suffix=".tiff") as tmpfile:
        tmpfile.write(resp.content)
        tmpfile.flush()
        
        with rasterio.open(tmpfile.name) as src:
            dem_array = src.read(1)
            dem_array = dem_array.astype(np.float32)
            
            # Handle nodata values
            if src.nodata is not None:
                dem_array[dem_array == src.nodata] = np.nan
            
            # Calculate slope
            pixel_size = 30  # 30m resolution
            dy, dx = np.gradient(dem_array, pixel_size)
            slope_radians = np.arctan(np.sqrt(dx**2 + dy**2))
            slope_degrees = np.degrees(slope_radians)
            
            # Return mean slope
            return np.nanmean(slope_degrees)

def main():
    powerplant_locations = get_hydropower_data()
    
    # Loop over plants and fetch slope
    results = []
    for _, row in powerplant_locations.head(20).iterrows():  # Start with 10 for testing
        print(f"Processing {row['name']} at ({row['latitude']}, {row['longitude']})")
        
        slope_val = get_slope(row['latitude'], row['longitude'])

        results.append({
            "name": row['name'],
            "latitude": row['latitude'],
            "longitude": row['longitude'],
            "slope_degrees": slope_val
        })
    
    slope_df = pd.DataFrame(results)
    print(slope_df)
    slope_df.to_csv("data/results/hydropower_slopes.csv", index=False)

if __name__ == "__main__":
    main()