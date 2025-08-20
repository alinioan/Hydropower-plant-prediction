import json
import pandas as pd
import requests
import numpy as np
import rasterio
import tempfile
import time
from tqdm import tqdm
from locations import get_hydropower_locations, get_locations

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

def refresh_token():
    """
    Refresh the access token using the refresh token
    """
    global ACCESS_TOKEN, HEADERS
    
    token_response = requests.post(AUTH_URL, data=AUTH_DATA)
    print(f"Token response: {token_response.status_code} {token_response.text}")
    ACCESS_TOKEN = token_response.json()["access_token"]
    HEADERS = {
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }

    if token_response.status_code == 200:
        new_token = token_response.json()
        ACCESS_TOKEN = new_token["access_token"]
        HEADERS["Authorization"] = f"Bearer {ACCESS_TOKEN}"
        print("Token refreshed successfully.")
    else:
        print(f"Failed to refresh token: {token_response.status_code} {token_response.text}")

def get_slope(lat, lon):
    """
    Simplified version using only DEM data without water masking
    Use this if the water masking version has issues
    """
    # Create bbox around the plant (~500m x 500m)
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
        if resp.status_code == 401 and "expired" in resp.text:
            refresh_token()
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
    locations = get_locations()
    
    # Load intermediate results if available
    try:
        inter_slope_df = pd.read_csv("data/intermediary/slope_intermediate.csv")
        print("Loaded intermediate slope results.")
    except FileNotFoundError:
        inter_slope_df = pd.DataFrame(columns=["name", "latitude", "longitude", "slope_degrees"])
        print("No intermediate slope results found. Starting fresh.")

    # Loop over plants and fetch slope
    results = []
    for _, row in tqdm(locations.iterrows(), total=len(locations), desc="Fetching slope values"):
        mask = (
            (inter_slope_df['longitude'] == row['longitude']) &
            (inter_slope_df['latitude'] == row['latitude'])
        )
        if mask.any():
            print(f"Skipping {row['name']} as it already exists in intermediate results.")
            results.append({
                "name": row['name'],
                "latitude": row['latitude'],
                "longitude": row['longitude'],
                "slope_degrees": inter_slope_df.loc[(inter_slope_df['longitude'] == row['longitude'])
                                                    & (inter_slope_df['latitude'] == row['latitude'])
                                                    , 'slope_degrees'].values[0]
            })
            continue

        print(f"Processing {row['name']} at ({row['latitude']}, {row['longitude']})")
        
        slope_val = get_slope(row['latitude'], row['longitude'])

        if slope_val is None:
            print(f"Failed to fetch slope data for {row['name']}. Retrying...")
            slope_val = get_slope(row['latitude'], row['longitude'])

        results.append({
            "name": row['name'],
            "latitude": row['latitude'],
            "longitude": row['longitude'],
            "slope_degrees": slope_val
        })

        if len(results) % 200 == 0 and len(results) > 0:
            inter_slope_df = pd.DataFrame(results)
            inter_slope_df.to_csv("data/intermediary/slope_intermediate.csv", index=False)

    slope_df = pd.DataFrame(results)
    print(slope_df)
    slope_df.to_csv("data/results/hydropower_slopes.csv", index=False)

if __name__ == "__main__":
    main()