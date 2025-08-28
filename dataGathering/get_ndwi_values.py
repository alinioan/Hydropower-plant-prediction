import json
import pandas as pd
import requests
import numpy as np
import rasterio
import tempfile

from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from locations import get_locations

AUTH_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

with open("../client_info.json", "r") as f:
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

# NDWI extraction function (Processing API)
def get_ndwi(lat, lon, start_date="2024-04-01", end_date="2024-09-30"):
    # Create small bbox around the plant (~100m x 100m)
    buffer_deg = 0.0009  # ~100 m at equator
    bbox = [lon - buffer_deg, lat - buffer_deg, lon + buffer_deg, lat + buffer_deg]

    evalscript = """
    //VERSION=3
    function setup() {
      return {
        input: [{
          bands: ["B03", "B08", "SCL"],
          units: "DN"
        }],
        output: [
          {
            id: "ndwi",
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
            continue; // Skip this sample
        }
        
        // Calculate NDWI using B03 (Green) and B08 (NIR)
        var ndwi = (sample.B03 - sample.B08) / (sample.B03 + sample.B08);
        
        if (!isNaN(ndwi) && isFinite(ndwi)) {
          validSamples.push(ndwi);
        }
      }
      
      if (validSamples.length === 0) {
        return {
          ndwi: [NaN],
          dataMask: [0]
        };
      }
      
      // Temporal mean
      var sum = 0;
      for (var j = 0; j < validSamples.length; j++) {
        sum += validSamples[j];
      }
      var meanNdwi = sum / validSamples.length;
      
      return {
        ndwi: [meanNdwi],
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
                "identifier": "ndwi",
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
        if resp.status_code == 401 and "expired" in resp.text:
            refresh_token()
        return None

    # Save TIFF and compute mean NDWI
    with tempfile.NamedTemporaryFile(suffix=".tiff") as tmpfile:
        tmpfile.write(resp.content)
        tmpfile.flush()
        with rasterio.open(tmpfile.name) as src:
            arr = src.read(1)
            arr = arr.astype(np.float32)
            arr[arr == src.nodata] = np.nan
            return np.nanmean(arr)

def process_location(row, inter_ndwi_df):
    """Fetch NDWI for a single location (thread-safe worker)."""
    if row['name'] in inter_ndwi_df['name'].values:
        return {
            "name": row['name'],
            "latitude": row['latitude'],
            "longitude": row['longitude'],
            "ndwi": inter_ndwi_df.loc[
                (inter_ndwi_df['longitude'] == row['longitude']) &
                (inter_ndwi_df['latitude'] == row['latitude']),
                'ndwi'
            ].values[0]
        }

    ndwi_val = get_ndwi(row['latitude'], row['longitude'])
    if ndwi_val is None:
        ndwi_val = get_ndwi(row['latitude'], row['longitude'])

    return {
        "name": row['name'],
        "latitude": row['latitude'],
        "longitude": row['longitude'],
        "ndwi": ndwi_val
    }

def main():
    locations = get_locations()

    # Load intermediate results if available
    try:
        inter_ndwi_df = pd.read_csv("../data/intermediary/ndwi_intermediate.csv")
        print("Loaded intermediate NDWI results.")
    except FileNotFoundError:
        inter_ndwi_df = pd.DataFrame(columns=["name", "latitude", "longitude", "ndwi"])
        print("No intermediate NDWI results found. Starting fresh.")

    results = []
    max_workers = 8  # adjust depending on your API rate limits

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_location, row, inter_ndwi_df): row['name']
            for _, row in locations.iterrows()
        }

        for i, future in enumerate(tqdm(as_completed(futures), total=len(futures), desc="Fetching NDWI")):
            try:
                res = future.result()
                if res:
                    results.append(res)
            except Exception as e:
                print(f"Error processing {futures[future]}: {e}")

            # Save progress every 75 results
            if (i + 1) % 75 == 0:
                inter_ndwi_df = pd.DataFrame(results)
                inter_ndwi_df.to_csv("../data/intermediary/ndwi_intermediate.csv", index=False)

    ndwi_df = pd.DataFrame(results)
    print(ndwi_df)
    ndwi_df.to_csv("../data/results/hydropower_ndwi.csv", index=False)

if __name__ == "__main__":
    main()
