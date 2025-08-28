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

# NDBI extraction function (Processing API)
def get_ndbi(lat, lon, start_date="2024-04-01", end_date="2024-09-30"):
    # Create small bbox around the plant (~100m x 100m)
    buffer_deg = 0.0009  # ~100 m at equator
    bbox = [lon - buffer_deg, lat - buffer_deg, lon + buffer_deg, lat + buffer_deg]

    evalscript = """
    //VERSION=3
    function setup() {
      return {
        input: [{
          bands: ["B08", "B11", "SCL"],
          units: "DN"
        }],
        output: [
          {
            id: "ndbi",
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
        
        // Calculate NDBI using B11 (SWIR) and B08 (NIR)
        var ndbi = (sample.B11 - sample.B08) / (sample.B11 + sample.B08);
        
        // Filter out invalid NDBI values
        if (!isNaN(ndbi) && isFinite(ndbi)) {
          validSamples.push(ndbi);
        }
      }
      
      if (validSamples.length === 0) {
        return {
          ndbi: [NaN],
          dataMask: [0]
        };
      }
      
      // Calculate temporal mean
      var sum = 0;
      for (var j = 0; j < validSamples.length; j++) {
        sum += validSamples[j];
      }
      var meanNdbi = sum / validSamples.length;
      
      return {
        ndbi: [meanNdbi],
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
                "identifier": "ndbi",
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

    # Save TIFF to temp file and compute mean NDBI
    with tempfile.NamedTemporaryFile(suffix=".tiff") as tmpfile:
        tmpfile.write(resp.content)
        tmpfile.flush()
        with rasterio.open(tmpfile.name) as src:
            arr = src.read(1)
            arr = arr.astype(np.float32)
            arr[arr == src.nodata] = np.nan
            return np.nanmean(arr)

def process_location(row, inter_ndbi_df):
    """Fetch NDBI for a single location (thread-safe worker)."""
    if row['name'] in inter_ndbi_df['name'].values:
        return {
            "name": row['name'],
            "latitude": row['latitude'],
            "longitude": row['longitude'],
            "ndbi": inter_ndbi_df.loc[
                (inter_ndbi_df['longitude'] == row['longitude']) &
                (inter_ndbi_df['latitude'] == row['latitude']),
                'ndbi'
            ].values[0]
        }

    ndbi_val = get_ndbi(row['latitude'], row['longitude'])
    if ndbi_val is None:
        ndbi_val = get_ndbi(row['latitude'], row['longitude'])

    return {
        "name": row['name'],
        "latitude": row['latitude'],
        "longitude": row['longitude'],
        "ndbi": ndbi_val
    }

def main():
    locations = get_locations()

    # Load intermediate results if available
    try:
        inter_ndbi_df = pd.read_csv("../data/intermediary/ndbi_intermediate.csv")
        print("Loaded intermediate NDBI results.")
    except FileNotFoundError:
        inter_ndbi_df = pd.DataFrame(columns=["name", "latitude", "longitude", "ndbi"])
        print("No intermediate NDBI results found. Starting fresh.")

    results = []
    max_workers = 8  # adjust depending on your API rate limits

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_location, row, inter_ndbi_df): row['name']
            for _, row in locations.iterrows()
        }

        # tqdm progress bar for completed futures
        for i, future in enumerate(tqdm(as_completed(futures), total=len(futures), desc="Fetching NDBI")):
            try:
                res = future.result()
                if res:
                    results.append(res)
            except Exception as e:
                print(f"Error processing {futures[future]}: {e}")

            # Save progress every 75 results
            if (i + 1) % 75 == 0:
                inter_ndbi_df = pd.DataFrame(results)
                inter_ndbi_df.to_csv("../data/intermediary/ndbi_intermediate.csv", index=False)

    ndbi_df = pd.DataFrame(results)
    print(ndbi_df)
    ndbi_df.to_csv("../data/results/hydropower_ndbi.csv", index=False)

if __name__ == "__main__":
    main()