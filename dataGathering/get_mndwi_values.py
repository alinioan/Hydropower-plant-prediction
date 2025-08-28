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

# MNDWI extraction function
def get_mndwi(lat, lon, start_date="2024-04-01", end_date="2024-09-30"):
    buffer_deg = 0.0009  # ~100 m
    bbox = [lon - buffer_deg, lat - buffer_deg, lon + buffer_deg, lat + buffer_deg]

    evalscript = """
    //VERSION=3
    function setup() {
      return {
        input: [{
          bands: ["B03", "B11", "SCL"],
          units: "DN"
        }],
        output: [
          {
            id: "mndwi",
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
      
      for (var i = 0; i < samples.length; i++) {
        var sample = samples[i];
        if (sample.SCL == 6 || sample.SCL == 3 || sample.SCL == 8 ||
            sample.SCL == 9 || sample.SCL == 10 || sample.SCL == 11) {
            continue;
        }
        
        // MNDWI = (Green - SWIR1) / (Green + SWIR1)
        var mndwi = (sample.B03 - sample.B11) / (sample.B03 + sample.B11);
        
        if (!isNaN(mndwi) && isFinite(mndwi)) {
          validSamples.push(mndwi);
        }
      }
      
      if (validSamples.length === 0) {
        return {
          mndwi: [NaN],
          dataMask: [0]
        };
      }
      
      var sum = 0;
      for (var j = 0; j < validSamples.length; j++) {
        sum += validSamples[j];
      }
      var meanMndwi = sum / validSamples.length;
      
      return {
        mndwi: [meanMndwi],
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
                "identifier": "mndwi",
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

    with tempfile.NamedTemporaryFile(suffix=".tiff") as tmpfile:
        tmpfile.write(resp.content)
        tmpfile.flush()
        with rasterio.open(tmpfile.name) as src:
            arr = src.read(1).astype(np.float32)
            arr[arr == src.nodata] = np.nan
            return np.nanmean(arr)

def process_location(row, inter_mndwi_df):
    """Fetch MNDWI for a single location (thread-safe worker)."""
    if row['name'] in inter_mndwi_df['name'].values:
        return {
            "name": row['name'],
            "latitude": row['latitude'],
            "longitude": row['longitude'],
            "mndwi": inter_mndwi_df.loc[
                (inter_mndwi_df['longitude'] == row['longitude']) &
                (inter_mndwi_df['latitude'] == row['latitude']),
                'mndwi'
            ].values[0]
        }

    mndwi_val = get_mndwi(row['latitude'], row['longitude'])
    if mndwi_val is None:
        mndwi_val = get_mndwi(row['latitude'], row['longitude'])

    return {
        "name": row['name'],
        "latitude": row['latitude'],
        "longitude": row['longitude'],
        "mndwi": mndwi_val
    }

def main():
    locations = get_locations()

    try:
        inter_mndwi_df = pd.read_csv("../data/intermediary/mndwi_intermediate.csv")
        print("Loaded intermediate MNDWI results.")
    except FileNotFoundError:
        inter_mndwi_df = pd.DataFrame(columns=["name", "latitude", "longitude", "mndwi"])
        print("No intermediate MNDWI results found. Starting fresh.")

    results = []
    max_workers = 8

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_location, row, inter_mndwi_df): row['name']
            for _, row in locations.iterrows()
        }

        for i, future in enumerate(tqdm(as_completed(futures), total=len(futures), desc="Fetching MNDWI")):
            try:
                res = future.result()
                if res:
                    results.append(res)
            except Exception as e:
                print(f"Error processing {futures[future]}: {e}")

            if (i + 1) % 75 == 0:
                inter_mndwi_df = pd.DataFrame(results)
                inter_mndwi_df.to_csv("../data/intermediary/mndwi_intermediate.csv", index=False)

    mndwi_df = pd.DataFrame(results)
    print(mndwi_df)
    mndwi_df.to_csv("../data/results/hydropower_mndwi.csv", index=False)

if __name__ == "__main__":
    main()
