import os
import json
import requests
import getpass
import numpy as np
import rasterio
import tempfile
import pandas as pd
from locations import get_hydropower_locations

CLIENT_ID = "cdse-public"
AUTH_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
API_URL = "https://sh.dataspace.copernicus.eu/api/v1/process"

def get_tokens(username, password):
    print("Authenticating with Copernicus Data Space...")
    response = requests.post(
        AUTH_URL,
        data={
            "client_id": CLIENT_ID,
            "username": username,
            "password": password,
            "grant_type": "password"
        }
    )
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("Failed to retrieve tokens:", response.status_code, response.text)

def refresh_access_token(refresh_token):
    print("Refreshing access token...")
    response = requests.post(
        AUTH_URL,
        data={
            "client_id": CLIENT_ID,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }
    )
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        raise Exception("Failed to refresh access token:", response.status_code, response.text)


def get_ndbi(lat, lon, start_date, end_date, session):
    buffer_deg = 0.0009
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
          }
        ],
        mosaicking: "ORBIT"
      };
    }

    function evaluatePixel(samples) {
      var validSamples = [];
      
      for (var i = 0; i < samples.length; i++) {
        var sample = samples[i];
        if ([3, 8, 9, 10, 11].includes(sample.SCL)) {
            continue;
        }
        
        let ndbi = (sample.B11 - sample.B08) / (sample.B11 + sample.B08);
        
        if (!isNaN(ndbi) && isFinite(ndbi)) {
          validSamples.push(ndbi);
        }
      }
      
      if (validSamples.length === 0) {
        return { ndbi: [NaN] };
      }
      
      var sum = 0;
      for (var j = 0; j < validSamples.length; j++) {
        sum += validSamples[j];
      }
      var meanNdbi = sum / validSamples.length;
      
      return { ndbi: [meanNdbi] };
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

    resp = session.post(API_URL, json=payload)
        
    if resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text}")
        return None

    with tempfile.NamedTemporaryFile(suffix=".tiff") as tmpfile:
        tmpfile.write(resp.content)
        tmpfile.flush()
        with rasterio.open(tmpfile.name) as src:
            arr = src.read(1)
            arr = arr.astype(np.float32)
            arr[arr == src.nodata] = np.nan
            return np.nanmean(arr)


def main():
    username = input("Enter your email:")
    password = getpass.getpass("Enter your password:")
    
    try:
        token_data = get_tokens(username, password)
        access_token = token_data["access_token"]
        refresh_token = token_data["refresh_token"]
        
        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {access_token}"})
        
    except Exception as e:
        print(f"Authentication failed: {e}")
        return

    powerplant_locations = get_hydropower_locations()
    
    results = []
    for _, row in powerplant_locations.head(10).iterrows():
        print(f"Processing {row['name']} at ({row['latitude']}, {row['longitude']})")
        try:
            ndbi_val = get_ndbi(row['latitude'], row['longitude'], start_date="2024-04-01", end_date="2024-09-30", session=session)
        except requests.exceptions.RequestException as e:
            if "401" in str(e):
                print("Access token expired. Refreshing...")
                try:
                    access_token = refresh_access_token(refresh_token)
                    session.headers.update({"Authorization": f"Bearer {access_token}"})
                    ndbi_val = get_ndbi(row['latitude'], row['longitude'], start_date="2024-04-01", end_date="2024-09-30", session=session)
                except Exception as refresh_e:
                    print(f"Failed to refresh token: {refresh_e}")
                    ndbi_val = None
            else:
                print(f"An error occurred: {e}")
                ndbi_val = None

        results.append({
            "name": row['name'],
            "latitude": row['latitude'],
            "longitude": row['longitude'],
            "ndbi": ndbi_val
        })

    ndbi_df = pd.DataFrame(results)
    print(ndbi_df)
    ndbi_df.to_csv("../data/results/hydropower_ndbi.csv", index=False)
    
if __name__ == "__main__":
    main()