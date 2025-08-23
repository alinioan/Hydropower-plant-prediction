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

def refresh_tokens(refresh_token):
    print("Refreshing tokens...")
    response = requests.post(
        AUTH_URL,
        data={
            "client_id": CLIENT_ID,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }
    )
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("Failed to refresh tokens:", response.status_code, response.text)


def get_ndwi(lat, lon, start_date, end_date, session):
    buffer_deg = 0.0009
    bbox = [lon - buffer_deg, lat - buffer_deg, lon + buffer_deg, lat + buffer_deg]
    
    evalscript = """
    //VERSION=3
    function setup() {
      return {
        input: [{
          bands: ["B08", "B03", "SCL"],
          units: "DN"
        }],
        output: [
          {
            id: "ndwi",
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
        
        let ndwi = (sample.B03 - sample.B08) / (sample.B03 + sample.B08);
        
        if (!isNaN(ndwi) && isFinite(ndwi)) {
          validSamples.push(ndwi);
        }
      }
      
      if (validSamples.length === 0) {
        return { ndwi: [NaN] };
      }
      
      var sum = 0;
      for (var j = 0; j < validSamples.length; j++) {
        sum += validSamples[j];
      }
      var meanNdwi = sum / validSamples.length;
      
      return { ndwi: [meanNdwi] };
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
                "identifier": "ndwi",
                "format": {
                    "type": "image/tiff"
                }
            }]
        }
    }

    resp = session.post(API_URL, json=payload)

    if resp.status_code == 401:  
        return None, 401
    elif resp.status_code != 200:
        print(f"Error {resp.status_code}: {resp.text}")
        return None, resp.status_code

    with tempfile.NamedTemporaryFile(suffix=".tiff") as tmpfile:
        tmpfile.write(resp.content)
        tmpfile.flush()
        with rasterio.open(tmpfile.name) as src:
            arr = src.read(1).astype(np.float32)
            arr[arr == src.nodata] = np.nan
            return np.nanmean(arr), 200


def main():
    username = "cristicristi532@gmail.com"
    password = "Nak$4MpK#H8YShY"
    
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
    results_file = "../data/results/hydropower_ndwi.csv"
    
    try:
        existing_df = pd.read_csv(results_file)
        processed_names = set(existing_df['name'])
        print(f"Found {len(processed_names)} existing records. They will be skipped.")
    except (FileNotFoundError, pd.errors.EmptyDataError):
        processed_names = set()
        pd.DataFrame(columns=['name', 'latitude', 'longitude', 'ndwi']).to_csv(results_file, index=False)

    
    for _, row in powerplant_locations.iterrows():
        
        if row["name"] in processed_names:
            continue
                
        print(f"Processing {row['name']} at ({row['latitude']}, {row['longitude']})")

        ndwi_val, status = get_ndwi(row['latitude'], row['longitude'],
                                start_date="2024-04-01", end_date="2024-09-30",
                                session=session)

        if status == 401:
            print("Access token expired. Refreshing...")
            try:
                new_token_data = refresh_tokens(refresh_token)
                access_token = new_token_data["access_token"]
                refresh_token = new_token_data["refresh_token"]
        
                print("Tokens refreshed successfully.")

                session.headers.update({"Authorization": f"Bearer {access_token}"})

                ndwi_val, status = get_ndwi(row['latitude'], row['longitude'],
                                        start_date="2024-04-01", end_date="2024-09-30",
                                        session=session)
            except Exception as e:
                print(f"Fatal error during token refresh: {e}")
                print("Exiting script. Please re-authenticate.")
                main()
                break
        if status == 200 and ndwi_val is not None:
            new_result_df = pd.DataFrame([{
                "name": row['name'],
                "latitude": row['latitude'],
                "longitude": row['longitude'],
                "ndwi": ndwi_val
            }])
            new_result_df.to_csv(results_file, mode='a', header=False, index=False)
            
            processed_names.add(row['name'])

    print("It Done")
    
if __name__ == "__main__":
    main()