import os
import requests
import json
import tempfile
import pandas as pd
import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_bounds
import numpy as np
from tqdm import tqdm
from dataGathering.locations import get_locations
from concurrent.futures import ThreadPoolExecutor, as_completed

BUFFER_DEG = 0.0025 # ~250m buffer
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


def get_indices_patch(lat, lon, start_date="2024-04-01", end_date="2024-10-01"):
    bbox = [lon - BUFFER_DEG, lat - BUFFER_DEG, lon + BUFFER_DEG, lat + BUFFER_DEG]

    evalscript = """
        //VERSION=3
        function setup() {
            return {
                input: [
                    {
                        bands: ["B03", "B04", "B08", "B11", "SCL"],
                        units: "DN"
                    }
                ],
                output: {
                    bands: 4,  // NDVI, NDWI, NDBI, MNDWI
                    sampleType: "FLOAT32"
                },
                mosaicking: "ORBIT"
            };
        }

        function evaluatePixel(samples) {
            let ndvi_vals = [];
            let ndwi_vals = [];
            let ndbi_vals = [];
            let mndwi_vals = [];

            for (var i = 0; i < samples.length; i++) {
                let s = samples[i];

                // Exclude invalid classes: water(6), cloud shadow(3), vegetation shadow(8),
                // cloud(9,10), cirrus(11). Same as your old script.
                if (s.SCL == 6 || s.SCL == 3 || s.SCL == 8 ||
                    s.SCL == 9 || s.SCL == 10 || s.SCL == 11) {
                    continue;
                }

                // Vegetation index (NDVI)
                let ndvi = (s.B08 - s.B04) / (s.B08 + s.B04);

                // Water index (NDWI)
                let ndwi = (s.B03 - s.B08) / (s.B03 + s.B08);

                // Built-up index (NDBI)
                let ndbi = (s.B11 - s.B08) / (s.B11 + s.B08);

                // MNDWI = (Green - SWIR1) / (Green + SWIR1)
                let mndwi = (s.B03 - s.B11) / (s.B03 + s.B11);

                if (isFinite(ndvi)) ndvi_vals.push(ndvi);
                if (isFinite(ndwi)) ndwi_vals.push(ndwi);
                if (isFinite(ndbi)) ndbi_vals.push(ndbi);
                if (isFinite(mndwi)) mndwi_vals.push(mndwi);
            }

            // Mean helper
            let mean = arr => arr.length > 0 ? arr.reduce((a, b) => a + b, 0) / arr.length : NaN;

            return [
                mean(ndvi_vals),
                mean(ndwi_vals),
                mean(ndbi_vals)
            ];
        }
    """

    payload = {
        "evalscript": evalscript,
        "input": {
            "bounds": {"bbox": bbox},
            "data": [{
                "type":"sentinel-2-l2a",
                "dataFilter":{
                    "timeRange":{"from":f"{start_date}T00:00:00Z","to":f"{end_date}T23:59:59Z"},
                    "maxCloudCoverPercentage":10
                }
            }]
        },
        "output":{
            "width":50,
            "height":50,
            "responses":[{"identifier":"default","format":{"type":"image/tiff"}}]
        }
    }

    resp = requests.post("https://sh.dataspace.copernicus.eu/api/v1/process", headers=HEADERS, json=payload)
    if resp.status_code != 200:
        print(resp.status_code, resp.text)
        refresh_token()
        return None

    with tempfile.NamedTemporaryFile(suffix=".tiff") as tmpfile:
        tmpfile.write(resp.content)
        tmpfile.flush()
        with rasterio.open(tmpfile.name) as src:
            arr = src.read().astype(np.float32)
            arr[arr == src.nodata] = np.nan
            return arr  # shape = (4, H, W)


def get_dem_slope_patch(lat, lon, width=50, height=50):
    bbox = [lon-BUFFER_DEG, lat-BUFFER_DEG, lon+BUFFER_DEG, lat+BUFFER_DEG]

    payload = {
        "evalscript":"//VERSION=3\nfunction setup(){return {input:[{bands:['DEM']}],output:{bands:1,sampleType:'FLOAT32'}}} function evaluatePixel(s){return [s.DEM]}",
        "input":{"bounds":{"bbox":bbox},"data":[{"type":"dem"}]},
        "output":{"width":width,"height":height,"responses":[{"identifier":"default","format":{"type":"image/tiff"}}]}
    }

    resp = requests.post("https://sh.dataspace.copernicus.eu/api/v1/process", headers=HEADERS, json=payload)
    if resp.status_code != 200:
        print(resp.status_code, resp.text)
        refresh_token()
        return None

    with tempfile.NamedTemporaryFile(suffix=".tiff") as tmpfile:
        tmpfile.write(resp.content)
        tmpfile.flush()
        with rasterio.open(tmpfile.name) as src:
            dem = src.read(1).astype(np.float32)
            dem[dem == src.nodata] = np.nan
            # Compute slope in degrees
            dy, dx = np.gradient(dem, 30)  # assuming 30m pixel size
            slope = np.degrees(np.arctan(np.sqrt(dx**2 + dy**2)))
            return dem, slope  # both shape = (H, W)

def save_feature_cube(filename, feature_cube, bbox, width=50, height=50, crs="EPSG:4326"):
    """
    Save a multi-band feature cube to GeoTIFF
    feature_cube: np.array of shape (H, W, C)
    bbox: [min_lon, min_lat, max_lon, max_lat]
    """
    # Rasterio expects (bands, height, width)
    bands_first = np.transpose(feature_cube, (2, 0, 1))

    transform = from_bounds(*bbox, width=width, height=height)

    with rasterio.open(
        filename,
        'w',
        driver='GTiff',
        height=height,
        width=width,
        count=bands_first.shape[0],
        dtype=bands_first.dtype,
        crs=crs,
        transform=transform
    ) as dst:
        dst.write(bands_first)
    print(f"Saved feature cube to {filename}")

def create_label_mask(row, height=50, width=50):
    """
    Create a full patch label mask:
    1 = good location (power plant)
    0 = bad location (random location)
    """
    if pd.notna(row['name']) and row['name'] != '':
        # Positive patch
        label_value = 1
    else:
        # Negative patch
        label_value = 0

    # Full patch mask
    label_mask = np.full((height, width), label_value, dtype=np.uint8)
    return label_mask

def process_location(idx, row, discharge_df, precipitation_df):
    # check if file already exists
    filename = f"data/feature_cubes/feature_cube_{idx}.tif"
    if os.path.exists(filename):
        print(f"File {filename} already exists. Skipping...")
        return
    lat = row['latitude']
    lon = row['longitude']

    indices = get_indices_patch(lat, lon)
    if indices is None:
        print(f"Retrying location {idx} at {lat}, {lon} due to error.")
        indices = get_indices_patch(lat, lon)
        if indices is None:
            print(f"Skipping location {idx} at {lat}, {lon} due to repeated errors.")
            return

    dem_slope = get_dem_slope_patch(lat, lon)
    if dem_slope is None:
        print(f"Retrying DEM/Slope for location {idx} at {lat}, {lon} due to error.")
        dem_slope = get_dem_slope_patch(lat, lon)
        if dem_slope is None:
            print(f"Skipping location {idx} at {lat}, {lon} due to repeated errors.")
            return
    dem, slope = dem_slope

    feature_cube = np.stack([indices[0], indices[1], indices[2], indices[3], dem, slope], axis=-1)

    # Discharge & precipitation
    discharge_mask = np.full(
        feature_cube.shape[:2],
        discharge_df.loc[(discharge_df['latitude'] == lat) & (discharge_df['longitude'] == lon), 'discharge'].values[0]
        if ((discharge_df['latitude'] == lat) & (discharge_df['longitude'] == lon)).any() else np.nan,
        dtype=np.float32
    )

    precipitation_mask = np.full(
        feature_cube.shape[:2],
        precipitation_df.loc[(precipitation_df['latitude'] == lat) & (precipitation_df['longitude'] == lon), 'precipitation'].values[0]
        if ((precipitation_df['latitude'] == lat) & (precipitation_df['longitude'] == lon)).any() else np.nan,
        dtype=np.float32
    )

    label_mask = create_label_mask(row)

    final_feature_cube = np.concatenate([feature_cube,
                                         discharge_mask[:, :, np.newaxis],
                                         precipitation_mask[:, :, np.newaxis],
                                         label_mask[:, :, np.newaxis]], axis=-1)

    bbox = [lon - BUFFER_DEG, lat - BUFFER_DEG, lon + BUFFER_DEG, lat + BUFFER_DEG]
    filename = f"data/feature_cubes/feature_cube_{idx}.tif"
    save_feature_cube(filename, final_feature_cube, bbox)

def main():
    locations = get_locations()

    discharge_df = pd.read_csv("data/results/average_discharge.csv")
    precipitation_df = pd.read_csv("data/results/hydropower_precipitation.csv")

    for df in [locations, discharge_df, precipitation_df]:
        df["latitude"] = df["latitude"].round(6)
        df["longitude"] = df["longitude"].round(6)

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(process_location, idx, row, discharge_df, precipitation_df): idx
                   for idx, row in locations.iterrows()}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching patches in parallel"):
            try:
                future.result()
            except Exception as e:
                idx = futures[future]
                print(f"Error processing location {idx}: {e}")


if __name__ == "__main__":
    main()