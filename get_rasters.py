import tempfile
import pandas as pd
from rasterio.transform import from_bounds
import numpy as np
import rasterio
import requests
import json
import matplotlib.pyplot as plt
from tqdm import tqdm
from dataGathering.locations import get_locations

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
    buffer_deg = 0.0045  # ~500m buffer
    bbox = [lon - buffer_deg, lat - buffer_deg, lon + buffer_deg, lat + buffer_deg]

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
    buffer_deg = 0.0045
    bbox = [lon-buffer_deg, lat-buffer_deg, lon+buffer_deg, lat+buffer_deg]

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

def visualize_feature_cube(filename):
    with rasterio.open(filename) as src:
        # Read all bands
        data = src.read()  # shape = (bands, height, width)
        band_names = ["NDVI", "NDWI", "NDBI", "MNDWI", "DEM", "Slope", "Label"]
        
        # Plot each band
        for i in range(data.shape[0]):
            plt.figure(figsize=(5,5))
            plt.title(f"{band_names[i]} - {filename}")
            plt.imshow(data[i], cmap='viridis' if i!=6 else 'gray')
            plt.colorbar()
            plt.show()

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

def main():
    locations = get_locations().head(5)

    for _, row in tqdm(locations.iterrows(), total=len(locations), desc="Fetching patches"):
        lat = row['latitude']
        lon = row['longitude']
        indices = get_indices_patch(lat, lon)
        dem, slope = get_dem_slope_patch(lat, lon)

        # final array: (H, W, C=6)
        feature_cube = np.stack([indices[0], indices[1], indices[2], indices[3], dem, slope], axis=-1)

        # Create label mask: 1 if power plant (row['name'] not empty), 0 if random location
        label_value = 1 if pd.notna(row['name']) and row['name'] != '' else 0
        label_mask = np.full(feature_cube.shape[:2], label_value, dtype=np.uint8)

        # Append label as the last channel
        feature_cube_with_label = np.concatenate([feature_cube, label_mask[:, :, np.newaxis]], axis=-1)

        save_feature_cube(
            filename=f"data/feature_cubes/feature_cube_{lat:.6f}_{lon:.6f}.tif",
            feature_cube=feature_cube_with_label,
            bbox=[lon - 0.0045, lat - 0.0045, lon + 0.0045, lat + 0.0045]
        )


if __name__ == "__main__":
    main()