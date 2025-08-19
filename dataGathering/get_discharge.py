import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from locations import get_hydropower_locations, get_locations

def get_average_discharge(locations_df):
    """
    Get average discharge values for rivers at specified locations.
    """
    rivers = gpd.read_file("data/HydroRIVERS_v10_eu_shp/HydroRIVERS_v10_eu.shp").to_crs(epsg=3857)
    
    locations_gdf = gpd.GeoDataFrame(
        locations_df,
        geometry=[Point(xy) for xy in zip(locations_df.longitude, locations_df.latitude)],
        crs="EPSG:4326"
    ).to_crs(epsg=3857)
    print(f"Found {len(locations_gdf)} locations.")

    # Spatial join: nearest river for each location
    locations_with_rivers = gpd.sjoin_nearest(
        locations_gdf, rivers[["geometry", "DIS_AV_CMS"]],
        how="left", distance_col="dist_to_river"
    )

    locations_with_rivers = locations_with_rivers.to_crs(epsg=4326)

    locations_with_rivers["longitude"] = locations_with_rivers.geometry.x.round(6)
    locations_with_rivers["latitude"] = locations_with_rivers.geometry.y.round(6)

    df = locations_with_rivers[["name", "latitude", "longitude", "DIS_AV_CMS"]].rename(
        columns={"DIS_AV_CMS": "discharge"}
    )

    df = df.drop_duplicates(subset=["latitude", "longitude"])
    return df

def main():
    locations = get_locations()
    discharge_df = get_average_discharge(locations)
    discharge_df.to_csv("data/results/average_discharge.csv", index=False)
    print("Average discharge values saved to data/results/average_discharge.csv")

if __name__ == "__main__":
    main()