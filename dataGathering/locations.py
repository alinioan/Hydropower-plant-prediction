import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

def get_hydropower_locations():
    """
    Fetches hydropower plant data from the global power plant database.
    Filters for European countries and returns a DataFrame with plant names and locations.
    """
    df = pd.read_csv('../data/GloHydroRes_vs1.csv', low_memory=False)
    df = df.rename(columns={
        'plant_lat': 'latitude',
        'plant_lon': 'longitude',
    })

    european_countries = [
        "Albania", "Andorra", "Armenia", "Austria", "Azerbaijan", "Belarus", "Belgium", "Bosnia and Herzegovina",
        "Bulgaria", "Croatia", "Cyprus", "Czech Republic", "Czechia", "Denmark", "Estonia", "Finland", "France",
        "Georgia", "Germany", "Greece", "Hungary", "Iceland", "Ireland", "Italy", "Kosovo", "Latvia", "Liechtenstein",
        "Lithuania", "Luxembourg", "Malta", "Moldova", "Monaco", "Montenegro", "Netherlands", "North Macedonia",
        "Norway", "Poland", "Portugal", "Romania", "Russia", "San Marino", "Serbia", "Slovakia", "Slovenia", "Spain",
        "Sweden", "Switzerland", "Turkey", "Ukraine", "United Kingdom", "Vatican City"
    ]

    hydro_europe_df = df[df['country'].isin(european_countries)]

    powerplant_locations = hydro_europe_df[['name', 'latitude', 'longitude']]
    return powerplant_locations

def get_random_river_locations(sample_size=9000, random_state=42):
    """
    Generates random locations alongside rivers, outside hydropower plant exclusion zones.
    """
    rivers = gpd.read_file("data/HydroRIVERS_v10_eu_shp/HydroRIVERS_v10_eu.shp")  # HydroSHEDS Europe shapefile
    
    # Filter rivers with average discharge >= 2 cubic meters per second
    rivers_big = rivers[rivers["DIS_AV_CMS"] >= 2]
    rivers = rivers_big

    rivers = rivers.to_crs(epsg=3857)  # Project for distance calculations
    powerplant_locations = get_hydropower_locations()

    # Load hydropower plants
    plants_gdf = gpd.GeoDataFrame(
        powerplant_locations,
        geometry=[Point(xy) for xy in zip(powerplant_locations.longitude, powerplant_locations.latitude)],
        crs="EPSG:4326"
    ).to_crs(epsg=3857)

    # Create buffer around plants (30 km)
    plants_buffer = plants_gdf.buffer(30_000)

    # Merge buffers into one exclusion area
    exclusion_zone = plants_buffer.union_all()

    # Keep only rivers outside exclusion zone
    rivers_far = rivers[~rivers.intersects(exclusion_zone)]
    print(f"Found {len(rivers_far)} river segments outside exclusion zone.")
    rivers_far = rivers_far.to_crs(epsg=4326)

    sampled_rivers = rivers_far.sample(n=sample_size, random_state=random_state)

    # uncomment the next line to save the sampled rivers to a shapefile
    # sampled_rivers.to_file("out/random_negative_rivers.shp")

    sampled_rivers["first_coord"] = sampled_rivers.geometry.apply(
        lambda geom: list(geom.coords)[0] if geom.geom_type == "LineString" 
        else list(list(geom.geoms)[0].coords)[0]
    )
    
    sampled_rivers[["longitude", "latitude"]] = pd.DataFrame(
        sampled_rivers["first_coord"].tolist(),
        index=sampled_rivers.index
    )
    return sampled_rivers[["latitude", "longitude"]]


def get_locations():
    powerplant_locations = get_hydropower_locations()
    random_rivers = get_random_river_locations()
    locations = pd.concat([powerplant_locations, random_rivers], ignore_index=True)

    print(locations)
    return locations