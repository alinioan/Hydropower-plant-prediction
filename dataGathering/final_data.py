import pandas as pd

def main():
    """
    Main function to execute the script.
    """
    ndvi_df = pd.read_csv('data/results/hydropower_ndvi.csv', low_memory=False)
    discharge_df = pd.read_csv('data/results/average_discharge.csv', low_memory=False)
    ndwi_df = pd.read_csv('data/results/hydropower_ndwi.csv', low_memory=False)
    precipitation_df = pd.read_csv('data/results/hydropower_precipitation.csv', low_memory=False)
    slope_df = pd.read_csv('data/results/hydropower_slopes.csv', low_memory=False)

    print(f"NDVI DataFrame shape: {ndvi_df.shape}")
    print(f"Discharge DataFrame shape: {discharge_df.shape}")
    print(f"NDWI DataFrame shape: {ndwi_df.shape}")
    print(f"Precipitation DataFrame shape: {precipitation_df.shape}")
    print(f"Slope DataFrame shape: {slope_df.shape}")

    for df in [ndvi_df, discharge_df, ndwi_df, precipitation_df, slope_df]:
        df["latitude"] = df["latitude"].round(6)
        df["longitude"] = df["longitude"].round(6)

    # Merge all DataFrames on 'latitude' and 'longitude'
    df = ndvi_df.merge(discharge_df, on=['name', 'latitude', 'longitude'], how='outer')\
            .merge(ndwi_df, on=['name', 'latitude', 'longitude'], how='outer')\
            .merge(precipitation_df, on=['name', 'latitude', 'longitude'], how='outer')\
            .merge(slope_df, on=['name', 'latitude', 'longitude'], how='outer')


    df = df.drop_duplicates(subset=['latitude', 'longitude'], keep='first')

    # add 'label' column based on 'name' if 'name' is not null
    df['label'] = df['name'].apply(lambda x: 1 if pd.notnull(x) else 0)

    print(f"Final DataFrame shape: {df.shape}")


    df.to_csv('data/results/final_data.csv', index=False)

if __name__ == "__main__":
    main()