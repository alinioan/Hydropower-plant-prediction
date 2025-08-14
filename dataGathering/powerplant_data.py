import pandas as pd


def get_hydropower_data():
    """
    Fetches hydropower plant data from the global power plant database.
    Filters for European countries and returns a DataFrame with plant names and locations.
    """
    # Load the global power plant database
    df = pd.read_csv('data/global_power_plant_database.csv', low_memory=False)

    european_countries = [
        "Albania", "Andorra", "Armenia", "Austria", "Azerbaijan", "Belarus", "Belgium", "Bosnia and Herzegovina",
        "Bulgaria", "Croatia", "Cyprus", "Czech Republic", "Czechia", "Denmark", "Estonia", "Finland", "France",
        "Georgia", "Germany", "Greece", "Hungary", "Iceland", "Ireland", "Italy", "Kosovo", "Latvia", "Liechtenstein",
        "Lithuania", "Luxembourg", "Malta", "Moldova", "Monaco", "Montenegro", "Netherlands", "North Macedonia",
        "Norway", "Poland", "Portugal", "Romania", "Russia", "San Marino", "Serbia", "Slovakia", "Slovenia", "Spain",
        "Sweden", "Switzerland", "Turkey", "Ukraine", "United Kingdom", "Vatican City"
    ]
    hydro_europe_df = df[(df['primary_fuel'] == 'Hydro') & (df['country_long'].isin(european_countries))]

    powerplant_locations = hydro_europe_df[['name', 'latitude', 'longitude']]
    return powerplant_locations
