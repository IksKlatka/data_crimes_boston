import os
import pandas as pd
from dotenv import load_dotenv


def file_to_df(path: str) -> pd.DataFrame:

    load_dotenv()
    file = pd.read_csv(os.getenv(path))
    dataframe = pd.DataFrame(file)

    return dataframe

def drop_missing_lat_long(df: pd.DataFrame):


    lat = df['Latitude'].isna()
    long = df['Longitude'].isna()
    lat_equals_long = lat.equals(long)

    percent = round(df["Latitude"].isnull().sum() / len(df.index) * 100, 2)

    if lat_equals_long:
        df = df.dropna(subset=["Latitude", "Longitude"])
        print(f'Deleted {percent}% of csv data.')

    return df

def fill_missing_lat_long(df: pd.DataFrame) -> pd.DataFrame:
    null_streets = df[(df['Lat'].isnull()) & (df['Long'].isnull())]['STREET']

    # print(len(null_streets.unique())) # 334

    street_dict = {}
    for street in null_streets.unique():
        street_dict[street] = {}

    for street in street_dict.keys():
        # Sprawdź, czy istnieją niepuste dane dla danej ulicy
        lat_data = df.loc[df['STREET'] == street, 'Lat']
        long_data = df.loc[df['STREET'] == street, 'Long']

        if not lat_data.isnull().all() and not long_data.isnull().all():
            mean_lat = lat_data.median()
            mean_long = long_data.median()
            street_dict[street]['mean_lat'] = mean_lat
            street_dict[street]['mean_long'] = mean_long
            street_dict[street]['mean_location'] = [mean_lat, mean_long]
        else:
            # W przypadku braku wystarczającej liczby danych, ustaw wartość NaN
            street_dict[street]['mean_lat'] = float('nan')
            street_dict[street]['mean_long'] = float('nan')
            street_dict[street]['mean_location'] = float('nan')


    for index, row in df.iterrows():
        street = row['STREET']
        if pd.isnull(row['Lat']) and pd.isnull(row['Long']):
            df.at[index, 'Lat'] = street_dict[street]['mean_lat']
            df.at[index, 'Long'] = street_dict[street]['mean_long']
            df.at[index, 'Location'] = street_dict[street]['mean_location']

    # print(street_dict)
    return df

def spit_date_and_time(df: pd.DataFrame, col_name: str):

    col_name = col_name.upper()
    occurred = df[col_name]
    time = [t.split()[-1] for t in occurred]
    date = [d.split()[0] for d in occurred]


    df.drop(col_name, axis=1, inplace=True)
    df.insert(7, 'Date', date, allow_duplicates=True)
    df.insert(8, 'Time', time, allow_duplicates=True)

    return df

def info(df: pd.DataFrame):
    df.info()
    print('--'*5, '\nmissing:')
    print(df.isna().sum())
    print('--'*15)
