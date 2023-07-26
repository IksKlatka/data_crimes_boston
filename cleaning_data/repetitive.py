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
