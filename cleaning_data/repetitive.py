import os
import pandas as pd
from dotenv import load_dotenv

def config():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

def file_to_df(path: str) -> pd.DataFrame:

    load_dotenv()
    file = pd.read_csv(os.getenv(path))
    dataframe = pd.DataFrame(file)
    dataframe.columns = dataframe.columns.str.upper()

    return dataframe

def drop_missing_lat_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    This is for when I have no longer idea how to input missing data in these columns.
    :param df: pd.DataFrame
    :return: pd.DataFrame
    """
    lat = df['LAT'].isna()
    long = df['LONG'].isna()
    lat_equals_long = lat.equals(long)

    percent = round(df["LAT"].isnull().sum() / len(df.index) * 100, 2)

    if lat_equals_long:
        df = df.dropna(subset=["LAT", "LONG"])
        print(f'Deleted {percent}% of csv data.')

    return df

def fill_na_ucr_and_shootings(df: pd.DataFrame) -> pd.DataFrame:
    df['SHOOTING'] = df['SHOOTING'].fillna('N')
    df['UCR_PART'] = df['UCR_PART'].fillna('Other')

    return df

def fill_missing_lat_long(df: pd.DataFrame) -> pd.DataFrame:
    null_streets = df[(df['LAT'].isnull()) & (df['LONG'].isnull())]['STREET']

    # print(len(null_streets.unique())) # 334

    street_dict = {}
    for street in null_streets.unique():
        street_dict[street] = {}

    for street in street_dict.keys():
        # Sprawdź, czy istnieją niepuste dane dla danej ulicy
        lat_data = df.loc[df['STREET'] == street, 'LAT']
        long_data = df.loc[df['STREET'] == street, 'LONG']

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
        if pd.isnull(row['LAT']) and pd.isnull(row['LONG']):
            df.at[index, 'LAT'] = street_dict[street]['mean_lat']
            df.at[index, 'LONG'] = street_dict[street]['mean_long']
            df.at[index, 'LOCATION'] = street_dict[street]['mean_location']

    # print(street_dict)
    return df

def spit_date_and_time(df: pd.DataFrame, col_name: str):

    col_name = col_name.upper()
    occurred = df[col_name]
    time = [t.split()[-1] for t in occurred]
    date = [d.split()[0] for d in occurred]


    df.drop(col_name, axis=1, inplace=True)
    df.insert(7, 'DATE', date, allow_duplicates=True)
    df.insert(8, 'TIME', time, allow_duplicates=True)

    return df

def info(df: pd.DataFrame):
    df.info()
    print('--'*5, '\nmissing:')
    print(df.isna().sum())
    print('--'*15)
