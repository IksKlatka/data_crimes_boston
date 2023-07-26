import pandas as pd
from repetitive import *

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def fill_na_year_2015(df: pd.DataFrame) -> pd.DataFrame:
    df['SHOOTING'] = df['SHOOTING'].fillna('N')
    df['UCR_PART'] = df['UCR_PART'].fillna('Other')

    # Missing Districts
    df.loc[df['LOCATION'] == '(42.33178000, -71.11328500)', 'DISTRICT'] = 'B2'
    df.loc[df['LOCATION'] == '(42.25794926, -71.16122880)', 'DISTRICT'] = 'B2'
    df.loc[df['LOCATION'] == '(42.38259978, -71.03980383)', 'DISTRICT'] = 'A7'

    # Missing Streets
    df.loc[df['LOCATION'] == '(42.32956715, -71.08597421)', 'STREET'] = 'Malcolm X Blvd'
    df.loc[df['LOCATION'] == '(42.28085450, -71.08458278)', 'STREET'] = 'Morton St'
    df.loc[df['LOCATION'] == '(42.32626055, -71.05508570)', 'STREET'] = "Msgr O'Callaghan Way"
    df.loc[df['LOCATION'] == '(42.29810706, -71.06246102)', 'STREET'] = 'Park St'

    return df

def drop_records(df: pd.DataFrame) -> pd.DataFrame:

    df = df.dropna(subset=['DISTRICT', 'LAT', 'LONG'], how='all')
    df = df.dropna(subset=['STREET', 'LONG', 'LAT'], how='all')

    # DORCHESTER AVENUE -> research
    df.loc[51183, 'DISTRICT'] = 'C11'
    df.loc[51184, 'DISTRICT'] = 'C11'

    df = df.dropna(subset=['DISTRICT'], how='all')

    return df


if __name__ == '__main__':

    df = file_to_df("YEAR_2015")
    df = spit_date_and_time(df, "OCCURRED_ON_DATE")
    df = fill_na_year_2015(df)
    df = fill_missing_lat_long(df)
    df = drop_records(df)
    df = drop_missing_lat_long(df) # 0.55%

    info(df)
    print(f'Deleted {round(1628*100/53596, 2)}% of whole dataframe.')
    print(f'Inserted {2172-1606} Latitude and Longitude values based on median values.')


