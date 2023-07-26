import pandas as pd
from repetitive import *

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def fill_na_year_2015(df: pd.DataFrame) -> pd.DataFrame:
    df['SHOOTING'] = df['SHOOTING'].fillna('N')
    df['UCR_PART'] = df['UCR_PART'].fillna('Other')

    # Missing Districts
    df.loc[df['Location'] == '(42.33178000, -71.11328500)', 'DISTRICT'] = 'B2'
    df.loc[df['Location'] == '(42.25794926, -71.16122880)', 'DISTRICT'] = 'B2'
    df.loc[df['Location'] == '(42.38259978, -71.03980383)', 'DISTRICT'] = 'A7'

    # Missing Streets
    df.loc[df['Location'] == '(42.32956715, -71.08597421)', 'STREET'] = 'Malcolm X Blvd'
    df.loc[df['Location'] == '(42.28085450, -71.08458278)', 'STREET'] = 'Morton St'
    df.loc[df['Location'] == '(42.32626055, -71.05508570)', 'STREET'] = "Msgr O'Callaghan Way"
    df.loc[df['Location'] == '(42.29810706, -71.06246102)', 'STREET'] = 'Park St'
    return df

def drop_records(df: pd.DataFrame) -> pd.DataFrame:

    df = df.dropna(subset=['DISTRICT', 'STREET'], how='all')
    df = df.dropna(subset=['DISTRICT', 'Lat'], how='all')
    df = df.dropna(subset=['DISTRICT', 'Long'], how='all')
    df = df.dropna(subset=['STREET', 'Long'], how='all')
    df = df.dropna(subset=['STREET', 'Lat'], how='all')
    return df

if __name__ == '__main__':

    df = file_to_df("YEAR_2015")
    df = spit_date_and_time(df, "occurred_on_date")
    df = fill_na_year_2015(df)
    df = drop_records(df)
    df = fill_missing_lat_long(df)

    info(df)
    print(df.head(10))



