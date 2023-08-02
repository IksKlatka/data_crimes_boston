import pandas as pd
from repetitive import *

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def fill_na_year_2015(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe['SHOOTING'] = dataframe['SHOOTING'].fillna('N')
    dataframe['UCR_PART'] = dataframe['UCR_PART'].fillna('Other')

    # Missing Districts
    dataframe.loc[dataframe['LOCATION'] == '(42.33178000, -71.11328500)', 'DISTRICT'] = 'B2'
    dataframe.loc[dataframe['LOCATION'] == '(42.25794926, -71.16122880)', 'DISTRICT'] = 'B2'
    dataframe.loc[dataframe['LOCATION'] == '(42.38259978, -71.03980383)', 'DISTRICT'] = 'A7'

    # Missing Streets
    dataframe.loc[dataframe['LOCATION'] == '(42.32956715, -71.08597421)', 'STREET'] = 'Malcolm X Blvd'
    dataframe.loc[dataframe['LOCATION'] == '(42.28085450, -71.08458278)', 'STREET'] = 'Morton St'
    dataframe.loc[dataframe['LOCATION'] == '(42.32626055, -71.05508570)', 'STREET'] = "Msgr O'Callaghan Way"
    dataframe.loc[dataframe['LOCATION'] == '(42.29810706, -71.06246102)', 'STREET'] = 'Park St'

    return dataframe

def drop_records(dataframe: pd.DataFrame) -> pd.DataFrame:

    dataframe = dataframe.dropna(subset=['DISTRICT', 'LAT', 'LONG'], how='all')
    dataframe = dataframe.dropna(subset=['STREET', 'LONG', 'LAT'], how='all')
    dataframe = dataframe.dropna(subset=["LAT", "LONG"], how='all')

    # DORCHESTER AVENUE -> research
    dataframe.loc[51183, 'DISTRICT'] = 'C11'
    dataframe.loc[51184, 'DISTRICT'] = 'C11'

    dataframe = dataframe.dropna(subset=['DISTRICT'], how='all')

    return dataframe

def clean_all(dataframe: pd.DataFrame) -> pd.DataFrame:

    dataframe = change_dtypes(dataframe)
    dataframe = dataframe.drop_duplicates(subset=['INCIDENT_NUMBER', 'OFFENSE_DESCRIPTION', 'DATE', 'TIME'])
    dataframe = fill_missing_ucr_and_shootings(dataframe)
    dataframe = fill_missing_lat_long_by_street(dataframe)
    dataframe = fill_missing_by(dataframe, "street", "district")
    dataframe = fill_missing_by(dataframe, "district", "street")
    dataframe = fill_missing_by(dataframe, "reporting_area", "street")

    dataframe = dataframe.dropna(subset=['STREET', 'DISTRICT'], how='all')
    dataframe = dataframe.dropna(subset=['STREET', 'LAT', 'LONG'], how='all')

    return dataframe


if __name__ == '__main__':

    df = file_to_df("YEAR_2015")
    df = clean_all(df)

    info(df)

