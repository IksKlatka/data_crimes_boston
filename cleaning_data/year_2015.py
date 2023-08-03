import pandas as pd
from repetitive import *

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

    config()
    df = file_to_df("YEAR_2015")

    # Missing Districts
    # df.loc[(df['LAT'] == '42.33178000') & (df['LONG'] == '-71.11328500'), 'DISTRICT'] = 'B2'
    # df.loc[(df['LAT'] == '42.25794926') & (df['LONG'] == '-71.16122880'), 'DISTRICT'] = 'B2'
    # df.loc[(df['LAT'] == '42.38259978') & (df['LONG'] == '-71.03980383'), 'DISTRICT'] = 'A7'

    # Missing Streets
    # df.loc[(df['LAT'] == '42.32956715') & (df['LONG'] == '-71.08597421'), 'STREET'] = 'Malcolm X Blvd'
    # df.loc[(df['LAT'] == '42.28085450') & (df['LONG'] == '-71.08458278'), 'STREET'] = 'Morton St'
    # df.loc[(df['LAT'] == '42.32626055') & (df['LONG'] == '-71.05508570'), 'STREET'] = "Msgr O'Callaghan Way"
    # df.loc[(df['LAT'] == '42.29810706') & (df['LONG'] == '-71.06246102'), 'STREET'] = 'Park St'

    df = clean_all(df)
    df = ultimate_drop(df)
    info(df)

    save = input("Do you want to save cleaned data to file? Y/N ").upper()
    if save == "Y": save_to_file(df, "year_2015")

