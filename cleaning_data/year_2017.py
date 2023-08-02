import pandas as pd

from repetitive import *

pd.set_option('display.max_columns', None)


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
    df = file_to_df("YEAR_2017")
    df = change_dtypes(df)
    df = clean_all(df)
    df = ultimate_drop(df)

    info(df)
