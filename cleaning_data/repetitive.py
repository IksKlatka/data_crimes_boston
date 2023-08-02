import os
import pandas as pd
from dotenv import load_dotenv

def config():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

def info(dataframe: pd.DataFrame):
    dataframe.info()
    print('--'*5, '\nmissing:')
    print(dataframe.isna().sum())
    print('--'*15)

def file_to_df(path: str) -> pd.DataFrame:
    """
    Upload data from a file. Make column names uppercase and all values lowercase.
    Split date and time to separate columns. Convert Date to datetime format.
    Sort values by DATE column, ascending. Drop Location column.
    :param path: to vsc file
    :return: pd.DataFrame
    """
    load_dotenv()
    file = pd.read_csv(os.getenv(path))
    dataframe = pd.DataFrame(file)


    dataframe.columns = dataframe.columns.str.upper()
    for column in dataframe.columns:
        dataframe[column]= dataframe[column].astype(str).str.lower()

    dataframe = spit_date_and_time(dataframe, 'OCCURRED_ON_DATE')
    dataframe['DATE'] = pd.to_datetime(dataframe['DATE'])

    dataframe = dataframe.drop('LOCATION', axis=1)

    dataframe = dataframe.sort_values(by=['DATE'])
    dataframe = dataframe.reset_index(drop=True)

    return dataframe

def change_dtypes(dataframe: pd.DataFrame) -> pd.DataFrame:

    dataframe['DATE'] = pd.to_datetime(dataframe['DATE'])
    dataframe['HOUR'] = dataframe['HOUR'].astype('int32')
    dataframe['MONTH'] = dataframe['MONTH'].astype('int32')
    dataframe['YEAR'] = dataframe['YEAR'].astype('int32')
    dataframe['OFFENSE_CODE'] = dataframe['OFFENSE_CODE'].astype('int32')
    dataframe['LAT'] = dataframe['LAT'].astype('float64')
    dataframe['LONG'] = dataframe['LONG'].astype('float64')
    dataframe['REPORTING_AREA'] = pd.to_numeric(dataframe['REPORTING_AREA'], errors="coerce")

    return dataframe

def spit_date_and_time(dataframe: pd.DataFrame, col_name: str):

    col_name = col_name.upper()
    occurred = dataframe[col_name]
    time = [t.split()[-1] for t in occurred]
    date = [d.split()[0] for d in occurred]


    dataframe.drop(col_name, axis=1, inplace=True)
    dataframe.insert(7, 'DATE', date, allow_duplicates=True)
    dataframe.insert(8, 'TIME', time, allow_duplicates=True)

    return dataframe

def fill_missing_ucr_and_shootings(dataframe: pd.DataFrame) -> pd.DataFrame:

    dataframe = dataframe.copy()
    dataframe[:] = dataframe[:].replace('nan', pd.NA)

    dataframe['SHOOTING'] = dataframe['SHOOTING'].fillna('N')
    dataframe['UCR_PART'] = dataframe['UCR_PART'].fillna('Other')

    return dataframe

def fill_missing_lat_long_by_street(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Finds median value of Latitude and Longitude on specific street and
    inputs the value into the field.
    Rounds Latitude and Longitude up to the same number of decimal places.
    :param dataframe:
    :return:
    """

    null_streets = dataframe[(dataframe['LAT'].isnull()) & (dataframe['LONG'].isnull())]['STREET']


    street_dict = {}
    for street in null_streets.unique():
        street_dict[street] = {}

    for street in street_dict.keys():
        # Sprawdź, czy istnieją niepuste dane dla danej ulicy
        lat_data = dataframe.loc[dataframe['STREET'] == street, 'LAT']
        long_data = dataframe.loc[dataframe['STREET'] == street, 'LONG']

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


    for index, row in dataframe.iterrows():
        street = row['STREET']
        if pd.isnull(row['LAT']) and pd.isnull(row['LONG']):
            dataframe.at[index, 'LAT'] = street_dict[street]['mean_lat']
            dataframe.at[index, 'LONG'] = street_dict[street]['mean_long']

    dataframe['LAT'] = dataframe['LAT'].round(7)
    dataframe['LONG'] = dataframe['LONG'].round(7)


    return dataframe

def fill_missing_by(dataframe: pd.DataFrame, column1: str, column2: str) -> pd.DataFrame:
    """
    Fills missing values in column1 by inserting most common value for this column
    based on column2.

    :param dataframe:
    :param column1: to fill null values
    :param column2: to look for most common value of column1
    :return: pd.DataFrame
    """

    column1, column2 = column1.upper(), column2.upper()

    counter = dataframe.groupby([column1, column2]).size().reset_index(name="COUNT")

    missing = dataframe[dataframe[column1].isnull()]

    for i, row in missing.iterrows():
        col2_value = row[column2]
        col2_col1_counter = counter[counter[column2] == col2_value]

        if not col2_col1_counter.empty:
            most_common_value = missing[missing[column2] == col2_value][column1].iloc[0]
            dataframe.at[i, column1] = most_common_value

    return dataframe

def ultimate_drop(dataframe: pd.DataFrame) -> pd.DataFrame:

    dataframe = dataframe.dropna(subset=['DISTRICT', 'LAT', 'LONG'], how='all')
    dataframe = dataframe.dropna(subset=['STREET', 'LONG', 'LAT'], how='all')
    dataframe = dataframe.dropna(subset=['LAT', 'LONG'], how='all')
    dataframe = dataframe.dropna(subset=['DISTRICT', 'STREET'], how='all')
    dataframe = dataframe.dropna(subset=['REPORTING_AREA', 'LAT', 'LONG'], how='all')
    dataframe = dataframe[dataframe['DISTRICT'].str.len() <= 3]
    dataframe = dataframe.dropna(subset='REPORTING_AREA')

    return dataframe


def save_to_file():
    pass