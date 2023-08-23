import os
import pandas as pd
from dotenv import load_dotenv
import re


def config():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

def info(dataframe: pd.DataFrame):
    dataframe.info()
    print('--'*5, '\nmissing:')
    print(dataframe.isna().sum())
    print('--'*15)

def file_to_df(path: str, separator: str, ind_col = None) -> pd.DataFrame:
    """
    Upload data from a file. Make column names uppercase and all values lowercase.
    Split date and time to separate columns. Convert Date to datetime format.
    Sort values by DATE column, ascending. Drop Location column.
    :param ind_col:
    :param separator:
    :param path: to vsc file
    :return: pd.DataFrame
    """
    load_dotenv()
    file = pd.read_csv(os.getenv(path), sep=separator, index_col=ind_col)
    dataframe = pd.DataFrame(file)
    return dataframe

def case_indexing_date_time(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Changing lettercase of columns and values. Splitting 'OCCURRED ON DATE' into
    DATE and TIME. Setting index to DATE. Drop LOCATION, which is combination of
    LAT and LONG. Changing notation of 'DAY OF WEEK' to numeric (from 1 to 7).
    :param dataframe:
    :return:
    """
    dataframe.columns = dataframe.columns.str.upper()
    dataframe['OFFENSE_DESCRIPTION']= dataframe['OFFENSE_DESCRIPTION'].astype(str).str.lower()
    dataframe['STREET']= dataframe['STREET'].astype(str).str.title()

    dataframe = spit_date_and_time(dataframe, 'OCCURRED_ON_DATE')
    dataframe['DATE'] = pd.to_datetime(dataframe['DATE'])

    dataframe = dataframe.drop('LOCATION', axis=1)
    dataframe = dataframe.sort_values(by=['DATE'])
    dataframe = dataframe.reset_index(drop=True)

    day_of_week = {
        'monday': 1,
        'tuesday': 2,
        'wednesday': 3,
        'thursday': 4,
        'friday': 5,
        'saturday': 6,
        'sunday': 7
    }

    dataframe['DAY_OF_WEEK'] = dataframe['DAY_OF_WEEK']\
        .map(lambda x: day_of_week.get(x.lower()))

    return dataframe

def change_dtypes(dataframe: pd.DataFrame) -> pd.DataFrame:

    dataframe['DATE'] = pd.to_datetime(dataframe['DATE'])
    dataframe['HOUR'] = dataframe['HOUR'].astype('Int32')
    dataframe['MONTH'] = dataframe['MONTH'].astype('Int32')
    dataframe['YEAR'] = dataframe['YEAR'].astype('Int32')
    dataframe['OFFENSE_CODE'] = dataframe['OFFENSE_CODE'].astype('Int32')
    dataframe['LAT'] = dataframe['LAT'].astype('float64')
    dataframe['LONG'] = dataframe['LONG'].astype('float64')
    dataframe['REPORTING_AREA'] = pd.to_numeric(dataframe['REPORTING_AREA'], errors="coerce").astype('Int32')
    dataframe['SHOOTING'] = dataframe['SHOOTING'].astype('Int32')

    return dataframe

def standardize_streets(dataframe):
    endings = ['St', 'Blvd', 'Ave', 'Ct', 'Dr', 'Rd', 'Sq', 'Brg', 'Pl']
    pattern = r'\s(' + '|'.join(endings) + r')\b'

    dataframe['STREET'] = dataframe['STREET'].apply(lambda x: re.sub(pattern, '', x, flags=re.IGNORECASE))
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
    dataframe['SHOOTING'] = dataframe['SHOOTING'].fillna('0')

    dataframe['SHOOTING'] = dataframe['SHOOTING'].astype(str).str.lower()
    dataframe['SHOOTING'] = dataframe['SHOOTING'].replace('y', '1')
    dataframe['UCR_PART'] = dataframe['UCR_PART'].fillna('Other')

    return dataframe

def fill_missing_lat_long_by_street(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Finds median value of Latitude and Longitude on specific street and
    inputs the value into the field.
    Rounds Latitude and Longitude up to the same number of decimal places (7).
    :param dataframe:
    :return:
    """
    null_streets = dataframe[(dataframe['LAT'].isnull()) & (dataframe['LONG'].isnull())]['STREET']
    missing_indexes = []
    street_dict = {}
    for street in null_streets.unique():
        if street.lower() != 'nan':
            street_dict[street] = {}

    for street in street_dict.keys():
        lat_data = dataframe.loc[dataframe['STREET'] == street, 'LAT']
        long_data = dataframe.loc[dataframe['STREET'] == street, 'LONG']

        if not lat_data.isnull().all() and not long_data.isnull().all():
            mean_lat = lat_data.mean()
            mean_long = long_data.mean()
            street_dict[street]['mean_lat'] = mean_lat
            street_dict[street]['mean_long'] = mean_long
        else:
            cleaned_lat = lat_data.dropna()
            cleaned_long = long_data.dropna()

            if len(cleaned_lat) > 0 and len(cleaned_long) > 0:
                mean_lat = lat_data.mean()
                mean_long = long_data.mean()
                street_dict[street]['mean_lat'] = mean_lat
                street_dict[street]['mean_long'] = mean_long
            else:
                street_dict[street]['mean_lat'] = float('nan')
                street_dict[street]['mean_long'] = float('nan')

    for index, row in dataframe.iterrows():
        street = row['STREET']
        if pd.isnull(row['LAT']) and pd.isnull(row['LONG']) and street in street_dict:
            dataframe.at[index, 'LAT'] = street_dict[street]['mean_lat']
            dataframe.at[index, 'LONG'] = street_dict[street]['mean_long']
            missing_indexes.append(index)


    dataframe['LAT'] = dataframe['LAT'].round(7)
    dataframe['LONG'] = dataframe['LONG'].round(7)


    return dataframe, street_dict

def fill_missing_by(dataframe: pd.DataFrame, key: str, values: str):
    """
    Creates a dict with keys = unique keys, then
    iterates through missing keys in DataFrame and updates null value if
    any(values) of missing keys is found in dict.values()
    """

    key, values = key.upper(), values.upper()

    keys_for_dict = (dataframe[dataframe[key].notnull()][key].unique())
    df_slice = dataframe.loc[:, [key, values]]

    key_dict = {}

    for keys in keys_for_dict:
        key_dict[keys] = []

    for i in range(len(df_slice)):
        if df_slice.loc[i, key] in key_dict.keys():
            key_dict[df_slice.loc[i, key]].append(df_slice.loc[i, values])

    missing_data = dataframe.loc[dataframe[key].isnull()][[key, values]]

    for index, row in missing_data.iterrows():
        value = row[values]
        for k, v in key_dict.items():
            if value in v:
                dataframe.at[index, key] = k
                break

    return dataframe

def ultimate_drop(dataframe: pd.DataFrame) -> pd.DataFrame:

    dataframe = dataframe.dropna(subset=['DISTRICT', 'LAT', 'LONG', 'STREET'], how='all')
    dataframe = dataframe.dropna(subset=['STREET', 'LONG', 'LAT'], how='all')
    dataframe = dataframe.dropna(subset=['LAT', 'LONG'], how='all')
    # dataframe = dataframe.dropna(subset=['DISTRICT', 'STREET'], how='all')
    # dataframe = dataframe.dropna(subset=['REPORTING_AREA', 'LAT', 'LONG'], how='all')
    dataframe = dataframe[dataframe['DISTRICT'].str.len() <= 3]
    dataframe = dataframe.dropna(subset='REPORTING_AREA')

    return dataframe


def save_to_file(df: pd.DataFrame, name: str):
    """
    :param df:
    :param name:
    :return:
    """
    df.to_csv(fr'C:\Users\igakl\Desktop\DataCrimesBoston\cleaned_data/{name}.csv', sep=';', index=False)
    print(f"File {name} saved.")