import os
import pandas as pd
from dotenv import load_dotenv
import re
import numpy as np


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
    file = pd.read_csv(os.getenv(path), sep=separator, index_col=ind_col, low_memory=False)
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

    dataframe = spit_date_and_time(dataframe)
    dataframe['DATE'] = pd.to_datetime(dataframe['DATE'], format="mixed")

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


def fill_missing_ucr_and_shootings(dataframe: pd.DataFrame) -> pd.DataFrame:

    dataframe = dataframe.copy()
    dataframe[:] = dataframe[:].replace('nan', pd.NA)
    dataframe['SHOOTING'] = dataframe['SHOOTING'].fillna('0')

    dataframe['SHOOTING'] = dataframe['SHOOTING'].astype(str).str.lower()
    dataframe['SHOOTING'] = dataframe['SHOOTING'].replace('y', '1')
    dataframe['UCR_PART'] = dataframe['UCR_PART'].fillna('Other')

    return dataframe

def fill_missing_lat_long_by(dataframe: pd.DataFrame,
                             location: str = 'DISTRICT' or  'STREET') -> pd.DataFrame:
    """
    Finds median value of Latitude and Longitude for specific street or district and
    inputs the value into the field.
    Rounds Latitude and Longitude up to the same number of decimal places.
    :param dataframe:
    :return:
    """
    null_loc = dataframe[(dataframe['LAT'].isnull()) & (dataframe['LONG'].isnull())][location]
    missing_indexes = []
    loc_dict = {}
    for loc in null_loc.unique():
        if loc != np.nan :
            loc_dict[loc] = {}

    for loc in loc_dict.keys():
        lat_data = dataframe.loc[dataframe[location] == loc, 'LAT']
        long_data = dataframe.loc[dataframe[location] == loc, 'LONG']

        if not lat_data.isnull().all() and not long_data.isnull().all():
            mean_lat = lat_data.mean()
            mean_long = long_data.mean()
            loc_dict[loc]['mean_lat'] = mean_lat
            loc_dict[loc]['mean_long'] = mean_long
        else:
            cleaned_lat = lat_data.dropna()
            cleaned_long = long_data.dropna()

            if len(cleaned_lat) > 0 and len(cleaned_long) > 0:
                mean_lat = lat_data.mean()
                mean_long = long_data.mean()
                loc_dict[loc]['mean_lat'] = mean_lat
                loc_dict[loc]['mean_long'] = mean_long
            else:
                loc_dict[loc]['mean_lat'] = float('nan')
                loc_dict[loc]['mean_long'] = float('nan')

    for index, row in dataframe.iterrows():
        loc = row[location]
        if pd.isnull(row['LAT']) and pd.isnull(row['LONG']) and loc in loc_dict:
            dataframe.at[index, 'LAT'] = loc_dict[loc]['mean_lat']
            dataframe.at[index, 'LONG'] = loc_dict[loc]['mean_long']
            missing_indexes.append(index)


    dataframe['LAT'] = dataframe['LAT'].round(7)
    dataframe['LONG'] = dataframe['LONG'].round(7)


    return dataframe, loc_dict

def fill_missing_by(dataframe: pd.DataFrame, key: str, values: str):
    """
    Creates a dictionary in which the keys are unique values from column1 whose deficiencies you want to fill.
    Iterates through the DataFrame collecting the values (column2) for those keys.
    It creates a new DataFrame from column1 and column2, where column1 has the missing values itself.
    If it finds a value in column2 that corresponds to a key, it inserts the value of the key into column1.
    """

    key = key.upper()
    values = values.upper()

    keys_for_dict = (dataframe[(dataframe[key].notnull())][key].unique())

    key_dict = {}

    for keys in keys_for_dict:
        key_dict[keys] = []

    for i in range(len(dataframe)):
        if dataframe.loc[i, key] in key_dict.keys():
            key_dict[dataframe.loc[i, key]].append(dataframe.loc[i, values])

    missing_data = dataframe.loc[dataframe[key].isnull()][[key, values]]

    for index, row in missing_data.iterrows():
        value = row[values]
        for k, v in key_dict.items():
            if value in v:
                dataframe.at[index, key] = k
                break

    return dataframe, key_dict

def fill_missing_by_other_df(df1: pd.DataFrame, df2: pd.DataFrame, key: str, val: str):
    """
    Designed to fill missing values in DataFrame (df2),
    where all values in specific column are missing (val)
    by dictionary values made from "other" DataFrame (df1)
    :param df1: DataFrame to take k:v pairs from
    :param df2: DataFrame to fill
    :param key, val: for dictionary
    :return: filled df2
    """
    key, val = key.upper(), val.upper()

    filler = {}
    filler_keys = (df1[(df1[key].notnull())][key].unique())

    for fk in filler_keys:
        filler[fk] = []

    for i in range(len(df1)):
        if df1.loc[i, key] in filler.keys():
            filler[df1.loc[i, key]].append(df1.loc[i, val])

    missing_data = df2.loc[df2[val].isnull()][[key, val]]

    for index, row in missing_data.iterrows():
        for k, v in filler.items():
            if row[key] == k:
                df2.at[index, val] = v[0]
                break

    return df2

def clean_first_four(dataframe: pd.DataFrame):

    dataframe = case_indexing_date_time(dataframe)
    dataframe = fill_missing_ucr_and_shootings(dataframe)
    dataframe = change_dtypes(dataframe)
    dataframe = standardize_streets(dataframe)
    dataframe, loc_dict1 = fill_missing_lat_long_by(dataframe, 'DISTRICT')
    dataframe, loc_dict2 = fill_missing_lat_long_by(dataframe, 'STREET')
    dataframe, kd1 = fill_missing_by(dataframe, 'district', 'street')
    dataframe, kd2= fill_missing_by(dataframe, 'reporting_area', 'street')
    dataframe, kd3  = fill_missing_by(dataframe, 'reporting_area', 'district')
    dataframe = dataframe.drop_duplicates()
    dataframe = dataframe.dropna(subset=['DISTRICT', 'REPORTING_AREA', 'LAT', 'LONG'], how='all')


    return dataframe

def clean_last_four(filler_df: pd.DataFrame, dataframe: pd.DataFrame):

    dataframe = case_indexing_date_time(dataframe)
    dataframe = fill_missing_ucr_and_shootings(dataframe)
    dataframe = change_dtypes(dataframe)
    dataframe = standardize_streets(dataframe)
    dataframe, loc_dict1 = fill_missing_lat_long_by(dataframe, 'DISTRICT')
    dataframe, loc_dict2 = fill_missing_lat_long_by(dataframe, 'STREET')
    dataframe = fill_missing_by_other_df(filler_df, dataframe, 'OFFENSE_CODE', 'OFFENSE_CODE_GROUP')
    dataframe = fill_missing_by_other_df(filler_df, dataframe, 'OFFENSE_CODE', 'UCR_PART')
    dataframe = fill_missing_by_other_df(filler_df, dataframe, 'DISTRICT', 'OFFENSE_CODE_GROUP')
    dataframe, kd = fill_missing_by(dataframe, 'district', 'street')
    dataframe, kd = fill_missing_by(dataframe, 'reporting_area', 'street')
    dataframe, kd = fill_missing_by(dataframe, 'reporting_area', 'district')
    dataframe, kd = fill_missing_by(dataframe, 'district', 'reporting_area') # NOT IN 2022
    dataframe, kd = fill_missing_by(dataframe, 'offense_code_group', 'offense_code') # NOT IN 2022
    dataframe = dataframe.dropna(subset=['DISTRICT', 'REPORTING_AREA', 'LAT', 'LONG'], how='all')
    dataframe = dataframe.dropna(subset=['LAT', 'LONG'], how='all')


    return dataframe

def clean_last(filler_df: pd.DataFrame, dataframe: pd.DataFrame):

    dataframe = case_indexing_date_time(dataframe)
    dataframe = fill_missing_ucr_and_shootings(dataframe)
    dataframe = change_dtypes(dataframe)
    dataframe = standardize_streets(dataframe)
    dataframe, loc_dict1 = fill_missing_lat_long_by(dataframe, 'DISTRICT')
    dataframe, loc_dict2 = fill_missing_lat_long_by(dataframe, 'STREET')
    dataframe = fill_missing_by_other_df(filler_df, dataframe, 'OFFENSE_CODE', 'OFFENSE_CODE_GROUP')
    dataframe = fill_missing_by_other_df(filler_df, dataframe, 'OFFENSE_CODE', 'UCR_PART')
    dataframe = fill_missing_by_other_df(filler_df, dataframe, 'DISTRICT', 'OFFENSE_CODE_GROUP')
    dataframe, kd = fill_missing_by(dataframe, 'district', 'street')
    dataframe, kd = fill_missing_by(dataframe, 'reporting_area', 'street')
    dataframe, kd = fill_missing_by(dataframe, 'reporting_area', 'district')
    dataframe = dataframe.dropna(subset=['DISTRICT', 'REPORTING_AREA', 'LAT', 'LONG'], how='all')
    dataframe = dataframe.dropna(subset=['LAT', 'LONG'], how='all')


    return dataframe

def save_to_file(df: pd.DataFrame, name: str):
    """
    :param df:
    :param name:
    :return:
    """
    df.to_csv(fr'C:\Users\igakl\Desktop\DataCrimesBoston\data\clean/{name}.csv', sep=';', index=False)
    print(f"File {name} saved.")

def spit_date_and_time(dataframe: pd.DataFrame):
    """Used in case_indexing_date_time()"""
    occurred = dataframe['OCCURRED_ON_DATE']
    time = [t.split()[-1] for t in occurred]
    date = [d.split()[0] for d in occurred]


    dataframe.drop('OCCURRED_ON_DATE', axis=1, inplace=True)
    dataframe.insert(7, 'DATE', date, allow_duplicates=True)
    dataframe.insert(8, 'TIME', time, allow_duplicates=True)

    return dataframe
