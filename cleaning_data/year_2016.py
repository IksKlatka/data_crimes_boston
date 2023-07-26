from repetitive import *

pd.set_option('display.max_columns', None)

def drop_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    This is last resort.
    :param df: pd.DataFrame
    :return: pd.DataFrame
    """

    df = df.dropna(subset=['DISTRICT', 'LAT', 'LONG'], how='all')
    df = df.dropna(subset=['STREET', 'LONG', 'LAT'], how='all')

    # DORCHESTER AVENUE -> research
    df.loc[51183, 'DISTRICT'] = 'C11'
    df.loc[51184, 'DISTRICT'] = 'C11'

    df = df.dropna(subset=['DISTRICT', 'STREET'], how='all')

    return df


if __name__ == '__main__':

    config()
    df = file_to_df("YEAR_2016")
    df = spit_date_and_time(df, "OCCURRED_ON_DATE")
    df = df.drop_duplicates(subset=['OFFENSE_DESCRIPTION', 'DATE', 'TIME'])
    df = fill_missing_lat_long(df)
    # df = fill_missing_loc(df)
    df = df.dropna(subset=['STREET', 'LAT', 'LONG', 'LOCATION'], how='all') # 4992 records
    df = df.dropna(subset=['DISTRICT', 'STREET'], how='all') # approximately 100 records
    df = fill_na_ucr_and_shootings(df)
    # df = drop_records(df)

    # print(df[df['DISTRICT'].isnull()]['STREET'])
    # loc_null_streets = df.loc[df['STREET'].isnull(), ['LAT', 'LONG']]
    # loc_null_street_location = df.loc[df['STREET'].isnull() & df['LAT'].notna() & df['LONG'].notna(), ['LAT', 'LONG']]
    # print(loc_null_street_location)



    # print(df[26403:26406][:])
    # print(df.loc[df['SHOOTING']]['OFFENSE_DESCRIPTION'])
    # print(df['SHOOTING'])
    # null_streets_lat = df[df['STREET'].isnull() and df['Lat'].isnull()]['Long']
    # null_streets_long = df[df['STREET'].isnull()]['Long'].notna()
    # print(len(null_streets_long) == len(null_streets_lat))
    # print(len(null_streets_lat))
    # print(df.loc[null_streets_long])

    info(df)