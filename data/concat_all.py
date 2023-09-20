from cleaning_algorithms import *

def concat_and_save() -> pd.DataFrame:
    df15 = file_to_df("CLEANED_2015", separator=';')
    df16 = file_to_df("CLEANED_2016", separator=';')
    df17 = file_to_df("CLEANED_2017", separator=';')
    df18 = file_to_df("CLEANED_2018", separator=';')
    df19 = file_to_df("CLEANED_2019", separator=';')
    df20 = file_to_df("CLEANED_2020", separator=';')
    df21 = file_to_df("CLEANED_2021", separator=';')
    df22 = file_to_df("CLEANED_2022", separator=';')

    all = pd.concat([df15, df16, df17, df18, df19, df20, df21, df22])
    all = all.dropna(subset=['REPORTING_AREA', 'DISTRICT'], how='all')
    all['REPORTING_AREA'] = (all['REPORTING_AREA']).astype("Int32")
    all = all.dropna(subset='OFFENSE_CODE_GROUP')
    save_to_file(all, name="all_cleaned_datasets")

    return all

if __name__ == '__main__':
    concat_and_save()