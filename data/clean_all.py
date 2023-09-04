import time

from dotenv import load_dotenv
from os import environ, path
from cleaning_algorithms import clean_first_four, clean_last_four, clean_last, file_to_df, save_to_file

basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, '.env'))

def get_all_csvs():

    datas = []
    mask = 'YEAR'
    for key in environ.keys():
        if key.startswith(mask):
            datas.append(key)

    return datas

def clean_first_four_years(csvs: list):

    for csv in csvs[:4]:
        df = file_to_df(csv, separator=',')
        save_to_file(df=clean_first_four(df), name="C_"+csv)

def clean_last_four_years(csvs: list):

    filler = file_to_df('CLEANED_2018', separator=';')

    for csv in csvs[4:]:
        df = file_to_df(csv, separator=',')
        if csv == "YEAR_2022":
            save_to_file(df=clean_last(filler_df=filler, dataframe=df),
                         name="C_"+csv)
            break
        save_to_file(df=clean_last_four(filler_df=filler, dataframe=df),
                     name="C_"+csv)


if __name__ == '__main__':

    s = time.time()
    # clean_first_four_years(get_all_csvs())
    clean_last_four_years(get_all_csvs())
    f = time.time()

    print(f"The whole process took {f-s:.1f}s.")