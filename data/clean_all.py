import time

from dotenv import load_dotenv
from os import environ, path
from cleaning_algorithms import clean_all, file_to_df, save_to_file

basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, '.env'))

def get_all_csvs():

    datas = []
    mask = 'YEAR'
    for key in environ.keys():
        if key.startswith(mask):
            datas.append(key)

    return datas

def clean_all_csvs(csvs: list[str]):

    for csv in csvs:
        df = file_to_df(csv, separator=',')
        save_to_file(clean_all(df), name='C_'+csv)

    return 1

if __name__ == '__main__':
    start = time.time()
    clean_all_csvs(get_all_csvs())
    end = time.time()

    print(f'Whole program took: {end-start:.1f}s')

