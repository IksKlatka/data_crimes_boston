import datetime
import os
import random

import pandas as pd
import pytest
from dotenv import load_dotenv

from data.cleaning_algorithms import (file_to_df, change_dtypes,
                                      case_indexing_date_time,
                                      fill_missing_ucr_and_shootings,
                                      standardize_streets,
                                      fill_missing_by)
from data.shithole import fill_missing_lat_long_by_street_II


class TestCleaningDataFuncs:

    load_dotenv()
    @pytest.fixture
    def load_data(self):
        df = file_to_df("YEAR_2015", separator=',')
        return df

    @pytest.fixture
    def updated_data(self, load_data):
        """
        Returns DataFrame cleaned with previously tested functions,
        necessary for further testing/
        """
        load_data = case_indexing_date_time(load_data)
        load_data = fill_missing_ucr_and_shootings(load_data)
        load_data = standardize_streets(load_data)
        return load_data


    def test_loads_dataframe(self, load_data):
        assert type(load_data) == pd.DataFrame

    def test_columns_case(self, load_data):
        load_data = case_indexing_date_time(load_data)
        assert all(load_data.columns.str.isupper())
        assert 'LOCATION' not in load_data.columns
        assert load_data['DAY_OF_WEEK'].dtype != 'str'
        assert all(load_data['DAY_OF_WEEK'].isin([1,2,3,4,5,6,7]))

    def test_fill_shootings_and_ucr_with_proper_values(self, load_data):
        load_data = fill_missing_ucr_and_shootings(load_data)
        assert any(load_data['SHOOTING'].isnull()) == False
        load_data['SHOOTING'] = load_data['SHOOTING'].astype('Int32')
        assert all(load_data['UCR_PART'].isin(['Part Two', 'Part Three', 'Part One', 'Other']))

    def test_datatypes(self, updated_data):
        df = change_dtypes(updated_data)
        assert df['DATE'].dtype == 'datetime64[ns]'
        assert df['HOUR'].dtype == 'Int32'
        assert df['MONTH'].dtype == 'Int32'
        assert df['YEAR'].dtype == 'Int32'
        assert df['OFFENSE_CODE'].dtype == 'Int32'
        assert df['LAT'].dtype == 'float64'
        assert df['LONG'].dtype == 'float64'
        assert df['REPORTING_AREA'].dtype == 'Int32'
        assert df['SHOOTING'].dtype == 'Int32'


    def test_filling_missing_location_by_street(self, updated_data):
        """test if function inserts data to all missing lat and long fields
            by counting mean for each street"""

        test_dataframe = updated_data.loc[:, ['STREET', 'LAT', 'LONG']]
        missing_before = test_dataframe[(test_dataframe['LAT'].isnull()) & (test_dataframe['LONG'].isnull())]
        test_dataframe, street_dict = fill_missing_lat_long_by_street_II(test_dataframe)
        for key in street_dict.keys():
            assert all(test_dataframe.loc[test_dataframe['STREET'] == key]) is not pd.NA
        assert len(missing_before) != len(test_dataframe[(test_dataframe['LAT'].isnull()) & (test_dataframe['LONG'].isnull())])


    def test_filling_missing_by(self, updated_data):

        missing_before = updated_data[updated_data['DISTRICT'].isnull()]
        updated_data, dictionary = fill_missing_by(updated_data, 'district', 'street')
        for row in updated_data.loc[updated_data['DISTRICT'].notnull()]['DISTRICT']:
            assert row in dictionary.keys()
        assert len(missing_before) != len(updated_data[updated_data['DISTRICT'].isnull()])

if __name__ == '__main__':
    pytest.main()


