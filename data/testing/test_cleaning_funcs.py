import pytest
from data.cleaning_algorithms import *


class TestCleaningDataFuncs:

    @pytest.fixture
    def load_data(self):
        df = file_to_df("TESTING_CSV", separator=';')
        # return df[75000:85000][:]
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
        assert len(load_data.columns) == 17

    def test_split_date_time(self, load_data):
        load_data = spit_date_and_time(load_data)
        assert 'OCCURRED_ON_DATE' not in load_data.columns
        assert 'TIME' and 'DATE' in load_data.columns

    def test_drop_columns_change_case(self, load_data):
        load_data = case_indexing_date_time(load_data)
        assert all(load_data.columns.str.isupper())
        assert all(load_data['STREET'].str.title())
        assert 'LOCATION' not in load_data.columns
        assert load_data['DAY_OF_WEEK'].dtype != 'str'
        assert all(load_data['DAY_OF_WEEK'].isin([1,2,3,4,5,6,7]))

    def test_fill_shootings_and_ucr_with_proper_values(self, load_data):
        load_data = fill_missing_ucr_and_shootings(load_data)
        assert any(load_data['SHOOTING'].isnull()) == False
        load_data['SHOOTING'] = load_data['SHOOTING'].astype('Int32')
        assert all(load_data['UCR_PART'].isin(['Part Two', 'Part Three', 'Part One', 'Other']))

    def test_standardize_streets(self, load_data):
        load_data = case_indexing_date_time(load_data)
        load_data = standardize_streets(load_data)
        endings = ['St', 'Blvd', 'Ave', 'Ct', 'Dr', 'Rd', 'Sq', 'Brg', 'Pl']
        for street in load_data['STREET']:
            street = street.split()
            assert any(endings) not in street


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

        missing_before = updated_data[(updated_data['LAT'].isnull()) & (updated_data['LONG'].isnull())]
        new_updated_data, street_dict = fill_missing_lat_long_by(updated_data, 'STREET')
        for key in street_dict.keys():
            assert all(new_updated_data.loc[new_updated_data['STREET'] == key]) is not pd.NA
        assert len(missing_before) != \
               len(updated_data[(updated_data['LAT'].isnull()) & (updated_data['LONG'].isnull())])
        assert len(new_updated_data[(new_updated_data['STREET'].isnull())]) == \
               len(updated_data[(updated_data['STREET'].isnull())])


    def test_filling_missing_by(self, updated_data):

        missing_before = updated_data[updated_data['DISTRICT'].isnull()]
        missing_streets = updated_data[updated_data['STREET'].isnull()]
        updated_data, key_dict = fill_missing_by(updated_data, 'district', 'street')
        for district in updated_data.loc[updated_data['DISTRICT'].notnull()]['DISTRICT']:
            assert district in key_dict.keys()

        assert pd.NA not in key_dict.keys()
        # district values have changed
        assert len(missing_before) != len(updated_data[updated_data['DISTRICT'].isnull()])
        # street values remain the same
        assert len(missing_streets) == len(updated_data[updated_data['STREET'].isnull()])



if __name__ == '__main__':
    pytest.main()



