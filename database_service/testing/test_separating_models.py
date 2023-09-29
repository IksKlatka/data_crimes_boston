import random

import pandas as pd
import pytest

from data.cleaning_algorithms import file_to_df
from database_service.separating_for_models import sep_areas, sep_offenses, sep_incidents, assign_district_and_street_ids
from database_service.separating_for_models import to_area_model, to_offense_model, to_incident_model
from database_service.models import Area, Incident, Offense

class TestTypes:
    @pytest.fixture
    def dataframe(self):
        df = file_to_df('CLEANED_2015', separator=';')
        return df

    @pytest.fixture
    def list_models(self, dataframe):
        areas = sep_areas(dataframe)
        offenses = sep_offenses(dataframe)
        incidents = sep_incidents(dataframe)

        return areas, offenses, incidents


    def test_separation_types(self, dataframe):
        areas = sep_areas(dataframe)
        offenses = sep_offenses(dataframe)
        incidents = sep_incidents(dataframe)
        assert type(areas) == list and type(areas[0]) == dict
        assert type(offenses) == list and type(offenses[0]) == dict
        assert type(incidents) == list and type(incidents[0]) == dict

    def test_models_types(self, list_models):
        areas, offenses, incidents = list_models
        areas = to_area_model(areas)
        offenses = to_offense_model(offenses)
        incidents = to_incident_model(incidents)

        assert type(areas[0]) == database_service.models.Area
        assert type(offenses[0]) == database_service.models.Offense
        assert type(incidents[0]) == database_service.models.Incident


class TestSeparatingAreas:

    @pytest.fixture
    def load_data(self):
        df = file_to_df("CLEANED_2020", separator=';')
        return df

    def test_has_only_valid_columns(self, load_data):
        areas = sep_areas(load_data)
        valid = ['REPORTING_AREA', 'STREET', 'DISTRICT']
        assert any(['DATE', 'TIME', 'DAY_OF_WEEK', 'OFFENSE_CODE','OFFENSE_CODE_GROUP', 'OFFENSE_DESCRIPTION',
                    'SHOOTING', 'MONTH', 'YEAR', 'HOUR', 'LAT', 'LONG',
                    'INCIDENT_NUMBER', 'UCR_PART']) not in areas[random.randint(0,len(areas)-1)].keys()
        for i in areas[random.randint(0,len(areas)-1)].keys():
            assert i in valid


    def test_sep_areas(self):

        data = pd.DataFrame(
                {'STREET': ['Street1', 'Street2', 'Street1'],
                'REPORTING_AREA': [1, 2, 1],
                'DISTRICT': ['A', 'B', 'A']})

        result = sep_areas(data)

        assert len(result) == 2
        assert result[0]['STREET'] == 'Street1'
        assert result[1]['STREET'] == 'Street2'


    def test_to_area_model(self):
        area_group = [{'STREET': 'Street1', 'REPORTING_AREA': 1, 'DISTRICT': 'A'},
                      {'STREET': 'Street2', 'REPORTING_AREA': 2, 'DISTRICT': 'B'}]

        result = to_area_model(area_group)

        assert isinstance(result[0], Area)
        assert isinstance(result[1], Area)

        assert result[0].street == 'Street1'
        assert result[1].street == 'Street2'

class TestSeparatingOffenses:
    @pytest.fixture
    def load_data(self):
        df = file_to_df("CLEANED_2021", separator=';')
        return df

    def test_has_only_valid_columns(self, load_data):
        offenses = sep_offenses(load_data)
        valid = ['OFFENSE_CODE','OFFENSE_CODE_GROUP', 'OFFENSE_DESCRIPTION']
        assert any(['DATE', 'TIME', 'DAY_OF_WEEK', 'REPORTING_AREA', 'STREET', 'DISTRICT',
                    'SHOOTING', 'MONTH', 'YEAR', 'HOUR', 'LAT', 'LONG',
                    'INCIDENT_NUMBER', 'UCR_PART']) not in offenses[random.randint(0,len(offenses)-1)].keys()
        for i in offenses[random.randint(0,len(offenses)-1)].keys():
            assert i in valid


    def test_sep_offenses(self):
        data = pd.DataFrame(
                {'OFFENSE_CODE': [100, 200, 100],
                'OFFENSE_CODE_GROUP': ['Group1', 'Group2', 'Group1'],
                'OFFENSE_DESCRIPTION': ['Desc1', 'Desc2', 'Desc1']})

        result = sep_offenses(data)

        assert len(result) == 2
        assert result[0]['OFFENSE_CODE'] == 100
        assert result[1]['OFFENSE_CODE'] == 200

    def test_to_offense_model(self):
        offense_group = [{'OFFENSE_CODE': 111, 'OFFENSE_CODE_GROUP': 1, 'OFFENSE_DESCRIPTION': 'A'},
                     {'OFFENSE_CODE': 121, 'OFFENSE_CODE_GROUP': 2, 'OFFENSE_DESCRIPTION': 'A'},
                     {'OFFENSE_CODE': 111, 'OFFENSE_CODE_GROUP': 1, 'OFFENSE_DESCRIPTION': 'A'}]

        result = to_offense_model(offense_group)

        assert isinstance(result[0], Offense)
        assert isinstance(result[1], Offense)

        assert result[0].code == 111
        assert result[1].code == 121


class TestSeparationIncidents:
    @pytest.fixture
    def load_data(self):
        df = file_to_df("CLEANED_2017", separator=';')
        return df

    def test_has_only_valid_columns(self, load_data):
        incidents = sep_incidents(load_data)
        valid = ['INCIDENT_NUMBER', 'OFFENSE_CODE', 'DISTRICT', 'STREET', 'SHOOTING', 'DATE', 'TIME', 'DAY_OF_WEEK']
        assert any(['OFFENSE_CODE_GROUP', 'OFFENSE_DESCRIPTION', 'REPORTING_AREA', 'MONTH', 'YEAR', 'HOUR', 'LAT',
                    'LONG', 'UCR_PART'])  not in incidents[random.randint(0,len(incidents)-1)].keys()

        for i in incidents[random.randint(0,len(incidents)-1)].keys():
            assert i in valid

    def test_sep_incidents(self):
        data = pd.DataFrame({'INCIDENT_NUMBER': ['Inc1', 'Inc2', 'Inc1'],
                'OFFENSE_CODE': [100, 200, 100],
                'DISTRICT': ['A', 'B', 'A'],
                'STREET': ['Street1', 'Street2', 'Street1'],
                'SHOOTING': [True, False, True],
                'DATE': ['2023-09-17', '2023-09-18', '2023-09-17'],
                'TIME': ['10:00', '11:00', '10:00'],
                'DAY_OF_WEEK': ['Monday', 'Tuesday', 'Monday']})

        result = sep_incidents(data)
        assert len(result) == 2
        assert result[0]['INCIDENT_NUMBER'] == 'Inc1'
        assert result[1]['INCIDENT_NUMBER'] == 'Inc2'

        return result

    def test_to_incident_model(self, load_data):
        incidents = load_data[:3]
        incident_group = sep_incidents(incidents)
        result = to_incident_model(incident_group)

        assert len(result) == 3
        assert isinstance(result[0], Incident)
        assert isinstance(result[2], Incident)

        assert result[0].incident_number == 'I182050863'
        assert result[1].incident_number == 'I172000161'
        assert result[2].incident_number == 'I172000150'

    def test_assigning_districts_and_streets_to_incidents(self):

        incidents = [Incident(1, 'ABC1', '1', 'no', True, '2017-09-09', '00:01:00', 3),
                     Incident(2, 'ABC2', '1', 'no', True, '2017-09-19', '00:02:00', 3),
                     Incident(3, 'ABC3', '1', 'fck no', True, '2017-09-29', '00:03:00', 3)]

        areas = [Area(1, 'no', 122, 'A11'), Area(3, 'fck no', 111, 'C11')]

        new_incidents = assign_district_and_street_ids(incidents, areas)

        assert len(new_incidents) == 3
        assert new_incidents[0].area_id == 1
        assert new_incidents[1].area_id == 1
        assert new_incidents[2].area_id == 3