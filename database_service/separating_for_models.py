import pandas as pd

from data.cleaning_algorithms import *
# from database_service.dataclass_models import Incident, Area, Offense
from database_service.models import Incident, Area, Offense

def sep_areas(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe[['STREET',
                           'REPORTING_AREA',
                           'DISTRICT']].drop_duplicates()

    areas = dataframe.to_dict(orient='records')

    return areas

def sep_offenses(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe[['OFFENSE_CODE',
                           'OFFENSE_CODE_GROUP',
                           'OFFENSE_DESCRIPTION']]

    dataframe = dataframe.groupby('OFFENSE_CODE').agg({
        'OFFENSE_CODE_GROUP': 'first',
        'OFFENSE_DESCRIPTION': 'first',
    }).reset_index()

    offenses = dataframe.to_dict(orient="records")

    return offenses

def sep_incidents(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe[['INCIDENT_NUMBER',
                           'OFFENSE_CODE',
                           'DISTRICT',
                           'REPORTING_AREA',
                           'SHOOTING',
                           'DATE',
                           'TIME',
                           'DAY_OF_WEEK']].drop_duplicates()

    incidents = dataframe.to_dict(orient='records')

    return incidents

def to_area_model(area_g: dict) -> list[Area]:
    areas = []
    for i, area in enumerate(area_g):
        areas.append(Area(reporting_area=area['REPORTING_AREA'],
                          street=area['STREET'],
                          district=area['DISTRICT']))

    return areas

def to_offense_model(offense_g: dict) -> list[Offense]:
    offenses = []
    for i, off in enumerate(offense_g):
        offenses.append(Offense(id=i+1,
                                code=off['OFFENSE_CODE'],
                                code_group=off['OFFENSE_CODE_GROUP'],
                                description=off['OFFENSE_DESCRIPTION']))

    return offenses

def to_incident_model(incident_g: dict) -> list[Incident]:
    incidents = []

    for i, inc in enumerate(incident_g):
        incidents.append(Incident(id=i+1,
                                  incident_number=inc['INCIDENT_NUMBER'],
                                  offence_code=inc['OFFENSE_CODE'],
                                  reporting_area=inc['REPORTING_AREA'],
                                  shooting=inc['SHOOTING'],
                                  date=inc['DATE'],
                                  time=inc['TIME'],
                                  day_of_week=inc['DAY_OF_WEEK']))

    return incidents

def to_models():

    area_group = sep_areas(df)
    offence_group = sep_offenses(df)
    incident_group = sep_incidents(df)

    ar = to_area_model(area_group)
    off = to_offense_model(offence_group)
    ig = to_incident_model(incident_group)

    return ar, off, ig


if __name__ == '__main__':
    config()
    df = file_to_df("ALL_YEARS", separator=';')
    ar, off, ig = to_models()

    print(ar[0])