import datetime

import pandas as pd
from cleaning_data.repetitive import file_to_df
from models import Area, Incident, Offense

def sep_areas(dataframe: pd.DataFrame) -> list[dict]:
    dataframe = dataframe[['STREET',
                           'REPORTING_AREA',
                           'DISTRICT']].drop_duplicates()

    areas = dataframe.to_dict(orient='records')

    return areas

def sep_offenses(dataframe: pd.DataFrame) -> list[dict]:
    dataframe = dataframe[['OFFENSE_CODE',
                           'OFFENSE_CODE_GROUP',
                           'OFFENSE_DESCRIPTION']]

    dataframe = dataframe.groupby('OFFENSE_CODE').agg({
        'OFFENSE_CODE_GROUP': 'first',
        'OFFENSE_DESCRIPTION': 'first',
    }).reset_index()

    offenses = dataframe.to_dict(orient="records")

    return offenses

def sep_incidents(dataframe: pd.DataFrame) -> list[dict]:
    dataframe = dataframe.drop_duplicates(subset='INCIDENT_NUMBER', keep='first')
    dataframe = dataframe[['INCIDENT_NUMBER',
                           'OFFENSE_CODE',
                           'DISTRICT',
                           'STREET',
                           'SHOOTING',
                           'DATE',
                           'TIME',
                           'DAY_OF_WEEK']].drop_duplicates()

    incidents = dataframe.to_dict(orient='records')

    return incidents

def assign_district_and_street_ids(incidents: list[Incident], areas: list[Area]) -> list[Incident]:

    area_dict = {area.street: area.id for area in areas}

    for incident in incidents:
        area_street = incident.area_id
        if area_street in area_dict:
            incident.area_id = area_dict[area_street]
        else:
            incident.area_id = None

    return incidents

def to_area_model(area_g: dict) -> list[Area]:
    areas = []
    for i, area in enumerate(area_g):
        areas.append(Area(id=i+1,
                          street=area['STREET'],
                          reporting_area=area['REPORTING_AREA'],
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
        if inc['SHOOTING'] == 'y':
            inc['SHOOTING'] = 1
        if type(inc['DATE']) == str:
            inc['DATE'] = datetime.datetime.strptime(inc['DATE'], "%Y-%m-%d").date()
        incidents.append(Incident(id=i+1,
                                  number=inc['INCIDENT_NUMBER'],
                                  offense_code=inc['OFFENSE_CODE'],
                                  area_id=inc['STREET'],
                                  shooting=int(inc['SHOOTING']),
                                  date=inc['DATE'],
                                  time=inc['TIME'],
                                  day_of_week=inc['DAY_OF_WEEK']))

    return incidents


def separate_all():
    a = sep_areas(df)
    i = sep_incidents(df)
    o = sep_offenses(df)

    return a, i, o



if __name__ == '__main__':
    df = file_to_df("CLEANED_2015", separator=';')
    areas, incidents, offenses = separate_all()
    areas = to_area_model(area_g= areas)
    offenses = to_offense_model(offense_g= offenses)
    incidents = assign_district_and_street_ids(to_incident_model(incidents), areas)
    print(incidents)
