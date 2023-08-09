from cleaning_data.repetitive import *
from nosqlalchemy_import.db_models import Area, Incident, Offense
from sqlalchemy_import.sqlalchemy_models import Area as AreasSQL
from sqlalchemy_import.sqlalchemy_models import Incident as IncidentSQL
from sqlalchemy_import.sqlalchemy_models import Offense as OffenseSQL



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
        incidents.append(Incident(id=i+1,
                                  incident_number=inc['INCIDENT_NUMBER'],
                                  offense_code=inc['OFFENSE_CODE'],
                                  area_id=inc['STREET'],
                                  shooting=inc['SHOOTING'],
                                  date=inc['DATE'],
                                  time=inc['TIME'],
                                  day_of_week=inc['DAY_OF_WEEK']))

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


def to_models():

    ar = to_area_model(area_group)
    of = to_offense_model(offence_group)
    new_ig = assign_district_and_street_ids(to_incident_model(incident_group), ar)


    return ar, new_ig, of


if __name__ == '__main__':
    config()
    df = file_to_df("CLEANED_2016", separator=';')

    area_group = sep_areas(df)
    offence_group = sep_offenses(df)
    incident_group = sep_incidents(df)

    areas_dc, incidents_dc, offenses_dc = to_models()

    for i, ar in enumerate(areas_dc):
        print()