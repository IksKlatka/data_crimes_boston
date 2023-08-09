import asyncio
from asyncio import run

from cleaning_data.repetitive import file_to_df
from sqlalchemy_import.incident_DAL import IncidentDAL
from dataclasses_to_models import to_incident_model, to_area_model, assign_district_and_street_ids, sep_incidents, \
    sep_areas


async def import_incidents():

    db = IncidentDAL()

    df = file_to_df("CLEANED_2015", separator=';')
    areas = to_area_model(sep_areas(df))
    incidents = assign_district_and_street_ids(to_incident_model(sep_incidents(df)), areas)

    for i, dic in enumerate(incidents):
        await db.insert_incident(dic)
        if i % 100 == 0:
            print(f'Import incidents in {i/len(incidents) * 100:.1f}% done.')

if __name__ == '__main__':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    run(import_incidents())