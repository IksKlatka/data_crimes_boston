import asyncio
from asyncio import run, create_task, gather
from models import Area, Incident, Offense
from db_service import AreaManagement, IncidentManagement, OffenseManagement
from separating_for_models import to_models
from data.cleaning_algorithms import file_to_df


async def import_areas(areas: list[Area]):

    db = AreaManagement()
    await db.connect()

    tasks = []
    for a, area in enumerate(areas):
        tasks.append(create_task(db.create_area(area)))
        if a % 100 == 0:
            print(f'import done in {a/len(areas)*100:.1f}%')

    await gather(*tasks)
    print('areas imported successfully')
    await db.disconnect()

async def import_offenses(offenses: list[Offense]):

    db = OffenseManagement()
    await db.connect()

    tasks = []
    for o, offense in enumerate(offenses):
        tasks.append(create_task(db.create_offense(offense)))
        if o % 100 == 0:
            print(f'import done in {o/len(offenses)*100:.1f}%')

    await gather(*tasks)
    print('offenses imported successfully')
    await db.disconnect()


async def import_incidents(incidents: list[Incident]):

    db = IncidentManagement()
    await db.connect()

    tasks = []
    for i, incident in enumerate(incidents):
        tasks.append(create_task(db.create_incident(incident)))
        if i % 100 == 0:
            print(f'import done in {i/len(incidents)*100:.1f}%')

    # note: why does it take so much time to gather tasks? ~7min
    await gather(*tasks)
    print('incidents imported successfully')
    await db.disconnect()


async def _main():
    ar, off, inc = to_models(file_to_df("ALL_YEARS", separator=';'))
    await import_areas(areas=ar)
    await import_offenses(offenses=off)
    await import_incidents(incidents=inc)

if __name__ == '__main__':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    run(_main())