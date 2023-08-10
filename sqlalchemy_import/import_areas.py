import asyncio
from asyncio import run

from sqlalchemy_import.dataclasses_to_models import sep_areas
from cleaning_data.repetitive import file_to_df
from sqlalchemy_import.area_DAL import AreaDAL
from dataclasses_to_models import to_area_model
from models import Incident, Area

async def import_areas():
    db = AreaDAL()

    df = file_to_df("CLEANED_2016", separator=';')
    areas = to_area_model(sep_areas(df))

    for i, dic in enumerate(areas):
        await db.insert_area(dic)
        if i % 100 == 0:
            print(f"import areas in {i / len(areas) * 100:.1f}% done.")


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    run(import_areas())
