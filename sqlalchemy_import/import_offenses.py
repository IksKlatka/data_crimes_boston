import asyncio
from asyncio import run

from cleaning_data.repetitive import file_to_df
from sqlalchemy_import.offense_DAL import OffenseDAL
from dataclasses_to_models import to_offense_model, sep_offenses


async def import_offenses():
    db = OffenseDAL()

    df = file_to_df("CLEANED_2015", separator=';')
    areas = to_offense_model(sep_offenses(df))

    for i, dic in enumerate(areas):
        await db.insert_offense(dic)
        if i % 100 == 0:
            print(f"import areas in {i / len(areas) * 100:.1f}% done.")


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    run(import_offenses())
