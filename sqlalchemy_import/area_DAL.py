import os
import asyncio
from asyncio import run
from typing import Optional

from sqlalchemy import update
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from dotenv import load_dotenv

from models import Area


class AreaDAL:

    def __init__(self):
        load_dotenv()
        self.engine = create_async_engine(os.getenv("PSQL_URL", None)) #, echo=True
        self.async_session = async_sessionmaker(self.engine, expire_on_commit=False)

        print("Connected!")

    # todo: GET by street, reporting_area, district
    async def get_area(self, area_id: int) -> Area:
        async with self.async_session() as session:
            result = await session.execute(select(Area).where(Area.id == area_id))
            return result.scalars().first()

    async def get_all_areas(self, limit: int) -> list[Area]:
        async with self.async_session() as session:
            result = await session.execute(select(Area).order_by(Area.id).limit(limit))
            return result.scalars().all()

    async def insert_area(self, area: Area) -> Area:
        async with self.async_session() as session:
            async with session.begin():
                session.add(area)
                await session.commit()

    async def upsert_area(self, area_id: int, street: Optional[str],
                          reporting_area: Optional[str], district: Optional[str]) -> Area:
        old_offense = await self.get_area(area_id)
        if not old_offense:
            await self.insert_area(Area(street=street, reporting_area=reporting_area, district=district))
        else:
            async with self.async_session() as session:
                async with session.begin():
                    updated_area = update(Area).where(Area.id == area_id)
                    if street:
                        updated_area = updated_area.values(street=street)
                    if reporting_area:
                        updated_area = updated_area.values(reporting_area=reporting_area)
                    if district:
                        updated_area = updated_area.values(district=district)
                    await session.execute(updated_area)

    # todo: DELETE by street, reporting_area, district
    async def delete_area(self, area_id: int):
        area = await self.get_area(area_id=area_id)
        async with self.async_session() as session:
            async with session.begin():
                await session.delete(area)
                await session.commit()



async def main_():
    db_service = AreaDAL()
    await db_service.insert_area(Area(street="test", reporting_area=111, district="c11"))


if __name__ == '__main__':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    run(main_())