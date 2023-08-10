import os
import asyncio
from asyncio import run
from typing import Optional

from sqlalchemy import update
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from dotenv import load_dotenv

from models import Offense



class OffenseDAL:
    def __init__(self):
        load_dotenv()
        self.engine = create_async_engine(os.getenv("PSQL_URL", None), echo=True)
        self.async_session = async_sessionmaker(self.engine, expire_on_commit=False)

        print("Connected!")

    async def get_offense(self, offense_code: int) -> Offense:
        async with self.async_session() as session:
            result = await session.execute(select(Offense).where(Offense.code == offense_code))
            return result.scalars().first()

    async def get_all_offenses(self, limit: int) -> list[Offense]:
        async with self.async_session() as session:
            result = await session.execute(select(Offense).order_by(Offense.id).limit(limit))
            return result.scalars().all()

    async def insert_offense(self, offense: Offense) -> Offense:
        async with self.async_session() as session:
            async with session.begin():
                session.add(offense)
                await session.commit()

    async def upsert_offense(self, code: int, code_group: Optional[str], description: Optional[str]) -> Offense:
        old_offense = await self.get_offense(code)
        if not old_offense:
            async with self.async_session() as session:
                async with session.begin():
                    session.add(Offense(code=code,
                                        code_group=code_group,
                                        description=description))
                    await session.commit()
        else:
            async with self.async_session() as session:
                async with session.begin():
                    new_offense = update(Offense).where(Offense.code == code)
                    if code_group:
                        new_offense = new_offense.values(code_group= code_group)
                    if description:
                        new_offense = new_offense.values(description= description)
                    await session.execute(new_offense)

    async def delete_offense(self, code: int):
        offense = await self.get_offense(offense_code=code)
        async with self.async_session() as session:
            async with session.begin():
                await session.delete(offense)
                await session.commit()


async def main_():

    offense_service = OffenseDAL()
    s = await offense_service.get_all_offenses(10)
    await offense_service.upsert_offense(code=3, code_group='666', description='exorcism')
    # await offense_service.delete_offense(code=3)

if __name__ == '__main__':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    run(main_())
