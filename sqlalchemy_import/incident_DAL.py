import datetime
import os
import asyncio
from asyncio import run
from typing import Optional

from sqlalchemy import update
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from dotenv import load_dotenv

from sqlalchemy_models import Incident


class IncidentDAL:

    def __init__(self):
        load_dotenv()
        self.engine = create_async_engine(os.getenv("PSQL_URL", None))
        self.async_session = async_sessionmaker(self.engine, expire_on_commit=False)

        print("Connected!")

    # todo: GET by number
    async def get_incident(self, incident_id: int) -> Incident:
        async with self.async_session() as session:
            result = await session.execute(select(Incident).where(Incident.id == incident_id))
            return result.scalars().first()

    async def get_all_incidents(self, limit: int) -> list[Incident]:
        async with self.async_session() as session:
            result = await session.execute(select(Incident).order_by(Incident.id).limit(limit))
            return result.scalars().all()


    async def insert_incident(self, incident: Incident) -> Incident:
        async with self.async_session() as session:
            async with session.begin():
                session.add(incident)
                await session.commit()

    async def update_incident(self, incident_id: int, number: Optional[str],
                              offense_code: Optional[int], area_id: Optional[int],
                              shooting: Optional[int], date: Optional[datetime.date],
                              time: Optional[str], day_of_week: Optional[int]):
        if not self.get_incident(incident_id):
            return False
        async with self.async_session() as session:
            async with session.begin():
                updated_incident = update(Incident).where(Incident.id==incident_id)
                if number:
                    updated_incident = updated_incident.values(number=number)
                if offense_code:
                    updated_incident = updated_incident.values(offense_code=offense_code)
                if area_id:
                    updated_incident = updated_incident.values(area_id=area_id)
                if shooting:
                    updated_incident = updated_incident.values(shooting=shooting)
                if date:
                    updated_incident = updated_incident.values(date=date)
                if time:
                    updated_incident = updated_incident.values(time=time)
                if day_of_week:
                    updated_incident = updated_incident.values(day_of_week=day_of_week)
                await session.execute(updated_incident)

    # todo: DELETE by number,
    async def delete_incident(self, incident_id: int):
        incident = await self.get_incident(incident_id=incident_id)
        async with self.async_session() as session:
            async with session.begin():
                await session.delete(incident)
                await session.commit()

async def main_():
    db_service = IncidentDAL()
    await db_service.insert_incident(Incident(number='ABC12345', offense_code=3, area_id=1,
                                              shooting=0, date=datetime.date(2023,1,1), time='00:00:01',
                                              day_of_week=7))

if __name__ == '__main__':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    run(main_())