import asyncio
import datetime
import os
from asyncio import run
from typing import Optional

import asyncpg
from sqlalchemy import MetaData, Table, select, insert, update, delete
import sqlalchemy.ext.declarative
from databases import Database
from dotenv import load_dotenv
from database_service.models import Area, Incident, Offense

load_dotenv()
URL = os.getenv("DB_URL", None)


class DatabaseInitializer:
    def __init__(self):
        self.db = Database(URL)
        self.metadata = MetaData()

    async def connect(self):
        await self.db.connect()

    async def disconnect(self):
        await self.db.disconnect()


class AreaManagement(DatabaseInitializer):
    """Defined to perform CRUD on 'areas' table in database."""

    async def get_area(self, reporting_area: int):
        """Get an offense by offense_id (PK)."""

        stmt = select(Area).where(Area.reporting_area == reporting_area)
        try:
            result = await self.db.fetch_one(stmt)
            if result:
                return f"Area: {(tuple(result.values()))}"
            return None
        except Exception as e:
            return f"An Exception occurred: {str(e)}"

    async def get_areas(self, offset: int = 0, limit: int = 100):
        # note: todo: może to nie będzie działało, gdy mam za mało rekordów w bazie?

        stmt = select(Area).limit(limit).offset(offset)
        try:
            result = await self.db.fetch_all(stmt)
            if result:
                return Area(*dict(result))
            return None
        except KeyError as e:
            return f"An Exception occurred: {str(e)}"

    async def create_area(self, area: Area):
        stmt = insert(Area).values(
            reporting_area = area.reporting_area,
            street = area.street,
            district = area.district
        )
        try:
            result = await self.db.execute(stmt)
            return "Area record created successfully"
        except Exception as e:
            return f"An error occurred while performing INSERT operation: {str(e)}"

    async def update_area(self, reporting_area: int, area: Area):
        record = await self.get_area(reporting_area)
        if record == Area:
            stmt = update(Area).where(Area.reporting_area == reporting_area) \
            .values(
                reporting_area= area.reporting_area,
                street= area.street,
                district= area.district
            )
            try:
                result = await self.db.execute(stmt)
                return "Area record updated successfully."
            except Exception as e:
                return f"An error occurred while performing UPDATE operation: {str(e)}"
        else:
            return f"No Area record with reporting_area = {reporting_area}."
    async def delete_area(self, reporting_area: int):
        record = await self.get_area(reporting_area)
        if record is not None:
            stmt = delete(Area).where(Area.reporting_area==reporting_area)
            try:
                result = await self.db.execute(stmt)
                if await self.get_area(reporting_area) is None:
                    return f"Area record with reporting_area = {reporting_area} deleted successfully."
                else:
                    return f"Couldn't delete Area with reporting_area = {reporting_area}"
            except Exception as e:
                return f"An Exception occurred while performing DELETE operation: {str(e)}"


#todo: Incidents

class IncidentManagement(DatabaseInitializer):
    async def get_incident(self, incident_number: str):
        """Get incident based on incident_number."""

        stmt = select(Incident).where(Incident.incident_number == incident_number)
        try:
            result = await self.db.fetch_one(stmt)
            if result:
                return f"Incident: {(tuple(result.values()))}"
            return None
        except Exception as e:
            return f"An Exception occurred: {str(e)}"

    async def get_incidents(self, offset: int = 0, limit: int = 100):
        # note: todo: może to nie będzie działało, gdy mam za mało rekordów w bazie?

        stmt = select(Incident).limit(limit).offset(offset)
        try:
            result = await self.db.fetch_all(stmt)
            if result:
                return Incident(*dict(result))
            return None
        except KeyError as e:
            return f"An Exception occurred: {str(e)}"

    async def create_incident(self, incident: Incident):
        stmt = insert(Incident).values(
            incident_number = incident.incident_number,
            offense_code = incident.offense_code,
            reporting_area = incident.reporting_area,
            shooting = incident.shooting,
            date = incident.date,
            time = incident.time,
            day_of_week = incident.day_of_week
        )
        try:
            result = await self.db.execute(stmt)
            return "Incident record created successfully."
        except Exception as e:
            return f"An error occurred while performing INSERT operation: {str(e)}"

    async def update_incident(self, incident_number: str, incident: Incident):
        """Updated incident based on incident_number."""

        record = await self.get_incident(incident_number)
        if record is not None:
            stmt = update(Incident).where(Incident.incident_number == incident_number) \
            .values(
                    incident_number = incident.incident_number,
                    offense_code = incident.offense_code,
                    reporting_area = incident.reporting_area,
                    shooting = incident.shooting,
                    date = incident.date,
                    time = incident.time,
                    day_of_week = incident.day_of_week
                )
            try:
                result = await self.db.execute(stmt)
                return "Incident record updated successfully."
            except Exception as e:
                return f"An error occurred while performing UPDATE operation: {str(e)}"
        else:
            return f"No Incident record with number = {incident_number}."
    async def delete_incident(self, incident_number: str):
        """Deletes incident based on incident_number"""

        record = await self.get_incident(incident_number)
        if record is not None:
            stmt = delete(Incident).where(Incident.incident_number == incident_number)
            try:
                result = await self.db.execute(stmt)
                if await self.get_incident(incident_number) is None:
                    return f"Incident record with number = {incident_number} deleted successfully."
                else:
                    return f"Couldn't delete Incident with number = {incident_number}."
            except Exception as e:
                return f"An Exception occurred while performing DELETE operation: {str(e)}"

class OffenseManagement(DatabaseInitializer):
    async def get_offense(self, offense_id: int):
        """Get offense from table by id."""

        stmt = select(Offense).where(Offense.id == offense_id)
        try:
            result = await self.db.fetch_one(stmt)
            if result:
                return f"Offense: {(tuple(result.values()))}"
            return None
        except Exception as e:
            return f"An Exception occurred: {str(e)}"

    async def get_offenses(self, offset: int = 0, limit: int = 100):
        # note: todo: może to nie będzie działało, gdy mam za mało rekordów w bazie?

        stmt = select(Offense).limit(limit).offset(offset)
        try:
            result = await self.db.fetch_all(stmt)
            if result:
                return [Offense(*dict(row)) for row in result]
            return None
        except KeyError as e:
            return f"An Exception occurred: {str(e)}"

    async def create_offense(self, offense: Offense):
        stmt = insert(Offense).values(
            code = offense.code,
            code_group = offense.code_group,
            description = offense.description
        )
        try:
            result = await self.db.execute(stmt)
            return "Offense record created successfully."
        except Exception as e:
            return f"An Exception occurred while performing INSERT operation: {str(e)}"

    async def update_offense(self, offense_id: int, offense: Offense):
        record = await self.get_offense(offense_id)
        if record == Offense:
            stmt = update(Offense).where(Offense.id == offense_id) \
                .values(
                    code = offense.code,
                    code_group = offense.code_group,
                    description = offense.description
                )
            try:
                result = await self.db.execute(stmt)
                return "Offense record updated successfully."
            except Exception as e:
                return f"An Exception occurred while performing UPDATE operation: {str(e)}"
        else:
            return f"No Offense record with id = {offense_id}."

    async def delete_offense(self, offense_id: int):
        record = await self.get_offense(offense_id)
        if record is not None:
            stmt = delete(Offense).where(Offense.id == offense_id)
            try:
                result = await self.db.execute(stmt)
                if await self.get_offense(offense_id) is None:
                    return f"Offense record with id = {offense_id} deleted successfully."
                else:
                    return f"Couldn't delete Offense with id = {offense_id}."
            except Exception as e:
                return f"An Exception occurred while performing DELETE operation: {str(e)}"


async def _main():
    am = AreaManagement()
    im = IncidentManagement()
    om = OffenseManagement()

    i = Incident()
    i.incident_number = "ABC123456"
    i.offense_code = 666
    i.reporting_area = 111
    i.shooting = False
    i.date = datetime.date(2021,11,11)
    i.time = datetime.time(10,10,11)
    i.day_of_week = 4

    a = Area()
    a.reporting_area = 111
    a.street = 'nowhere st'
    a.district = 'C11'

    o = Offense()
    o.code = 666
    o.code_group = 'religion violence'
    o.description = 'czary mary'

    # await am.connect()
    # print(await am.create_area(a))
    # print(await am.get_area(111))
    # await am.disconnect()

    # await om.connect()
    # print(await om.create_offense(o))
    # print(await om.get_offense(1))
    # await om.disconnect()

    await im.connect()
    # print(await im.create_incident(i))
    print(await im.get_incident("ABC123456"))
    # print(await im.update_incident("ABC123456", i))
    # print(await im.delete_incident("ABC123456"))
    await im.disconnect()



if __name__ == '__main__':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    run(_main())