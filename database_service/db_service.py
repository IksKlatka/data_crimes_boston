import asyncio
import os
from asyncio import run

import asyncpg
from sqlalchemy import MetaData, select, insert, update, delete
from databases import Database
from dotenv import load_dotenv
from database_service.models import Area

load_dotenv()
URL = os.getenv("DB_URL", None)


class DatabaseInitializer:
    def __init__(self):
        self.db = Database(URL)
        metadata = MetaData()

    async def connect(self):
        await self.db.connect()

    async def disconnect(self):
        await self.db.disconnect()


class AreaManagement(DatabaseInitializer):
    """Defined to perform CRUD on 'areas' table in database."""
    async def get_area(self, reporting_area: int):
        """Get an area by reporting_area (PK)."""

        stmt = select(Area).where(Area.reporting_area == reporting_area)
        try:
            result = await self.db.fetch_one(stmt)
            if result:
                return Area(*dict(result))
            return None
        except Exception as e:
            return str(e)
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
        if record is not None:
            stmt = update(Area).where(Area.reporting_area == reporting_area) \
            .values(reporting_area= area.reporting_area,
                    street= area.street,
                    district= area.district)
            try:
                result = await self.db.execute(stmt)
                return "Area record updated successfully"
            except Exception as e:
                return f"An error occurred while performing UPDATE operation: {str(e)}"
        else:
            return f"No Area record with reporting area = {reporting_area}"
    async def delete_area(self, reporting_area: int):
        record = await self.get_area(reporting_area)
        if record is not None:
            stmt = delete(Area).where(Area.reporting_area==reporting_area)
            try:
                result = await self.db.execute(stmt)
                if await self.get_area(reporting_area) is None:
                    return f"Area record with reporting_area={reporting_area} deleted successfully."
                else:
                    return f"Couldn't delete Area with reporting_area={reporting_area}"
            except Exception as e:
                return str(e)


#todo: Incidents

class IncidentManagement(DatabaseInitializer):
    async def get_incident(self):
        pass

    async def upsert_incident(self):
        pass

    async def delete_incident(self):
        pass


#todo: Offenses
class OffenseManagement(DatabaseInitializer):
    async def get_offense(self):
        pass

    async def upsert_offense(self):
        pass

    async def delete_offense(self):
        pass


async def _main():
    am = AreaManagement()
    a = Area()
    a.reporting_area = 777
    a.street = 'twoj stary'
    a.district = '420'

    await am.connect()
    # print(await am.create_area(a))
    # print(await am.update_area(777, a))
    # print(await am.get_area(333))
    print(await am.delete_area(777))
    await am.disconnect()

if __name__ == '__main__':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    run(_main())