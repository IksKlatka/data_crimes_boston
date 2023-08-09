import os
import asyncpg
from dotenv import load_dotenv
from db_models import Area, Incident, Offense


class DBService:

    def initialize(self):
        load_dotenv()
        self.pool = await asyncpg.create_pool(os.getenv("PSQL_URL", None), timeout=30,
                                              command_timeout=5)

        print("Connected!")

    # todo: AREAS
    async def get_area_by_id(self, area_id: int) -> Area:
        async with self.pool.acquire() as connection:
            area = await connection.fetchrow("select * from areas where id=$1", area_id)
        return Area(**dict(area))

    async def get_areas_by_street(self, street: str) -> list[Area]:
        async with self.pool.acquire() as connection:
            areas = await connection.fetchrow("select * from areas where street=$1", street)
        return [Area(**dict(a)) for a in areas]

    async def get_all_areas(self, limit: int = 100) -> list[Area]:
        async with self.pool.acquire() as connection:
            areas = await connection.fetch("select * from areas limit=$1", limit)
        return [Area(**dict(a)) for a in areas]

    async def upsert_area(self, area: Area) -> Area:
        if area.id is None:
            async with self.pool.acquire() as connection:
                area = await connection.fetchrow("insert into areas(street, reporting_Area, district) "
                                                 "values ($1, $2, $3)", area.street, area.reporting_area, area.district)

        async with self.pool.acquire() as connection:
            area = await connection.fetchrow("update areas set street=$2, reporting_area=$3, district=$4"
                                             "where id=$1", area.id, area.street, area.reporting_area, area.district)

        return Area(**dict(area))

    async def delete_area(self, area_id: int):
        async with self.pool.acquire() as connection:
            await connection.fetchrow("delete from areas where id=$1", area_id)
        return f"Area {area_id} deleted."

    # todo: OFFENSES

    async def get_offense(self, offense_id: int) -> Offense:
        async with self.pool.acquire() as connection:
            offense = await connection.fetchrow("select * from offenses where id=$1", offense_id)
            return Offense(**dict(offense))

    async def get_all_offenses(self, limit: int = 100) -> list[Offense]:
        async with self.pool.acquire() as connection:
            offenses = await connection.fetch("select * from offenses limit=$1", limit)
            return [Offense(**dict(offense)) for offense in offenses]

    async def upsert_offense(self, offense: Offense) -> Offense:

        if self.get_offense(offense.id) is None:
            async with self.pool.acquire() as connection:
                offense = await connection.fetchrow("insert into offenses (code, code_group, description) values"
                                                    "($1, $2, $3)", offense.code, offense.code_group,
                                                    offense.description)
        async with self.pool.acquire() as connection:
            offense = await connection.fetchrow(
                "update offenses set code=$2, code_group=$3, description=$4 where id=$1",
                offense.id, offense.code, offense.code_group, offense.description)
            return Offense(**dict(offense))

    async def delete_offense(self, offense_id: int):
        with self.pool.acquire() as connection:
            await connection.fetchrow("delete from offenses where id=$1", offense_id)
        return f"OOffense {offense_id} deleted successfully."

    # todo: INCIDENTS

    async def get_incident(self, incident_id: int) -> Incident:
        async with self.pool.acquire() as connection:
            incident = await connection.fetchrow("select * from incidents where id=$1", incident_id)
        return Incident(**dict(incident))

    async def get_all_incidents(self, limit: int = 100) -> list[Incident]:
        async with self.pool.acquire() as connection:
            incidents = await connection.fetch("select * from incidents limit=$1", limit)
        return [Incident(**dict(incident)) for incident in incidents]

    async def upsert_incident(self, incident: Incident) -> Incident:

        if self.get_incident(incident.id) is None:
            async with self.pool.acquire() as connection:
                incident = await connection.fetchrow("insert into incidents (number,offense_code,area_id,"
                                                     "shooting,date,time,day_of_week) values ($1,$2,$3,$4,$5,$6,$7)",
                                                     incident.incident_number, incident.offense_code, incident.area_id,
                                                     incident.shooting, incident.date, incident.time,
                                                     incident.day_of_week)
        async with self.pool.acquire() as connection:
            incident = connection.fetchrow("update incidents set number=$2, offense_code=$3, area_id=$4,"
                                           "shooting=$5, date=$6, time=$7, day_of_week=$8 where id=$1",
                                           incident.id, incident.incident_number, incident.offense_code,
                                           incident.area_id, incident.shooting, incident.date, incident.time,
                                           incident.day_of_week)

        return Incident(**dict(incident))

    async def delete_incident(self, incident_id: int):
        async with self.pool.acquire() as connection:
            await connection.fetchrow("delete from incidents where id=$1", incident_id)
        return f"Incident {incident_id} deleted successfully."
