import datetime
from dataclasses import dataclass

@dataclass
class Incident:
    id: int
    incident_number: str
    offence_code: int
    street_id: int
    shooting: bool
    date: datetime.datetime
    time: datetime.time
    day_of_week: str

@dataclass
class District:
    id: int
    name: str

@dataclass
class Street:
    id: int
    name: str
    district_id: int
    reporting_area: int


