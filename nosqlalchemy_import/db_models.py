import datetime
from dataclasses import dataclass

@dataclass
class Incident:
    id: int
    incident_number: str
    offense_code: int #ForeignKey (Offenses:code)
    area_id: int
    shooting: bool
    date: datetime.datetime
    time: str
    day_of_week: str

@dataclass
class District:
    id: int
    name: str

@dataclass
class Area:
    id: int
    street: str
    reporting_area: int
    district: str

@dataclass
class Offense:
    id: int
    code: int
    code_group: str
    description: str

