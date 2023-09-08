from dataclasses import dataclass
import datetime

@dataclass
class Incident:
    id: int
    incident_number: str
    offence_code: int
    area_id: int
    shooting: bool
    date: datetime.datetime
    time: datetime.time
    day_of_week: str

@dataclass
class Offense:
    id: int
    code: int
    code_group: str
    description: str

@dataclass
class Area:
    id: int
    street: str
    reporting_area: int
    district: str