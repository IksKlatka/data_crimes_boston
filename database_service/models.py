from sqlalchemy import Column, Boolean, String, Integer, Date, Time
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Incident(Base):
    __tablename__ = "incidents"

    id = Column(Integer, primary_key=True)
    incident_number = Column(String, nullable=False, unique=True)
    offense_code = Column(Integer, nullable=False)
    reporting_area = Column(Integer, nullable=False)
    shooting = Column(Boolean, nullable=False)
    date = Column(Date, nullable=False)
    time = Column(Time, nullable=False)
    day_of_week = Column(Integer, nullable=False)


    def __repr__(self):
        return f"Incident: id={self.id}, incident_number={self.incident_number}, offense_code={self.offense_code}, " \
               f"reporting_area={self.reporting_area}, shooting={self.shooting}, date={self.date}, time={self.time}, weekday={self.day_of_week}"
class Area(Base):
    __tablename__ = "areas"

    reporting_area = Column(Integer, primary_key=True)
    street = Column(String, nullable=False)
    district = Column(String, nullable=False)

    def __repr__(self):
        return f"Area: reporting_area={self.reporting_area},street={self.street}, district={self.district}"

class Offense(Base):
    __tablename__ = "offenses"

    id = Column(Integer, primary_key=True)
    code = Column(Integer, nullable=False, unique=True)
    code_group = Column(String, nullable=False)
    description = Column(String, nullable=False)

    def __repr__(self):
        return f"Offense: id={self.id}, offense_code={self.code}, group={self.code_group}, description={self.description}"