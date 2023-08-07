from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

class Offense(Base):
    __tablename__ = "offenses"

    id = Column(Integer, primary_key=True)
    code = Column(Integer, nullable=False, unique=True)
    code_group = Column(String, nullable=False)
    description = Column(String, nullable=False)

    # incidents = relationship("Incident", back_populates="area")

class Area(Base):
    __tablename__ = "areas"

    id = Column(Integer, primary_key=True)
    street = Column(String, nullable=False)
    reporting_area = Column(Integer, nullable=False)
    district = Column(String, nullable=False)

    # incidents = relationship("Incident", back_populates="area")

class Incident(Base):
    __tablename__ = "incidents"

    id = Column(Integer, primary_key=True)
    number = Column(String, nullable=False, unique=True)
    offense_code = Column(Integer, ForeignKey("offenses.code"), nullable=False)
    area_id = Column(Integer, ForeignKey("areas.id"), nullable=False)
    shooting = Column(Integer, nullable=False, default=0)
    date = Column(DateTime, nullable=False)
    time = Column(String)
    day_of_week = Column(Integer)

    # offense = relationship("Offense", back_populates="incidents")
    # area = relationship("Area", back_populates="incidents")

