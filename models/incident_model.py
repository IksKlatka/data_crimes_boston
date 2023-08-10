from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship, declarative_base
# from models import Offense, Area
Base = declarative_base()


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

    offense = relationship("Offense", back_populates="incidents")
    area = relationship("Area", back_populates="incidents")

