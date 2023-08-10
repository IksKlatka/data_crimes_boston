from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship, declarative_base
# from models import Incident
Base = declarative_base()


class Area(Base):
    __tablename__ = "areas"

    id = Column(Integer, primary_key=True)
    street = Column(String, nullable=False)
    reporting_area = Column(Integer, nullable=False)
    district = Column(String, nullable=False)

    # incidents = relationship("Incident", back_populates="area")

