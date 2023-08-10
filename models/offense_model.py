from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship, declarative_base
# from models import Incident
Base = declarative_base()


class Offense(Base):
    __tablename__ = "offenses"

    id = Column(Integer, primary_key=True)
    code = Column(Integer, nullable=False, unique=True)
    code_group = Column(String, nullable=False)
    description = Column(String, nullable=False)

    incidents = relationship("Incident", back_populates="area")