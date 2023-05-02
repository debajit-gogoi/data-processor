import os

import pandas as pd
from sqlalchemy import create_engine, UniqueConstraint
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.schema import MetaData

db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "monitor.db")

Base = declarative_base()
engine = create_engine(f"sqlite:///{db_path}")
# metadata = MetaData(engine)
# metadata.create_all()
session = Session(engine)


class Monitoring(Base):
    """
    Model class to represent table in DB
    """
    __tablename__ = "monitoring"
    appId = Column(String, nullable=True)
    appName = Column(String, nullable=False)
    appType = Column(String, nullable=False)
    appState = Column(String, nullable=True)
    appFinalStatus = Column(String, nullable=True)
    startedTime = Column(DateTime, nullable=True)
    launchTime = Column(DateTime, nullable=True)
    finishedTime = Column(DateTime, nullable=True)
    elapsedTime = Column(Integer, nullable=True)
    pdDedupKey = Column(String, primary_key=True)
    pdCreatedTime = Column(DateTime, nullable=False)
    __table_args__ = (UniqueConstraint("appName", "appType", "pdDedupKey", "pdCreatedTime", name="_appIdNameDedupKey"),)

    # Map field names from YARN REST API to DB names
    field_map_dict = {
        "id": "appId",
        "name": "appName",
        "type": "appType",
        "state": "appState",
        "finalStatus": "appFinalStatus",
        "startedTime": "startedTime",
        "launchTime": "launchTime",
        "finishedTime": "finishedTime",
        "elapsedTime": "elapsedTime",
        "dedup_key": "pdDedupKey",
        "timestamp": "pdCreatedTime"
    }

    @classmethod
    def from_dict(cls, app_dict: dict):
        """
        Helper function to create active record object from dictionary
        :param app_dict: Application dictionary
        :return: SQLA model object
        """
        return Monitoring(**{cls.field_map_dict[key]: app_dict[key] for key in cls.field_map_dict})
