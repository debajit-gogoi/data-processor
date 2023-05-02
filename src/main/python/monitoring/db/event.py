import sqlite3
import uuid
from typing import Dict, List
from dataclasses import dataclass

import pandas as pd


@dataclass
class Event:
    appId: str = None
    appName: str = None
    appType: str = None
    appState: str = None
    appFinalState: str = None
    startedTime: str = None
    launchTime: str = None
    finishedTime: str = None
    elapsedTime: int = None
    pdCreatedBy: str = None
    pdDedupKey: str = None
    pdCreatedTime: str = None
    pdUpdatedTime: str = None
    pdResolvedTime: str = None

    def insert(self, con: sqlite3.Connection):
        try:
            dml_query = self.create_dml()
            query = f"INSERT INTO events VALUES ({dml_query})"
            cursor = con.execute(query)
            return cursor
        except sqlite3.IntegrityError as ie:
            con.rollback()
            print(f"IntegrityError: '{ie}'")
        except Exception as e:
            con.rollback()
            print(f"Exception: '{e}'")
        finally:
            con.commit()

    @classmethod
    def insert_many(cls, record_list: List, con: sqlite3.Connection):
        query = f"INSERT INTO events VALUES"
        insert_query = [f"({event.create_dml()})" for event in record_list]
        query = f"{query} {', '.join(insert_query)}"
        print(f"Query: '{query}'")

        try:
            cursor = con.execute(query)
            return cursor
        except sqlite3.IntegrityError as ie:
            con.rollback()
            print(f"IntegrityError: '{ie}'")
        except Exception as e:
            con.rollback()
            print(f"Exception: '{e}'")
        finally:
            con.commit()

    def get(self, con: sqlite3.Connection):
        query = (f"SELECT guid, extract_date, file_name, received_at, pushed_at FROM events e "
                 f"WHERE extract_date = '{self.extract_date}' OR guid = '{self.guid}'")
        return [self.from_tuple(row) for row in con.execute(query)]

    @classmethod
    def list(cls, con):
        query = "SELECT guid, extract_date, file_name, received_at, pushed_at FROM events e WHERE e.pushed_at IS NULL"
        return [cls.from_tuple(row) for row in con.execute(query)]

    def update(self, con: sqlite3.Connection):
        try:
            query = (f"UPDATE events SET "
                     f"extract_date = '{self.extract_date}', "
                     f"file_name = '{self.file_name}', "
                     f"received_at = '{self.received_at}', "
                     f"pushed_at = '{self.pushed_at}' "
                     f"WHERE guid = '{self.guid}'").strip()

            cursor = con.execute(query)
            return cursor
        except sqlite3.IntegrityError as ie:
            con.rollback()
            print(f"IntegrityError: '{ie}'")
        except Exception as e:
            con.rollback()
            print(f"Exception: '{e}'")
        finally:
            con.commit()

    def create_dml(self) -> str:
        field_map_dict = {
            "appId": self.appId,
            "appName": self.appName,
            "appType": self.appType,
            "appState": self.appState,
            "appFinalState": self.appFinalState,
            "startedTime": self.startedTime,
            "launchTime": self.launchTime,
            "finishedTime": self.finishedTime,
            "elapsedTime": self.elapsedTime,
            "pdCreatedBy": self.pdCreatedBy,
            "pdDedupKey": self.pdDedupKey,
            "pdCreatedTime": self.pdCreatedTime,
            "pdUpdatedTime": self.pdUpdatedTime,
            "pdResolvedTime": self.pdResolvedTime
        }

        return ", ".join([self.value_or_null(value) for field, value in field_map_dict.items()])

    @classmethod
    def from_dict(cls, event: dict):
        return Event(appId=event.get("id"), appName=event.get("name"), appType=event.get("type"),
                     appState=event.get("state"), appFinalState=event.get("finalStatus"),
                     startedTime=event.get("startedTime"), launchTime=event.get("launchTime"),
                     finishedTime=event.get("finishedTime"), elapsedTime=event.get("elapsedTime"))

    @classmethod
    def from_list(cls, app_list: list):
        return [cls.from_dict(event_dict) for event_dict in app_list]

    @classmethod
    def value_or_null(cls, value) -> str:
        if isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, int) or isinstance(value, float):
            return f"{value}"
        elif isinstance(value, pd._libs.tslibs.timestamps.Timestamp):
            return f"'{str(value).split('.')[0]}'"
        elif value is None:
            return 'NULL'

        return 'NULL'
