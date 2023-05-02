import sqlite3
import uuid
from typing import Dict, List
from dataclasses import dataclass


@dataclass
class Event:
    guid: str = str(uuid.uuid4())
    extract_date: str = None
    file_name: str = None
    received_at: str = None
    pushed_at: str = None

    def insert(self, con: sqlite3.Connection):
        try:
            query = (f"INSERT INTO events VALUES ('{self.guid}', '{self.extract_date}', '{self.file_name}', "
                     f"'{self.received_at}', NULL)")
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
    def insert_many(cls, record_list: list, con: sqlite3.Connection):
        query = f"INSERT INTO events VALUES"
        insert_query = [f"('{event.guid}', '{event.extract_date}', '{event.file_name}', '{event.received_at}', NULL)"
                        for event in record_list]
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

    def mark_push(self, con: sqlite3.Connection):
        if self.pushed_at is None:
            raise ValueError("Event field 'pushed_at' can not be None")

        try:
            query = f"UPDATE events SET pushed_at = '{self.pushed_at}' WHERE guid = '{self.guid}'"
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

    def to_dict(self) -> dict:
        return {"guid": self.guid, "extract_date": self.extract_date, "file_name": self.file_name,
                "received_at": self.received_at, "pushed_at": self.pushed_at}

    @classmethod
    def from_dict(cls, event: dict):
        return Event(guid=event.get("guid", str(uuid.uuid4())), extract_date=event.get("extract_date"),
                     file_name=event.get("file_name"), received_at=event.get("received_at"),
                     pushed_at=event.get("pushed_at"))

    @classmethod
    def from_tuple(cls, event: tuple):
        return Event(guid=event[0], extract_date=event[1], file_name=event[2], received_at=event[3], pushed_at=event[4])
