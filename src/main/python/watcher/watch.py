import os
import sys
import time
import sqlite3
import datetime
import uuid

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from streaming.producer import Producer
from streaming.slack import Slack
from db.event import Event
import util

base_path = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_path, "config", "configuration.json")
db_path = os.path.join(base_path, "config", "events.db")

if not os.path.exists(config_path):
    print(f"'{config_path}' does not exist, exiting the script.")
    sys.exit(-1)

if not os.path.exists(db_path):
    print(f"'{db_path}' does not exist, exiting the script.")
    sys.exit(-1)

logger = util.setup_logging(os.path.join(base_path, "log", "watcher.log"))


class MonitorDirectory(FileSystemEventHandler):

    def on_created(self, system_event):
        logger.info(f"'{system_event.src_path}' {system_event.event_type}")
        self.process_file(system_event)

    def on_modified(self, system_event):
        logger.info(f"'{system_event.src_path}' {system_event.event_type}")
        pass

    def on_closed(self, system_event):
        logger.info(f"'{system_event.src_path}' {system_event.event_type}")
        pass

    def on_moved(self, system_event):
        logger.info(f"'{system_event.src_path}' {system_event.event_type}")
        self.process_file(system_event)

    @classmethod
    def process_file(cls, system_event):

        file_path = system_event.src_path

        if system_event.event_type == "moved":
            file_path = system_event.dest_path

        file_name = os.path.basename(file_path)

        if (file_name.startswith("Enterprise Analytics Prior 30 Days") or file_name.startswith(
                "Onedebajit Enterprise Analytics Prior 30 Days")) and (
                file_name.endswith(".csv") or file_name.endswith(".csv.done")):
            # Wait for data write to complete
            logger.debug(f"Waiting for file write to complete")
            time.sleep(30)
            logger.info(f"Processing file '{file_path}'")
            data_list = Slack.to_dict_list(file_path)
            if data_list is not None:
                event = Event(guid=str(uuid.uuid4()), extract_date=data_list[0]["Date"],
                              file_name=os.path.basename(file_path),
                              received_at=util.get_creation_time(file_path))

                con: sqlite3.Connection = None
                try:
                    con = util.connect_sqlite(db_path)
                    event.insert(con)
                    logger.debug(f"Record inserted successfully")

                    push_status = Producer.push(data_list[0], logger)

                    if push_status:
                        """
                        processed_path = os.path.join(os.path.dirname(system_event.src_path),
                                                      f"{os.path.splitext(os.path.basename(system_event.src_path))[0]}.processed")
                        os.rename(system_event.src_path, processed_path)
                        logger.debug(f"File renamed successfully")
                        """

                        event.pushed_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        event.mark_push(con)
                        logger.debug(f"Record push marked successfully")

                    else:
                        logger.info(f"Could not push message to Kafka")
                except Exception as e:
                    logger.error(f"Exception: '{e}'")
                finally:
                    con.close()
            else:
                logger.info(f"Empty file received")
        else:
            logger.info(f"Ignored unexpected file '{os.path.basename(system_event.file_path)}'")


if __name__ == "__main__":
    src_path = "/home/rwi/slack"

    Producer.read_config(config_path)

    event_handler = MonitorDirectory()
    observer = Observer()
    observer.schedule(event_handler, path=src_path, recursive=False)

    logger.info(f"Monitoring '{src_path}'")

    observer.start()
    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        observer.stop()
        observer.join()
