import os
import datetime
import logging
import hashlib
import sqlite3

import pandas as pd

field_map_dict = {"Date": "Date",
                  "Total Enabled Membership": "Total_Enabled_Membership",
                  "Enabled Full Members": "Enabled_Full_Members",
                  "Enabled Guests": "Enabled_Guests",
                  "Daily active members": "Daily_active_members",
                  "Daily members posting messages": "Daily_members_posting_messages",
                  "Weekly active members": "Weekly_active_members",
                  "Weekly members posting messages": "Weekly_members_posting_messages",
                  "Messages in public channels": "Messages_in_public_channels",
                  "Messages in private channels": "Messages_in_private_channels",
                  "Messages in shared channels": "Messages_in_shared_channels",
                  "Messages in DMs": "Messages_in_DMs",
                  "Percent of messages, public channels": "Percent_of_messages_public_channels",
                  "Percent of messages, private channels": "Percent_of_messages_private_channels",
                  "Percent of messages, DMs": "Percent_of_messages_DMs",
                  "Percent of views, public channels": "Percent_of_views_public_channels",
                  "Percent of views, private channels": "Percent_of_views_private_channels",
                  "Percent of views, DMs": "Percent_of_views_DMs",
                  "Total Full Members": "Total_Full_Members",
                  "Total Guests": "Total_Guests",
                  "Total Claimed Full Members": "Total_Claimed_Full_Members",
                  "Total Claimed Guests": "Total_Claimed_Guests",
                  "Total Members": "Total_Members",
                  "Total Claimed Members": "Total_Claimed_Members",
                  "Files uploaded": "Files_uploaded",
                  "Messages posted by members": "Messages_posted_by_members",
                  "Name": "Name",
                  "Public channels, single-workspace": "Public_channels_single_workspace",
                  "Messages posted": "Messages_posted",
                  "Messages posted by apps": "Messages_posted_by_apps"}


def csv_to_dict(fpath):
    return pd.read_csv(fpath).rename(columns=field_map_dict).to_dict("records")


def get_sha256sum(fpath):
    with open(fpath, "rb") as f:
        bytes_str = f.read()
        return hashlib.sha256(bytes_str).hexdigest()


def get_creation_time(fpath) -> str:
    return datetime.datetime.fromtimestamp(os.path.getctime(fpath)).strftime("%Y-%m-%d %H:%M:%S")


def connect_sqlite(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def setup_logging(log_path: str) -> logging.Logger:
    if not os.path.exists(os.path.dirname(log_path)):
        os.mkdir(os.path.dirname(log_path))

    logger = logging.getLogger(__name__)

    # Create handlers
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler(log_path)
    c_handler.setLevel(logging.DEBUG)
    f_handler.setLevel(logging.DEBUG)

    # Create formatters and add it to handlers
    format_str = "%(asctime)s - %(levelname)s - %(message)s"
    c_format = logging.Formatter(format_str)
    f_format = logging.Formatter(format_str)
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

    logger.setLevel(logging.DEBUG)

    return logger
