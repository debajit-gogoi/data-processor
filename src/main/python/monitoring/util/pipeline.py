import json
import os
import logging

import pandas as pd


class Pipeline:
    """
    Helper methods to read configuration files
    """

    def __init__(self, pipeline_path):
        self.pipeline_path = pipeline_path

    @classmethod
    def read_json(cls, pipeline_path) -> dict:
        """
        Read configuration file
        :param pipeline_path: Absolute path of the configuration file
        :return: Dictionary of the configuration file
        """
        with open(pipeline_path, mode="r", encoding="utf-8") as fp:
            return json.load(fp)

    @classmethod
    def epoch_to_local_ts(cls, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Convert Epoch timestamp (milliseconds) to Datetime object
        :param df: Data Frame
        :param column: Name of the column
        :return: Data Frame with Epoch fields replaced with Datetime object
        """
        # return pd.to_datetime(df[column], unit="ms").dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
        return pd.to_datetime(df[column], unit="ms")

    @classmethod
    def setup_logger(cls, log_path):
        """
        Setup logger
        :param log_path: Absolute path to store logs at
        :return: Logger object
        """
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
