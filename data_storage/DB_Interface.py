from abc import ABC, abstractmethod
import logging
from logging.handlers import TimedRotatingFileHandler
import yaml
import os
import pandas as pd
import datetime
import pytz


class DB_Interface(ABC):
    def __init__(self, project_path=".", config_file="count_config", section="local_db"):
        super().__init__()
        self.project_path = project_path
        """
        initialize logging
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        if not os.path.exists(self.project_path + "/" + 'logs'):
            os.makedirs(self.project_path + "/" + 'logs')
        handler = TimedRotatingFileHandler(self.project_path + "/" + "logs/%s.log" % section, when='D', interval=1,
                                           backupCount=5)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        """
        read config file
        """
        self.config_file = config_file
        self.database_section = section
        if not os.path.exists(self.project_path + "/" + self.config_file):
            self.logger.error("cannot find config_file=%s" % self.config_file)
            raise Exception("config file not found")

        with open(self.project_path + "/" + self.config_file, "r") as fp:
            self.config = yaml.safe_load(fp)
        self.logger.info("successfully loaded config_file=%s" % self.config_file)

    @abstractmethod
    def save_to_db(self, data: pd.DataFrame, *args) -> bool:
        pass

    @abstractmethod
    def read_from_db(self, start_time, end_time, *args):
        pass

    @abstractmethod
    def clean_db(self):
        pass

    @abstractmethod
    def delete_data_from_db_based_on_time(self, data, time_threshold, *args) -> bool:
        pass

    @abstractmethod
    def close_connection(self):
        pass

    @staticmethod
    def format_time(ts):
        if isinstance(ts, str):
            ts = pd.to_datetime(ts)
        elif isinstance(ts, datetime.datetime):
            if ts.tzinfo is None:
                ts = pd.to_datetime(ts).tz_localize(pytz.UTC)
            else:
                ts = pd.to_datetime(ts).tz_convert(pytz.UTC)
        return ts
