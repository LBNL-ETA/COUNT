from abc import ABC, abstractmethod
import os
import logging
from logging.handlers import TimedRotatingFileHandler
import yaml
import pandas as pd

class Data_Processor(ABC):

    def __init__(self, project_path=".", config_file="count_config", section="data_processor"):
        super().__init__()
        self.project_path = project_path
        """
        initialize logging
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        if not os.path.exists(self.project_path+"/"+'logs'):
            os.makedirs(self.project_path+"/"+'logs')
        handler = TimedRotatingFileHandler(self.project_path+"/"+"logs/data_processor.log", when='D', interval=1, backupCount=5)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        """
        read config file
        """
        self.config_file = config_file
        self.data_processor_section = section
        if not os.path.exists(self.project_path + "/" + self.config_file):
            self.logger.error("cannot find config_file=%s" % self.config_file)
            raise Exception("config file not found")

        with open(self.project_path + "/" + self.config_file, "r") as fp:
            self.config = yaml.safe_load(fp)
        self.logger.info("successfully loaded config_file=%s" % self.config_file)


    @abstractmethod
    def parse(self, df: pd.DataFrame, *args) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_metadata_columns(self) -> list:
        pass

    @abstractmethod
    def get_timeseries_columns(self) -> list:
        pass

    def process(self, df: pd.DataFrame, *args) -> pd.DataFrame:
        return df

    def filter(self, df: pd.DataFrame, *args) -> pd.DataFrame:
        return df
