import subprocess
import pandas as pd
import os
import logging
from io import StringIO
from logging.handlers import TimedRotatingFileHandler
import yaml

class Wifi_Gatherer(object):
    """
    This class gets the wifi data from the controller/file, outputs a dataframe with AP connection counts
    This also hashes the mac addresses (currently not used)
    """

    def __init__(self, project_path=".", config_file="count_config.yaml", section="snmp_config"):

        self.project_path = project_path
        """
        initialize logging
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        if not os.path.exists(self.project_path + "/" + 'logs'):
            os.makedirs(self.project_path + "/" + 'logs')
        handler = TimedRotatingFileHandler(self.project_path + "/" + "logs/wifi_gatherer.log", when='D', interval=1,
                                           backupCount=5)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        """
        read config file
        """
        self.config_file = config_file
        self.snmp_section = section
        if not os.path.exists(self.project_path + "/" + self.config_file):
            self.logger.error("cannot find config_file=%s" % self.config_file)
            raise Exception("config file not found")

        with open(self.project_path + "/" + self.config_file, "r") as fp:
            self.config = yaml.safe_load(fp)
        self.logger.info("successfully loaded config_file=%s" % self.config_file)

        try:
            snmp_cfg = self.config[self.snmp_section]
            self.input_from_file = snmp_cfg.get("input_from_file", False)
            self.input_file_name = snmp_cfg.get("input_file_name", None)

            if self.input_file_name:
                if self.input_file_name is None:
                    raise Exception("Missing configuration parameter: input_file_name")

            self.community = str(snmp_cfg.get("community"))
            self.controller_ip = str(snmp_cfg.get("controller_ip"))
            self.oid = str(snmp_cfg.get("count_oid"))
            if not self.oid.startswith('.'):
                self.oid = '.'+self.oid
            if self.oid.endswith('.'):
                self.oid = self.oid[:-1]
        except Exception as e:
            self.logger.error(
                "unexpected error while setting configuration from config_file=%s, section=%s, error=%s" % (
                self.config_file, self.snmp_section, str(e)))
            raise e

    def _get_data_SMNP(self):

        """
        This method calls SNMP through subprocess #inputs parms of the SNMP query from config file
        """

        if not self.input_from_file:
            cmd = ['snmpwalk', '-v', '2c', '-c', self.community, '-Onaq', self.controller_ip, self.oid]
            try:
                p = subprocess.Popen(cmd,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
                out, err = p.communicate()

            except Exception as e:
                self.logger.error("unexpected error when running snmpwalk command, error=%s" % str(e))

            if p.returncode != 0:
                self.logger.error("snmpwalk exited with status %r: %r" % (p.returncode, err))
                raise Exception('snmpwalk exited with status %r: %r' % (p.returncode, err))
            else:
                try:
                    df = pd.read_csv(StringIO(out.decode('utf-8')), header=None, names=['output'])
                    self.logger.info("successfully imported dataFrame from snmp output", )
                    return df
                except Exception as e:
                    self.logger.error("unexpected error while reading from snmp output, error=%s" % (str(e)))

        else:
            self.logger.error("currently non implemented AP - SNMP query")

    def _get_data_from_file(self):

        """
        This method reads from file the results of a SNMP query (testing or alternative path)
        """
        try:
            df = pd.read_csv(self.project_path + "/data/" + self.input_file_name, header=None, names=['output'])
            self.logger.info("successfully imported dataFrame from csv file=%s" % self.input_file_name)
        except Exception as e:
            self.logger.error("unexpected error while reading from csv %s, error=%s" % (self.input_file_name, str(e)))
            raise e

        return df

    def _get_utc_timenow(self):
        return pd.Timestamp.utcnow()

    def get_wifi_data(self):

        """
        This method gets the wi-fi data from file or snmp query calling the corresponding methods
        """

        if self.input_from_file:
            data = self._get_data_from_file()  # use sample file to test
        else:
            data = self._get_data_SMNP()  # run real query

        time_now = self._get_utc_timenow()
        data['time'] = time_now
        return data
