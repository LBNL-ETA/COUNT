from influxdb import DataFrameClient
from data_storage.DB_Interface import DB_Interface, pd


class InfluxDB_Connector(DB_Interface):
    def __init__(self, project_path=".", config_file="count_config.yaml", section="remote_db"):
        super.__init__(project_path, config_file, section)

        try:
            db_cfg = self.config[self.database_section]
            self.host = db_cfg.get("host", "localhost")
            self.port = db_cfg.get("port", 8086)
            self.username = db_cfg.get("username", "")
            self.password = db_cfg.get("password", "")
            self.database = db_cfg.get("database", "wifi")
            self.ssl = db_cfg.get("ssl", False)
            self.verify_ssl = db_cfg.get("verify_ssl", False)
            self.measurement = db_cfg.get("measurement", "wifi_count")

        except Exception as e:
            self.logger.error("unexpected error while setting configuration from config_file={0}, error={1}".format(
                self.config_file, str(e)))
            raise e

        """
        create an influxDB DataFrameClient to connect to the db 
        """
        self._create_influx_client()

    def _create_influx_client(self):
        """
        this method creates an influxDB client
        """
        try:
            self.client = DataFrameClient(host=self.host, port=self.port, username=self.username,
                                          password=self.password,
                                          database=self.database, ssl=self.ssl, verify_ssl=self.verify_ssl)
            self.logger.info("successfully created influxDB client to {0}".format(self.host))
        except Exception as e:
            self.logger.error("failed to create client to influxDB due to {0}".format(str(e)))
            raise e

    def close_connection(self):
        """
        this method closes the influxDB client
        """
        self.client.close()
        self.logger.info("closed influxDB client")

    def save_to_db(self, data, tag_columns=None, field_columns=None):
        """
        this method saves data to the influxDB database
        """
        if data.index.name != 'time':
            data = data.set_index('time')
            data.index = pd.to_datetime(data.index, unit="s")

        if field_columns is None:
            if 'count' in data.columns:
                field_columns = ['count']
            else:
                field_columns = data.columns

        if tag_columns is None:
            tag_columns = list(data.columns)
            for col in field_columns:
                tag_columns.remove(col)

        try:
            ret = self.client.write_points(dataframe=data, measurement=self.measurement, tag_columns=tag_columns,
                                           field_columns=field_columns)
            if ret:
                self.logger.info("values successfully inserted into InfluxDB measurement {0}".format(self.measurement))
            else:
                self.logger.error("failed to save data to influxDB measurement {0}".format(self.measurement))

        except Exception as e:
            self.logger.error(
                "Unexpected error while saving data to influxDB {0}, error={1}".format(self.measurement, str(e)))
            return False

        return ret

    def read_from_db(self, start_time=None, end_time=None):
        """
        this method queries data (between timestamps if specified) from the database
        """

        if start_time is not None:
            start_time = DB_Interface.format_time(start_time).strftime("%Y-%m-%dT%H:%M:%SZ")

        if end_time is not None:
            end_time = DB_Interface.format_time(end_time).strftime("%Y-%m-%dT%H:%M:%SZ")

        query = "select * from {0}".format(self.measurement)

        if start_time is not None and end_time is not None:
            query += " and time >= '{0}' and time <= '{1}'".format(start_time, end_time)
        elif start_time is not None:
            query += " and time >= '{0}'".format(start_time)
        elif end_time is not None:
            query += " and time <= '{0}'".format(end_time)

        try:
            data = self.influx_client.query(query)
            self.logger.info("successfully read values from the influxDB measurement {0}".format(self.measurement))
        except Exception as e:
            self.logger.error(
                "failed to retrieve data to influxDB measurement {0} error={1}".format(self.measurement, str(e)))
        return data.get(self.measurement, pd.DataFrame())

    def clean_db(self):
        """
        this method drops the whole measurement from the influxDB database
        """
        try:
            query = "drop measurement {0}".format(self.measurement)
            self.influx_client.query(query)
            self.logger.info("successfully dropped InfluxDB measurement {0}".format(self.measurement))
        except Exception as e:
            self.logger.error(
                "unexpected error while dropping InfluxDB measurement {0}, error={1}".format(self.measurement, str(e)))

    def delete_data_from_db_based_on_time(self, data, time_threshold, *args):
        """
        this method deletes data until a partcular time from the influxDB database
        """
        if time_threshold is None:
            time_threshold = pd.Timestamp.utcnow()
        else:
            time_threshold = DB_Interface.format_time(time_threshold)

        q = "delete from {0} where time<={1}".format(self.measurement, time_threshold.strftime("%Y-%m-%dT%H:%M:%SZ"))

        try:
            self.client.query(q)
            self.logger.info(
                "data that was sent has been removed from the InfluxDB measurement {0}".format(self.measurement))
            return True
        except Exception as e:
            self.logger.error("Exception occurred when trying to delete data from influx measurement {0}: {1}"
                              .format(self.measurement, e))
            return False
