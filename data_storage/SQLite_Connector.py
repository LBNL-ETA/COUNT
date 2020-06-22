import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, DBAPIError
import pytz

from data_storage.DB_Interface import DB_Interface


class SQLite_Connector(DB_Interface):
    """
    This class saves the data from pandas dataframe to a local db (currently sqlite3 on disk) as buffer while pushing data
    to another DB/API.
    """

    def __init__(self, project_path=".", config_file="count_config.yaml", section="local_db"):
        super.__init__(project_path, config_file, section)

        try:
            db_cfg = self.config[self.database_section]
            self.db = db_cfg.get("filename", "sqlite:///%s/wifi_buffer.db")
            self.table = db_cfg.get("table", "wifi_buffer_table")
        except Exception as e:
            self.logger.error("unexpected error while setting configuration from config_file={0}, error={1}".format(
                self.config_file, str(e)))
            raise e

        """
        create a sqlachemy engine (pool of connections) to connect to the db
        """
        self.engine = self._create_db_engine()

    def _create_db_engine(self):

        """
        this method creates a sqlalchemy DB engine (pool of connection)
        """

        try:
            engine = create_engine(self.db % self.project_path, echo=False)
            self.logger.info("sql alchemy engine successfully created")
            return engine

        except (SQLAlchemyError, DBAPIError) as e:
            self.logger.error("cannot create sqlalchemy engine, error={0}".format(str(e)))
            raise e

    def save_to_db(self, data, mode="append"):

        """
        this method saves the data from a pandas dataframe into a db table, currently on disk
        """
        try:
            if not data.empty:
                data.to_sql(name=self.table, con=self.engine, if_exists=mode, index=False)
                self.logger.info("values successfully inserted into SQLite database table {0}".format(self.table))
            else:
                self.logger.warning("no data to save to SQLite database")
        except ValueError as e:
            self.logger.error(
                "cannot insert values to table {0}, data might already exist, error={1}".format(self.table, str(e)))
            return False
        except Exception as e:
            self.logger.error(
                "Unexpected error while appending values to SQLite database table {0}, error={1}".format(self.table,
                                                                                                         str(e)))
            return False
        return True

    def read_from_db(self, start_time=None, end_time=None, query=None):

        """
        this method reads the data from a db table back to a pandas dataframe
        """
        try:
            if query is None:
                if start_time is not None:
                    start_time = DB_Interface.format_time(start_time).strftime("%s")

                if end_time is not None:
                    end_time = DB_Interface.format_time(end_time).strftime("%s")

                query = "SELECT * FROM {}".format(self.table)

                if start_time is not None and end_time is not None:
                    query += " and time >= '{0}' and time <= '{1}'".format(start_time, end_time)
                elif start_time is not None:
                    query += " and time >= '{0}'".format(start_time)
                elif end_time is not None:
                    query += " and time <= '{0}'".format(end_time)

            data = pd.read_sql_query(query, self.engine)
            data['time'] = pd.to_datetime(data['time'])
            data['time'] = data['time'].dt.tz_localize(pytz.timezone("UTC"))
            self.logger.info("successfully read values from SQLite table {0}".format(self.table))
            return data
        except Exception as e:
            self.logger.error("The SQLite table {0} was not found, error={1}".format(self.table, str(e)))
            return pd.DataFrame()

    def clean_db(self):

        """
        this method drops the whole table on the db; use delete_data_sent for normal operation
        """
        try:
            pd.io.sql.execute("DROP TABLE IF EXISTS {}".format(self.table), self.engine)
            self.logger.info("successfully dropped SQLite table {0}".format(self.table))
        except Exception as e:
            self.logger.error("unexpected error while dropping SQLite table {0}, error={1}".format(self.table, str(e)))

    def close_connection(self):

        """
        this method closes the connection to the database
        """
        self.engine.dispose()
        self.logger.info("closed sqlalchemy engine")
        return

    def _select_timestamps(self, data, time_threshold):

        """
        this method selects the dates to be removed form local database based on a time threshold check
        It uses data time comparison and manipulation in pandas. The method is called by "delete_data_sent"
        """
        try:
            if not data.empty:
                data_to_be_removed = data.loc[data.time < time_threshold]
                times_to_be_removed = '("' + '","'.join(
                    list((data_to_be_removed.time.dt.tz_localize(None).astype(str).unique()))) + '")'
                return times_to_be_removed
            else:
                self.logger.warning("data to be selected to be removed is empty, check this")
                return '()'
        except Exception as e:
            self.logger.error(
                "unexpected error while selecting data to be remove from SQLite db, error={0}".format(str(e)))
            return '()'
        # ex datetime_to_remove = "('20180627141830', '20180627141834', '20180627142745', '20180627142753'')"

    def delete_data_from_db_based_on_time(self, data, time_threshold=None, *args):

        """
        this method deletes data until a partcular time from the SQLite database
        """

        if time_threshold is None:
            time_threshold = pd.Timestamp.utcnow()
        else:
            time_threshold = DB_Interface.format_time(time_threshold)

        datetime_to_remove = self._select_timestamps(data, time_threshold=time_threshold)

        try:
            self.engine.execute(
                "DELETE FROM {} WHERE time in {} ".format(self.table, datetime_to_remove))  # or engine.engine
            self.logger.info("data that was sent has been removed from the SQLITE db table {0}".format(self.table))
            return True
        except Exception as e:
            self.logger.error(
                "unexpected error occurred while removing data sent from the SQLite table {0}, error={1}".format(
                    self.table, str(e)))
            return False
