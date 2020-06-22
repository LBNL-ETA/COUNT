from data_storage.SQLite_Connector import SQLite_Connector
from data_storage.InfluxDB_Connector import InfluxDB_Connector
import os

project_path = os.path.dirname(os.path.realpath(__file__))
config_file = "count_config.yaml"

local_database_obj = SQLite_Connector(project_path=project_path, config_file=config_file, section="local_db")
remote_database_obj = InfluxDB_Connector(project_path=project_path, config_file=config_file, section="remote_db")

data = local_database_obj.read_from_db()
save_status = remote_database_obj.save_to_db(data)
if not save_status:
    raise Exception("Failed to save data to remote database")

local_database_obj.delete_data_from_db_based_on_time(data)

local_database_obj.close_connection()
remote_database_obj.close_connection()