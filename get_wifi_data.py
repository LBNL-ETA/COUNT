from data_source.Wifi_Gatherer import Wifi_Gatherer
from data_processor.Cisco_Processor import Cisco_Processor
from data_storage.SQLite_Connector import SQLite_Connector
import os

project_path = os.path.dirname(os.path.realpath(__file__))
config_file = "count_config.yaml"

data_source_obj = Wifi_Gatherer(project_path=project_path, config_file=config_file, section="snmp")
data_processor_obj = Cisco_Processor(project_path=project_path, config_file=config_file, section="data_processor")
local_database_obj = SQLite_Connector(project_path=project_path, config_file=config_file, section="local_db")

data = data_source_obj.get_wifi_data()

data = data_processor_obj.parse(df=data)
data = data_processor_obj.process(df=data)
data = data_processor_obj.filter(df=data)

save_status = local_database_obj.save_to_db(data)
if not save_status:
    raise Exception("Failed to save data to local buffer")

local_database_obj.close_connection()