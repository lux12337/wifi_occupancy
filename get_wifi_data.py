# @author : Marco Pritoni <mpritoni@lbl.gov>
# @author : Anand Prakash <akprakash@lbl.gov>

from WiFi_Gatherer import wifi_gatherer
from Local_DB import local_db
import os

project_path = os.path.dirname(os.path.realpath(__file__))

# file get_wifi_data
g = wifi_gatherer(project_path = project_path, config_file="config.ini", section="SNMP_config_aruba")
g2 = wifi_gatherer(project_path = project_path, config_file="config.ini", section="SNMP_config_cisco")
engine = local_db(project_path = project_path)

data_aruba = g._get_data_from_file()
data_aruba = g.parse_connection_count_per_AP(data_aruba, formatOpt="Melrok")

data_cisco = g2._get_data_from_file()
data_cisco = g2.parse_connection_count_per_AP(data_cisco, formatOpt="Melrok")

engine.save_to_local_DB(data_aruba, mode="append")
engine.save_to_local_DB(data_cisco, mode="append")

engine.dispose_DB_engine() # need to fix this one