import dataframe_manip as hp
import pandas as pd

data = hp.csv_to_dataframe("wifi_data_until_20190204.csv")
print('Successfully got data')

print(hp.get_building_accesspoints(data, 'SCC'))
