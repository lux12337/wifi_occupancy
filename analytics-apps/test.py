import helpers as hp
import pandas as pd

data = hp.getData("wifi_data_until_20190204.csv")
print('Successfully got data')

y = len(hp.get_building_accesspoints(data, ''))

print(y)
