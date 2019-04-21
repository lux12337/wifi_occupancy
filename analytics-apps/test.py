import helpers as hp
import pandas as pd

data = hp.getData("wifi_data_until_20190204.csv")

y = data['time'].resample('MS').mean()

print(data)
