import dataframe_manip as hp
import pandas as pd

data = hp.csv_to_dataframe("wifi_data_until_20190204.csv")
print('Successfully got data')

scc_building = data[hp.get_building_accesspoints(data, 'SCC')].fillna(0).copy()

scc_building['total'] = scc_building.sum(axis=1)

scc_building.index = pd.to_datetime(scc_building.index, utc=True)

scc_building = scc_building.resample('H').mean()

scc_building['weekday'] = scc_building.index.weekday

#monday = scc_building[['weekday'] == 0]

day = [0]
print(scc_building[scc_building['weekday'].isin(day)])
