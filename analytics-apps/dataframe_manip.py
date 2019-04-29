import pandas as pd
from typing import Optional, List, Dict


def get_building_accesspoints(lis: List[str], bui: str) -> List[str]:
	"""
	Takes in wifi_data's column (list) and
	a name/keyword of a building (string)
	to find its access points
	:return: list of string
	"""
	ret = []
	for i in lis:
		if bui in i:
			ret.append(i)
	return ret


def csv_to_dataframe(filepath: str, nrows: Optional[int] = None) -> pd.DataFrame:
	"""
	Loads data from a csv into a pandas dataframe.
	The csv is expected to have a datetime-formattable string in the first column.
	The dataframe will have a DateTimeIndex in the first column.
	The other series will be float64
	:return: pandas dataframe
	"""
	# Assumptions
	time_col_index: int = 0

	data: pd.DataFrame = pd.read_csv(
		filepath_or_buffer=filepath,
		# Instead of filtering na's, replace them with fillna()
		na_filter=True,
		# time column
		index_col=time_col_index,
		parse_dates=[time_col_index],
		infer_datetime_format=True,
		# how many rows to read in
		nrows=nrows
	)

	return data


def get_hourly_data_building(data, building_name):
	"""
	Extracts all the APs present in a building and fills NaN with 0.
	Sets index to datetime and adjusts for timezone. Then it calculates
	the hourly mean occupancy of each AP.

	:input:
		data 			-> dataframe output from csv_to_dataframe
		building_name 	-> specific building name in string(eg. 'SCC')
	:return:
		pandas dataframe
	"""

	building = data[get_building_accesspoints(data, building_name)].fillna(0).copy()
	building.index = pd.to_datetime(building.index, utc=True)
	building = building.resample('H').mean()

	return building


def get_daily_average(data, building_name):
	"""
	Adds the UTC offset in the datetime index. Sums all the APs together in column 'y',
	and calculates the daily average occupancy from 'y'. Removes offset string after
	calculations. Changes 'time' from an index to a column.

	:input:
		data 			-> dataframe output from csv_to_dataframe
		building_name 	-> specific building name in string(eg. 'SCC')
	:return:
		pandas dataframe
	"""

	building = data[hp.get_building_accesspoints(data, building_name)].copy()
	building.index = pd.to_datetime(alexander.index, utc=True)
	building['y'] = building.sum(axis=1)
	building = building.resample('D').mean()
	building = building['y']
	building = pd.DataFrame(building).reset_index()
	building.columns = ['ds', 'y']
	building['ds'] = building['ds'].astype(str).str[:-15]

	return building


if __name__ == '__main__':
	data: pd.DataFrame = csv_to_dataframe('./wifi_data_until_20190204.csv')
	print(data.columns)
	print(data.dtypes)
	print(data.index)
