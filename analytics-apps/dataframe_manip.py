import pandas as pd
from typing import Optional, List


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
	The other series will be int32 with -1 for 'None' values.
	:return: pandas dataframe
	"""
	# Assumptions
	time_col_index: int = 0
	occupancy_type = 'int32'
	occupancy_na = -1

	data: pd.DataFrame = pd.read_csv(
		filepath_or_buffer=filepath,
		# time column
		index_col=time_col_index,
		parse_dates=[time_col_index],
		infer_datetime_format=True,
		# how many rows to read in
		nrows=nrows
	).fillna(occupancy_na)

	time_col_name = data.index.name

	# All columns of the DataFrame should be of an unsigned integer type
	for name, series in data.iteritems():
		if name != time_col_name:
			data[name] = series.astype(dtype=occupancy_type, copy=False)
