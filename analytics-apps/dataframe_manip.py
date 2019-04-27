from typing import Optional, List
import numpy as np
import pandas as pd
import seaborn
import matplotlib.pyplot as plt


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

	dataframe: pd.DataFrame = pd.read_csv(
		filepath_or_buffer=filepath,
		# This csv includes na values, which should be
		na_filter=True,
		# parse the time column and make it the index
		index_col=time_col_index,
		parse_dates=True,
		date_parser=lambda col: pd.to_datetime(col, utc=True),
		# how many rows to read in
		nrows=nrows
	)

	if not isinstance(dataframe.index, pd.DatetimeIndex):
		dataframe.index = pd.to_datetime(
			dataframe.index, utc=True
		)

	return dataframe


def occupancy_totals_timeseries(df: pd.DataFrame) -> pd.DataFrame:

	total_occupancy_vs_time = df.sum(
		# sum across columns.
		axis=1,
		# treat na values as 0
		skipna=True,
		# skip non-numeric columns
		numeric_only=True
	)

	return total_occupancy_vs_time


if __name__ == '__main__':
	data: pd.DataFrame = csv_to_dataframe(
		filepath='./wifi_data_until_20190204.csv'
	)
	print(data.columns)
	print(data.dtypes)
	print(data.index)
	print(data.index.dtype)
	print(data.shape)

	data_collapsed = occupancy_totals_timeseries(data)
	print(data_collapsed.size)

	ts = pd.Series(data_collapsed, index=data_collapsed.index)

	fig, ax = plt.subplots(figsize=(2, 5))
	seaborn.boxplot(data_collapsed.index.hour, data_collapsed, ax=ax)
