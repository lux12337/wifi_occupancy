"""
A module of tools for manipulating our Pandas Dataframes.
"""

from typing import Optional, List, Union
import numpy as np
import pandas as pd
import pytz
from .misc import XY

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


def csv_to_timeseries_df(
        filepath: str,
        nrows: Optional[int] = None,
        timezone: Optional[pytz.timezone] = None
) -> pd.DataFrame:
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

    if timezone is not None:
        dataframe.index = dataframe.index.tz_convert(timezone)

    return dataframe


def fill_intervening_nas(
        df_or_series: Union[pd.DataFrame, pd.Series],
        inplace: bool = False,
        fill_val: any = 0
) -> Union[pd.DataFrame, pd.Series]:

    if not inplace:
        df_or_series = df_or_series.copy(deep=True)

    def fill_series(series: pd.Series) -> pd.Series:

        if not series.hasnans:
            return series

        na_map = series.isna()
        not_na_map = ~na_map

        not_na_indices = np.where(not_na_map)[0]
        first_not_na = not_na_indices[0]
        last_not_na = not_na_indices[-1]

        # Start with the na_map.
        intervening_na_map = na_map
        # Eliminate initial and trailing na's.
        intervening_na_map.loc[:first_not_na] = False
        intervening_na_map.loc[last_not_na+1:] = False

        series.loc[intervening_na_map] = fill_val

        return series

    if isinstance(df_or_series, pd.Series):
        return fill_series(df_or_series)

    return df_or_series.apply(
        func=lambda ser: ser if isinstance(ser, pd.Index) else fill_series(ser),
        axis=0
    )


def na_coords(df: pd.DataFrame) -> XY:
    """
    x = row indices of na values
    y = column indices of na values
    :param df:
    :return:
    """

    na_map = df.isna()

    row_indices, col_indices = np.where(na_map)

    return XY(x=row_indices, y=col_indices)


def row_totals(df: pd.DataFrame) -> pd.DataFrame:
    """
    :param df: a dataframe with numeric columns.
    :return: a 1-dimensional dataframe of totals per row.
    """
    total_vs_time = df.sum(
        # sum across columns.
        axis=1,
        # treat na values as 0
        skipna=True,
        # skip non-numeric columns
        numeric_only=True
    )
    return total_vs_time


def column_totals(df: pd.DataFrame) -> pd.DataFrame:
    """
    :param df: a dataframe with numeric columns.
    :return: a 1-dimensional dataframe of totals per column.
    """
    sum_per_column = df.sum(
        # sum across rows.
        axis=0,
        # treat na values as 0
        skipna=True,
        # skip non-numeric columns
        numeric_only=True
    )
    return sum_per_column


def column_means(df: pd.DataFrame) -> pd.DataFrame:
    """
    :param df: a dataframe with numeric columns.
    :return: a 1-dimensional dataframe of means per column.
    """
    mean_per_column = df.mean(
        # operate across rows.
        axis=0,
        skipna=True,
        # skip non-numeric columns
        numeric_only=True
    )
    return mean_per_column


def row_means(df: pd.DataFrame) -> pd.DataFrame:
    """
    :param df: a dataframe with numeric columns.
    :return: a 1-dimensional dataframe of means per column.
    """
    mean_per_column = df.mean(
        # operate across rows.
        axis=1,
        skipna=True,
        # skip non-numeric columns
        numeric_only=True
    )
    return mean_per_column


def column_medians(df: pd.DataFrame) -> pd.DataFrame:
    """
    :param df: a dataframe with numeric columns.
    :return: a 1-dimensional dataframe of medians per column.
    """
    median_per_column = df.median(
        # operate across rows.
        axis=0,
        skipna=True,
        # skip non-numeric columns
        numeric_only=True
    )
    return median_per_column


def row_quartiles(
        df: pd.DataFrame,
        interpolation: str = 'linear',
        numeric_only: bool = False
) -> pd.DataFrame:
    """
    # TODO test
    :param df: numeric (or datetime
    :param interpolation: how to interpolate between values.
    :param numeric_only: whether or not to ignore datetime/timedelta iqr's.
    :return:
    """
    iqr_per_row = df.quantile(
        q=[0, 0.25, 0.50, 0.75, 1.0],
        axis=1,
        numeric_only=numeric_only,
        interpolation=interpolation
    )
    return iqr_per_row


def column_quartiles(
        df: pd.DataFrame,
        interpolation: str = 'linear',
        numeric_only: bool = False
) -> pd.DataFrame:
    """
    # TODO test
    :param df: numeric (or datetime
    :param interpolation: how to interpolate between values.
    :param numeric_only: whether or not to ignore datetime/timedelta iqr's.
    :return:
    """
    iqr_per_column = df.quantile(
        q=[0, 0.25, 0.50, 0.75, 1.0],
        axis=0,
        numeric_only=numeric_only,
        interpolation=interpolation
    )
    return iqr_per_column


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

	building = data[get_building_accesspoints(data, building_name)].copy()
	building.index = pd.to_datetime(building.index, utc=True)
	building['y'] = building.sum(axis=1)
	building = building.resample('D').mean()
	building = building['y']
	building = pd.DataFrame(building).reset_index()
	building.columns = ['ds', 'y']
	building['ds'] = building['ds'].astype(str).str[:-15]

	return building


def test_all():
    """
    Unit tests for this file's functions.
    :return:
    """
    def test_fill_intervening_nas() -> None:
        series1 = pd.Series([np.nan, 3, np.nan, 3, 3, np.nan])
        series1_filled = pd.Series([np.nan, 3, 0, 3, 3, np.nan])

        series2 = pd.Series([3, 3, 3, 3, 3, 3])
        series2_filled = series2.copy(deep=True)

        series3 = pd.Series([np.nan, 3, 3, 3, 3, 3])
        series3_filled = pd.Series([np.nan, 3, 3, 3, 3, 3])

        series4 = pd.Series([3, 3, 3, 3, 3, np.nan])
        series4_filled = pd.Series([3, 3, 3, 3, 3, np.nan])

        df = pd.DataFrame.from_dict({
            'col1': series1,
            'col2': series2,
            'col3': series3,
            'col4': series4
        })
        df_original = df.copy(deep=True)

        df_filled = pd.DataFrame.from_dict({
            'col1': series1_filled,
            'col2': series2_filled,
            'col3': series3_filled,
            'col4': series4_filled
        })

        # this should not affect the original.
        fill_intervening_nas(df, inplace=False)
        assert df.equals(df_original)

        fill_intervening_nas(df, inplace=True)
        assert df.equals(df_filled)
        pass

    test_fill_intervening_nas()


if __name__ == '__main__':
    test_all()
