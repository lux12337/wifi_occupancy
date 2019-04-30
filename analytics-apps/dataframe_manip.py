from typing import Optional, List, Union, Set
import numpy as np
import pandas as pd
import seaborn
import matplotlib.pyplot as plt
import re


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


college_pattern = re.compile(
    '^\\w+'
)

building_pattern = re.compile(
    '\\w+\\d*'
    + '(-\\w*\\d*)*'
)

acpt_pattern = re.compile(
    'AP\\d*'
    + '(-\\d*)*'
)

col_name_pattern = re.compile(
    '^'
    # college
    + '\\w+'
    + '-'
    # building
    + '\\w+\d*' + '(-\\w*\\d*)*'
    # access point tag
    + '-'
    + 'AP\\d*' + '(-\\d*)*'
    + '$'
)


def col_name_to_building(col_name: str) -> Union[str, None]:
    """
    # TODO test
    :param col_name: name of a column in the csv
    :return: the building name substring or None if something went wrong.
    """

    buildings: List[str] = re.findall(
        building_pattern, col_name
    )

    if len(buildings) != 1:
        return None

    return buildings[0]


def col_names_to_building_indices(col_names: List[str]) -> List[int]:
    """
    :param col_names:
    :return:
    """
    groupids: List[int] = []

    building_names: Set[str] = set({})
    i = -1

    for b in col_names:
        if b in building_names:
            groupids.append(i)
        else:
            bn = col_name_to_building(b)
            building_names.add(bn)
            groupids.append(++i)

    return groupids


def csv_to_timeseries_df(filepath: str, nrows: Optional[int] = None) -> pd.DataFrame:
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

    # if not isinstance(dataframe.index, pd.DatetimeIndex):
    # 	dataframe.index = pd.to_datetime(
    # 		dataframe.index, utc=True
    # 	)

    return dataframe


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


def column_means(df: pd.DataFrame, skipna: bool = True) -> pd.DataFrame:
    """
    :param df: a dataframe with numeric columns.
    :param skipna: should missing values be skipped?
    :return: a 1-dimensional dataframe of means per column.
    """
    mean_per_column = df.mean(
        # operate across rows.
        axis=0,
        # treat na values as 0
        skipna=skipna,
        # skip non-numeric columns
        numeric_only=True
    )
    return mean_per_column


def row_means(df: pd.DataFrame, skipna: bool = True) -> pd.DataFrame:
    """
    :param df: a dataframe with numeric columns.
    :param skipna: should missing values be skipped?
    :return: a 1-dimensional dataframe of means per column.
    """
    mean_per_column = df.mean(
        # operate across rows.
        axis=1,
        # treat na values as 0
        skipna=skipna,
        # skip non-numeric columns
        numeric_only=True
    )
    return mean_per_column


def column_medians(df: pd.DataFrame, skipna: bool = True) -> pd.DataFrame:
    """
    :param df: a dataframe with numeric columns.
    :param skipna: should missing values be skipped?
    :return: a 1-dimensional dataframe of medians per column.
    """
    median_per_column = df.median(
        # operate across rows.
        axis=0,
        # treat na values as 0
        skipna=skipna,
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


if __name__ == '__main__':
    data: pd.DataFrame = csv_to_timeseries_df(
        filepath='./wifi_data_until_20190204.csv'
    )
    print(data.columns)
    print(data.dtypes)
    print(data.index)
    print(data.index.dtype)
    print(data.shape)

    building_indices = col_names_to_building_indices(
        data.columns
    )

    print(list(filter(lambda x: x is None, building_indices)))
