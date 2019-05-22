from typing import Optional, List, Union, Dict, NamedTuple
import numpy as np
import pandas as pd
import pytz


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


class ColNameComponents(NamedTuple):
    college: str
    building: str
    acpt_num: str


def decompose_col_name(col_name: str) -> Union[ColNameComponents, None]:
    """
    :param col_name: name of a column in the csv
    :return: the building name substring or None if something went wrong.
    """

    split_by_AP: List[str] = col_name.split('-AP')

    # Fail if there is no '-AP' substring or too many.
    if len(split_by_AP) != 2:
        return None

    index_of_first_dash: int = split_by_AP[0].find('-')

    if index_of_first_dash == -1:
        return None

    # TODO get rid of number after building.

    college: str = split_by_AP[0][0:index_of_first_dash].strip()
    building: str = split_by_AP[0][index_of_first_dash+1:].strip()
    acpt: str = split_by_AP[1].strip()

    return ColNameComponents(college=college, building=building, acpt_num=acpt)


def col_names_to_building_indices(
    col_names: List[str]
) -> Union[np.ndarray, None]:
    """
    :param col_names:
    :return:
    """
    groupids: np.ndarray = np.zeros(len(col_names))

    building_names: Dict[str, int] = {}
    unique_count = 0

    for i in range(0, len(col_names)):

        comps: Union[None, ColNameComponents] = decompose_col_name(col_names[i])

        if comps is None:
            return None

        if comps.building not in building_names:
            building_names[comps.building] = unique_count = unique_count + 1

        groupids[i] = building_names[comps.building]

    return groupids


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

    print(building_indices)
