from typing import Optional, List, Union, Dict, NamedTuple, Set
import numpy as np
import pandas as pd
import pytz
import re
import math


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
    building_amendum: str
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

    college: str = split_by_AP[0][0:index_of_first_dash].strip()
    building_w_amendum: str = split_by_AP[0][index_of_first_dash + 1:].strip()
    acpt: str = split_by_AP[1].strip()

    amendum_match = re.search(r'[^a-zA-Z]', building_w_amendum)

    if amendum_match is None or amendum_match.start() == 0:
        building: str = building_w_amendum
        building_amendum = ''
    else:
        amendum_start: int = amendum_match.start()
        building: str = building_w_amendum[:amendum_start]
        building_amendum: str = building_w_amendum[amendum_start:]

    return ColNameComponents(
        college=college,
        building=building,
        building_amendum=building_amendum,
        acpt_num=acpt
    )


def col_names_to_building_indices(
        col_names: List[str]
) -> Union[np.ndarray, None]:
    """
    :param col_names:
    :return:
    """
    groupids: np.ndarray = np.zeros(len(col_names), dtype=np.uint64)

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


def col_names_to_building_names(col_names: List[str]) -> Set[str]:

    def building_name_or_none(col: str):
        comps = decompose_col_name(col)
        return None if comps is None else comps.building

    return set(map(
        lambda n: building_name_or_none(n),
        col_names
    ))


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

    df_or_series.apply(
        func=lambda ser: ser if isinstance(ser, pd.Index) else fill_series(ser),
        axis=0
    )
    return df_or_series


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
        filepath='./wifi_data_until_20190204.csv',
        timezone=pytz.timezone('US/Pacific')
    )
    fill_intervening_nas(df_or_series=data, inplace=True, fill_val=0)
    # print(data.columns)
    # print(data.dtypes)
    # print(data.index)
    # print(data.index.dtype)
    # print(data.shape)
    #
    # building_indices = col_names_to_building_indices(data.columns)
    #
    # print(building_indices)
    #
    # auto_names = col_names_to_building_names(data.columns)
    # manual_names = {
    #     'POMONA', '118-8TH', '1567TH', '345C',
    #     'ALEXANDER', 'ANDREW', 'BALDWIN', 'BRACKETT', 'BRIDGES', 'CARNEGIE',
    #     'CLARK3', 'CLARKI', 'CLARKV', 'CROOKSHANK',
    #     'DRAPER', 'FARM', 'FRANK', 'FRARY', 'GIBONEY', 'GIBSON', 'GROUNDS',
    #     'HAHN', 'HALDEMAN', 'HARWOOD', 'ITB', 'KENYON', 'LAWRY', 'LEB', 'LEBUS',
    #     'MASON', 'MCCARTHY', 'MERRIT', 'MILLIKAN', 'MUSEUM', 'NORTON',
    #     'OLDENBORG', 'PAULEY', 'PEARSON', 'PENDLETON', 'POMONA', 'RAINS',
    #     'REMBRANDT', 'SCC', 'SEAVER', 'SGM', 'SMILEY', 'SMITH', 'SONTAG',
    #     'STUDIOART', 'SUMNER', 'THATCHER', 'WALKER', 'WALTON', 'WIG'
    # }
    #
    # print('auto - manual')
    # print(sorted(list(auto_names - manual_names)))
    # print('manual - auto')
    # print(sorted(list(manual_names - auto_names)))
    # print('auto')
    # print(sorted(list(auto_names)))
    # print('manual')
    # print(sorted(list(manual_names)))

    series = pd.Series([math.nan, 3, math.nan, 3, 3, math.nan])

    df = pd.DataFrame.from_dict({
        'col1': series.copy(deep=True),
        'col2': series.copy(deep=True)
    })

    print(fill_intervening_nas(df, inplace=False))
    print(df)
    fill_intervening_nas(df, inplace=True)
    print(df)

    print(fill_intervening_nas(series, inplace=False))
    print(series)
    fill_intervening_nas(series, inplace=True)
    print(series)