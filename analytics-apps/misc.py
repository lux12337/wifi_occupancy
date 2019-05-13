"""
A module of miscellaneous tools for analysis.
These tools should ideally be moved to themed modules later on.
"""

from typing import List, Union, Dict, NamedTuple, Set
import numpy as np
from .specifics.specifics import AcPtTimeSeries


class XY(NamedTuple):
    """
    Matplotlib commonly requires parallel lists of x and y coordinates
    for plotting.
    """
    x: Union[np.ndarray, List]
    y: Union[np.ndarray, List]


def col_names_to_building_indices(
        specific: AcPtTimeSeries, col_names: List[str]
) -> Union[np.ndarray, None]:
    """
    :param specific: class instance specifying how building names should be
    extracted from column names.
    :param col_names: The names of columns.
    :return: A np vector (1-d ndarray) of indices classifying which building
    each column belongs to.
    """
    groupids: np.ndarray = np.zeros(len(col_names), dtype=np.uint64)

    building_names: Dict[str, int] = {}
    unique_count = 0

    for i in range(0, len(col_names)):
        building: str = specific.col_to_building(col_names[i], i)

        if building not in building_names:
            building_names[building] = unique_count = unique_count + 1

        groupids[i] = building_names[building]

    return groupids


def col_names_to_building_names(
        specific: AcPtTimeSeries, col_names: List[str]
) -> List[str]:
    return list(map(
        lambda ci: specific.col_to_building(ci[0], ci[1]),
        zip(col_names, range(0, len(col_names)))
    ))
