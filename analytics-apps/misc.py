"""
A module of miscellaneous tools for analysis.
These tools should ideally be moved to themed modules later on.
"""

from typing import List, Union, Dict, NamedTuple, Tuple
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
        schema: AcPtTimeSeries, col_names: List[str]
) -> Tuple[np.ndarray, Dict[str, int]]:
    """
    Useful for grouping columns of access points by building if one doesn't
    care what the
    :param schema: class instance specifying how building names should be
    extracted from column names.
    :param col_names: The names of columns.
    :return: A tuple containing two results:
    A np vector (1-d ndarray) of indices classifying which building
    each column belongs to.
    A dictionary mapping building names to the the indices in the np vector.
    """
    groupids: np.ndarray = np.zeros(len(col_names), dtype=np.uint64)

    building_names: Dict[str, int] = {}
    unique_count = 0

    for i in range(0, len(col_names)):
        building: str = schema.col_to_building(col_names[i], i)

        if building not in building_names:
            building_names[building] = unique_count = unique_count + 1

        groupids[i] = building_names[building]

    return groupids, building_names


def col_names_to_building_names(
        schema: AcPtTimeSeries, col_names: List[str]
) -> List[str]:
    return list(map(
        lambda ci: schema.col_to_building(ci[0], ci[1]),
        zip(col_names, range(0, len(col_names)))
    ))
