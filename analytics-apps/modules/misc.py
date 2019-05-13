"""
A module of miscellaneous tools for analysis.
These tools should ideally be moved to themed modules later on.
"""

from typing import List, Union, Dict, NamedTuple, Tuple, Set, Sequence
import numpy as np
from .specifics import AcPtTimeSeries


class XY(NamedTuple):
    """
    Matplotlib commonly requires parallel lists of x and y coordinates
    for plotting.
    """
    x: Union[np.ndarray, List]
    y: Union[np.ndarray, List]


def classifying_indices(lst: Sequence[any]) -> np.ndarray:
    """
    Useful for grouping items in a list together.
    :param lst: A list of immutable items.
    :return: A numpy vector (1d ndarray) of indices pointing to the first
    element in lst which matches that element.
    """
    ids: np.ndarray = np.zeros(len(lst), dtype=np.uint64)

    items_to_ids: Dict[any, int] = {}
    id_count = -1  # the first id will be 0

    for i in range(0, len(lst)):
        if lst[i] not in items_to_ids:
            items_to_ids[lst[i]] = id_count = id_count+1

        ids[i] = items_to_ids[lst[i]]

    return ids


def col_names_to_building_indices(
        schema: AcPtTimeSeries, col_names: List[str],
        include_building_names: bool = False
) -> Union[np.ndarray, Tuple[np.ndarray, List[str]]]:
    """
    Useful for grouping columns of access points by building if one doesn't
    care what the
    :param schema: class instance specifying how building names should be
    extracted from column names.
    :param col_names: The names of columns.
    :param include_building_names: should the building names be returned?
    :return:
    A np vector (1-d ndarray) of indices classifying which building
    each column belongs to.
    A dictionary mapping building names to the the indices in the np vector.
    """
    building_names = col_names_to_building_names(schema, col_names)
    ids: np.ndarray = classifying_indices(building_names)

    return (ids, building_names) if include_building_names else ids


def col_names_to_building_names(
        schema: AcPtTimeSeries, col_names: List[str]
) -> List[str]:
    return list(map(
        lambda ci: schema.col_to_building(ci[0], ci[1]),
        zip(col_names, range(0, len(col_names)))
    ))


def col_names_to_in_buildings(
        schema: AcPtTimeSeries, col_names: List[str], buildings: Set[str]
) -> np.ndarray:
    """
    Useful for finding the columns associated with a list of buildings.
    :param schema: class instance specifying how building names should be
    extracted from column names.
    :param col_names: Column names.
    :param buildings: The buildings one wants to match.
    :return:
    """
    return np.ndarray(
        shape=(len(col_names),),
        dtype=bool,
        buffer=list(map(
            lambda col: col in buildings,
            col_names_to_building_names(schema, col_names)
        ))
    )
