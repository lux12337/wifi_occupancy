"""
A module of miscellaneous tools for analysis.
These tools should ideally be moved to themed modules later on.
"""

from typing import List, Union, Dict, NamedTuple, Tuple, Set, Sequence,\
    Hashable, Iterable, Callable
import numpy as np
from .schemas import AcPtTimeSeries


class XY(NamedTuple):
    """
    Matplotlib commonly requires parallel lists of x and y coordinates
    for plotting.
    """
    x: Union[np.ndarray, List]
    y: Union[np.ndarray, List]


def classifying_indices(lst: Sequence[Hashable]) -> np.ndarray:
    """
    Useful for grouping items in a list together.
    :param lst: A list of immutable items.
    :return: A numpy vector (1d ndarray) of indices pointing to the first
    element in lst which matches that element.
    """
    ids: np.ndarray = np.zeros(len(lst), dtype=np.uint64)

    items_to_ids: Dict[Hashable, int] = {}
    id_count = -1  # the first id will be 0

    for i in range(0, len(lst)):
        if lst[i] not in items_to_ids:
            items_to_ids[lst[i]] = id_count = id_count+1

        ids[i] = items_to_ids[lst[i]]

    return ids


def classify_cols_by_building(
        schema: AcPtTimeSeries, cols: List[Hashable],
        include_buildings: bool = False
) -> Union[np.ndarray, Tuple[np.ndarray, List[Hashable]]]:
    """
    Useful for grouping columns of access points by building if one doesn't
    care what the
    :param schema: class instance specifying how building names should be
    extracted from column names.
    :param cols: The names of columns.
    :param include_buildings: should the building names be returned?
    :return:
    A np vector (1-d ndarray) of indices classifying which building
    each column belongs to.
    A dictionary mapping building names to the the indices in the np vector.
    """
    buildings: List[Hashable] = cols_to_buildings(schema, cols, safe=False)
    ids: np.ndarray = classifying_indices(buildings)

    return (ids, buildings) if include_buildings else ids


def cols_to_buildings(
        schema: AcPtTimeSeries, cols: List[Hashable], safe: bool = False
) -> List[Union[Hashable, None]]:
    return list(map(
        lambda c: schema.col_to_building(col=c, safe=safe),
        cols
    ))


def cols_to_in_buildings(
        schema: AcPtTimeSeries, cols: List[Hashable], buildings: Set[Hashable]
) -> np.ndarray:
    """
    Useful for finding the columns associated with a list of buildings.
    :param schema: class instance specifying how building names should be
    extracted from column names.
    :param cols: Column names.
    :param buildings: The buildings one wants to match.
    :return:
    """
    return np.array(
        list(map(
            lambda col: col is not None and col in buildings,
            cols_to_buildings(schema, cols, safe=True)
        )),
        dtype=bool
    )


def longest_substr(string: str, substrs: Iterable[str]) -> List[str]:
    """
    Returns a list of equally-lengthed substrings of string.
    :param string:
    :param substrs: A collection of possible substrings.
    :return: The longest substrs member which are substrings of string.
    """
    matches: List[str] = []
    for s in substrs:
        if s in string:
            if len(matches) == 0 or len(s) > len(matches[0]):
                matches = [s]
            elif len(s) == len(matches[0]):
                matches.append(s)
    return matches


def longest_substr_matcher(substrs: Iterable[str]) -> Callable[[str], List[str]]:
    """
    Binds misc.longest_substr to a particular collection of substrs.
    :param substrs:
    :return:
    """
    def bound_matcher(string: str) -> List[str]:
        return longest_substr(string=string, substrs=substrs)
    return bound_matcher
