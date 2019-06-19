"""
Users will need to provide some specifics about their data for the analytics to
work.
Users will create subclasses out of the following classes to provide these
specifics.
"""
from typing import Set, Union, Hashable
from abc import abstractmethod


class AcPtTimeSeries:
    """
    Meant for time-series Dataframes where each column (except the first) is
    an access point.
    """

    @classmethod
    @abstractmethod
    def buildings(cls) -> Set[Hashable]:
        """
        :return: the set of all building names.
        """
        pass

    @classmethod
    @abstractmethod
    def col_to_building(
            cls, col: Hashable, safe: bool = False
    ) -> Union[any, None]:
        """
        :param col: the column name.
        :param safe: Should this function throw an exception or safely return
        None if there are 0 or more than 1 match?
        :return: The building name corresponding to col.
        """
        pass
