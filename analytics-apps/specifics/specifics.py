"""
Users will need to provide some specifics about their data for the analytics to
work.
Users will create subclasses out of the following classes to provide these
specifics.
"""
from typing import Optional, Set, List, Union
from abc import ABC, abstractmethod
from functools import lru_cache

class AcPtTimeSeries:
    """
    Meant for time-series Dataframes where each column (except the first) is
    an access point.
    """

    @classmethod
    @abstractmethod
    def buildings(cls) -> Set[str]:
        pass

    @classmethod
    @abstractmethod
    def col_to_building(cls, name: str, index: Optional[int]) -> str:
        pass
