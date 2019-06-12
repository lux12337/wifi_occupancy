from typing import Set, List, Union
from functools import lru_cache
from .schemas import AcPtTimeSeries
from .misc import longest_substr


class College1AcPtTimeSeries(AcPtTimeSeries):
    _buildings = {
        'BUILDING00', 'BUILDING01', 'BUILDING02', 'BUILDING03', 'BUILDING04',
        'BUILDING05', 'BUILDING06', 'BUILDING07', 'BUILDING08', 'BUILDING09',
        'BUILDING10', 'BUILDING11', 'BUILDING12', 'BUILDING13', 'BUILDING14',
        'BUILDING15', 'BUILDING16', 'BUILDING17', 'BUILDING18', 'BUILDING19',
        'BUILDING20', 'BUILDING21', 'BUILDING22', 'BUILDING23', 'BUILDING24',
        'BUILDING25', 'BUILDING26', 'BUILDING27', 'BUILDING28', 'BUILDING29',
        'BUILDING30', 'BUILDING31', 'BUILDING32', 'BUILDING33', 'BUILDING34',
        'BUILDING35', 'BUILDING36', 'BUILDING37', 'BUILDING38', 'BUILDING39',
        'BUILDING40', 'BUILDING41', 'BUILDING42', 'BUILDING43', 'BUILDING44',
        'BUILDING45', 'BUILDING46', 'BUILDING47', 'BUILDING48', 'BUILDING49',
        'BUILDING50', 'BUILDING51'
    }

    @classmethod
    def buildings(cls) -> Set[str]:
        return College1AcPtTimeSeries._buildings

    @classmethod
    @lru_cache()
    def col_to_building(
            cls, col: str, safe: bool = False
    ) -> Union[str, None]:
        """
        Overrides AcPtTimeSeries method.
        This uses the longest string match strategy to find the appropriate
        building.
        :param col: the column index. The type should match this schema's
        column index type. e.g. integers, strings, datetimes, etc.
        :param safe: Should this function throw an exception or safely return
        None if there are no matches?
        :return: The building name corresponding to col.
        """
        if not isinstance(col, str):
            if safe:
                return None
            else:
                raise Exception('col should be of type str')

        matches: List[str] = longest_substr(col, cls.buildings())

        if len(matches) == 0:
            if safe:
                return None
            raise Exception("No matching buildings for '{}'".format(col))
        elif len(matches) > 1:
            raise Exception("Multiple matching buildings for '{}'".format(col))
        else:
            return matches[0]
