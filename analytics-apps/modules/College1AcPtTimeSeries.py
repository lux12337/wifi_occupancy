from typing import Set, List, Union
from functools import lru_cache
from .schemas import AcPtTimeSeries
from .misc import longest_substr_matches


class College1AcPtTimeSeries(AcPtTimeSeries):
    @classmethod
    @lru_cache()
    def buildings(cls) -> Set[str]:
        return {
            'POMONA', '118-8TH', '1567TH', '345C',
            'ALEXANDER', 'ANDREW', 'BALDWIN', 'BRACKETT', 'BRIDGES', 'CARNEGIE',
            'CLARK', 'CROOKSHANK',
            'DRAPER', 'FARM', 'FRANK', 'FRARY', 'GIBONEY', 'GIBSON', 'GROUNDS',
            'HAHN', 'HALDEMAN', 'HARWOOD', 'ITB', 'KENYON', 'LAWRY', 'LEB', 'LEBUS',
            'MASON', 'MCCARTHY', 'MERRIT', 'MILLIKAN', 'MUSEUM', 'NORTON',
            'OLDENBORG', 'PAULEY', 'PEARSON', 'PENDLETON', 'POMONA', 'RAINS',
            'REMBRANDT', 'SCC', 'SEAVER', 'SGM', 'SMILEY', 'SMITH', 'SONTAG',
            'STUDIOART', 'SUMNER', 'THATCHER', 'WALKER', 'WALTON', 'WIG'
        }

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

        matches: List[str] = longest_substr_matches(col, cls.buildings())

        if len(matches) == 0 and safe:
            return None
        elif len(matches) == 0:
            raise Exception("No matching buildings for '{}'".format(col))
        elif len(matches) == 1:
            return matches[0]
        else:
            raise Exception("Multiple matching buildings for '{}'".format(col))

    @classmethod
    def run_tests(cls) -> None:
        """
        Run unit tests.
        """
        def test_col_to_building() -> None:
            """
            A unit test for col_to_building.
            """
            for colname, building in [
                ('POM-SUMNER212-AP205-2', 'SUMNER'),
                ('POM-CLARKV401-AP205-1', 'CLARK'),
                ('POM-CARNEGIE215-AP215-5', 'CARNEGIE')
            ]:
                assert cls.col_to_building(colname) == building

        test_col_to_building()
        print('all tests passed')


if __name__ == '__main__':
    College1AcPtTimeSeries.run_tests()