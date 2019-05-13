from typing import Optional, Set, List, Union
from functools import lru_cache
from .specifics import AcPtTimeSeries


class College1AcPtTimeSeries(AcPtTimeSeries):
    @classmethod
    @lru_cache()
    def buildings(cls) -> Set[str]:
        return {
            'POMONA', '118-8TH', '1567TH', '345C',
            'ALEXANDER', 'ANDREW', 'BALDWIN', 'BRACKETT', 'BRIDGES', 'CARNEGIE',
            'CLARK3', 'CLARKI', 'CLARKV', 'CROOKSHANK',
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
            cls, name: str, index: Optional[int] = None, safe: bool = False
    ) -> Union[str, None]:
        matches: List[str] = list(filter(
            lambda build: build in name, cls.buildings()
        ))

        if len(matches) == 0 and safe:
            return None
        elif len(matches) == 0:
            raise Exception("No matching buildings for '{}'".format(name))
        elif len(matches) == 1:
            return matches[0]

        # sort ascending in string length.
        matches.sort(key=len)
        # return the longest match.
        return matches[-1]

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
                ('POM-CLARKV401-AP205-1', 'CLARKV'),
                ('POM-CARNEGIE215-AP215-5', 'CARNEGIE')
            ]:
                assert cls.col_to_building(colname) == building

        test_col_to_building()
        print('all tests passed')


if __name__ == '__main__':
    College1AcPtTimeSeries.run_tests()