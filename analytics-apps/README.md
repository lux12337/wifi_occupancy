# Analytics Apps

## Description

This directory holds a series of Jupyter Notebooks (`*.ipynb`), some
sample data (`*.csv`), and a small collection of modules used by the
notebooks (`modules/*`).

The Jupyter Notebooks show the data's distribution and/or make forecasts into
the future based on the dataset.

## Customizing the Notebooks

The notebooks each have a few parameters which can be changed by the user.
These often include:

* The dataset to analyze.
* The timezone of a time-series dataset.
* The schema of the dataset.
* The buildings to focus on.

### Changing the datasets

Our Jupyter Notebooks use two types of datasets, and we offer samples for each:
`sample_data.csv` and `course_enrollment_sample_data.csv`.

`sample_data.csv` is an **access point time series** dataset with values for
the number of devices connected to access points through some timeframe.

`course_enrollment_sample_data.csv` is a simple dataset about course enrollment.
Replacing this dataset is simple. One just has to replace the values of the csv
and *keep the column names*.

#### Changing the Access Point Time Series 

Our current requirements for the dataset are as follows:

* The first row is the header.
* The rows represent points in time.
* The first column holds timestamps (whose format is inferred by pandas).
* The columns represent access points.
  * Each access point corresponds to a single building, and its column name can
  be mapped to its building.

The timezone should be specified in the notebook parameters.

In addition to replacing the csv, one must also specify how access point names
are mapped to buildings. This is done by either editing `modules/College1AcPtTimeSeries.py`
or creating a new subclass of `AcPtTimeSeries` in `modules/schemas.py`.

##### `AcPtTimeSeries`

`AcPtTimeSeries` is an abstract class with 2 functions in `modules/schemas.py`.

```python
from typing import Set, Union, Hashable
from abc import abstractmethod

class AcPtTimeSeries:
    @classmethod
    @abstractmethod
    def buildings(cls) -> Set[Hashable]:
        pass

    @classmethod
    @abstractmethod
    def col_to_building(
            cls, col: Hashable, safe: bool = False
    ) -> Union[any, None]:
        pass
```

`buildings` returns the set of all building names in the time-series.

`col_to_building` takes a column name (`col`) and returns the name of the building
corresponding to the column's access point. If no building matches or too many buildings
match, then an exception is thrown. If `safe` (the second, optional parameter) is true,
then `None` is returned instead of an error being thrown.

This abstract class provides the interface our notebooks expect. However, subclasses must
be created in order to use the notebooks.

##### A sample subclass: `College1AcPtTimeSeries`

In order to show how this works, and to provide a simple template, we created a subclass
named `College1AcPtTimeSeries` in `modules/College1AcPtTimeSeries.py`.

It contains a large set of buildings names. **A simple solution would be to replace these
with your own building names**. However, they'd have to satisfy the longest-substring-match
strategy described below.

```python
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
```

The `col_to_building` function uses the longest-substring-match strategy to match
column names to building names.

```
buildings = { b0, b1, b2, b00 }
columns = [ a1-b0, a2-b0, a3-b1, a4-b2, a3-b00 ]

a1-b0 matches b0
a2-b0 matches b0
a3-b1 matches b1
a4-b2 matches b2
a3-b00 matches b00
```

Notice how `a3-b00` matches with `b00` instead of `b0` even though `b0` is a substring of
`a3-b00`. This is because `b00` is the longer substring match.

The longest-substring-match strategy works well for most cases, so it's usually fine to just
replace the building names.

##### Using the subclass

In our notebooks, you'll often see this parameter near the top of the notebook:
```python
from modules import schemas, College1AcPtTimeSeries
# ...
schema: schemas.AcPtTimeSeries = College1AcPtTimeSeries.College1AcPtTimeSeries
```

This is how our notebook takes the class. You use your subclass like so:

```python
from modules import schemas
# ...
schema: schemas.AcPtTimeSeries = MySubclass
```
