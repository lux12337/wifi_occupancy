from Local_DB import local_db
from Remote_DB import remote_db
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from pandas import DataFrame, to_datetime, DatetimeIndex
from typing import Dict

# @author : Marco Pritoni <mpritoni@lbl.gov>
# @author : Anand Prakash <akprakash@lbl.gov>
project_path = os.path.dirname(os.path.realpath(__file__))

logger = logging.getLogger("push_to_melrok")
logger.setLevel(logging.DEBUG)
if not os.path.exists(project_path+'/logs'):
    os.makedirs(project_path+'/logs')
handler = TimedRotatingFileHandler(project_path+"/logs/api.log", when='D', interval=1, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

engine = local_db(project_path = project_path)
data: DataFrame = engine.read_local_DB()
data_as_dict = data.to_dict()

"""manipulate the dataframe"""

def preprocess_data(data: DataFrame) -> DataFrame:
    """
    TODO ask how data should be processed to avoid bug where
    elements of repeating timestamps are consolidated.
    :param data:
    :return:
    """
    # debugging
    data_as_dict: Dict = data.to_dict()

    columns = list(data.columns)

    # for influx, ts must in datetime format
    data['ts'] = DatetimeIndex(data['ts'].apply(
        # "YYYY-MM-DD HH:MM:SS"
        lambda ts: to_datetime(
            arg=ts, format='%Y%m%d%H%M%S'
        )
    ))

    # debugging
    data_as_dict = data.to_dict()

    # swap to ensure that ts is the first column
    ts_index: int = columns.index('ts')
    columns[0], columns[ts_index] = columns[ts_index], columns[0]
    data = data[columns]

    # debugging
    data_as_dict = data.to_dict()

    # ensure that index is DatetimeIndex
    data.set_index('ts', inplace=True)

    #debugging
    data_as_dict = data.to_dict()

    return data


data = preprocess_data(data)

"""push to remote db"""

# TODO: push to external db - add code
remote = remote_db()
# remote.push_to_remote(data)
# engine.delete_data_sent(data)
# remote.drop_table()
# remote.test(data)
remote.print_influx_status()
remote.push_to_remote_db(
    data=data, influx_measurement='wifi'
)
print('after push')
remote.print_influx_status()
