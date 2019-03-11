from Local_DB import local_db
from Remote_DB import remote_db
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from pandas import DataFrame, to_datetime, DatetimeIndex

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

"""manipulate the dataframe"""

columns = list(data.columns)

# for influx, ts must in datetime format
data['ts'] = DatetimeIndex(data['ts'].apply(
    # YYYY-MM-DD HH:MM:SS
    lambda ts: to_datetime(
        arg=ts, format='%Y%m%d%H%M%S'
        # ts[:4] + '-' + ts[4:6] + '-' + ts[6:8]
        # + ' ' + ts[8:10] + ':' + ts[10:12] + ':' + ts[12:14]
    )
))
ts_index: int = columns.index('ts')
# swap to ensure that ts is the first column
columns[0], columns[ts_index] = columns[ts_index], columns[0]
# ensure DateTimeIndex
data = data[columns]
data.set_index('ts', inplace=True)

"""push to remote db"""

# TODO: push to external db - add code
remote = remote_db()
# remote.push_to_remote(data)
# engine.delete_data_sent(data)
# remote.drop_table()
# remote.test(data)
remote.push_to_influx_database(
    data=data, measurement='wifi'
)
