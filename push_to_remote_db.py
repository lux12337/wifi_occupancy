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

"""push to remote db"""

# TODO: push to external db - add code
remote = remote_db()
# remote.push_to_remote(data)
# engine.delete_data_sent(data)
# remote.drop_table()
# remote.test(data)
# remote.print_influx_status()
remote.push_to_remote_db(
    data=data, influx_measurement='wifi'
)
print('after push')
# remote.print_influx_status()
