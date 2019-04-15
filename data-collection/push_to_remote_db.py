from Local_DB import local_db
from Remote_DB import remote_db
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from pandas import DataFrame
from typing import Dict

# @author : Marco Pritoni <mpritoni@lbl.gov>
# @author : Anand Prakash <akprakash@lbl.gov>

"""set the project path"""

project_path = os.path.dirname(os.path.realpath(__file__))

"""set up logging"""

logger = logging.getLogger("push_to_melrok")
logger.setLevel(logging.DEBUG)
if not os.path.exists(project_path+'/logs'):
    os.makedirs(project_path+'/logs')
handler = TimedRotatingFileHandler(project_path+"/logs/api.log", when='D', interval=1, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

"""acquire data to push"""

# Get the data (as a DataFrame) from the local database.
engine = local_db( project_path=project_path )
data: DataFrame = engine.read_local_DB()

"""push to the remote db"""

remote = remote_db()
remote.push_to_remote_db(data=data)
# remote.drop_table()
engine.delete_data_sent(data)
print('Success')
