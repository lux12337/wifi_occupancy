from Local_DB import local_db
from remote_db import uri
from remote_db.RemoteDB import RemoteDB
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys
import configparser
from typing import Dict, Callable

# Luigi, Jose, Jasmine, Katelyn

# command-line argument
config_filename = sys.argv[1]
# directory where the program resides
project_path = os.path.dirname(os.path.realpath(__file__))

"""set up logger"""

logger = logging.getLogger("push_to_melrok")
logger.setLevel(logging.DEBUG)

# create directory
if not os.path.exists(project_path+'/logs'):
    os.makedirs(project_path+'/logs')

handler = TimedRotatingFileHandler(
    project_path+"/logs/api.log",
    when='D',
    interval=1,
    backupCount=5
)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)

"""set up local db"""

engine = local_db(project_path = project_path)
data = engine.read_local_DB()

"""read the config file"""

if not os.path.exists(project_path + "/" + config_filename):
    logger.error("cannot find config_file={}".format(config_filename))
    raise Exception("config file not found")

Config = configparser.ConfigParser()
Config.read(project_path + "/" + config_filename)
logger.info("successfully loaded config_file={}".format(config_filename))


uri_args: Dict[str, str] = {}

for parameter in [
    'database_type', 'host', 'username', 'password', 'database', 'filename'
]:
    try:
        uri_args[parameter] = Config.get('remote_db', parameter)
    except Exception as e:
        print('did not find parameter "{}" in config file'.format(parameter))


"""create the database uri"""

# get the uri function for this type of database
uri_creator: Callable[..., str] = uri.get_uri_function(uri_args['database_type'])

# apply the arguments
uri_: str = uri_creator(**uri_args)

"""push to external db"""

remote = RemoteDB(dal_or_uri=uri_)
remote.push_to_remote(data)
engine.delete_data_sent(data)
