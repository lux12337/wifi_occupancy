from Local_DB import local_db
from remote_db import uri
from remote_db.RemoteDB import RemoteDB
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys
import configparser

# @author : Marco Pritoni <mpritoni@lbl.gov>
# @author : Anand Prakash <akprakash@lbl.gov>

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
Config.read(project_path + "/" + config_file)
logger.info("successfully loaded config_file={}".format(config_filename))

try:
    """
    TODO expect different config file values based on remote_db.uri resources,
    which explicitly state the optional/required parameters for each database.
    """
    host = Config.get('remote_db', 'host')
    username = Config.get('remote_db', 'username')
    password = Config.get('remote_db', 'password')
    database = Config.get('remote_db', 'database')
except Exception as e:
    logger.error(
        "unexpected error while setting configuration from config_file={}, error={}"
        .format(config_filename, str(e))
    )
    raise e

# TODO dynamically assign db_name and arguments
uri_ = uri.dbs['mysql'][uri.uri_](
    usename=username,
    password=password,
    host=host,
    database=database
)

"""push to external db"""

#TODO: push to external db - add code
remote = RemoteDB(uri_or_db=uri_)
remote.push_to_remote(data)
engine.delete_data_sent(data)
