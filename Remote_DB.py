import os
import logging
import configparser
from logging.handlers import TimedRotatingFileHandler
from pydal import DAL

class remote_db():
    """
    This class establishes a connection with a remote db(TimescaleDB, InfluxDB, ORM) and pushes local data to it.
    """

    def __init__(self, project_path = ".", config_file="config.ini"):

        self.project_path = project_path
        """
        initialize logging
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        if not os.path.exists(self.project_path+"/"+'logs'):
            os.makedirs(self.project_path+"/"+'logs')
        handler = TimedRotatingFileHandler(self.project_path+"/"+"logs/remote_db.log", when='D', interval=1, backupCount=5)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        """
        read config file
        """
        self.config_file = config_file
        if not os.path.exists(self.project_path+"/"+self.config_file):
            self.logger.error("cannot find config_file={}".format(self.config_file))
            raise Exception("config file not found")

        Config = configparser.ConfigParser()
        Config.read(self.project_path+"/"+self.config_file)
        self.logger.info("successfully loaded config_file={}".format(self.config_file))

        try:
            self.host = Config.get('remote_db', 'host')
            self.username = Config.get('remote_db', 'username')
            self.password = Config.get('remote_db', 'password')
        except Exception as e:
            self.logger.error("unexpected error while setting configuration from config_file={}, error={}".format(self.config_file, str(e)))
            raise e

        #db = DAL('mysql://testing:admin@localhost/test')

if __name__ == '__main__':
    remote = remote_db()
