import os
import logging
import pandas
import configparser
from logging.handlers import TimedRotatingFileHandler
from pydal import DAL, Field

# Luigi, Katelyn, Jasmine, Jose

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
            self.database = Config.get('remote_db', 'database')
            self.port = Config.get('remote_db', 'port')
        except Exception as e:
            self.logger.error("unexpected error while setting configuration from config_file={}, error={}".format(self.config_file, str(e)))
            raise e

        """
        create a connection to the remote db
        """

        self.db = self.create_DB_connection()


    def create_DB_connection(self):

        """
        this method trys to establish a db connection
        """

        try:
            db = DAL('postgres://{}:{}@{}:{}/{}'.format(self.username, self.password, self.host, self.port, self.database))
            db.define_table('wifi_table', Field('AP_id'), Field('value'), Field('ts'))
            self.logger.info("remote db connection successfully established")
            return db

        except Exception as e:
            self.logger.error("could not connect to remote db")
            raise e


    def push_to_remote(self, data):

        """
        this method pushes a pandas dataframe to the remote db
        """

        try:
            for i, row in data.iterrows():
                self.db.wifi_table.insert(AP_id=row['id'], value=row['value'], ts=row['ts'])
            self.db.commit()
            self.logger.info("data successfully pushed to remote db")

        except Exception as e:
            self.logger.error("pushing to remote database failed")
            raise e


if __name__ == '__main__':
    remote = remote_db()
