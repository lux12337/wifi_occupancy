import os
import logging
import pandas as pd
from pandas import DataFrame
import configparser
import time
import datetime
from logging.handlers import TimedRotatingFileHandler
from pydal import DAL, Field
from influxdb import DataFrameClient

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

        # self.create_DB_connection()
        self.set_up_influx_client()

    def create_DB_connection(self):
        """
        this method trys to establish a db connection
        """
        try:
            self.db = DAL('postgres://{}:{}@{}:{}/{}'.format(self.username, self.password, self.host, self.port, self.database))
            self.create_table()
            self.create_HT_timescaledb()
            self.logger.info("remote db connection successfully established")

        except Exception as e:
            self.logger.error("could not connect to remote db")
            raise e

    def set_up_influx_client(self) -> DataFrameClient:
        self.influx_client = DataFrameClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database
        )
        self.influx_client.ping()
        return self.influx_client

    def safe_create_influx_database(self) -> None:
        """
        Creates a new influx database with database_name.
        If one already exists, then nothing happens.
        """
        # The CREATE DATABASE command does nothing if the database
        # already exists. Hopefully, this client's method acts similarly.
        self.influx_client.create_database(self.database)

    def push_to_influx_database(
        self, data: DataFrame, measurement: str
    ) -> None:
        """
        Will create database if it doesn't exist, then push to it.
        :param data: pandas DataFrame indexed by timestamp
        :param database_name: the name of the database to push to
        :param measurement: the name of this measurement
        :return:
        """
        self.safe_create_influx_database()
        self.influx_client.write_points(
            dataframe=data,
            measurement=measurement,
            database=self.database,
            protocol='json'
        )

    def create_table(self):
        try:
            self.db.define_table('wifi_table', Field('AP_id'), Field('value'), Field('time', type='datetime'))
            self.logger.info("wifi_table was created in remote db")

        except Exception as e:
            self.db.commit()
            self.logger.warning("wifi_table could already exist, return message '{}'".format(str(e)))

    def create_HT_timescaledb(self):
        try:
            self.db.executesql("SELECT create_hypertable('wifi_table', 'time');")
            self.logger.info("wifi_table turned into hypertable")

        except Exception as e:
            self.db.commit()
            self.logger.warning("wifi_table could be a hypertable already, message returned is '{}'".format(str(e)))

    def test(self, data):
        print(data)
        # for i, row in data.iterrows():
        #     str = row['ts'][:4] + '-' + row['ts'][4:6] + '-' + row['ts'][6:8] + ' ' + row['ts'][8:10] + ':' + row['ts'][10:12] + ':' + row['ts'][12:14]
        #     print(str)

    def push_to_remote(self, data):
        """
        this method pushes a pandas dataframe to the remote db
        """
        try:
            for i, row in data.iterrows():
                self.db.wifi_table.insert(AP_id=row['id'], value=row['value'], time=(row['ts'][:4] + '-' + row['ts'][4:6] + '-' + row['ts'][6:8] + ' ' + row['ts'][8:10] + ':' + row['ts'][10:12] + ':' + row['ts'][12:14]))
            self.db.commit()
            self.logger.info("data successfully pushed to remote db")

        except Exception as e:
            self.logger.error("pushing to remote database failed")
            raise e

    def drop_table(self):

        """
        this method drops wifi_table from the remote db
        """

        try:
            self.db.wifi_table.drop()
            self.logger.info("wifi_table successfully dropped")

        except Exception as e:
            self.logger.error("wifi_table could not be dropped")
            raise e

if __name__ == '__main__':
    remote = remote_db()
