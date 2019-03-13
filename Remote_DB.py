import os
import logging
import pandas
import configparser
import time
import csv
import datetime
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
            self.db_type = Config.get('remote_db', 'db_type')
        except:
            pass
        try:
            self.host = Config.get('remote_db', 'host')
        except:
            pass
        try:
            self.port = Config.get('remote_db', 'port')
        except:
            pass
        try:
            self.username = Config.get('remote_db', 'username')
        except:
            pass
        try:
            self.password = Config.get('remote_db', 'password')
        except:
            pass
        try:
            self.database = Config.get('remote_db', 'database')
        except:
            pass
        try:
            self.filename = Config.get('remote_db', 'filename')
        except:
            pass

        """
        create a connection to the remote db
        """
        self.create_DB_connection()

    def create_DB_connection(self):
        """
        this method tries to establish a db connection
        """
        try:
            if self.db_type == "mysql":
                self.db = DAL('mysql://{}:{}@{}:{}/{}?set_encoding=utf8mb4'.format(self.username, self.password, self.host, self.port, self.database))
                self.create_table()

            elif self.db_type == "sqlite":
                self.db = DAL('sqlite://{}'.format(self.filename))
                self.create_table()

            elif self.db_type == "postgres":
                self.db = DAL('postgres://{}:{}@{}:{}/{}'.format(self.username, self.password, self.host, self.port, self.database))
                self.create_table()

            elif self.db_type == "timescale":
                self.db = DAL('postgres://{}:{}@{}:{}/{}'.format(self.username, self.password, self.host, self.port, self.database))
                self.create_table_timescale()
                self.create_hypertable_timescale()

            elif self.db_type == "sqlite":
                self.db = DAL('sqlite://{}'.format(self.username))
                self.create_table()

            else:
                raise Exception('Database type string invalid.')

            self.logger.info("remote db connection successfully established")

        except Exception as e:
            self.logger.error("could not connect to remote db")
            raise e


    def create_table(self):

        """
        this method creates a SQL type of table in the remote db, if it fails, it catches the warning and logs it
        """
        try:
            self.db.define_table('wifi_table', Field('AP_id'), Field('value'), Field('time', type='datetime'))
            self.logger.info("wifi_table was created in remote db")

        except Exception as e:
            self.logger.warning("wifi_table could already exist, return message '{}'".format(str(e)))


    def create_table_timescale(self):

        """
        this method creates a postgres table in preparation for a hypertable in timescale
        """

        try:
            self.db.executesql("CREATE TABLE IF NOT EXISTS wifi_table(time TIMESTAMP, AP_id CHAR(512), value CHAR(512));")
            self.db.commit()
            self.logger.info("wifi_table created in remote db")

        except Exception as e:
            self.logger.warning("creation of wifi_table failed, error='{}'".format(str(e)))
            raise e


    def create_hypertable_timescale(self):

        """
        this method tries to turn wifi_table into a hypertable, and it if it fails, it will catch the warning and rollback the commit
        """

        try:
            self.db.executesql("SELECT create_hypertable('wifi_table', 'time');")
            self.db.commit()
            self.logger.info("successfully created hypertable from wifi_table")

        except Exception as e:
            self.db.rollback()
            self.logger.warning("tried to create hypertable from wifi_table, returned message='{}'".format(str(e)))

    def push_to_remote_db(self, data):
        try:
            if self.db_type == "mysql"\
                    or self.db_type == "sqlite"\
                    or self.db_type == "postgres":
                self.push_to_remote(data)

            elif self.db_type == "timescale":
                self.push_to_remote_timescale(data)

            elif self.db_type == "sqlite":
                self.push_to_remote(data)

            self.logger.info("push to remote successful")

        except Exception as e:
            self.logger.error("push failed")
            raise e

    def push_to_remote(self, data):
        """
        this method pushes a pandas dataframe to the remote db
        """
        try:
            print(
                self.db.get_instances()
            )
            print(
                self.db.wifi_table.as_dict()
            )
            for i, row in data.iterrows():
                self.db.wifi_table.insert(
                    AP_id=row['id'],
                    value=row['value'],
                    time=(
                        row['ts'][:4] + '-' + row['ts'][4:6] + '-' + row['ts'][6:8]
                        + ' '
                        + row['ts'][8:10] + ':' + row['ts'][10:12] + ':' + row['ts'][12:14]
                    )
                )
            self.db.commit()
            self.logger.info("data successfully pushed to remote db")

        except Exception as e:
            self.logger.error("pushing to remote database failed")
            raise e


    def push_to_remote_timescale(self, data):

        """
        this method pushes a pandas dataframe to a remote timescale db
        """

        try:
            #self.db.executesql("COPY wifi_table(time, ap_id, value) FROM './temp_data.csv' DELIMITER ',' CSV HEADER;")
            for i, row in data.iterrows():
                time = (row['ts'][:4] + '-' + row['ts'][4:6] + '-' + row['ts'][6:8] + ' ' + row['ts'][8:10] + ':' + row['ts'][10:12] + ':' + row['ts'][12:14])
                self.db.executesql("INSERT INTO wifi_table VALUES('{}', '{}', '{}')".format(time, row['id'], row['value']))
            self.db.commit()
            self.logger.info("data successfully pushed to remote db")

        except Exception as e:
            self.logger.error("pushing to remote database failed")
            raise e


    def drop_table(self):

        """
        this method drops wifi_table from a SQL remote db
        """

        try:
            self.db.wifi_table.drop()
            self.logger.info("wifi_table successfully dropped")

        except Exception as e:
            self.logger.error("wifi_table could not be dropped")
            raise e


    def drop_table_timescale(self):

        """
        this method drops wifi_table from a timescale remote db
        """

        try:
            self.db.executesql('DROP TABLE wifi_table;')
            self.db.commit()
            self.logger.info("wifi_table successfully dropped")

        except Exception as e:
            self.logger.warning("wifi_table could not be dropped, returned message='{}'".format(str(e)))


if __name__ == '__main__':
    remote = remote_db()
