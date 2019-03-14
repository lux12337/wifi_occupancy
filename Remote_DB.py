import os
import logging
import pandas as pd
from pandas import DataFrame, Series, DatetimeIndex, to_datetime, datetime
import configparser
import time
import csv
import datetime
from logging.handlers import TimedRotatingFileHandler
from pydal import DAL, Field
from influxdb import DataFrameClient
from typing import Optional, Generator, List, Dict

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

            elif self.db_type == "influx":
                self.db = None
                self.set_up_influx_client()

            else:
                raise Exception('Database type string invalid.')

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
        return self.influx_client

    def print_influx_status(self) -> None:
        print(self.influx_client.ping())
        print(self.influx_client.get_list_database())
        print(self.influx_client.get_list_measurements())
        print(self.influx_client.get_list_users())
        print(self.influx_client.get_list_privileges(username=self.username))
        # print(self.influx_client.query('select * from "wifi"').items())

    def safe_create_influx_database(self) -> None:
        """
        Creates a new influx database with database_name.
        If one already exists, then nothing happens.
        """
        # The CREATE DATABASE command does nothing if the database
        # already exists. Hopefully, this client's method acts similarly.
        self.influx_client.create_database(self.database)

    def safe_create_user(self, make_admin: bool = False) -> None:
        self.influx_client.create_user(
            username=self.username,
            password=self.password,
            admin=make_admin
        )

    def push_to_influx_database(
        self, data: DataFrame, measurement: str
    ) -> None:
        """
        Will create database if it doesn't exist, then push to it.
        :param data: pandas DataFrame indexed by timestamp
        :param measurement: the name of this measurement
        :return:
        """
        def preprocess_data(data_: DataFrame) -> DataFrame:
            """
            :param data_: the original data
            :return: data in the desired format
            """
            columns = list(data_.columns)

            # for influx, ts must in datetime format
            data_['ts'] = DatetimeIndex(data_['ts'].apply(
                # "YYYY-MM-DD HH:MM:SS"
                lambda ts: to_datetime(
                    arg=ts, format='%Y%m%d%H%M%S'
                )
            ))

            # swap to ensure that ts is the first column
            ts_index: int = columns.index('ts')
            columns[0], columns[ts_index] = columns[ts_index], columns[0]
            data_ = data_[columns]

            return data_

        def data_chunks(data_: DataFrame) -> Generator[DataFrame, None, None]:
            """
            It's necessary to break up the data into multiple chunks.
            Each chunk will be written separately.
            :param data_: original data
            :return: a generator for the chunks
            """
            row_count: int = data_.shape[0]

            # keep track of which chunk each row belongs to.
            # a series of repeated -1's.
            chunk_numbers: Series = Series([-1] * row_count)

            # keep track of unique timestamps we find
            repeats_per_ts: Dict[datetime, int] = {}

            for i, row in data_.iterrows():

                ts: datetime = row['ts']

                # ensure that there's an entry for ts
                if ts not in repeats_per_ts:
                    repeats_per_ts[ts] = -1

                # Record that this we've seen this timestamp n times.
                repeats_per_ts[ts] = repeats_per_ts[ts] + 1

                # this row's chunk_number = n
                chunk_numbers[i] = repeats_per_ts[ts]

            last_chunk_number: int = max(repeats_per_ts.values())

            # just for debugging
            data_as_dict = data_.to_dict()

            for chunk_n in range(0, last_chunk_number+1):
                chunk = data_.loc[
                    # return the rows whose chunk_number matches chunk_n
                    chunk_numbers == chunk_n
                ].copy()

                # just for debugging
                chunk_as_dict = chunk.to_dict()

                chunk.set_index('ts', inplace=True)

                yield chunk

        self.safe_create_influx_database()
        self.safe_create_user(make_admin=False)

        data = preprocess_data(data)

        for chunk in data_chunks(data):
            self.influx_client.write_points(
                dataframe=chunk,
                measurement=measurement,
                database=self.database,
                protocol='line'
            )

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

    def push_to_remote_db(self, data: DataFrame, influx_measurement: Optional[str] = None):
        try:
            if self.db_type == "mysql"\
                    or self.db_type == "sqlite"\
                    or self.db_type == "postgres":
                self.push_to_remote_dal(data)

            elif self.db_type == "timescale":
                self.push_to_remote_timescale(data)

            elif self.db_type == "influx":
                self.print_influx_status()
                self.push_to_influx_database(
                    data=data,
                    measurement=influx_measurement
                )
                self.print_influx_status()

            self.logger.info("push to remote successful")

        except Exception as e:
            self.logger.error("push failed")
            raise e

    def push_to_remote_dal(self, data):
        """
        this method pushes a pandas dataframe to the remote db
        """
        try:
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
