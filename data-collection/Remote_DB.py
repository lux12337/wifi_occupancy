import os
import logging
from pandas import DataFrame, Series, DatetimeIndex, to_datetime, datetime
import configparser
import datetime
from logging.handlers import TimedRotatingFileHandler
from pydal import DAL, Field
from influxdb import DataFrameClient
from typing import Optional, Generator, Dict

# Luigi, Katelyn, Jasmine, Jose


class remote_db():
    """
    This class establishes a connection with a remote db(TimescaleDB, InfluxDB, ORM) and pushes local data to it.
    """

    def __init__(self, project_path=".", config_file="config.ini"):

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

        config = configparser.ConfigParser()
        config.read(self.project_path+"/"+self.config_file)
        self.logger.info("successfully loaded config_file={}".format(self.config_file))

        """
        Gather arguments from config file.
        Note: some arguments are optional.
        """
        try:
            self.db_type = config.get('remote_db', 'db_type')
        except:
            self.db_type = None
        try:
            self.host = config.get('remote_db', 'host')
        except:
            self.host = None
        try:
            self.port = config.get('remote_db', 'port')
        except:
            self.port = None
        try:
            self.username = config.get('remote_db', 'username')
        except:
            self.username = None
        try:
            self.password = config.get('remote_db', 'password')
        except:
            self.password = None
        try:
            self.database = config.get('remote_db', 'database')
        except:
            self.database = None
        try:
            self.filename = config.get('remote_db', 'filename')
        except:
            self.filename = None
        try:
            self.table_name = config.get('remote_db', 'table_name')
        except:
            self.table_name = None
        """Optional Influx Arguments"""
        self.influx_optional_args: Dict[str, any] = {}
        try:
            self.influx_optional_args['pool_size'] = self.pool_size\
                = int(config.get('remote_db', 'pool_size'))
        except:
            self.pool_size = None
        try:
            self.influx_optional_args['ssl'] = self.ssl\
                = bool(config.get('remote_db', 'ssl'))
        except:
            self.ssl = None
        try:
            self.influx_optional_args['verify_ssl'] = self.verify_ssl\
                = bool(config.get('remote_db', 'verify_ssl'))
        except:
            self.verify_ssl = None
        try:
            self.influx_optional_args['timeout'] = self.timeout\
                = int(config.get('remote_db', 'timeout'))
        except:
            self.timeout = None
        try:
            self.influx_optional_args['retries'] = self.retries\
                = int(config.get('remote_db', 'retries'))
        except:
            self.retries = None
        try:
            self.influx_optional_args['use_udp'] = self.use_udp\
                = bool(config.get('remote_db', 'use_udp'))
        except:
            self.use_udp = None
        try:
            self.influx_optional_args['udp_port'] = self.udp_port\
                = int(config.get('remote_db', 'udp_port'))
        except:
            self.udp_port = None
        try:
            self.influx_optional_args['path'] = self.path\
                = str(config.get('remote_db', 'path'))
        except:
            self.path = None

        """
        create a connection to the remote db
        """

        # self.db is filled by create_DB_connection()
        self.db = None
        self.create_DB_connection()

    def create_DB_connection(self):
        """
        this method tries to establish a db connection
        """
        try:
            if self.db_type == "mysql":
                self.db = DAL('mysql://{}:{}@{}:{}/{}?set_encoding=utf8mb4'.format(
                    self.username, self.password, self.host, self.port, self.database
                ))
                self.create_table()

            elif self.db_type == "sqlite":
                self.db = DAL('sqlite://{}'.format(self.filename))
                self.create_table()

            elif self.db_type == "postgres":
                self.db = DAL('postgres://{}:{}@{}:{}/{}'.format(
                    self.username, self.password, self.host, self.port, self.database
                ))
                self.create_table()

            elif self.db_type == "timescale":
                self.db = DAL('postgres://{}:{}@{}:{}/{}'.format(
                    self.username, self.password, self.host, self.port, self.database
                ))
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
            database=self.database,
            **self.influx_optional_args
        )
        return self.influx_client

    def push_to_influx_database(
        self, data: DataFrame, measurement: str
    ) -> None:
        """
        Push dataframe to database.
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

            for chunk_n in range(0, last_chunk_number+1):
                chunk = data_.loc[
                    # return the rows whose chunk_number matches chunk_n
                    chunk_numbers == chunk_n
                ].copy()

                chunk.set_index('ts', inplace=True)

                yield chunk


        data: DataFrame = preprocess_data(data)

        for chunk in data_chunks(data):
            self.influx_client.write_points(
                dataframe=chunk,
                measurement=measurement,
                database=self.database,
                tag_columns=['id']
            )

    def create_table(self):
        """
        this method creates a SQL type of table in the remote db, if it fails, it catches the warning and logs it
        """
        try:
            self.db.define_table(self.table_name, Field('AP_id'), Field('value'), Field('time', type='datetime'))
            self.logger.info("{} was created in remote db".format(self.table_name))

        except Exception as e:
            self.db.rollback()
            self.logger.warning(
                "{} could already exist, return message '{}'".format(
                    str(e), self.table_name
                )
            )

    def create_table_timescale(self):
        """
        this method creates a postgres table in preparation for a hypertable in timescale
        """
        try:
            self.db.executesql(
                "CREATE TABLE IF NOT EXISTS {}(time TIMESTAMP, AP_id CHAR(512), value CHAR(512));".format(
                    self.table_name
                )
            )
            self.db.commit()
            self.logger.info("{} created in remote db".format(self.table_name))

        except Exception as e:
            self.logger.warning("creation of {} failed, error='{}'".format(
                self.table_name, str(e))
            )
            raise e

    def create_hypertable_timescale(self):
        """
        this method tries to turn the table into a hypertable, and it if it fails, it will catch the warning and rollback the commit
        """
        try:
            self.db.executesql(
                "SELECT create_hypertable('{}', 'time');".format(self.table_name)
            )
            self.db.commit()
            self.logger.info(
                "successfully created hypertable from {}".format(self.table_name)
            )

        except Exception as e:
            self.db.rollback()
            self.logger.warning("tried to create hypertable from {}, returned message='{}'".format(
                self.table_name, str(e))
            )

    def push_to_remote_db(self, data: DataFrame):
        try:
            if self.db_type == "mysql"\
                    or self.db_type == "sqlite"\
                    or self.db_type == "postgres":
                self.push_to_remote_dal(data)

            elif self.db_type == "timescale":
                self.push_to_remote_timescale(data)

            elif self.db_type == "influx":
                self.push_to_influx_database(
                    data=data,
                    measurement=self.table_name
                )
            else:
                raise Exception('Database type string invalid.')

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
                self.db[self.table_name].insert(
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
            for i, row in data.iterrows():
                time = (row['ts'][:4] + '-' + row['ts'][4:6] + '-' + row['ts'][6:8] + ' ' + row['ts'][8:10] + ':' + row['ts'][10:12] + ':' + row['ts'][12:14])
                self.db.executesql(
                    "INSERT INTO {} VALUES('{}', '{}', '{}')".format(
                        self.table_name, time, row['id'], row['value']
                    )
                )
            self.db.commit()
            self.logger.info("data successfully pushed to remote db")

        except Exception as e:
            self.logger.error("pushing to remote database failed")
            raise e

    def drop_table(self):

        """
        this method drops the table from a SQL remote db
        """

        try:
            self.db[self.table_name].drop()
            self.logger.info(
                "{} successfully dropped".format(self.table_name)
            )

        except Exception as e:
            self.db.rollback()
            self.logger.error("{} could not be dropped".format(self.table_name))
            raise e

    def drop_table_timescale(self):

        """
        this method drops the table from a timescale remote db
        """

        try:
            self.db.executesql('DROP TABLE {};'.format(self.table_name))
            self.db.commit()
            self.logger.info("{} successfully dropped".format(self.table_name))

        except Exception as e:
            self.logger.warning(
                "{} could not be dropped, returned message='{}'".format(
                    self.table_name, str(e)
                )
            )


if __name__ == '__main__':
    remote = remote_db()
