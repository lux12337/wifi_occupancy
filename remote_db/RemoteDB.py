import os
import logging
from pandas import DataFrame
from logging.handlers import TimedRotatingFileHandler
from pydal import DAL, Field
from influxdb import InfluxDBClient
from typing import Union, Optional, Dict, List

# Luigi, Katelyn, Jasmine, Jose


class RemoteDB:
    """
    This class establishes a connection with a remote db
    (e.g. TimescaleDB, InfluxDB, or those accessible through
    PyDAL's Object Relational Mapping)
    and pushes local data to it.
    """

    def __init__(
        self,
        dal_or_uri: Union[None, DAL, str] = None,
        influx_client_or_args: Union[None, InfluxDBClient, Dict] = None,
        project_path: str = "."
    ):
        def count_None(arr: List[any]):
            """
            :return: the number of None elements in a list
            """
            count = 0
            for a in arr:
                if a is None:
                    count = count + 1
            return count

        db_configs = [
            dal_or_uri,
            influx_client_or_args
        ]

        # There should be exactly one db config object
        if not 1 == len(db_configs) - count_None(db_configs):
            raise Exception(
                '0 or 1< databases requested. Only 1 is allowed'
            )

        self._init_logging_and_project_path(project_path)

        if dal_or_uri is not None:
            self.connect_db_with_dal(dal_or_uri)
        # TODO continue with influx and timescale

    def _init_logging_and_project_path(self, project_path: str) -> None:

        self.project_path = project_path

        # create and store logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # create the directory if it doesn't exist
        if not os.path.exists(self.project_path + "/" + 'logs'):
            os.makedirs(self.project_path + "/" + 'logs')

        handler = TimedRotatingFileHandler(
            self.project_path + "/" + "logs/remote_db.log",
            when='D',
            interval=1,
            backupCount=5
        )

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def connect_db_with_dal(self, uri_or_db: Union[str, DAL]) -> DAL:
        """
        This method tries to establish a db connection. If it's successful,
        it'll return a DAL instance. If it fails, it'll throw an error.
        :param uri_or_db: Either a uri string meant for the DAL function
        or a DAL instance itself.
        :return: The DAL instance representing the database.
        """

        # TODO test whether this method would safely change databases.

        if isinstance(uri_or_db, str):
            try:
                self.db = DAL(uri_or_db)
            except:
                raise Exception('invalid uri')
        elif isinstance(uri_or_db, DAL):
            self.db = uri_or_db
        else:
            raise Exception('invalid parameter uri_or_db')

        self.logger.info("remote db connection successfully established")
        return self.db


    def push_to_remote(self, data: DataFrame) -> None:
        """
        Pushes a pandas dataframe to the database
        :param data: the pandas dataframe to push.
        """

        # TODO only create table if it doesn't exist.
        # TODO allow for custom table/field names.

        self.db.define_table(
            'wifi_table',
            Field('AP_id'), Field('value'), Field('ts')
        )

        try:
            for i, row in data.iterrows():
                self.db.wifi_table.insert(AP_id=row['id'], value=row['value'], ts=row['ts'])
            self.db.commit()
            self.logger.info("data successfully pushed to remote db")

        except Exception as e:
            self.logger.error("pushing to remote database failed")
            raise e
