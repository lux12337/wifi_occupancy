import os
import logging
from pandas import DataFrame
from logging.handlers import TimedRotatingFileHandler
from pydal import DAL, Field
from typing import Union

# Luigi, Katelyn, Jasmine, Jose

class remote_db():
    """
    This class establishes a connection with a remote db
    (e.g. TimescaleDB, InfluxDB, or those accessible through
    PyDAL's Object Relational Mapping)
    and pushes local data to it.
    """

    def __init__(
        self,
        uri_or_db: Union[str, DAL],
        project_path: str = "."
    ):
        self._init_logging_and_project_path(project_path)
        self.set_db_connection(uri_or_db)

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

    def set_db_connection(self, uri_or_db: Union[str, DAL]) -> DAL:
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


if __name__ == '__main__':
    remote = remote_db()
