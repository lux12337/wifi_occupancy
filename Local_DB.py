import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, DBAPIError
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import json
import configparser
import datetime

# @author : Marco Pritoni <mpritoni@lbl.gov>
# @author : Anand Prakash <akprakash@lbl.gov>


class local_db(object):
    """
    This class saves the data from pandas dataframe to a local db (currently sqlite3 on disk) as buffer while pushing data
    to another DB/API.
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
        handler = TimedRotatingFileHandler(self.project_path+"/"+"logs/local_db.log", when='D', interval=1, backupCount=5)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        """
        read config file
        """
        self.config_file = config_file
        if not os.path.exists(self.project_path+"/"+self.config_file):
            self.logger.error("cannot find config_file=%s"%self.config_file)
            raise Exception("config file not found")

        Config = configparser.ConfigParser()
        Config.read(self.project_path+"/"+self.config_file)
        self.logger.info("successfully loaded config_file=%s"%self.config_file)

        try:
            self.local_db = Config.get('local_db', 'filename')
            self.table = Config.get('local_db', 'table')
        except Exception as e:
            self.logger.error("unexpected error while setting configuration from config_file={}, error={}".format(self.config_file, str(e)))
            raise e

        """
        create a sqlachemy engine (pool of connections) to connect to the local db
        """

        self.engine = self.create_DB_engine()


    def create_DB_engine(self):

        """
        this method creates a sqlalchemy DB engine (pool of connection)
        """

        try:
            engine = create_engine(self.local_db.format(self.project_path), echo=False)
            self.logger.info("sql alchemy engine successfully created")
            return engine

        except (SQLAlchemyError, DBAPIError) as e:
            self.logger.error("cannot create sqlalchemy engine, error=%s"%str(e))
            raise e

    def save_to_local_DB(self, data, mode="append"):

        """
        this method saves the data from a pandas dataframe into a db table, currently on disk
        """
        try:
            if data.empty == False:
                # TODO: apply mapping or filtering or data manipulations if any, none in this case right now
                mapped_data = data
                mapped_data.to_sql(name=self.table, con=self.engine, if_exists=mode, index=False)
                self.logger.info("values successfully inserted into local database table %s"%self.table)
            else:
                self.logger.warn("data to save to local datbase is None, check this")
        except ValueError as e:
            self.logger.error("cannot insert values to table %s, data might already exist, error=%s"%(self.table, str(e)))
            raise e
        except Exception as e:
            self.logger.error("Unexpected error while appending values to local database table %s, error=%s"%(self.table, str(e)))
            raise e
        return

    def read_local_DB(self):

        """
        this method reads the data from a db table back to a pandas dataframe
        """
        try:
            data = pd.read_sql_query("SELECT * FROM {}".format(self.table), self.engine)
            self.logger.info("successfully read values from table %s"%self.table)
            return data
        except Exception as e:
            self.logger.error("The table %s was not found, error=%s"%(self.table, str(e)))
            return pd.DataFrame()

    def clean_local_DB(self):

        """
        this method drops the whole table on the db; use deleta_data_sent for normal operation
        """
        try:
            pd.io.sql.execute("DROP TABLE IF EXISTS {}".format(self.table), self.engine)
            self.logger.info("successfully dropped table %s"%self.table)
        except Exception as e:
            self.logger.error("unexpected error while dropping table %s, error=%s"%(self.table, str(e)))
        return

    def dispose_DB_engine(self):

        """
        this method closes the connection to the database
        NOTE: # this does not work properly !!! TODO: fix
        """
        self.engine.dispose()
        self.logger.info("closed sqlalchemy engine")
        return

    def _select_data_sent(self, data, time_threshold):

        """
        this method selects the dates to be removed form local database based on a time threshold check
        It uses data time comparison and manipulation in pandas. The method is called by "delete_data_sent"
        """
        try:
            if data.empty == False:
                datetime_to_remove = str((data.loc[pd.to_datetime(data["ts"]) < time_threshold,"ts"].unique().tolist())).replace("u'",'\'').replace("[","(").replace("]",")")
                return datetime_to_remove
            else:
                self.logger.warn("data to be selected to be removed is None, check this")
                return "(\'%s\')"%datetime.datetime(1970, 1, 1, 0, 0, 0).strftime("%Y%m%d%H%M%S")
        except Exception as e:
            self.logger.error("unexpected error while selecting data to be remove from local db, error=%s"%str(e))
            return "(\'%s\')"%datetime.datetime(1970, 1, 1, 0, 0, 0).strftime("%Y%m%d%H%M%S")
        # ex datetime_to_remove = "('20180627141830', '20180627141834', '20180627142745', '20180627142753', '20180627142759')"


    def delete_data_sent(self, data, time_threshold=None):

        """
        this method removes the rows that have been sent
        """

        if not isinstance(time_threshold, pd.datetime):

            time_threshold = pd.datetime.utcnow()

        datetime_to_remove = self._select_data_sent(data, time_threshold = time_threshold)

        try:
            self.engine.execute("DELETE FROM {} WHERE ts in {} ".format(self.table, datetime_to_remove)) # or engine.engine
            self.logger.info("data that was sent has been removed from the local db table %s"%self.table)

        except Exception as e:
            self.logger.error("unexpected error occured while removing data sent, error=%s"%str(e))

        return

    def delete_data_based_on_ts(self, ts_to_remove):

        """
        this method removes the rows that have been sent
        """

        try:
            self.engine.execute("DELETE FROM {} WHERE ts = {} ".format(self.table, ts_to_remove)) # or engine.engine
            self.logger.info("data with ts=%s that was sent has been removed from the local db table %s"%(ts_to_remove, self.table))

        except Exception as e:
            self.logger.error("unexpected error occured while removing data sent, ts=%s, error=%s"%(ts_to_remove, str(e)))

        return



if __name__ == '__main__':

    engine = local_db()
