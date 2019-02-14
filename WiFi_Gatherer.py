import argparse
import configparser
import subprocess
import pandas as pd
import hashlib
import datetime
import os
import logging
from io import StringIO
from logging.handlers import TimedRotatingFileHandler

# @author : Marco Pritoni <mpritoni@lbl.gov>
# @author : Anand Prakash <akprakash@lbl.gov>


class wifi_gatherer():
    """
    This class gets the wifi data from the controller/file, outputs a dataframe with AP connection counts
    This also hashes the mac addresses (currently not used)
    """

    def __init__(self, project_path = ".", config_file="config.ini", section="SNMP_config_aruba"):

        self.project_path = project_path
        """
        initialize logging
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        if not os.path.exists(self.project_path+"/"+'logs'):
            os.makedirs(self.project_path+"/"+'logs')
        handler = TimedRotatingFileHandler(self.project_path+"/"+"logs/wifi_gatherer.log", when='D', interval=1, backupCount=5)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        """
        read config file
        """
        self.config_file = config_file
        self.snmp_section = section
        if not os.path.exists(self.project_path+"/"+self.config_file):
            self.logger.error("cannot find config_file={}".format(self.config_file))
            raise Exception("config file not found")
        self.logger.info("section = {}".format(self.snmp_section))

        Config = configparser.ConfigParser()
        Config.read(self.project_path+"/"+self.config_file)
        self.logger.info("successfully loaded config_file={}".format(self.config_file))

        try:
            self.method = Config.get(self.snmp_section, "method")
            self.source = Config.get(self.snmp_section, "source")
            self.input_from_file = Config.get(self.snmp_section, "input_from_file")
            self.input_file_name = Config.get(self.snmp_section, "input_file_name")
            self.community = Config.get(self.snmp_section, "community")
            self.switchname = Config.get(self.snmp_section, "switchname")
            self.oid = Config.get(self.snmp_section, "oid")
        except Exception as e:
            self.logger.error("unexpected error while setting configuration from config_file={}, section={}, error={}".format(self.config_file, self.snmp_section, str(e)))
            raise e

        # self.parse_script_arg()  #to get data from python call of the .py file - currently not used

    def parse_script_arg(self):

        """
        This method parses the args in the .py script call (called from cron job) - currently not used
        """
        try:
            parser = argparse.ArgumentParser()
            parser.add_argument("community", help="pseudo auth")
            parser.add_argument("switchname", help="controller IP")
            parser.add_argument("oid", help="Object identifier based on MIB. Use 1.3.6.1.4.1.14823.2.2.1.4.1.2.1.10")
            #parser.add_argument("--input_from_file", help="True to read loacal file, False to run smnp query")
            args = parser.parse_args()
            #self.input_from_file = args.input_from_file
        except:
            print ("no argument passed")
            pass

    def _get_data_SMNP(self):

        """
        This method calls SNMP through subprocess #inputs parms of the SNMP query from config file
        """

        if self.source=="controller":
            cmd = ['snmpwalk','-v','2c','-c',self.community,'-Onaq',self.switchname,self.oid]
            try:
                p = subprocess.Popen(cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE)
                out, err = p.communicate()

            except Exception as e:
                self.logger.error("unexpected error when running snmpwalk command, error={}".format(str(e)))

            if p.returncode != 0:
                self.logger.error("snmpwalk exited with status %r: %r"% (p.returncode, err))
                raise Exception('snmpwalk exited with status %r: %r' % (p.returncode, err))
            else:
                try:
                    df = pd.read_csv(StringIO(out.decode('utf-8')), sep="\s+", header=None, names=["oid_mac_ip", "id"])
                    self.logger.info("successfully imported dataframe from snmp output",)
                    return df
                except Exception as e:
                    self.logger.error("unexpected error while reading from snmp output, error{}".format(str(e)))

        else:
            self.logger.error("currently non implemented AP - SNMP query")

    def _get_data_from_file(self):

        """
        This method reads from file the results of a SNMP query (testing or alternative path)
        """
        try:
            data = pd.read_csv(self.project_path+"/"+self.input_file_name, sep="\s+", header=None, names=["oid_mac_ip", "id"])
            self.logger.info("successfully imported dataframe from csv file={}".format(self.input_file_name))
        except Exception as e:
            self.logger.error("unexpected error while reading from csv {}, error={}".format(self.input_file_name, str(e)))
            raise e

        return data

    def get_wifi_data(self):

        """
        This method gets the wi-fi data from file or snmp query calling the corresponding methods
        """

        if self.input_from_file==True:
            data = self._get_data_from_file() # use sample file to test
        else:
            data = self._get_data_SMNP() # run real query

    def parse_mac_address(self, data, regex=None):

        """
        This method parse mac address from long strings
        """
        try:
            if not regex:
                regex = r"(?:.1.3.6.1.4.1.14823.2.2.1.4.1.2.1.10)(?:\.)(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(?:\.)(?:\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"

            mac_original = data["oid_mac_ip"].str.extract(regex, expand=True)
            mac_original.columns = ["mac_orig"]
            data = data.join(mac_original).drop("oid_mac_ip", axis=1)
            self.logger.info("successfully parsed mac address")
        except Exception as e:
            self.logger.error("unexpected error while parsing mac address, error={}".format(str(e)))
            raise e
        return data

    def anonymize_single_MAC_address(self, key_string, salt="1Ha7"):

        """
        This method anonymizes single mac address string

        """
        try:
            hashed = hashlib.md5( salt + key_string ).hexdigest()
            self.logger.info("successfully anonymized the data")
        except Exception as e:
            self.logger.error("expected error while anonymized the data")
            raise e
        return hashed

    def anonymize_MAC_address_df(self, data, salt="1Ha7#a8(:^"):

        """
        This method anonymizes multiple mac addresses and returns a dataframe

        """
        try:
            mac_hashed = pd.DataFrame()
            mac_hashed["mac_hashed"] = data["mac_orig"].apply(self.anonymize_single_MAC_address, salt=salt)
            data = data.join(mac_hashed)
            data = data.drop("mac_orig", axis=1)
            self.logger.info("successfully parsed mac address dataframe")
        except Exception as e:
            self.logger.error("unexpected error while parsing mac address dataframe, error={}".format(str(e)))
            raise e
        return data

    def get_current_time_utc(self, formatOpt="influxDB"):

        """
        This method generate a timestamp for "now" to attach to the data extracted

        """
        self.logger.debug("time format = {}".format(formatOpt))
        if formatOpt=="influxDB":
            return datetime.datetime.utcnow().strftime("%Y-%m-%dT%-H:%M:%-SZ") # influxDB format

        if formatOpt=="Melrok":
            return datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S") # Melrok format

    # retrieve count of connected devices for each AP
    def parse_connection_count_per_AP(self, data, include_time=True, formatOpt="Melrok"):

        """
        This method counts the connected devices to each AP and return

        """

        try:
            if data.empty == False:
                data = data.groupby(["id"]).count() # hp single AP per read
                data.columns = ["value"]

                if include_time:
                    data["ts"] = self.get_current_time_utc(formatOpt)
                    data = data.reset_index()
                self.logger.info("successfully counted connected devices")
            else:
                self.logger.warn("data to obtain count from is None, check this")
        except Exception as e:
            self.logger.error("unexpected error while counting devices error={}".format(str(e)))
            raise e

        ## need to futher parse AP, Building, Room - figure out what is the structure of name

        return data

if __name__ == '__main__':

    w = wifi_gatherer()
