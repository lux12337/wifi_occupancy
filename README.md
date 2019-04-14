# wifi_occupancy
scripts to gather occupancy information from wifi networks

### Python Version
2.7

### Python Mouldes
pip install pandas
pip install sqlalchemy
pip install requests
pip install ConfigParser
pip install argparse

### How to Run? 
1. python get_wifi_data.py: gets data from file or by quering the wifi controller and stores it to a localdb
2. python push_to_melrok.py: gets data from localdb and pushes it to a remote db (TODO)

## Setting Up Databases

### Timescale
Adapted from https://docs.timescale.com/v1.2/getting-started/installation/ubuntu/installation-apt-ubuntu
Install method was apt and PostgreSQL version is 11
1. Update the package resource list for your OS.  `lsb_release -c -s` returns the correct codename of the OS.
```bash
sudo sh -c "echo 'deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -c -s`-pgdg main' >> /etc/apt/sources.list.d/pgdg.list"
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
```
2. Add Timescale's PPA (Personal Package Archives).
```bash
sudo add-apt-repository ppa:timescale/timescaledb-ppa
sudo apt-get update
```
2. Install the correct package depending on the Postgresql Version.
```bash
sudo apt install timescaledb-postgresql-11
```
3. Update the postgresql config file with your preferred settings
```bash
sudo timescaledb-tune
```
4. Restart the service
```bash
sudo service postgresql restart
```