# wifi_occupancy
scripts to gather occupancy information from wifi networks

### Python Version
3.7

### Python Mouldes
```bash
pip install pandas
pip install sqlalchemy
pip install requests
pip install ConfigParser
pip install argparse
```

### How to Run? 
1. python get_wifi_data.py: gets data from file or by quering the wifi controller and stores it to a localdb
2. python push_to_melrok.py: gets data from localdb and pushes it to a remote db (TODO)

## Setting Up Databases
### Timescale
##### Key Components
* Adapted from https://docs.timescale.com/v1.2/getting-started/installation/ubuntu/installation-apt-ubuntu
* Install method was apt and PostgreSQL version is 11
* Database was set in Ubuntu Server 18.04.2 amd64
* Timescale is built on top of postgres so you will need to install the same technologies as Timescale

##### Setting up
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

### Postgres

##### Setting up
1. To run the library scrips
```bash
docker build -t wifi-script .
docker run wifi-script
docker run -it wifi-script /bin/bash
```
2. Start the timescale scale database:
```bash
docker run -d --timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=123 timescale/timeescaledb:latest-pg11
```
3. To create a database in the Docker container run in PowerShell:
```bash
docker exec -ti timescaledb /bin/bash  //access server through the docker
psql -U postgres
create database alphademodb; //don’t forget the ;
```
4. Execute python commands
```bash
python get_wifi_data.py
python push_to_remote_db.py
```

### Influx
##### Key Components
* Database was set in a Docker image

##### Setting up
1. Go to https://portal.influxdata.com/downloads/
2. Click on the “v2.0.0-alpha” button under InfluxDB.
3. Run the following command, in a terminal of choice, to download a copy of the Docker image.
```bash
docker pull quay.io/influxdb/influxdb:2.0.0-alpha
```
4. Run the Docker image and expose it to port 9999
```bash
docker run --name influxdb -p 9999:9999 quay.io/influxdb/influxdb:2.0.0-alpha
```
5. Optional: Run a console  inside InfluxDB Container and setup influx
```bash
docker exec -it influxdb /bin/bash
influx setup # <--- In Influx Console
```
6. Continue setting up Influx in web browser by typing in: `localhost:9999`

### MySQL
##### Key Components
* MySQL is configured within a Docker container (Remote DB)
* The wifi script is ran on the host computer (Local DB)
##### Setting up
1. Pull a Docker Image with the latest MySQL version. The following command will automatically pull a MySQL image if the latest sql image cannot be found on the host computer.
```bash
docker run --name mysqldb -p 3306:3306 -e MYSQL_ROOT_PASSWORD= -d mysql:latest
```
2. The container will close down when the previous command finishes. To open a running, functioning container we run this command. This command will run the database server and we will have a shell where we can look through the container.
```bash
docker run -p 3306:3306 -it mysql:latest /bin/bash
```

3. Make sure you know your MySQL’s hostname, port number, admin username/password, and database name. These will be important for pushing the local data to the remote database.

This resource also looks helpful: https://coreos.com/quay-enterprise/docs/latest/mysql-container.html
#### Pushing to a Remote Database
Needed Information:
* Hostname of Remote Database
* Port Number
* Admin User
* Admin Password
* Database Name

To perform a push, you must hardcode your database into the configuration file.

1. Under `[remote_db]` in config.ini, fill in the following parameters:
```bash
[remote_db]
db_type = mysql
host = {ip address | hostname | localhost} ; use localhost if remotedb is a container
Port = 3306
Username = myUserName
Password = p@ssw0rd
Database = my_database
```
2. Next, in your host computer, you will type in the commands:
```bash
$ python3 get_wifi_data.py
$ python3 push_to_remote_db.py
```
Note: A warning will pop up with the phrase “Please consider using UTF8MB4 in order to be unambiguous”. This can be ignored as it does not affect the results.

3. To check that the transfer has been successful, go into your mysql command line and type the following to confirm a new table called wifi_table has been added.
```mysql
mysql> show tables;
+------------------+
|Tables_in_test |
+---------------+
|wifi_table     |
+---------------+
1 row in set (0.01sec)
```

4. Confirm that most of the data has been transferred over
```mysql
mysql> select * from wifi_table;
```


### SQLite
##### Key Components
* SQLite is configured on the local host (SQLite sends/transfers data through files. It is technically not a service.)
* The wifi script is ran on the host computer (Local DB)
##### Setting up (In a Docker)
1. Create a file called Dockerfile with the following content:
```bash
FROM ubuntu:trusty
RUN sudo apt-get -y update
RUN sudo apt-get -y upgrade
RUN sudo apt-get install -y sqlite3 libsqlite3-dev
RUN mkdir /db
RUN /usr/bin/sqlite3 /db/test.db
CMD /bin/bash
```
2. Then persist the db file inside host OS folder /home/dbfolder
```bash
docker run -it -v /home/dbfolder/:/db imagename
```
#### Pushing to Database
Needed information:
* Filename
Note: Unlike the other databases, sqlite only needs a filename that’s name has not been taken. You do not need to put in a file path.

1. Under `[remote_db]` in config.ini, fill in the following parameters:
```bash
[remote_db]
db_type = sqlite
filename = storage.db
```
2. Next, in your host computer, you will type in the commands:
```bash
$ python3 get_wifi_data.py
$ python3 push_to_remote_db.py
```
3. To check that the transfer has been successful, go into your sqlite3 command line and type the following:
```mysql
sqlite> .open storage.db
sqlite> select * from wifi_table;
```
Note that the transferring of data involves a file which can be renamed based on time or location for easier organizing.

The reason for SQLite’s uniqueness in only needing a name for a file is because one of SQLite’s most notable properties is being serverless. This means that “The database engine runs within the same process, thread, and address space as the application. There is no message passing or network activity.” (https://www.sqlite.org/serverless.html)

