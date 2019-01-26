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