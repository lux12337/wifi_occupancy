import os
import logging
from pydal import DAL

class remote_db():
    def __init__(self):
        db = DAL('mysql://testing:admin@localhost/test')

if __name__ == '__main__':
    remote = remote_db()
