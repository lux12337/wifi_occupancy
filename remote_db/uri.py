"""
This module was created by us to simplify the process of creating uri's for
connecting to PyDAL-supported databases.

Currently, it only takes inspiration from the uri examples given by PyDAL
(seen below).

Ideally, this module would be replaced by a third-party module dedicated to
creating uri's. We've had trouble finding such a module.

If such a third-party module isn't found, we should consider leaving the
responsibility of creating uri's to users instead of offering to do it for them.

SQLite    sqlite://storage.sqlite
MySQL	mysql://username:password@localhost/test?set_encoding=utf8mb4
PostgreSQL	postgres://username:password@localhost/test
MSSQL (legacy)	mssql://username:password@localhost/test
MSSQL (>=2005)	mssql3://username:password@localhost/test
MSSQL (>=2012)	mssql4://username:password@localhost/test
FireBird	firebird://username:password@localhost/test
Oracle	oracle://username/password@test
DB2	db2://username:password@test
Ingres	ingres://username:password@localhost/test
Sybase	sybase://username:password@localhost/test
Informix	informix://username:password@test
Teradata	teradata://DSN=dsn;UID=user;PWD=pass;DATABASE=test
Cubrid	cubrid://username:password@localhost/test
SAPDB	sapdb://username:password@localhost/test
IMAP	imap://user:password@server:port
MongoDB	mongodb://username:password@localhost/test
Google/SQL	google:sql://project:instance/database
Google/NoSQL	google:datastore
Google/NoSQL/NDB	google:datastore+ndb
"""


def sqlite():
    """
    :return: the uri for a sqlite database
    """
    return 'sqlite://storage.sqlite'

def mysql(username, password, host, database, set_encoding=None):
    """
    :param username:
    :param password:
    :param host:
    :param database:
    :param set_encoding: Argument for set_encoding parameter. If None, ignored.
    :return: the uri for a sqlite database
    """
    return 'mysql://{}:{}@/{}{}' \
        .format(
            username,
            password,
            host,
            database,
            ('?set_encoding='+set_encoding) if set_encoding else ''
        )
