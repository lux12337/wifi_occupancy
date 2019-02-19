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

taken from:
http://www.web2py.com/books/default/chapter/29/06/the-database-abstraction-layer#Connection-strings-the-uri-parameter-
updated 17 Feb 2019
"""

from typing import Optional, Callable, Dict, Set
from inspect import signature


def sqlite() -> str:
    """
    :return: the uri for a sqlite database
    """
    return 'sqlite://storage.sqlite'


def mysql(
    username: str,
    password: str,
    host: str,
    database: str,
    # include optional parameters in the dbs dictionary below
    set_encoding: Optional[str]
) -> str:
    """
    :param username:
    :param password:
    :param host:
    :param database:
    :param set_encoding: Argument for set_encoding parameter. If None, ignored.
    :return: the uri for a sqlite database
    """
    return 'mysql://{}:{}@/{}{}'.format(
        username,
        password,
        host,
        database,
        ('?set_encoding='+set_encoding) if set_encoding else ''
    )

def params(fun: Callable[..., any]) -> Set[str]:
    """
    :param fun: any function
    :return: the set of parameter names
    """
    return set(
        signature( fun ).parameters.keys()
    )

# used to prevent silly typos
uri_ = 'uri'
uri_params_ = 'uri_params'
uri_optional_params_ = 'uri_optional_params'
uri_required_params_ = 'uri_required_params'

dbs: Dict[str, Dict[str,any]] = {
    sqlite.__name__: {
        uri_: sqlite,
        uri_params_: params(sqlite),
        uri_optional_params_: {}
    },
    mysql.__name__: {
        uri_: mysql,
        uri_params_: params(mysql),
        uri_optional_params_: {'set_encoding'}
    }
}
"""
keys: names of the databases supported by this module.
values: dictionaries of details.
"""


def required_uri_params(db_name: str) -> Set[str]:
    """
    Creates the set of required parameters for a uri function,
    stores it in this module's dbs object,
    and returns it.
    :param db_name:
    :return: the set of required parameters for this db's uri function.
    """

    try:
        db = dbs[db_name]
    except KeyError:
        raise Exception('invalid db name')

    # if we already created it
    if uri_required_params_ in db and db[uri_required_params_]:
        return db[uri_required_params_]

    all_params = dbs[db_name][uri_params_]
    optional_params = dbs[db_name][uri_optional_params_]

    # the required params remain after removing the optional params
    db[uri_required_params_] = all_params - optional_params

    return db[uri_required_params_]


def sufficient_uri_args(db_name: str, args: Set[str]) -> bool:
    """
    :param db_name: the name of the database
    :param args: argument names (not values) potentially used by this
    module's functions to create a uri.
    :return: whether args would be sufficient if applied to a function from
    this module.
    """

    all_params = dbs[db_name][uri_params_]
    optional_params = dbs[db_name][uri_optional_params_]

    required_params = all_params - optional_params

    # args must be a subset of the required uri parameters.
    return args <= required_params
