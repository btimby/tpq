"""
Utilities.

Mostly database related utilities.
"""

from __future__ import absolute_import

import os
import logging

from contextlib import contextmanager
from urllib.parse import urlparse

from psycopg2.pool import ThreadedConnectionPool


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


# TODO: use **kwargs and dict
def get_db_env():
    LOGGER.debug('Read database config from environment')
    url = os.environ.get('TPQ_URL', None)
    if url:
        LOGGER.debug('Parsing TPQ_URL')
        urlp = urlparse(url)
        host = urlp.hostname
        dbname = urlp.path.strip('/')
        user = urlp.username
        password = urlp.password
    else:
        LOGGER.debug('Looking for TPQ_{HOST, DB, USER, PASS}')
        host = os.environ.get('TPQ_HOST', None)
        dbname = os.environ.get('TPQ_DB', None)
        user = os.environ.get('TPQ_USER', None)
        password = os.environ.get('TPQ_PASS', None)
    if any((host, dbname, user, password)):
        LOGGER.debug('Database config found')
    return (host, dbname, user, password)


# TODO: use **kwargs and dict
def connect(host=None, dbname=None, user=None, password=None, minconn=1,
            maxconn=4):
    """
    Attempts to connect to Postgres.
    """
    if not any((host, dbname, user, password)):
        host, dbname, user, password = get_db_env()
    if not any((host, dbname, user, password)):
        raise Exception('No database connection provided or configured.')
    return ThreadedConnectionPool(minconn, maxconn, host=host, dbname=dbname,
                                  user=user, password=password)


@contextmanager
def savepoint(conn):
    with conn.cursor() as cursor:
        cursor.execute('SAVEPOINT tpq')
        try:
            LOGGER.debug('savepoint started')
            yield cursor
        except:
            LOGGER.debug('savepoint reverting')
            cursor.execute('ROLLBACK TO SAVEPOINT tpq')
            raise
        else:
            LOGGER.debug('savepoint releasing')
            cursor.execute('RELEASE SAVEPOINT tpq')


@contextmanager
def transaction(conn):
    with conn.cursor() as cursor:
        try:
            LOGGER.debug('transaction started')
            yield cursor
        except:
            LOGGER.debug('transaction rollback')
            conn.rollback()
            raise
        else:
            LOGGER.debug('transaction commit')
            conn.commit()


class Literal(object):
    """String wrapper to make a query parameter literal."""

    __slots__ = "s",

    def __init__(self, s):
        self.s = str(s).encode('utf-8')

    def __conform__(self, quote):
        return self

    def __str__(self):
        return self.s.decode('utf-8')

    @classmethod
    def mro(cls):
        return (object, )

    def getquoted(self):
        return self.s
