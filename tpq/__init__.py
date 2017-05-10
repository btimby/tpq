from __future__ import absolute_import

import os
import time
import logging

from urllib.parse import urlparse
from select import select
from queue import Empty as QueueEmpty

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

EXIST_SQL = """
SELECT EXISTS (
    SELECT *
    FROM information_schema.tables
    WHERE table_name='poque_%(name)s'
)
"""

CREATE_SQL = """
DO $$ BEGIN

CREATE TABLE "poque_%(name)s" (
    id          bigserial       PRIMARY KEY,
    data        json            NOT NULL
);

END $$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS poque_notify_%(name)s() CASCADE;

CREATE FUNCTION poque_notify_%(name)s() RETURNS TRIGGER AS $$ BEGIN
    PERFORM pg_notify('%(name)s', '');
    RETURN null;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER poque_insert_%(name)s
AFTER INSERT ON "poque_%(name)s"
FOR EACH ROW
EXECUTE PROCEDURE poque_notify_%(name)s();
"""

PUT_SQL = """
INSERT INTO poque_%(name)s (data) VALUES (%(data)s) RETURNING id;
"""

GET_SQL = """
DELETE FROM poque_%(name)s
WHERE id = (
    SELECT id
    FROM poque_%(name)s
    ORDER BY id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING data;
"""

LEN_SQL = """
WITH queued as (
    SELECT *
    FROM poque_%(name)s
    FOR UPDATE SKIP LOCKED
)
SELECT COUNT(*) FROM queued;
"""


def connect(host=None, dbname=None, user=None, password=None):
    """
    Attempts to connect to Postgres.
    """
    if not any((host, dbname, user, password)):
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
    if not any((host, dbname, user, password)):
        raise Exception('No database connection provided or configured.')
    return psycopg2.connect(host=host, dbname=dbname, user=user,
                            password=password)


def maybe_create_queue(conn, table):
    """
    Creates a table to house a queue.
    """
    with conn.cursor() as cursor:
        # Check if our table already exists...
        cursor.execute(EXIST_SQL, {'name': table})
        if not cursor.fetchone()[0]:
            LOGGER.debug('Creating schema')
            # Nope, create it.
            cursor.execute(CREATE_SQL, {'name': table})


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


class Queue(object):
    def __init__(self, name, conn=None, host=None, dbname=None, user=None,
                 password=None):
        self.name = name
        self.table = Literal(name)
        if conn:
            self.conn_managed, self.conn = False, conn
        else:
            self.conn_managed = True
            self.conn = connect(host=host, dbname=dbname, user=user,
                                password=password)
        maybe_create_queue(self.conn, self.table)

    def __len__(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute(LEN_SQL, {'name': self.table})
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.conn_managed:
            self.conn.commit()
            self.conn.close()

    def put(self, data):
        """
        Place data on queue.

        Inserts a record into the queue. A trigger calls notify to wake any
        potential listeners.
        """
        with self.conn.cursor() as cursor:
            cursor.execute(PUT_SQL, {'name': self.table, 'data': data})
            return cursor.fetchone()[0]

    def get(self, wait=-1):
        """
        Retrieve data from queue.

        deletes the oldest record and returns it's data. If cursor is passed in
        then this operation becomes part of a larger transaction. The delete
        will not finalize until this transaction is committed.

        On rollback, the item will return to the queue.

        The wait parameter controls what happens when the queue is empty.

         - wait < 0: Don't wait, raises QueueEmpty.
         - wait > 0: Wait `wait` seconds. Then return data or raise QueueEmpty.
         - wait = 0: Wait forever.
        """
        with self.conn.cursor() as cursor:

            def _get():
                """Attempts to get the next item."""
                LOGGER.debug('Attempting to read item')
                cursor.execute(GET_SQL, {'name': self.table})
                row = cursor.fetchone()
                if row:
                    LOGGER.debug('Item read, returning')
                    return row[0]

            def _wait():
                """Uses PostgreSQL LISTEN and select() to wait for item."""
                # TODO: I want to make sure this does not commit any open
                # transactions. If so, I will need to use a separate connection.
                saved = (self.conn.isolation_level, self.conn.autocommit)
                # This is needed for LISTEN to work properly...
                self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                try:
                    cursor.execute('LISTEN "%s"', (self.table, ))
                    args = [[self.conn], [], []]
                    if wait != 0:
                        # 0 is wait forever, select() requires we omit timeout.
                        args.append(wait)
                    if not any(select(*args)):
                        # Timeout expired while waiting. We know there is no
                        # wait time remaining, so we can just raise and be done.
                        raise QueueEmpty()
                finally:
                    cursor.execute('UNLISTEN "%s"', (self.table, ))
                    self.conn.isolation_level, self.conn.autocommit = saved

            while True:
                start = time.time()
                data = _get()
                if data:
                    return data
                if wait < 0:
                    LOGGER.debug('Empty, waiting disabled')
                    raise QueueEmpty()
                if wait == 0:
                    LOGGER.debug('Waiting indefinitely')
                else:
                    LOGGER.debug('Waiting for %ss', wait)
                _wait()
                # There is a race condition, listen might return, but another
                # listener scoops us. Therefore, we may end up waiting some
                # more. Calculate how long we should continue waiting in that
                # case.
                wait = 0 if wait == 0 else wait - time.time() - start
                LOGGER.debug('Waiting done. %ss remaining', wait)


def put(name, data, **kwargs):
    """
    Shortcut for writing to queue.

    This function is useful when writing a single item. If you write more than
    one item, use a dedicated queue instance.
    """
    with Queue(name, **kwargs) as q:
        return q.put(data)


def get(name, wait=-1, **kwargs):
    """
    Shortcurt for reading from queue.

    This function is useful when reading a single item. If you read more than
    one item, use a dedicated queue instance.
    """
    with Queue(name, **kwargs) as q:
        return q.get(wait=wait)
