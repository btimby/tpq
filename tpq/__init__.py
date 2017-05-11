from __future__ import absolute_import

import time
import logging
import threading

from select import select
from queue import Empty as QueueEmpty
from contextlib import contextmanager

from psycopg2.extras import Json
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from tpq.utils import (
    Literal, connect, transaction, savepoint
)
from tpq.sql import (
    LEN, EXIST, CREATE, PUT, GET, DEL
)


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


class Queue(object):
    """
    Queue class.

    Handles interaction with Postgres queue.
    """

    def __init__(self, name, conn=None, pool=None, host=None, dbname=None,
                 user=None, password=None):
        self.name = name
        self.table = Literal(name)
        # The user may have provided their own connection or pool for our use.
        self.threads = set()
        self.managed = False
        self.conn = conn
        self.pool = pool
        # If not, we will create a pool using the provided connection details
        # or environment. In this case managed is set to True signaling that we
        # "own" the connection pool.
        if not conn and not pool:
            self.managed = True
            self.pool = connect(host=host, dbname=dbname, user=user,
                                password=password)

    def __len__(self):
        with self._atomic() as cursor:
            cursor.execute(LEN, {'name': self.table})
            return cursor.fetchone()[0]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @contextmanager
    def _atomic(self):
        """
        Manage savepoint or transaction.

        Depending on our database connection details (connection or pool) we
        use a different atomic method. This context manager delegates to the
        proper one.
        """
        if self.conn:
            # Not perfect, but if we see two distinct thread ids through here
            # while using a shared connection, warn the user. While this is
            # technically OK, it is NOT safe from a transactional standpoint.
            self.threads.add(threading.get_ident())
            if len(self.threads) > 0:
                LOGGER.warning('Possible threading with connection sharing. '
                               'Use a pool instead.')
            # Use a savepoint with a shared connection so we don't affect
            # possible in-flight transactions.
            with savepoint(self.conn) as cursor:
                yield cursor
        else:
            conn = self.pool.getconn()
            try:
                # Use a true transaction with a pool since we "own" the
                # connection
                with transaction(conn) as cursor:
                    yield cursor
            finally:
                # Return connection to pool
                self.pool.putconn(conn)

    def close(self):
        """
        Closes database connection.

        Only closes the connection if this object opened it in the first place.
        If the connection pool was passed it, this function does nothing.
        """
        if self.managed:
            self.pool.closeall()
        else:
            LOGGER.warning('Not closing unmanaged connections')

    def create(self):
        with self._atomic() as cursor:
            # Check if our table already exists...
            cursor.execute(EXIST, {'name': self.table})
            if cursor.fetchone()[0]:
                LOGGER.warning('Create called, but schema exists')
                return
            LOGGER.debug('Creating schema')
            # Nope, create it.
            cursor.execute(CREATE, {'name': self.table})

    def put(self, data):
        """
        Place data on queue.

        Inserts a record into the queue. A trigger calls notify to wake any
        potential listeners.
        """
        if isinstance(data, dict):
            data = Json(data)

        with self._atomic() as cursor:
            cursor.execute(PUT, {'name': self.table, 'data': data})
            return cursor.fetchone()[0]

    @contextmanager
    def get(self, wait=-1):
        """
        Retrieve data from queue.

        Deletes the oldest record and returns it's data. This method is a
        context manager which holds an open transaction within it's context.
        This means that any database operations done in response to the
        returned message will be atomic along with the message removal.

        In case of an exception within the context, the message will be
        returned to the queue.

        The wait parameter controls what happens when the queue is empty.

         - wait < 0: Don't wait, raises QueueEmpty.
         - wait > 0: Wait `wait` seconds. Then return data or raise QueueEmpty.
         - wait = 0: Wait forever.
        """

        def _get(cursor):
            """Attempts to get the next item."""
            LOGGER.debug('Attempting to read item')
            cursor.execute(GET, {'name': self.table})
            row = cursor.fetchone()
            if row:
                LOGGER.debug('Item read, returning')
                return row[0]

        def _wait(cursor):
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
            # Record our time so we can use it to calculate how long we have
            # waited overall in case this takes more than one iteration.
            start = time.time()

            # Try to get an item off the queue
            with self._atomic() as cursor:
                data = _get(cursor)
                if data:
                    yield data
                    return

            # We were unable to get an item, if wait is negative, then return
            # immediately by raising QueueEmpty.
            if wait < 0:
                LOGGER.debug('Empty, waiting disabled')
                raise QueueEmpty()

            # We were unable to get an item but we were instructed to wait for
            # one. Here we wait.
            if wait == 0:
                LOGGER.debug('Waiting indefinitely')
            else:
                LOGGER.debug('Waiting for %ss', wait)
            with self._atomic() as cursor:
                _wait(cursor)

            # There is a possible race condition, listen might return, but
            # another listener scoops us. Therefore, we may end up waiting some
            # more. Calculate how long we should continue waiting in that
            # case.
            wait = 0 if wait == 0 else wait - time.time() - start
            LOGGER.debug('Waiting done. %ss remaining', wait)

    def clear(self):
        """
        Delete all items from queue.
        """
        with self._atomic() as cursor:
            cursor.execute(DEL, {'name': self.table})


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


def clear(name, **kwargs):
    """
    Shortcurt for clearing a queue.
    """
    with Queue(name, **kwargs) as q:
        q.clear()


def create(name, **kwargs):
    """
    Shortcurt for creating a queue.
    """
    with Queue(name, **kwargs) as q:
        q.create()
