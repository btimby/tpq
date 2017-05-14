"""
TPQ.

Trivial Postgres Queue
"""

from __future__ import absolute_import

import time
import logging
import threading

from select import select
from queue import Empty as QueueEmpty
from contextlib import contextmanager
# from collections import UserDict

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


# class QueueItem(UserDict):
#     """
#     Queue Item.
#
#     Returned by get(). Handles transaction implicitly via context manager or
#     explicitly. Allowing caller to use either:
#
#         item = queue.get()
#         ...
#         item.done()
#
#     or:
#
#         with queue.get() as item:
#             ...
#     """
#     def __init__(self, conn, data={}):
#         super().__init__(data)
#         self.conn = conn
#         self.t = transaction(conn)
#
#     def __enter__(self):
#         self.ctx = transaction(self.conn).__enter__()
#
#     def __exit__(self, *args):
#         self.ctx.__exit__(*args)


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
    def _connect(self):
        if self.pool:
            conn = self.pool.getconn()
            try:
                yield conn
            finally:
                # Return connection to pool
                self.pool.putconn(conn)
        elif self.conn:
            # Not perfect, but if we see two distinct thread ids through here
            # while using a shared connection, warn the user. While this is
            # technically OK, it is NOT safe from a transactional standpoint.
            self.threads.add(threading.get_ident())
            if len(self.threads) > 0:
                LOGGER.warning('Possible threading with connection sharing. '
                               'Use a pool instead.')
            yield self.conn
        else:
            raise AssertionError('self.conn or self.pool must be set')

    @contextmanager
    def _atomic(self):
        """
        Manage savepoint or transaction.

        Depending on our database connection details (connection or pool) we
        use a different atomic method. This context manager delegates to the
        proper one.
        """
        if self.pool:
            with self._connect() as conn, transaction(conn) as cursor:
                yield cursor
        else:
            with self._connect() as conn, savepoint(conn) as cursor:
                yield cursor

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

    # TODO: allow this to be used as a context manager or not. To do this, we
    # would need to subclass UserDict and make _it_ a context manager. We could
    # then copy our JSON decoded object to that and return it.
    #
    # It would then be the caller's responsibility to commit, via Job.commit()
    # or the connection directly. Job.commit() would be called by __exit__().
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

        def _wait():
            """Uses PostgreSQL LISTEN and select() to wait for item."""
            with self._connect() as conn:
                # Things are a bit hairy. We need autocommit for async LISTEN,
                # which we use for wait timeout. Without async LISTEN, we have
                # only blocking LISTEN, which is only good for indefinite
                # timeout. So if autocommit is False while wait is > 0, we want
                # to set autocommit to True BUT, if we do so on a shared
                # connection, we will implicitly COMMIT the caller's open
                # transaction (if there is one). This is undesirable, so we
                # will avoid doing so by raising a Warning.
                if wait != 0:
                    saved = None
                    # Caller requested a timeout.
                    if not conn.autocommit:
                        # But autocommit is False.
                        if not self.pool:
                            # But we cannot provide a timeout since autocommit
                            # is False, we dare not change it since we are
                            # sharing caller's connection with potentially open
                            # transaction. The safe this is to raise.
                            raise Warning('Using shared connection and '
                                          'autocommit=False. Use a pool if you'
                                          ' wish to wait with a timeout')
                        # Pooled, so we can change autocommit.
                        saved = (conn.isolation_level, conn.autocommit)
                        # This is needed for LISTEN to work properly...
                        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                    # Finally do our async LISTEN.
                    with conn.cursor() as cursor:
                        cursor.execute('LISTEN "%s"', (self.table, ))
                        try:
                            if not any(select([conn], [], [], wait)):
                                # Timeout expired while waiting. We know there
                                # is no wait time remaining, so we can just
                                # raise and be done.
                                raise QueueEmpty()
                            # A result was returned, so we exit, which will
                            # loop back to _get().
                        finally:
                            cursor.execute('UNLISTEN "%s"', (self.table, ))
                            if saved:
                                # Restore before returning to pool, unsure if
                                # this is strictly necessary.
                                conn.isolation_level, conn.autocommit = saved
                else:
                    # Wait is 0, just do blocking LISTEN
                    with conn.cursor() as cursor:
                        cursor.execute('LISTEN "%s"', (self.table, ))

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
            _wait()

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
        with q.get(wait=wait) as item:
            return item


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
