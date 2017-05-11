import os
import logging
import sys
import json
import threading
import time
import tempfile
import unittest

from io import StringIO, BytesIO
from contextlib import contextmanager

import psycopg2

import tpq

from tpq.utils import (
    get_db_env, transaction, savepoint
)
from tpq.__main__ import main


# Useful to debug threading issues.
logging.basicConfig(
    stream=sys.stderr,
    # Change level to DEBUG here if you need to.
    level=logging.CRITICAL,
    format='%(thread)d: %(message)s'
)
LOGGER = logging.getLogger(__name__)


@contextmanager
def setenv(add, remove):
    _environ = dict(os.environ)
    os.environ.update(add)
    for k in remove:
        os.environ.pop(k, None)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(_environ)


class TestError(Exception):
    """
    Error used for some tests.
    """

    pass


class ThreadedConsumer(threading.Thread):
    """
    Helper to get() items from queue on a thread.
    """

    def __init__(self, queue, wait=-1, work=0, exit=False, once=False):
        self.queue = queue
        self.wait = wait
        self.work = work
        self.exit = exit
        self.once = once
        self.items = []
        self.errors = []
        self.loops = 0
        self.stopping = threading.Event()
        threading.Thread.__init__(self)
        self.start()

    def run(self):
        LOGGER.debug('Starting')
        while not self.stopping.is_set():
            LOGGER.debug('Looping')
            # TODO: make get() a context manager.
            try:
                LOGGER.debug('get()ing')
                with self.queue.get(wait=self.wait) as item:
                    LOGGER.debug('got: %s', item)
                    self.items.append(item)
                LOGGER.debug('Success')
            except Exception as e:
                if isinstance(e, tpq.QueueEmpty):
                    LOGGER.debug('Empty')
                self.errors.append(e)
                if self.exit:
                    LOGGER.debug('Exiting')
                    self.stopping.set()
                    continue
            else:
                # Interruptable sleep. Simulates working on a task with txn
                # open.
                LOGGER.debug('Sleeping for %s', self.work)
                self.stopping.wait(self.work)
                LOGGER.debug('Awoke')
                LOGGER.debug('Commited')
            if self.once:
                LOGGER.debug('Exiting')
                self.stopping.set()
            self.loops += 1
        LOGGER.debug('Stopping')

    def stop(self):
        LOGGER.debug('Signaling')
        self.stopping.set()
        LOGGER.debug('Joining')
        self.join()


class ThreadedProducer(threading.Thread):
    """
    Helper to put items in queue on a thread.
    """

    def __init__(self, queue, items, work=0):
        self.queue = queue
        self.work = work
        self.items = items[:]
        self.stopping = threading.Event()
        threading.Thread.__init__(self)
        self.start()

    def run(self):
        LOGGER.debug('Starting')
        while self.items and not self.stopping.is_set():
            LOGGER.debug('Looping')
            item = self.items.pop(0)
            LOGGER.debug('put: %s', item)
            self.queue.put(item)
            self.stopping.wait(self.work)
        LOGGER.debug('Stopping')

    def stop(self):
        LOGGER.debug('Signaling')
        self.stopping.set()
        LOGGER.debug('Joining')
        self.join()


class Tests(object):
    """
    Test normal operations.
    """

    def test_empty(self):
        """
        Ensure empty queue behavior is correct.

        Queue should raise QueueEmpty when empty.
        """
        with self.assertRaises(tpq.QueueEmpty):
            # Note that since this is a context manager, we MUST use with...
            with self.queue.get() as item:
                print(item)


class ThreadedTests(object):
    """
    Test thread interactions.
    """

    def test_skip(self):
        """
        Ensure concurrent consumers do not collide.

        This test ensures that if one consumer get()s a message inside a
        transaction that another consumer ignores that message (no
        double-dipping).
        """
        self.queue.put({'test':'test'})
        c1 = ThreadedConsumer(self.queue, work=10)
        time.sleep(0.1)
        c2 = ThreadedConsumer(self.queue)
        time.sleep(0.1)
        c1.stop()
        c2.stop()
        # Make sure consumer one got the message.
        self.assertEqual(1, len(c1.items))
        # Make sure consumer two did not.
        self.assertEqual(0, len(c2.items))
        # Make sure consumer two tried at least once.
        self.assertTrue(c2.loops > 0)

    def test_fair(self):
        """
        Ensure concurrent consumers can both get().

        This test ensures that if there are two mesages in the queue, that each
        consumer can get one. In other words they don't compete.
        """
        self.queue.put({'test': 'test'})
        self.queue.put({'test': 'test'})
        c1 = ThreadedConsumer(self.queue, work=10)
        c2 = ThreadedConsumer(self.queue, work=10)
        time.sleep(0.1)
        c1.stop()
        c2.stop()
        # Make sure consumer one got a message.
        self.assertEqual(1, len(c1.items))
        # Make sure consumer two got a message.
        self.assertEqual(1, len(c2.items))

    def test_order(self):
        """
        Ensure FIFO.

        Compare dequeued items to queued items and assert equality (same order).
        """
        put, got = [], []
        for i in range(10):
            put.append({'test': i})
        p = ThreadedProducer(self.queue, put)
        p.join()
        c = ThreadedConsumer(self.queue, exit=True)
        c.join()
        self.assertEqual(len(put), len(c.items))
        self.assertEqual(put, c.items)

    def test_len(self):
        """Ensure len() works for queue."""
        self.assertEqual(0, len(self.queue))
        ThreadedProducer(self.queue, [{'a': 'b'} for i in range(10)]).join()
        self.assertEqual(10, len(self.queue))
        c = ThreadedConsumer(self.queue, exit=True)
        c.join()
        self.assertEqual(10, len(c.items))
        self.assertEqual(0, len(self.queue))

    def test_wait_forever(self):
        """Ensure waiting forever works.

        Whether pooled or not, or threaded or not, waiting without a timeout
        should always work.
        """
        c = ThreadedConsumer(self.queue, wait=0, once=True)
        # Make it wait...
        time.sleep(0.1)
        self.assertTrue(c.is_alive())
        self.queue.put({'test': 'test'})
        c.stop()
        self.assertEqual(1, len(c.items))


class PooledTestCase(Tests, ThreadedTests, unittest.TestCase):
    """
    Test queue with connection pool.
    """

    def setUp(self):
        self.queue = tpq.Queue('test')
        self.queue.create()
        self.queue.clear()

    def tearDown(self):
        self.queue.clear()
        self.queue.close()

    def test_wait_timeout_interrupted(self):
        """We should be able to wait just fine."""
        c = ThreadedConsumer(self.queue, wait=10)
        # Make it wait...
        time.sleep(0.1)
        self.queue.put({'test': 'test'})
        c.stop()
        self.assertEqual(1, len(c.items))
        self.assertIsInstance(c.items[0], dict)

    def test_wait_timeout_expires(self):
        start = time.time()
        c = ThreadedConsumer(self.queue, wait=1, once=True)
        c.stop()
        self.assertLess(1, time.time() - start)
        self.assertEqual(1, len(c.errors))
        self.assertIsInstance(c.errors[0], tpq.QueueEmpty)


class SharedTestCase(Tests, ThreadedTests, unittest.TestCase):
    """
    Test queue with shared connection.
    """

    def setUp(self):
        host, dbname, user, password = get_db_env()
        self.conn = psycopg2.connect(host=host, dbname=dbname, user=user,
                                     password=password)
        self.queue = tpq.Queue('test', conn=self.conn)
        self.queue.clear()

    def tearDown(self):
        self.queue.clear()
        self.conn.close()

    def test_wait_timeout(self):
        """This one should result in a warning."""
        c = ThreadedConsumer(self.queue, wait=10, exit=True)
        # Make it wait...
        time.sleep(0.1)
        self.queue.put({'test': 'test'})
        c.stop()
        self.assertEqual(1, len(c.errors))
        self.assertIsInstance(c.errors[0], Warning)

# TODO: we need to test a shared connection, ensuring an open transaction is
# not committed under put() or get() with or without wait.


class ShortcutTestCase(unittest.TestCase):
    """
    Test module-level shortcut functions.
    """

    def setUp(self):
        self.queue = tpq.Queue('test')
        self.queue.create()

    def tearDown(self):
        self.queue.clear()
        self.queue.close()

    def test_get(self):
        item_put = {'test': 'test'}
        self.queue.put(item_put)
        item_get = tpq.get('test')
        self.assertEqual(item_put, item_get)

    def test_put(self):
        item_put = {'test': 'test'}
        tpq.put('test', item_put)
        with self.queue.get('test') as item_get:
            pass
        self.assertEqual(item_put, item_get)

    def test_create(self):
        tpq.create('test')

    def test_clear(self):
        tpq.create('test')
        tpq.clear('test')


class CommandTestCase(unittest.TestCase):
    """
    Test Command Line Interface.
    """

    def setUp(self):
        self.queue = tpq.Queue('test')
        self.queue.create()

    def tearDown(self):
        self.queue.clear()
        self.queue.close()

    def test_main_get(self):
        """Ensure we can get from a queue using CLI."""
        item_put, stdout = {'test': 'test'}, StringIO()
        self.queue.put(item_put)

        try:
            main({
                '--debug': False,
                '<name>': 'test',
                'consume': True,
                'produce': False,
                '--wait': False,
            }, stdout=stdout)
        except SystemExit as e:
            self.assertEqual(0, e.args[0])
        else:
            self.fail('Did not raise SystemExit')

        self.assertEqual(item_put, json.loads(stdout.getvalue()))

    def test_main_put_stdin(self):
        """Ensure we can put to a queue from stdin using CLI."""
        item_put = {'test': 'test'}

        try:
            main({
                '--debug': False,
                '<name>': 'test',
                'consume': False,
                'produce': True,
                '--file': '-',
                '--create': False,
            }, stdin=StringIO(json.dumps(item_put)))
        except SystemExit as e:
            self.assertEqual(0, e.args[0])
        else:
            self.fail('Did not raise SystemExit')

        with self.queue.get() as item_get:
            self.assertEqual(item_put, item_get)

    def test_main_put_file(self):
        """Ensure we can put to a queue from a file using CLI."""
        item_put = {'test': 'test'}

        with tempfile.NamedTemporaryFile() as t:
            t.write(json.dumps(item_put).encode('utf-8'))
            t.flush()

            try:
                main({
                    '--debug': False,
                    '<name>': 'test',
                    'consume': False,
                    'produce': True,
                    '--file': t.name,
                    '--create': False,
                }, stdin=StringIO(json.dumps(item_put)))
            except SystemExit as e:
                self.assertEqual(0, e.args[0])
            else:
                self.fail('Did not raise SystemExit')

            with self.queue.get() as item_get:
                self.assertEqual(item_put, item_get)

    def test_main_put_file_json_error(self):
        """Ensure put fails with invalid json."""
        try:
            main({
                '--debug': False,
                '<name>': 'test',
                'consume': False,
                'produce': True,
                '--file': '/dev/null',
                '--create': False,
            })
        except SystemExit as e:
            self.assertEqual(1, e.args[0])
        else:
            self.fail('Did not raise SystemExit')

    def test_main_put_file_decode_error(self):
        """Ensure put fails with invalid data."""
        with open('/dev/random', 'rb') as r:
            try:
                main({
                    '--debug': False,
                    '<name>': 'test',
                    'consume': False,
                    'produce': True,
                    '--file': '-',
                    '--create': False,
                }, stdin=BytesIO(r.read(10)))
            except SystemExit as e:
                self.assertEqual(1, e.args[0])
            else:
                self.fail('Did not raise SystemExit')

    def test_main_get_fail_emptyqueue(self):
        """Ensure get fails when queue is empty."""
        import pdb; pdb.set_trace()
        try:
            main({
                '--debug': False,
                '<name>': 'test',
                'consume': True,
                'produce': False,
                '--create': False,
            })
        except SystemExit as e:
            self.assertEqual(1, e.args[0])
        else:
            self.fail('Did not raise SystemExit')

    def test_main_get_fail_missing(self):
        """Ensure get fails when queue is missing."""
        try:
            main({
                '--debug': False,
                '<name>': 'bubba',
                'consume': True,
                'produce': False,
                '--file': '-',
                '--create': False,
            })
        except SystemExit as e:
            self.assertEqual(1, e.args[0])
        else:
            self.fail('Did not raise SystemExit')

    def test_main_put_fail_missing(self):
        """Ensure put fails when queue is missing."""
        try:
            main({
                '--debug': False,
                '<name>': 'bubba',
                'consume': False,
                'produce': True,
                '--file': '-',
                '--create': False,
            }, stdin=StringIO('{"test": "test"'))
        except SystemExit as e:
            self.assertEqual(1, e.args[0])
        else:
            self.fail('Did not raise SystemExit')


class TransactionTestCase(unittest.TestCase):
    def setUp(self):
        host, dbname, user, password = get_db_env()
        self.conn = psycopg2.connect(host=host, dbname=dbname, user=user,
                                     password=password)
        with self.conn.cursor() as cursor:
            cursor.execute('CREATE TABLE '
                           'IF NOT EXISTS test_t ('
                               'id serial primary key, '
                               'v varchar(1) not null'
                            ')')
            self.conn.commit()

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute('DROP TABLE test_t')
            self.conn.commit()

    def test_transaction(self):
        """If transaction works, row should disappear after leaving context."""
        with self.assertRaises(TestError):
            with transaction(self.conn) as cursor:
                cursor.execute('INSERT INTO test_t (v) '
                               'VALUES (\'a\')')
                raise TestError()

        with self.conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) '
                           'FROM test_t '
                           'WHERE v = \'a\'')
            self.assertEqual(0, cursor.fetchone()[0])

    def test_savepoint(self):
        """If savepoint works, row should disappear after leaving context."""
        with self.assertRaises(TestError):
            with savepoint(self.conn) as cursor:
                cursor.execute('INSERT INTO test_t (v) '
                               'VALUES (\'a\')')
                raise TestError()

        with self.conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) '
                           'FROM test_t '
                           'WHERE v = \'a\'')
            self.assertEqual(0, cursor.fetchone()[0])


class DBConfigTestCase(unittest.TestCase):
    def test_url(self):
        with setenv({'TPQ_URL': 'postgresql://foo:bar@baz/qux'}):
            self.assertEqual(('baz', 'qux', 'foo', 'bar'), get_db_env())


    def test_url(self):
        with setenv({
            'TPQ_HOST': 'baz',
            'TPQ_DB': 'qux',
            'TPQ_USER': 'foo',
            'TPQ_PASS': 'bar',
        }, ('TPQ_URL', )):
            self.assertEqual(('baz', 'qux', 'foo', 'bar'), get_db_env())


if __name__ == '__main__':
    unittest.main()
