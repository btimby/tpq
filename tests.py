import logging
import sys
import threading
import time
import unittest

import tpq


# Useful to debug threading issues.
logging.basicConfig(
    stream=sys.stderr,
    # Change level to DEBUG here if you need to.
    level=logging.ERROR,
    format='%(thread)d: %(message)s'
)
LOGGER = logging.getLogger(__name__)


class ThreadedConsumer(threading.Thread):
    def __init__(self, queue, wait=-1, work=0, exit=False):
        self.queue = queue
        self.wait = wait
        self.work = work
        self.exit = exit
        self.items = []
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
            except tpq.QueueEmpty:
                LOGGER.debug('Empty')
                if self.exit:
                    LOGGER.debug('Exiting')
                    break
            else:
                # Interruptable sleep.
                LOGGER.debug('Sleeping for %s', self.work)
                self.stopping.wait(self.work)
                LOGGER.debug('Awoke')
                LOGGER.debug('Commited')
            self.loops += 1
        LOGGER.debug('Stopping')

    def stop(self):
        self.stopping.set()
        self.join()


class ThreadedProducer(threading.Thread):
    def __init__(self, queue, items):
        self.queue = queue
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
        LOGGER.debug('Stopping')

    def stop(self):
        self.stopping.set()
        self.join()


class QueueTestCase(unittest.TestCase):
    def setUp(self):
        self.queue = tpq.Queue('test')
        self.queue.clear()

    def tearDown(self):
        self.queue.clear()

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
        """
        Ensure len() works for queue.
        """
        self.assertEqual(0, len(self.queue))
        ThreadedProducer(self.queue, [{'a': 'b'} for i in range(10)]).join()
        self.assertEqual(10, len(self.queue))
        c = ThreadedConsumer(self.queue, exit=True)
        c.join()
        self.assertEqual(10, len(c.items))
        self.assertEqual(0, len(self.queue))

if __name__ == '__main__':
    unittest.main()
