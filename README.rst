.. image:: https://travis-ci.org/btimby/tpq.svg?branch=master
   :alt: Travis CI Status
   :target: https://travis-ci.org/btimby/tpq

.. image:: https://coveralls.io/repos/github/btimby/tpq/badge.svg?branch=master
    :target: https://coveralls.io/github/btimby/tpq?branch=master
    :alt: Code Coverage

Trivial Postgres Queue
======================

This is a simple library that can place JSON blobs into a FIFO queue and later
retrieve them.

The difference between this queue and other similar queues is that it utilizes
``FOR UPDATE SKIP LOCKED``.

The advantage here is that queue items remain in the queue until the current
transaction is committed. Upon rollback, the item is left in the queue
untouched.

If other records are modified as part of a larger transaction, those changes are
also rolled back. Making the larger queue processing operation atomic. Take the
follow order of operations


.. code:: sql

    BEGIN
    SELECT ... FROM queue FOR UPDATE SKIP LOCKED

    INSERT INTO ...
    DELETE FROM ...

    ROLLBACK

In the above, none of the statements have any affect, and the queue item remains
in the table to be "retried" by another consumer. Since `FOR UPDATE` is used,
the queue item remains locked to avoid multiple consumers obtaining that item
from the queue.

Python Library Usage
--------------------

Database connection information can be provided via the library API.

.. code:: python

    import tpq

    # Explicitly provide database connection information
    q = tpq.Queue('queue_name', host='localhost', dbname='foobar')
    q.put('{"foo": "bar"}')

    # Or use shortcut functions:
    tpq.put('queue_name', '{"foo": "bar"}', host='localhost', dbname='foobar')
    tpq.get('queue_name', host='localhost', dbname='foobar')

    # Or to take advantage of cooperative transactions, provide a connection:
    q = tpq.Queue('queue_name', conn=connection)
    q.put('{"foo": "bar"}')

    # Which is also supported by shortcut functions:
    tpq.put('queue_name', '{"foo": "bar"}', conn=connection)
    tpq.get('queue_name', conn=connection)

Or, you can set the connection info in the environment:

::

    $ # Export as URL
    $ export TPQ_URL="postgresql://user:pass@localhost/foobar"

    $ # Or separately
    $ export TPQ_HOST=localhost
    $ export TPQ_DB=foobar
    $ export TPQ_USER=user
    $ export TPQ_PASS=pass

Then omit the parameters:

.. code:: python

    import tpq

    # Use an instance for multiple operations
    with tpq.Queue('queue_name') as q:
        q.put('{"foo": "bar"}')
        data = q.get()

    # Or use shortcut functions:
    tpq.put('queue_name', '{"foo": "bar"}')
    tpq.get('queue_name')

Waiting
-------

You can wait for an item to arrive using the `wait` argument.

.. code:: python

    import tpq

    # Wait forever
    tpq.get('queue_name', wait=0)

    # Don't wait (also can omit the param).
    tpq.get('queue_name', wait=-1)

    # Wait specified number of seconds.
    tpq.get('queue_name', wait=5)

Command Line Interface
----------------------

Command line interface is also provided. JSON can be provided via a file or
stdin (the default).

::

    $ # Configure your database.
    $ export TPQ_URL="postgresql://user:pass@localhost/foobar"

    $ # JSON via stdin (default).
    $ echo "{\"foo\": \"bar\"}" | tpq produce queue_name

    $ # JSON via file.
    $ tpq produce queue_name --file=message.json

    $ # Explicitly provide JSON via stdin.
    $ tpq produce queue_name --file=- < message.json

    $ # Then read the item to stdout.
    $ tpq consume queue_name
    {'foo': 'bar'}

    $ # If you have trouble (or for logging). Debug output goes to stderr.
    $ TPQ_URL="postgresql://user:pass@localhost/foobar" tpq consume queue_name --debug
    Read database config from environment
    Parsing TPQ_URL
    Database config found
    Attempting to read item
    Item read, returning
    {'foo': 'bar'}

    $ # You can wait on the CLI too...
    $ # Forever:
    $ tpq consume queue_name --wait=0

    $ # Specified number of seconds:
    $ tpq consume queue_name --wait=5

    $ # The return code signals whether an item was received or not.
    $ tpq consume queue_name --wait=-1
    {'foo': 'bar'}
    $ echo $?
    0

    # For an empty queue, you get 1
    $ tpq consume queue_name --wait=-1
    Queue empty
    Traceback (most recent call last):
      File "/home/btimby/Code/tpq/tpq/__main__.py", line 24, in consume
        print(get(opt['<name>'], wait=opt['--wait']))
      File "/home/btimby/Code/tpq/tpq/__init__.py", line 266, in get
        return q.get(wait=wait)
      File "/home/btimby/Code/tpq/tpq/__init__.py", line 233, in get
        raise QueueEmpty()
    queue.Empty
    $ echo $?
    1
