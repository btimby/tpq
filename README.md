Trivial Postgres Queue
======================

This is a simple queue that can place JSON blobs into a FIFO queue and later
retrieve them.

The difference between this queue and other similar queues is that it utilizes
FOR UPDATE SKIP LOCKED.

The advantage here is that queue items remain in the queue until the current
transaction is committed. Upon rollback, the item is left in the queue
untouched.

If other records are modified as part of a larger transaction, those changes are
also rolled back. Making the larger queue processing operation atomic. Take the
follow order of operations

```sql
BEGIN
SELECT ... FROM queue FOR UPDATE SKIP LOCKED

INSERT INTO ...
DELETE FROM ...

ROLLBACK
```

In the above, none of the statements have any affect, and the queue item remains
in the table to be "retried" by another consumer. Since FOR UPDATE is used, the
queue item remains locked to avoid multiple consumers obtaining that item from
the queue.

Usage
-----

Database connection information can be provided via the library API or
environment variables.

```python
import tpq

# Explicitly provide database connection information
q = tpq.Queue('queue_name', host='localhost', dbname='foobar')
q.put('{"foo": "bar"}')

# Or use shortcut functions:
tpq.put('queue_name', '{"foo": "bar"}', host='localhost', dbname='foobar')
tpq.get('queue_name', host='localhost', dbname='foobar')
```

Or, you can set the connection info in the environment:

```bash
$ # Export as URL
$ export TPQ_URL="postgresql://user:pass@localhost/foobar"

$ # Or separately
$ export TPQ_HOST=localhost
$ export TPQ_DB=foobar
$ export TPQ_USER=user
$ export TPQ_PASS=pass
```

Then omit the parameters:

```python
import tpq

# Use an instance for multiple operations

with tpq.Queue('queue_name') as q:
    q.put('{"foo": "bar"}')
    data = q.get()

# Or use shortcut functions:
tpq.put('queue_name', '{"foo": "bar"}')
tpq.get('queue_name')

```

Command line interface is also provided. JSON can be provided via a file or
stdin (the default).

```bash
$ export TPQ_URL="postgresql://user:pass@localhost/foobar"
$ echo "{\"foo\": \"bar\"}" | tpq produce queue_name
$ tpq produce queue_name --file=message.json
$ tpq produce queue_name --file=- < message.json
$ tpq consume queue_name
{"foo": "bar"}
```