"""
TPQ CLI

Trivial Postgres Queue Command Line Interface
"""

from __future__ import absolute_import

import json
import sys
import logging

from docopt import docopt, DocoptExit
from schema import Schema, Use, SchemaError

from tpq import (
    QueueEmpty, get, put
)


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


def consume(opt, stdout):
    """
    Read from queue.
    """
    try:
        # Print to stdout, errors are on stderr
        item = get(opt['<name>'], wait=opt['--wait'])
        stdout.write('%s\n' % json.dumps(item))
        sys.exit(0)
    except QueueEmpty:
        LOGGER.exception('Queue empty', exc_info=True)
        sys.exit(1)


def produce(opt, stdin):
    """
    Write to queue.
    """
    if opt['--file'] == '-':
        data = stdin.read()

    else:
        with open(opt['--file'], 'rb') as f:
            data = f.read()

    try:
        data = data.decode('utf-8')
    except AttributeError:
        pass
    except Exception as e:
        LOGGER.exception('Could not decode data', exc_info=True)
        sys.exit(1)

    try:
        json.loads(data)
    except ValueError as e:
        LOGGER.exception('JSON data expected in stdin', exc_info=True)
        sys.exit(1)

    try:
        put(opt['<name>'], data)
    except Exception as e:
        LOGGER.exception('Could not write to queue', exc_info=True)
        sys.exit(1)
    else:
        sys.exit(0)

def main(opt, stdin=sys.stdin, stdout=sys.stdout):
    """
    tpq - Trivial Postgress Queue

    Usage:
        tpq produce <name> [--debug --file=<path>]
        tpq consume <name> [--debug --wait=<seconds>]

    Commands:
        produce  write to queue.
        consume  read from queue.

    Options:
        --file=<path>     The file containing JSON or - for stdin [default: -]
        --wait=<seconds>  Seconds to wait for item [default: -1]
        --debug           Print more output.
    """
    if opt['--debug']:
        LOGGER.setLevel(logging.DEBUG)

    if opt['consume']:
        consume(opt, stdout)

    elif opt['produce']:
        produce(opt, stdin)

    else:
        LOGGER.warning('Nothing to do')


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler(sys.stderr))

    try:
        opt = docopt(main.__doc__)
        opt = Schema({
            '--wait': Use(int),

            object: object,
        }).validate(opt)
    except (SchemaError, DocoptExit) as e:
        print(e)
    else:
        main(opt)
