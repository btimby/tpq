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

import tpq


LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())


def consume(opt, stdout):
    """
    Read from queue.
    """
    try:
        # Print to stdout, errors are on stderr
        item = tpq.get(opt['<name>'], wait=opt['--wait'])
        stdout.write('%s\n' % json.dumps(item))
    except tpq.QueueEmpty:
        LOGGER.error('Queue empty')
        sys.exit(1)
    except Exception as e:
        LOGGER.error('Could not read from queue')
        LOGGER.debug(e, exc_info=True)
        sys.exit(1)
    else:
        sys.exit(0)


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
        LOGGER.error('Could not decode data')
        LOGGER.debug(e, exc_info=True)
        sys.exit(1)

    try:
        json.loads(data)
    except ValueError as e:
        LOGGER.error('Could not parse json')
        LOGGER.debug(e, exc_info=True)
        sys.exit(1)

    if opt['--create']:
        tpq.create(opt['<name>'])

    try:
        tpq.put(opt['<name>'], data)
    except Exception as e:
        LOGGER.error('Could not write to queue')
        LOGGER.debug(e, exc_info=True)
        sys.exit(1)
    else:
        sys.exit(0)


def main(opt, stdin=sys.stdin, stdout=sys.stdout):
    """
    tpq - Trivial Postgress Queue

    Usage:
        tpq produce <name> [--debug --file=<path> --create]
        tpq consume <name> [--debug --wait=<seconds>]

    Commands:
        produce  write to queue.
        consume  read from queue.

    Options:
        --file=<path>     The file containing JSON or - for stdin [default: -]
        --wait=<seconds>  Seconds to wait for item [default: -1]
        --create          Attempt to create the queue before writing to it
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
        LOGGER.error(str(e))
        sys.exit(1)
    else:
        main(opt)
