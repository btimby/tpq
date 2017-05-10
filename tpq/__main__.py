from __future__ import absolute_import

import json
import sys
import logging

from docopt import docopt, DocoptExit
from schema import Schema, Use, SchemaError

from tpq import QueueEmpty, get, put


LOGGER = logging.getLogger('')
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(logging.StreamHandler(sys.stderr))


def consume(opt):
    """
    Read from queue.
    """
    try:
        # Print to stdout, errors are on stderr
        print(get(opt['<name>'], wait=opt['--wait']))
        sys.exit(0)
    except QueueEmpty:
        LOGGER.exception('Queue empty', exc_info=True)
        sys.exit(1)


def produce(opt):
    """
    Write to queue.
    """
    if opt['--file'] == '-':
        data = sys.stdin.read()

    else:
        with file(opt['--file'], 'r') as f:
            data = f.read()

    try:
        json.loads(data)
    except ValueError as e:
        LOGGER.exception('JSON data expected in stdin', exc_info=True)
        return

    put(opt['<name>'], data)


def main(opt):
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
        consume(opt)

    elif opt['produce']:
        produce(opt)


if __name__ == '__main__':
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
