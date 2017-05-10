import json, sys, logging

from docopt import docopt, DocoptExit
from schema import Schema, Use, SchemaError

from . import Queue, get, put


LOGGER = logging.getLogger('')
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(logging.StreamHandler())


def consume(opt):
    """
    Read from queue.
    """
    LOGGER.info(get(opt['<name>']))


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
        tpq produce <name> [--file=<path> --debug]
        tpq consume <name> [--debug]

    Commands:
        produce  write to queue.
        consume  read from queue.

    Options:
        --file=<path>  The file containing JSON or - for stdin [default: -]
        --debug        Print more output.
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
    except (SchemaError, DocoptExit) as e:
        print(e)
    else:
        main(opt)
