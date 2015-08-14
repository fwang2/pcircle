#!/usr/bin/env python
from __future__ import print_function

import sys
import logging
import argparse
import signal
import cPickle as pickle

from _version import get_versions
from utils import bytes_fmt, getLogger
from fdef import ChunkSum
from globals import G

logger = logging.getLogger("fdiff")

xchunks = None
ychunks = None
ARGS = None
__version__ = get_versions()['version']
del get_versions

def sig_handler(signal, frame):
    # catch keyboard, do nothing
    # eprint("\tUser cancelled ... cleaning up")
    sys.exit(1)

def parse_args():
    parser = argparse.ArgumentParser(description="fdiff")
    parser.add_argument("-v", "--version", action="version", version="{version}".format(version=__version__))
    parser.add_argument("--loglevel", default="ERROR", help="log level")
    parser.add_argument("file1", type=argparse.FileType('r'), help="src checksum file")
    parser.add_argument("file2", type=argparse.FileType('r'), help="dest checksum file")

    return parser.parse_args()

def load_chunks(f):
    try:
        return set(pickle.load(f))
    except Exception as e:
        logger.error(e)
        sys.exit(1)

def main():

    global ARGS, logger, xchunks, ychunks
    signal.signal(signal.SIGINT, sig_handler)
    ARGS = parse_args()
    G.loglevel = ARGS.loglevel

    xchunks = load_chunks(ARGS.file1)
    ychunks = load_chunks(ARGS.file2)

    diffset = xchunks - ychunks
    fileset = set()

    if len(diffset) == 0:
        print("No difference found")
    else:
        for piece in diffset:
            fileset.add(piece.filename)
        print("File difference found:\n")

        for fn in fileset:
            print("\t%s" % fn)

        print("\n")

if __name__ == "__main__": main()
