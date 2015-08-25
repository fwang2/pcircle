#!/usr/bin/env python
from __future__ import print_function

import sys
import argparse
import signal
import os

from _version import get_versions
from globals import G
from itertools import izip_longest
from fdef import ChunkSum
__version__ = get_versions()['version']

ARGS = None
del get_versions

MISSING = []


class Signature(object):
    def __init__(self):
        self.sha1 = None
        self.prefix = None


def sig_handler():
    # catch keyboard, do nothing
    print("User cancelled ... cleaning up")
    sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(description="fdiff")
    parser.add_argument("-v", "--version", action="version", version="{version}".format(version=__version__))
    parser.add_argument("--loglevel", default="ERROR", help="log level")
    parser.add_argument("--break-on-first", action="store_true", help="break out")
    parser.add_argument("src", type=argparse.FileType('r'), help="src signature file")
    parser.add_argument("dest", type=argparse.FileType('r'), help="dest signature file")

    return parser.parse_args()


def next_block(f):
    for line in f:
        yield line

    # block_start = False
    # for line in f:
    #     if block_start:
    #         yield line
    #     elif line.startswith("----block"):
    #         block_start = True

def check_signature_file(f):
    sig = Signature()
    block_checksum = False
    for line in f:
        if line.startswith("sha1"):
            sig.sha1 = line.split(":")[1].strip()
        elif line.startswith("src"):
            sig.prefix = line.split(":")[1].strip()
        elif line.startswith("----block"):
            block_checksum = True
            break
    if sig.prefix and sig.sha1 and block_checksum:
        return sig
    else:
        print("Error: [%s] is not a valid signature file." % f.name)
        sys.exit(1)


def print_src_dest():
    src_dest = SRC - DEST
    print("Present in src, not in destination")


def gen_chunksum(b, sig):
    b = os.path.relpath(b, sig.prefix)
    try:
        fn, offset, length, digest = b.split("!@")
    except ValueError as e:
        print("Parsing error: %s" % b)
        sys.exit(1)

    c = ChunkSum(fn)
    c.offset = int(offset)
    c.length = int(length)
    c.digest = digest.strip()
    return c


def main():
    global ARGS, SRC, DEST
    signal.signal(signal.SIGINT, sig_handler)
    ARGS = parse_args()
    G.loglevel = ARGS.loglevel

    sig1 = check_signature_file(ARGS.src)
    sig2 = check_signature_file(ARGS.dest)

    if sig1.sha1 == sig2.sha1:
        print("Signature match.")
        exit(0)

    print("Signature differ, proceed to comparison")

    f1 = next_block(ARGS.src)
    f2 = next_block(ARGS.dest)
    for b1, b2 in izip_longest(f1, f2, fillvalue=None):
        if not b1:
            break
        if not b2:
            # b1 still got blocks, we list them as diff
            c1 = gen_chunksum(b1, sig1)
            print("missing blocks: [%r]" % c1)
            continue

        c1 = gen_chunksum(b1, sig1)
        c2 = gen_chunksum(b2, sig2)
        if c1 != c2:
            print("block differ: [%r] -> [%r]" % (c1, c2))
            if ARGS.break_on_first:
                sys.exit(1)

if __name__ == "__main__":
    main()
