#!/usr/bin/env python
from __future__ import print_function

import sys
import argparse
import signal
import os

from _version import get_versions
from globals import G
from itertools import izip_longest

xchunks = None
ychunks = None
ARGS = None
__version__ = get_versions()['version']
del get_versions

SRC = set()
DEST = set()


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


def next_block(f, sig):
    block_start = False
    for line in f:
        if block_start:
            yield line
        elif line.startswith("----block"):
            block_start = True

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

    for b1, b2 in izip_longest(f1, f2, None):
        if b1:
            b1rel = os.path.relpath(b1, sig1.prefix)
            # SRC.add(b1rel)
        if b2:
            b2rel = os.path.relpath(b2, sig2.prefix)
            # DEST.add(b2rel)

        if b1rel != b2rel and ARGS.break_on_first:
                print("First miss match:")
                print("\t src: %s" % b1)
                print("\t dest: %s" % b2)
                sys.exit(1)


if __name__ == "__main__":
    main()
