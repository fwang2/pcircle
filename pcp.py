#!/usr/bin/env python
from __future__ import print_function

from task import BaseTask
from circle import Circle
from globals import G
from mpi4py import MPI
import stat
import os
import os.path
import logging
import argparse
import utils
from pwalk import PWalk
import sys

ARGS    = None
logger  = logging.getLogger("pcp")

def parse_args():
    parser = argparse.ArgumentParser(description="A MPI-based Parallel Copy Tool")
    parser.add_argument("--loglevel", default="ERROR", help="log level")
    parser.add_argument("-", "--interval", type=int, default=10, help="interval")
    parser.add_argument("-c", "--checksum", action="store_true", help="verify")
    parser.add_argument("src", help="copy from")
    parser.add_argument("dest", help="copy to")

    return parser.parse_args()


class PCP(BaseTask):
    def __init__(self, circle, treewalk, src, dest):
        BaseTask.__init__(self, circle)
        self.circle = circle
        self.treewalk = treewalk
        self.src = os.path.abspath(src)
        self.srcbase = os.path.basename(src)
        self.dest = os.path.abspath(dest)

        # cache
        fd_cache = {}

        self.cnt_dirs = 0
        self.cnt_files = 0
        self.cnt_filesize = 0

        self.blocksize = 2
        self.chunksize = 2

        # reduce
        self.reduce_items = 0
        self.buf = [0] * 3
        self.buf[0] = G.MSG_VALID

        # debug
        self.d = {"rank": "rank %s" % circle.rank}
        self.wtime_started = MPI.Wtime()
        self.wtime_ended = None


    def enq_file(self, f):
        """
        f (path mode size) - we enq all in one shot
        CMD = copy src  dest  off_start  last_chunk
        """
        chunks    = f[2] / self.chunksize
        remaining = f[2] % self.chunksize

        d = {}
        d['cmd'] = 'copy'
        d['src'] = f[0]
        d['dest'] = os.path.abspath(self.dest + self.srcbase + "/" + f[0])

        if f[2] == 0:
            # empty file
            d['off_start'] = 0
            d['length'] = 0
            self.enq(d)
            logger.debug("%s" % d, extra=self.d)

        for i in range(chunks):
            d['off_start'] = i * self.chunksize
            d['length'] = self.chunksize
            self.enq(d)
            logger.debug("%s" % d, extra=self.d)

        if remaining > 0:
            # send remainder
            d['off_start'] = chunks * self.chunksize
            d['length' ] = remaining
            self.enq(d)
            logger.debug("%s" % d, extra=self.d)


    def create(self):
        # construct and enable all copy operations
        for f in self.treewalk.flist:
            if stat.S_ISREG(f[1]):
                self.enq_file(f)

    def abort(self, code):
        self.circle.abort()
        exit(code)

    def do_copy(self, work):
        logger.debug("copy: %s" % work, extra=self.d)

    def process(self):
        work = self.deq()
        if work['cmd'] == 'copy':
            self.do_copy(work)
        else:
            logger.error("Unknown command %s" % work['cmd'], extra=self.d)
            self.abort()

    def reduce_init(self):
        pass

    def reduce(self, buf1, buf2):
        pass

    def reduce_finish(self, buf):
        pass

    def epilogue():
        pass

def verify_path(src, dest):

    if not os.path.exists(src) or not os.access(src, os.R_OK):
        print("source directory %s is not readable" % src)
        sys.exit(1)

    if not os.path.exists(dest):
        try:
            os.mkdir(dest)
        except:
            print("Error: failed to create %s" % dest)
            sys.exit(1)
    else:
        if not os.access(dest, os.W_OK):
            print("Error: destination %s is not writable" % dest)
            sys.exit(1)


def main():

    global ARGS, logger
    ARGS = parse_args()

    circle = Circle(reduce_interval=5)
    circle.setLevel(logging.ERROR)
    logger = utils.logging_init(logger, ARGS.loglevel)
    if circle.rank == 0: verify_path(ARGS.src, ARGS.dest)

    # first task
    treewalk = PWalk(circle, ARGS.src, ARGS.dest)
    treewalk.set_loglevel(ARGS.loglevel)
    circle.begin(treewalk)
    circle.finalize()

    # second task
    print(treewalk.flist)
    pcp = PCP(circle, treewalk, ARGS.src, ARGS.dest)
    circle.begin(pcp)
    circle.finalize()

    pcp.wtime_ended = MPI.Wtime()


if __name__ == "__main__": main()

