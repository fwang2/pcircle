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
from collections import Counter

ARGS    = None
logger  = logging.getLogger("pcp")

def parse_args():
    parser = argparse.ArgumentParser(description="A MPI-based Parallel Copy Tool")
    parser.add_argument("--loglevel", default="ERROR", help="log level")
    parser.add_argument("--chunksize", type=int, default=1048576 )
    parser.add_argument("-", "--interval", type=int, default=10, help="interval")
    parser.add_argument("-c", "--checksum", action="store_true", help="verify")
    parser.add_argument("-f", "--force", action="store_true", default=False, help="force unlink")
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
        self.rfd_cache = {}
        self.wfd_cache = {}

        self.cnt_dirs = 0
        self.cnt_files = 0
        self.cnt_filesize = 0

        self.blocksize = 1024*1024
        self.chunksize = 1024*1024

        # reduce
        self.reduce_items = 0
        self.buf = [0] * 3
        self.buf[0] = G.MSG_VALID

        # debug
        self.d = {"rank": "rank %s" % circle.rank}
        self.wtime_started = MPI.Wtime()
        self.wtime_ended = None

        # fini_check
        self.fini_cnt = Counter()



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

        d['dest'] = self.dest + "/" + self.srcbase + "/" + os.path.relpath(f[0], start=self.src)

        workcnt = 0

        if f[2] == 0:
            # empty file
            d['off_start'] = 0
            d['length'] = 0
            self.enq(d)
            logger.debug("%s" % d, extra=self.d)
        else:
            for i in range(chunks):
                d['off_start'] = i * self.chunksize
                d['length'] = self.chunksize
                self.enq(d)
                logger.debug("%s" % d, extra=self.d)
            workcnt += chunks
        if remaining > 0:
            # send remainder
            d['off_start'] = chunks * self.chunksize
            d['length' ] = remaining
            self.enq(d)
            logger.debug("%s" % d, extra=self.d)
            workcnt += 1


        # make finish token for this file

        t = {'cmd' : 'fini_check', 'workcnt' : workcnt, 'src' : f[0], 'dest': d['dest'] }
        self.enq(t)


    def create(self):
        # construct and enable all copy operations
        for f in self.treewalk.flist:
            if stat.S_ISREG(f[1]):
                self.enq_file(f)

    def abort(self, code):
        self.circle.abort()
        exit(code)

    def do_copy(self, work):
        src = work['src']
        dest = work['dest']
        rfd = None
        wfd = None


        if src in self.rfd_cache:
            rfd = self.rfd_cache[src]

        if dest in self.wfd_cache:
            wfd = self.wfd_cache[dest]

        basedir = os.path.dirname(dest)
        if not os.path.exists(basedir):
            os.mkdir(basedir)

        if not rfd:
            rfd = os.open(src, os.O_RDONLY)
            self.rfd_cache[src] = rfd

        if not wfd:
            wfd = os.open(dest, os.O_WRONLY|os.O_CREAT)
            if wfd < 0:
                if ARGS.force:
                    os.unlink(dest)
                    wfd = os.open(dest, os.O_WRONLY)
                else:
                    logger.error("Failed to create output file %s" % dest, extra=self.d)
                    return

            self.wfd_cache[dest] = wfd

        # do the actual copy
        self.write_bytes(rfd, wfd, work)

        self.fini_cnt[src] += 1
        logger.debug("Inc workcnt for %s (workcnt=%s)" % (src, self.fini_cnt[src]), extra=self.d)

    def do_fini_check(self, work):
        src = work['src']
        dest = work['dest']
        workcnt = work['workcnt']

        mycnt = self.fini_cnt[src]
        if workcnt == mycnt:
            # all job finished
            # we should have cached this before
            rfd = self.rfd_cache[src]
            wfd = self.wfd_cache[dest]
            os.close(rfd)
            os.close(wfd)
            logger.debug("Finish done: %s" % src, extra=self.d)
        elif mycnt < workcnt:
            # worked some, but not yet done
            work['workcnt'] -= mycnt
            self.enq(work)
            logger.debug("Finish enq: %s (workcnt=%s) " % (src, work['workcnt']), extra=self.d)
        # either way, clear the cnt
        del self.fini_cnt[src]



    def process(self):
        work = self.deq()
        if work['cmd'] == 'copy':
            self.do_copy(work)
        elif work['cmd'] == 'fini_check':
            self.do_fini_check(work)
        else:
            logger.error("Unknown command %s" % work['cmd'], extra=self.d)
            self.abort()

    def reduce_init(self):
        pass

    def reduce(self, buf1, buf2):
        pass

    def reduce_finish(self, buf):
        pass

    def epilogue(self):
        pass


    def write_bytes(self, rfd, wfd, work):
        os.lseek(rfd, work['off_start'], os.SEEK_SET)
        os.lseek(wfd, work['off_start'], os.SEEK_SET)

        buf = os.read(rfd, work['length'])
        os.write(wfd, buf)

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
    src = os.path.abspath(ARGS.src)
    dest = os.path.abspath(ARGS.dest)
    treewalk = PWalk(circle, src, dest)
    treewalk.set_loglevel(ARGS.loglevel)
    circle.begin(treewalk)
    circle.finalize()

    # second task
    pcp = PCP(circle, treewalk, src, dest)
    pcp.chunksize = ARGS.chunksize
    circle.begin(pcp)
    circle.finalize()

    pcp.wtime_ended = MPI.Wtime()

    pcp.epilogue()

if __name__ == "__main__": main()

