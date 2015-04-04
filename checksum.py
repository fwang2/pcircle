#!/usr/bin/env python
#
# author: Feiyi Wang
#
# This program provides a MPI-based parallization for checksumming
#
#
from __future__ import print_function
import os
import logging
import hashlib
import argparse
import stat
import sys
import signal
import cPickle as pickle

from mpi4py import MPI
from cStringIO import StringIO

from circle import Circle
from _version import get_versions
from task import BaseTask
from utils import logging_init, bytes_fmt
from fwalk import FWalk
from cio import readn, writen

logger = logging.getLogger("checksum")

# 512 MiB = 536870912
# 128 MiB = 134217728
# 4 MiB = 4194304
CHUNKSIZE = 536870912
BLOCKSIZE = 4194304

ARGS    = None
__version__ = get_versions()['version']
del get_versions

def sig_handler(signal, frame):
    # catch keyboard, do nothing
    # eprint("\tUser cancelled ... cleaning up")
    sys.exit(1)

def parse_args():
    parser = argparse.ArgumentParser(description="fchecksum")
    parser.add_argument("-v", "--version", action="version", version="{version}".format(version=__version__))
    parser.add_argument("--loglevel", default="ERROR", help="log level")
    parser.add_argument("path", default=".", help="path")
    parser.add_argument("-i", "--interval", type=int, default=10, help="interval")
    parser.add_argument("-o", "--output", default="sha1.sig", help="sha1 output file")

    return parser.parse_args()

class Chunk:
    def __init__(self, filename, off_start=0, length=0):
        self.filename = filename
        self.off_start = off_start
        self.length = length
        self.digest = None

    # FIXME: this is not Python 3 compatible
    def __cmp__(self, other):
        assert isinstance(other, Chunk)
        return cmp((self.filename, self.off_start),
                   (other.filename, other.off_start))


class Checksum(BaseTask):
    def __init__(self, circle, treewalk, totalsize=0):
        global logger
        BaseTask.__init__(self, circle)
        self.circle = circle
        self.treewalk = treewalk
        self.totalsize = totalsize
        self.workcnt = 0
        self.chunkq = []

        # debug
        self.d = {"rank": "rank %s" % circle.rank}
        self.wtime_started = MPI.Wtime()
        self.wtime_ended = None

        # reduce
        self.vsize = 0

        if self.circle.rank == 0:
            print("Start parallel checksumming ...")


    def create(self):

        #if self.workq:  # restart
        #    self.setq(self.workq)
        #    return

        for f in self.treewalk.flist:
            if stat.S_ISREG(f[1]):
                self.enq_file(f)

        # right after this, we do first checkpoint

        #if self.checkpoint_file:
        #    self.do_no_interrupt_checkpoint()
        #    self.checkpoint_last = MPI.Wtime()

    def enq_file(self, f):
        '''
        f[0] path f[1] mode f[2] size - we enq all in one shot
        CMD = copy src  dest  off_start  last_chunk
        '''
        chunks    = f[2] / CHUNKSIZE
        remaining = f[2] % CHUNKSIZE


        workcnt = 0

        if f[2] == 0: # empty file
            ck = Chunk(f[0])
            ck.off_start = 0
            ck.length = 0
            self.enq(ck)
            logger.debug("%s" % ck, extra=self.d)
            workcnt += 1
        else:
            for i in range(chunks):
                ck = Chunk(f[0])
                ck.off_start = i * CHUNKSIZE
                ck.length = CHUNKSIZE
                self.enq(ck)
                logger.debug("%s" % ck, extra=self.d)
            workcnt += chunks

        if remaining > 0:
            # send remainder
            ck = Chunk(f[0])
            ck.off_start = chunks * CHUNKSIZE
            ck.length  = remaining
            self.enq(ck)
            logger.debug("%s" % ck, extra=self.d)
            workcnt += 1

        # tally work cnt
        self.workcnt += workcnt



    def process(self):
        ck = self.deq()
        logger.debug("process: %s" % ck, extra = self.d)
        blocks = ck.length / BLOCKSIZE
        remaining = ck.length % BLOCKSIZE

        chunk_digests = StringIO()

        fd = os.open(ck.filename, os.O_RDONLY)
        os.lseek(fd, ck.off_start, os.SEEK_SET)

        for i in range(blocks):
            chunk_digests.write(hashlib.sha1(readn(fd, BLOCKSIZE)).hexdigest())

        if remaining > 0:
            chunk_digests.write(hashlib.sha1(readn(fd, remaining)).hexdigest())

        ck.digest = chunk_digests.getvalue()

        self.chunkq.append(ck)

        self.vsize += ck.length
        os.close(fd)

    def setLevel(self, level):
        global logger
        logging_init(logger, level)


    def reduce_init(self, buf):
        buf['vsize'] = self.vsize

    def reduce_report(self, buf):
        out = ""
        if self.totalsize != 0:
            out += "%.2f %% block checksummed, " % (100 * float(buf['vsize'])/self.totalsize)

        out += "%s bytes done" % bytes_fmt(buf['vsize'])
        print(out)

    def reduce_finish(self, buf):
        #self.reduce_report(buf)
        pass

    def reduce(self, buf1, buf2):
        buf1['vsize'] += buf2['vsize']
        return buf1

    def epilogue(self):
        self.wtime_ended = MPI.Wtime()
        if self.circle.rank == 0:
            print("")
            if self.totalsize == 0: return
            time = self.wtime_ended - self.wtime_started
            rate = float(self.totalsize)/time
            print("Checksumming Completed In: %.2f seconds" % (time))
            print("Average Rate: %s/s\n" % bytes_fmt(rate))

def read_in_blocks(chunks, chunksize=26214):
    idx = 0
    total = len(chunks)
    buf = StringIO()

    while True:
        if idx == total:
            break
        buf.write(chunks[idx].digest)
        idx += 1
        if idx % chunksize == 0 or idx == total:
            yield hashlib.sha1(buf.getvalue()).hexdigest()
            buf = StringIO()

def do_checksum(chunks):

    buf = StringIO()
    for block in read_in_blocks(chunks):
        buf.write(block)

    return hashlib.sha1(buf.getvalue()).hexdigest()

def export_checksum(chunks):
    fullpath = os.path.abspath(ARGS.output)
    ex_base = os.path.basename(fullpath).split(".")[0] + ".checksums"
    ex_dir = os.path.dirname(fullpath)
    ex_path = os.path.join(ex_dir, ex_base)
    with open(ex_path, "wb") as f:
        pickle.dump(chunks, f, pickle.HIGHEST_PROTOCOL)

    return os.path.basename(ex_path)

def main():

    global ARGS, logger
    signal.signal(signal.SIGINT, sig_handler)

    ARGS = parse_args()
    root = os.path.abspath(ARGS.path)
    circle = Circle(reduce_interval = ARGS.interval)
    logger = logging_init(logger, ARGS.loglevel)

    fwalk = FWalk(circle, root)
    circle.begin(fwalk)
    circle.finalize()
    totalsize = fwalk.epilogue()

    fcheck = Checksum(circle, fwalk, totalsize)
    fcheck.setLevel(ARGS.loglevel)
    circle.begin(fcheck)
    circle.finalize()

    if circle.rank == 0:
        sys.stdout.write("\nAggregating ... ")

    chunkl = circle.comm.gather(fcheck.chunkq)

    if circle.rank == 0:
        chunks = [ item for sublist in chunkl for item in sublist]
        chunks.sort()
        sys.stdout.write("%s chunks\n" % len(chunks))
        sha1val = do_checksum(chunks)
        with open(ARGS.output, "w") as f:
            f.write(sha1val + "\n")

        print("\nSHA1: %s" % sha1val)
        print("Exporting singular signature to [%s]" % ARGS.output)
        print("Exporting block signatures to [%s] \n" % export_checksum(chunks))


    fcheck.epilogue()

if __name__ == "__main__": main()