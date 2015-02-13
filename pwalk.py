#!/usr/bin/env python
from __future__ import print_function

from task import BaseTask
from circle import Circle
from globals import G
from mpi4py import MPI
from utils import logging_init
import stat
import os
import os.path
import logging
import argparse

ARGS    = None
logger  = logging.getLogger("pwalk")

def parse_args():
    parser = argparse.ArgumentParser(description="pwalk")
    parser.add_argument("--loglevel", default="ERROR", help="log level")
    parser.add_argument("path", default=".", help="path")
    parser.add_argument("-i", "--interval", type=int, default=10, help="interval")

    return parser.parse_args()

class PWalk(BaseTask):
    def __init__(self, circle, src, dest=None):
        BaseTask.__init__(self, circle)
        self.circle = circle
        self.src = src
        self.dest = dest

        self.flist = []  # element is (filepath, filemode, filesize)
        self.src_flist = self.flist

        self.cnt_dirs = 0
        self.cnt_files = 0
        self.cnt_filesize = 0

        # reduce
        self.buf = [0] * 3
        self.buf[0] = G.MSG_VALID
        self.reduce_items = 0

        # debug
        self.d = {"rank": "rank %s" % circle.rank}

    def create(self):
        self.enq(self.src)

    def process_dir(self, dir):

        entries = os.listdir(dir)
        for e in entries:
            self.enq(dir + "/" + e)

    def process(self):

        path = self.deq()
        logger.debug("process: %s" %  path, extra=self.d)

        if path:
            st = os.stat(path)
            self.flist.append( (path, st.st_mode, st.st_size ))
            self.reduce_items += 1
            # recurse into directory
            if stat.S_ISDIR(st.st_mode):
                self.process_dir(path)

    def tally(self, t):
        """ t is a tuple element of flist """
        if stat.S_ISDIR(t[1]):
            self.cnt_dirs += 1
        elif stat.S_ISREG(t[1]):
            self.cnt_files += 1
            self.cnt_filesize += t[2]

    def summarize(self):
        map(self.tally, self.flist)

    def reduce_init(self):
        self.circle.reduce(self.reduce_items)

    def reduce(self, buf1, buf2):
        self.circle.reduce(buf1[1] + buf2[1])

    def reduce_finish(self, buf):
        # get result of reduction
        pass

    def total_tally(self):
        self.summarize()
        total_dirs = self.circle.comm.reduce(self.cnt_dirs, op=MPI.SUM)
        total_files = self.circle.comm.reduce(self.cnt_files, op=MPI.SUM)
        total_filesize = self.circle.comm.reduce(self.cnt_filesize, op=MPI.SUM)
        return total_dirs, total_files, total_filesize

    def set_loglevel(self, loglevel):
        global logger
        logger = logging_init(logger, loglevel)

def main():

    global ARGS, logger
    ARGS = parse_args()
    #root = os.path.abspath(ARGS.path)
    root = os.path.abspath(ARGS.path)
    circle = Circle(reduce_interval=ARGS.interval)
    logger = logging_init(logger, ARGS.loglevel)

    task = PWalk(circle, root)
    circle.begin(task)
    circle.finalize()
    total_dirs, total_files, total_filesize = task.total_tally()

    if circle.rank == 0:
        print("\tDirectory count: %s" % total_dirs)
        print("\tFile count: %s" % total_files)
        print("\tFile size: %s bytes" % total_filesize)

if __name__ == "__main__": main()

