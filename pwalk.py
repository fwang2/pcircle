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

ARGS    = None
logger  = logging.getLogger("pwalk")

def logging_init(loglevel, circle):
    global logger

    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: %s" % loglevel)

    logger.setLevel(level=numeric_level)
    circle.set_loglevel(level=numeric_level)

    fmt = logging.Formatter(G.simple_fmt)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    circle.set_loglevel(numeric_level)
def parse_args():
    parser = argparse.ArgumentParser(description="pwalk")
    parser.add_argument("--loglevel", default="ERROR", help="log level")
    parser.add_argument("-p", "--path", default=".", help="path")

    return parser.parse_args()

class PWalk(BaseTask):
    def __init__(self, circle, path):
        BaseTask.__init__(self, circle)
        self.circle = circle
        self.root = path
        self.flist = []  # element is (filepath, filemode, filesize)

        self.cnt_dirs = 0
        self.cnt_files = 0
        self.cnt_filesize = 0

        # reduce
        self.reduce_items = 0
        self.buf = [0] * 3
        self.buf[0] = G.MSG_VALID

        # debug
        self.d = {"rank": "rank %s" % circle.rank}


    def create(self):
        self.enq(self.root)

    def process_dir(self, dir):

        entries = os.listdir(dir)
        for e in entries:
            self.enq(os.path.abspath(dir + "/" + e))

    def process(self):

        path = self.deq()
        logger.debug("process: %s" %  path, extra=self.d)

        if path:
            self.reduce_items += 1
            st = os.stat(path)
            self.flist.append( (path, st.st_mode, st.st_size ))

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
        self.circle.reduce(self.buf)

    def reduce(self, buf1, buf2):
        self.buf[1] = buf1[1] + buf2[1]
        self.buf[2] = buf2[2] + buf2[2]
        self.circle.reduce(self.buf)

    def reduce_finish(self, buf):
        # get result of reduction
        pass

def main():

    global ARGS
    ARGS = parse_args()
    root = os.path.abspath(ARGS.path)
    circle = Circle(reduce_interval=5)

    logging_init(ARGS.loglevel, circle)

    # create this task
    task = PWalk(circle, root)
    # start
    circle.begin(task)

    # end
    circle.finalize()

    # summarize results
    task.summarize()

    total_dirs = circle.comm.reduce(task.cnt_dirs, op=MPI.SUM)
    total_files = circle.comm.reduce(task.cnt_files, op=MPI.SUM)
    total_filesize = circle.comm.reduce(task.cnt_filesize, op=MPI.SUM)

    if circle.rank == 0:
        print("\tDirectory count: %s" % total_dirs)
        print("\tFile count: %s" % total_files)
        print("\tFile size: %s bytes" % total_filesize)

if __name__ == "__main__": main()

