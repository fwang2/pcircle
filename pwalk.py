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

def logging_init(level=logging.INFO):
    global logger
    fmt = logging.Formatter(G.simple_fmt)
    logger.setLevel(level)
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

def parse_args():
    parser = argparse.ArgumentParser(description="pwalk")
    parser.add_argument("-v", "--verbose", action="store_true", help="debug output")
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
        buf = [0] * 3
        self.circle.reduce(buf)

    def reduce(self, buf1, buf2):

        buf = [0] * 3
        buf[1] = buf1[1] + buf2[1]
        buf[2] = buf1[2] + buf2[2]

        self.circle.reduce(buf)

    def reduce_finish(self, buf):
        # get result of reduction
        print("Items walked %s" % buf)

def main():

    global ARGS
    ARGS = parse_args()
    root = os.path.abspath(ARGS.path)
    circle = Circle()
    if ARGS.verbose:
        logging_init(logging.DEBUG)
        circle.set_loglevel(logging.DEBUG)
    else:
        logging_init()

    # create this task
    task = PWalk(circle, root)
    if circle.rank == 0:
        print("Calculating work:"),
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

