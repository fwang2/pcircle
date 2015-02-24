#!/usr/bin/env python
from __future__ import print_function

from task import BaseTask
from circle import Circle
from globals import G
from mpi4py import MPI
from utils import logging_init, bytes_fmt, hprint
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
        self.reduce_items = 0

        # debug
        self.d = {"rank": "rank %s" % circle.rank}

    def create(self):
        if self.circle.rank == 0:
            self.enq(self.src)
            hprint("Starting tree walk ...")

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

            if stat.S_ISREG(st.st_mode):
                self.cnt_files += 1
                self.cnt_filesize += st.st_size
            elif stat.S_ISDIR(st.st_mode):
                self.cnt_dirs += 1
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

    def reduce_init(self, buf):
        buf['cnt_files'] = self.cnt_files
        buf['cnt_dirs'] = self.cnt_dirs
        buf['cnt_filesize'] = self.cnt_filesize



    def reduce(self, buf1, buf2):
        buf1['cnt_dirs'] += buf2['cnt_dirs']
        buf1['cnt_files'] += buf2['cnt_files']
        buf1['cnt_filesize'] += buf2['cnt_filesize']

        return buf1

    def reduce_report(self, buf):
        print("Processed files: %s " % buf['cnt_files'])


    def reduce_finish(self, buf):
        # get result of reduction
        pass

    def total_tally(self):
        # self.summarize()
        total_dirs = self.circle.comm.reduce(self.cnt_dirs, op=MPI.SUM)
        total_files = self.circle.comm.reduce(self.cnt_files, op=MPI.SUM)
        total_filesize = self.circle.comm.reduce(self.cnt_filesize, op=MPI.SUM)
        return total_dirs, total_files, total_filesize

    def epilogue(self):
        total_dirs, total_files, total_filesize = self.total_tally()

        if self.circle.rank == 0:
            print("")
            print("Directory count: %s" % total_dirs)
            print("File count: %s" % total_files)
            print("File size: %s" % bytes_fmt(total_filesize))
            print("")
        return total_filesize

    def set_loglevel(self, loglevel):
        global logger
        logger = logging_init(logger, loglevel)

def main():

    global ARGS, logger
    ARGS = parse_args()
    #root = os.path.abspath(ARGS.path)
    root = os.path.abspath(ARGS.path)
    circle = Circle(reduce_interval = ARGS.interval)
    logger = logging_init(logger, ARGS.loglevel)

    task = PWalk(circle, root)
    circle.begin(task)
    circle.finalize()
    task.epilogue()

if __name__ == "__main__": main()

