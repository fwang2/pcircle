#!/usr/bin/env python
from __future__ import print_function
from mpi4py import MPI

from task import BaseTask
from circle import Circle
from stat import *
import os
import os.path
import logging
import argparse

ARGS    = None
logger  = logging.getLogger("app.pwalk")

def parse_args():
    parser = argparse.ArgumentParser(description="pwalk")
    parser.add_argument("-v", "--verbose", action="store_true", help="debug output")
    parser.add_argument("-p", "--path", default=".", help="path")

    return parser.parse_args()

class PWalk(BaseTask):
    def __init__(self, circle, path):
        BaseTask.__init__(self, circle)
        self.root = path
        self.flist = []  # element is (filepath, filemode, stats)

    def create(self):
        self.enq(self.root)

    def process_dir(self, dir):
        entries = os.listdir(dir)
        for e in entries:
            self.enq(os.path.abspath(e))

    def process(self):
        path = self.deq()
        st = os.stat(path)
        self.flish.append( (path, st.st_mode, st) )

        # recurse into directory
        if S_ISDIR(st.st_mode):
            self.process_dir(path)


    def reduce_init(self):
        pass

    def reduce_op(self):
        pass

    def reduce_fini(self):
        pass

def main():

    global ARGS
    ARGS = parse_args()
    abspath = os.path.abspath(ARGS.path)

    # create circle
    circle = Circle()

    # create this task
    task = PWalk(circle, path)

    # register the task
    circle.register(task)



if __name__ == "__main__": main()

