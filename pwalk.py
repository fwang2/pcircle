#!/usr/bin/env python
from __future__ import print_function

from task import BaseTask
from circle import Circle
import stat
import os
import os.path
import logging
import argparse

ARGS    = None
logger  = logging.getLogger("pwalk")

def setup_logging(level):
    global logger

    fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
        self.root = path
        self.flist = []  # element is (filepath, filesize, filemode, stats)
        self.reduce_items = 0

    def create(self):
        self.enq(self.root)

    def process_dir(self, dir):
        entries = os.listdir(dir)
        for e in entries:
            self.enq(os.path.abspath(dir + "/" + e))

    def process(self):
        path = self.deq()
        logger.debug("process: %s", path)
        st = os.stat(path)
        self.flist.append( (path, st.st_size, st.st_mode, st) )

        # recurse into directory
        if stat.S_ISDIR(st.st_mode):
            self.process_dir(path)

    def reduce_op(self):
        pass

    def reduce_fini(self):
        pass

def main():

    global ARGS

    ARGS = parse_args()

    if ARGS.verbose:
        setup_logging(logging.DEBUG)
    else:
        setup_logging(logging.INFO)

    abspath = os.path.abspath(ARGS.path)

    # create circle
    circle = Circle()

    # create this task
    task = PWalk(circle, abspath)

    # register the task
    circle.register(task)

    # start
    circle.begin()

    # end
    circle.finalize()

if __name__ == "__main__": main()

