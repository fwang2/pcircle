#!/usr/bin/env python
from __future__ import print_function

from task import BaseTask
from circle import Circle
from globals import G
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
        if path:
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
    root = os.path.abspath(ARGS.path)
    circle = Circle()
    if ARGS.verbose:
        logging_init(logging.DEBUG)
        circle.set_loglevel(logging.DEBUG)
    else:
        logging_init()

    # create this task
    task = PWalk(circle, root)

    # register the task
    circle.register(task)

    # start
    circle.begin()

    # end
    circle.finalize()

if __name__ == "__main__": main()

