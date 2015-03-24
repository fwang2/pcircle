#!/usr/bin/env python
from __future__ import print_function

from task import BaseTask
from circle import Circle
from globals import G
from mpi4py import MPI
from utils import logging_init, bytes_fmt,destpath
import stat
import os
import os.path
import logging
import argparse
import xattr
from _version import get_versions

ARGS    = None
logger  = logging.getLogger("pwalk")
__version__ = get_versions()['version']

def parse_args():
    parser = argparse.ArgumentParser(description="pwalk")
    parser.add_argument("-v", "--version", action="version", version="{version}".format(version=__version__))
    parser.add_argument("--loglevel", default="ERROR", help="log level")
    parser.add_argument("path", default=".", help="path")
    parser.add_argument("-i", "--interval", type=int, default=10, help="interval")

    return parser.parse_args()

class FWalk(BaseTask):
    def __init__(self, circle, src, dest=None, preserve=False, force=False):
        BaseTask.__init__(self, circle)
        self.circle = circle
        self.src = src
        self.dest = dest
        self.preserve = preserve
        self.force = force
        self.flist = []  # element is (filepath, filemode, filesize)
        self.src_flist = self.flist

        self.cnt_dirs = 0
        self.cnt_files = 0
        self.cnt_filesize = 0

        # reduce
        self.reduce_items = 0

        # debug
        self.d = {"rank": "rank %s" % circle.rank}

        self.time_started = MPI.Wtime()
        self.time_ended = None

    def create(self):
        if self.circle.rank == 0:
            self.enq(self.src)
            print("Start analyzing ...")

    def copy_xattr(self, src, dest):
        attrs = xattr.listxattr(src)
        for k in attrs:
            val = xattr.getxattr(src, k)
            xattr.setxattr(dest, k, val)

    def process_dir(self, i_dir):
        """
            i_dir should be absolute path
        """
        if self.dest:
            # we create destination directory
            o_dir = destpath(self.src, self.dest, i_dir)
            os.mkdir(o_dir, stat.S_IRWXU)
            if self.preserve:
                self.copy_xattr(i_dir, o_dir)

        entries = None
        try:
            entries = os.listdir(i_dir)
        except OSError as err:
            logger.error("access error %s, skipping ..." % i_dir, extra=self.d)
            return False
        for entry in entries:
            self.enq(i_dir + "/" + entry) # conv to absolute path
        return True

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
                if self.preserve:
                    o_file = destpath(self.src, self.dest, path)
                    try:
                        os.mknod(o_file, stat.S_IRWXU | stat.S_IFREG)
                    except OSError as e:
                        logger.warn("failed to mknod() for %s", o_file, extra=self.d)
                    else:
                        self.copy_xattr(path, o_file)
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
        self.time_ended = MPI.Wtime()

        if self.circle.rank == 0:

            print("")
            print("Directory count: %s" % total_dirs)
            print("File count: %s" % total_files)
            print("File size: %s" % bytes_fmt(total_filesize))
            print("Tree talk time: %.2f seconds" % (self.time_ended - self.time_started))
            print("")
        return total_filesize

    def set_loglevel(self, loglevel):
        global logger
        logger = logging_init(logger, loglevel)

def main():

    global ARGS, logger
    ARGS = parse_args()
    root = os.path.abspath(ARGS.path)
    circle = Circle(reduce_interval = ARGS.interval)
    logger = logging_init(logger, ARGS.loglevel)

    task = FWalk(circle, root)
    circle.begin(task)
    circle.finalize()
    task.epilogue()

if __name__ == "__main__": main()

