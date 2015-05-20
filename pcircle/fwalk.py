#!/usr/bin/env python
from __future__ import print_function

from task import BaseTask
from circle import Circle
from globals import G
from mpi4py import MPI
from utils import getLogger, bytes_fmt,destpath
from dbstore import DbStore
from fdef import FileItem
from scandir import scandir

import stat
import os
import os.path
import logging
import argparse
import filecmp

import xattr
from _version import get_versions

ARGS = None
logger = None
__version__ = get_versions()['version']

def parse_args():
    parser = argparse.ArgumentParser(description="fwalk")
    parser.add_argument("-v", "--version", action="version", version="{version}".format(version=__version__))
    parser.add_argument("--loglevel", default="ERROR", help="log level")
    parser.add_argument("path", default=".", help="path")
    parser.add_argument("-i", "--interval", type=int, default=10, help="interval")
    parser.add_argument("--use-store", action="store_true", help="Use persistent store")
    return parser.parse_args()

class FWalk(BaseTask):
    def __init__(self, circle, src, dest=None, preserve=False, force=False):
        BaseTask.__init__(self, circle)
        self.circle = circle
        self.src = src
        self.dest = dest
        self.preserve = preserve
        self.force = force

        self.workdir = os.getcwd()
        self.tempdir = os.path.join(self.workdir, ".pcircle")
        if not os.path.exists(self.tempdir):
            os.mkdir(self.tempdir)

        if G.use_store:
            self.dbname = "%s/fwalk.%s" % (self.tempdir, circle.rank)
            self.flist = DbStore(self.dbname)
            self.flist_buf = []
        else:
            self.flist = [] # element is (filepath, filemode, filesize, uid, gid)
        self.src_flist = self.flist

        self.cnt_dirs = 0
        self.cnt_files = 0
        self.cnt_filesize = 0
        self.last_cnt = 0
        self.last_reduce_time = MPI.Wtime()

        # reduce
        self.reduce_items = 0

        # debug
        self.d = {"rank": "rank %s" % circle.rank}

        self.time_started = MPI.Wtime()
        self.time_ended = None

    def create(self):
        if self.circle.rank == 0:
            self.circle.enq(FileItem(self.src))
            print("Analyzing workload ...")

    def copy_xattr(self, src, dest):
        attrs = xattr.listxattr(src)
        for k in attrs:
            val = xattr.getxattr(src, k)
            xattr.setxattr(dest, k, val)

    def flushdb(self):
        if len(self.flist_buf) !=0:
            self.flist.mput(self.flist_buf)


    def process_dir(self, i_dir):
        """
            i_dir should be absolute path
        """
        if self.dest:
            # we create destination directory
            o_dir = destpath(self.src, self.dest, i_dir)
            try:
                os.mkdir(o_dir, stat.S_IRWXU)
            except OSError as e:
                logger.warn("Skipping creation of %s" % o_dir)

            if self.preserve:
                self.copy_xattr(i_dir, o_dir)

        count = 0

        for entry in scandir(i_dir):
            # entry.path should be equivalent to:
            # self.circle.enq(FileItem(os.path.join(i_dir, entry.name)))
            self.circle.enq(FileItem(entry.path))


    def do_metadata_preserve(self, src_file, dest_file):
        if self.preserve:
            try:
                os.mknod(dest_file, stat.S_IRWXU | stat.S_IFREG)
            except OSError as e:
                logger.warn("failed to mknod() for %s", dest_file, extra=self.d)
            else:
                self.copy_xattr(src_file, dest_file)


    def check_dest_exists(self, src_file, dest_file):
        '''
        :param src_file:
        :param dest_file:
        :return: True if dest exists and checksum verified correct
                False if (1) no overwrite (2) destination doesn't exist
        '''
        if not self.force:
            return False

        if not os.path.exists(dest_file):
            return False

        # well, destination exists, now we have to check
        if filecmp.cmp(src_file, dest_file):
            logger.warn("Check Okay: src: %s, dest=%s" % (src_file, dest_file))
            return True
        else:
            logger.warn("Retransfer: %s" % src_file)
            os.unlink(dest_file)
            return False

    def process_retired(self):
        ''' process a work unit, spath, dpath refers to
            source and destination respectively
        '''
        spath = self.deq()
        logger.debug("process: %s" %  spath, extra=self.d)
        if spath:
            st = None
            try:
                st = os.stat(spath)
            except OSError as e:
                logger.error("OSError({0}):{1}, skipping {2}".format(e.errno, e.strerror, spath),
                             extra=self.d)
                return False

            self.flist_append(spath, st)
            if stat.S_ISREG(st.st_mode) and self.dest:
                dpath = destpath(self.src, self.dest, spath)
                if not self.check_dest_exists(spath, dpath):
                    self.do_metadata_preserve(spath, dpath)
                else: # dest exist and same as source
                    self.flist_pop(spath, st)
            elif stat.S_ISDIR(st.st_mode):
                self.cnt_dirs += 1
                self.process_dir(spath)

    def append_fitem(self, fitem):
        if G.use_store:
            self.flist_buf.append(fitem)
            if len(self.flist_buf) == G.DB_BUFSIZE:
                self.flist.mput(self.flist_buf)
                del self.flist_buf[:]

        else:
            self.flist.append(fitem)

    def process(self):
        ''' process a work unit, spath, dpath refers to
            source and destination respectively
        '''
        fitem = self.circle.deq()
        spath = fitem.path

        logger.debug("process: %s" %  spath, extra=self.d)
        if spath:
            st = None
            try:
                st = os.stat(spath)
            except OSError as e:
                logger.error("OSError({0}):{1}, skipping {2}".format(e.errno, e.strerror, spath),
                             extra=self.d)
                return False

            fitem = FileItem(spath, st_mode=st.st_mode,
                             st_size = st.st_size, st_uid = st.st_uid, st_gid = st.st_gid)

            self.reduce_items += 1

            if stat.S_ISREG(st.st_mode):
                self.cnt_files += 1
                self.cnt_filesize += fitem.st_size

                if not self.dest:
                    self.append_fitem(fitem)
                else:
                    # self.dest specified, need to check if it is there
                    dpath = destpath(self.src, self.dest, spath)
                    flag = self.check_dest_exists(spath, dpath)
                    if not flag:
                        # if src and dest not the same
                        # including the case dest is not there
                        # then we do the following
                        self.append_fitem(fitem)
                        self.do_metadata_preserve(spath, dpath)

            elif stat.S_ISDIR(st.st_mode):
                self.cnt_dirs += 1
                self.process_dir(spath)
        # END OF if spath

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
        # progress report
        rate = (buf['cnt_files'] - self.last_cnt)/(MPI.Wtime() - self.last_reduce_time)
        print("Processed objects: %s, estimated process rate: %d/s" % (buf['cnt_files'], rate))
        self.last_cnt = buf['cnt_files']
        self.last_reduce_time = MPI.Wtime()

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


    def cleanup(self):
        if G.use_store:
            self.flist.cleanup()


def main():

    global ARGS, logger
    ARGS = parse_args()
    G.use_store = ARGS.use_store
    G.loglevel = ARGS.loglevel
    logger = getLogger(__name__)
    root = os.path.abspath(ARGS.path)
    circle = Circle(reduce_interval = ARGS.interval)

    treewalk = FWalk(circle, root)
    circle.begin(treewalk)

    if G.use_store:
        treewalk.flushdb()

    treewalk.epilogue()
    treewalk.cleanup()
    circle.finalize()

if __name__ == "__main__": main()

