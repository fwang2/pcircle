#!/usr/bin/env python
from __future__ import print_function

from task import BaseTask
from circle import Circle, tally_hosts
from globals import G
from mpi4py import MPI
from utils import timestamp, bytes_fmt, destpath
from dbstore import DbStore
from fdef import FileItem
from scandir import scandir
import utils

import stat
import os
import os.path
import sys
import argparse
import filecmp

import xattr
from _version import get_versions

ARGS = None
__version__ = get_versions()['version']
logger = None
taskloads = []


def parse_args():
    parser = argparse.ArgumentParser(description="fwalk")
    parser.add_argument("-v", "--version", action="version", version="{version}".format(version=__version__))
    parser.add_argument("--loglevel", default="ERROR", help="log level")
    parser.add_argument("path", default=".", help="path")
    parser.add_argument("-i", "--interval", type=int, default=10, help="interval")
    parser.add_argument("--use-store", action="store_true", help="Use persistent store")
    return parser.parse_args()


class FWalk(BaseTask):
    def __init__(self, circle, src, dest=None, preserve=False, force=False, sizeonly=True):
        BaseTask.__init__(self, circle)

        self.circle = circle
        self.src = src
        self.dest = dest
        self.preserve = preserve
        self.force = force
        self.sizeonly = sizeonly

        self.sym_links = 0
        self.follow_sym_links = False

        self.workdir = os.getcwd()
        self.tempdir = os.path.join(self.workdir, ".pcircle")
        if not os.path.exists(self.tempdir):
            os.mkdir(self.tempdir)

        if G.use_store:
            self.dbname = "%s/fwalk.%s" % (self.tempdir, circle.rank)
            self.flist = DbStore(self.dbname)
            self.flist_buf = []
        else:
            self.flist = []  # element is (filepath, filemode, filesize, uid, gid)
        self.src_flist = self.flist

        self.cnt_dirs = 0
        self.cnt_files = 0
        self.cnt_filesize = 0
        self.last_cnt = 0
        self.skipped = 0
        self.last_reduce_time = MPI.Wtime()

        # reduce
        self.reduce_items = 0

        # debug
        self.d = {"rank": "rank %s" % circle.rank}

        self.time_started = MPI.Wtime()
        self.time_ended = None

        self.logger = utils.getLogger(__name__)

    def create(self):
        if self.circle.rank == 0:
            self.circle.enq(FileItem(self.src))
            print("\nAnalyzing workload ...")

    def copy_xattr(self, src, dest):
        attrs = xattr.listxattr(src)
        for k in attrs:
            val = xattr.getxattr(src, k)
            xattr.setxattr(dest, k, val)

    def flushdb(self):
        if len(self.flist_buf) != 0:
            self.flist.mput(self.flist_buf)

    def process_dir(self, i_dir):
        """ i_dir should be absolute path """
        if self.dest:
            # we create destination directory
            o_dir = destpath(self.src, self.dest, i_dir)
            try:
                os.mkdir(o_dir, stat.S_IRWXU)
            except OSError as e:
                self.logger.warn("processing [%s], %s" % (i_dir, e), extra=self.d)

            if self.preserve:
                self.copy_xattr(i_dir, o_dir)

        count = 0
        try:
            entries = scandir(i_dir)
        except OSError as e:
            self.logger.warn(e)
            self.skipped += 1

        else:
            for entry in entries:
                # entry.path should be equivalent to:
                # self.circle.enq(FileItem(os.path.join(i_dir, entry.name)))
                self.circle.enq(FileItem(entry.path))

    def do_metadata_preserve(self, src_file, dest_file):
        if self.preserve:
            try:
                os.mknod(dest_file, stat.S_IRWXU | stat.S_IFREG)
            except OSError as e:
                self.logger.warn("failed to mknod() for %s", dest_file, extra=self.d)
            else:
                self.copy_xattr(src_file, dest_file)

    def check_dest_exists(self, src_file, dest_file):
        """ return True if dest exists and checksum verified correct
            return False if (1) no overwrite (2) destination doesn't exist
        """
        if not self.force:
            return False

        if not os.path.exists(dest_file):
            return False

        # well, destination exists, now we have to check
        if self.sizeonly:
            if os.path.getsize(src_file) == os.path.getsize(dest_file):
                self.logger.warn("Check sizeonly Okay: src: %s, dest=%s" % (src_file, dest_file),
                                 extra=self.d)
                return True
        elif filecmp.cmp(src_file, dest_file):
            self.logger.warn("Check Okay: src: %s, dest=%s" % (src_file, dest_file), extra=self.d)
            return True

        # check failed
        self.logger.warn("Retransfer: %s" % src_file, extra=self.d)
        os.unlink(dest_file)
        return False

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

        # self.logger.info("process: %s" % spath, extra=self.d)

        if spath:
            try:
                st = os.stat(spath)
            except OSError as e:
                self.logger.warn(e, extra=self.d)
                self.skipped += 1
                return False

            fitem = FileItem(spath, st_mode=st.st_mode,
                             st_size=st.st_size, st_uid=st.st_uid, st_gid=st.st_gid)

            self.reduce_items += 1

            if os.path.islink(spath):
                self.append_fitem(fitem)
                self.sym_links += 1
                # if not self.follow_sym_links:
                # NOT TO FOLLOW SYM LINKS SHOULD BE THE DEFAULT
                return

            if stat.S_ISREG(st.st_mode):

                if not self.dest:
                    # fwalk without destination, simply add to process list
                    self.append_fitem(fitem)
                else:
                    # self.dest specified, need to check if it is there
                    dpath = destpath(self.src, self.dest, spath)
                    flag = self.check_dest_exists(spath, dpath)
                    if flag:
                        return
                    else:
                        # if src and dest not the same
                        # including the case dest is not there
                        # then we do the following
                        self.append_fitem(fitem)
                        self.do_metadata_preserve(spath, dpath)
                self.cnt_files += 1
                self.cnt_filesize += fitem.st_size

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
        buf['reduce_items'] = self.reduce_items

    def reduce(self, buf1, buf2):
        buf1['cnt_dirs'] += buf2['cnt_dirs']
        buf1['cnt_files'] += buf2['cnt_files']
        buf1['cnt_filesize'] += buf2['cnt_filesize']
        buf1['reduce_items'] += buf2['reduce_items']
        return buf1

    def reduce_report(self, buf):
        # progress report
        # rate = (buf['cnt_files'] - self.last_cnt)/(MPI.Wtime() - self.last_reduce_time)
        # print("Processed objects: %s, estimated processing rate: %d/s" % (buf['cnt_files'], rate))
        # self.last_cnt = buf['cnt_files']

        rate = (buf['reduce_items'] - self.last_cnt) / (MPI.Wtime() - self.last_reduce_time)
        print("Processed objects: %s, estimated processing rate: %d/s" % (buf['reduce_items'], rate))
        self.last_cnt = buf['reduce_items']
        self.last_reduce_time = MPI.Wtime()

    def reduce_finish(self, buf):
        # get result of reduction
        pass

    def total_tally(self):
        global taskloads
        total_dirs = self.circle.comm.reduce(self.cnt_dirs, op=MPI.SUM)
        total_files = self.circle.comm.reduce(self.cnt_files, op=MPI.SUM)
        total_filesize = self.circle.comm.reduce(self.cnt_filesize, op=MPI.SUM)
        total_symlinks = self.circle.comm.reduce(self.sym_links, op=MPI.SUM)
        total_skipped = self.circle.comm.reduce(self.skipped, op=MPI.SUM)
        taskloads = self.circle.comm.gather(self.reduce_items)
        return total_dirs, total_files, total_filesize, total_symlinks, total_skipped

    def epilogue(self):
        total_dirs, total_files, total_filesize, total_symlinks, total_skipped = self.total_tally()
        self.time_ended = MPI.Wtime()

        if self.circle.rank == 0:
            print("")
            print("{:<20}{:<20}".format("Directory count:", total_dirs))
            print("{:<20}{:<20}".format("Sym Links count:", total_symlinks))
            print("{:<20}{:<20}".format("File count:", total_files))
            print("{:<20}{:<20}".format("Skipped count:", total_skipped))
            print("{:<20}{:<20}".format("File size:", bytes_fmt(total_filesize)))
            print("{:<20}{:<20}".format("Tree talk time:", "%.2f seconds" % (self.time_ended - self.time_started)))
            print("FWALK Loads: %s" % taskloads)
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
    logger = utils.getLogger(__name__)
    root = os.path.abspath(ARGS.path)
    root = os.path.realpath(root)
    if not os.path.exists(root):
        if MPI.COMM_WORLD.Get_rank() == 0:
            print("Root path: %s doesn't exist!" % root)
        MPI.Finalize()
        sys.exit(0)

    hosts_cnt = tally_hosts()
    circle = Circle(reduce_interval=ARGS.interval)


    if circle.rank == 0:
        print("Running Parameters:\n")
        print("\t{:<20}{:<20}".format("FWALK version:", __version__))
        print("\t{:<20}{:<20}".format("Num of hosts:", hosts_cnt))
        print("\t{:<20}{:<20}".format("Num of processes:", MPI.COMM_WORLD.Get_size()))
        print("\t{:<20}{:<20}".format("Root path:", root))

    treewalk = FWalk(circle, root)
    circle.begin(treewalk)

    if G.use_store:
        treewalk.flushdb()

    treewalk.epilogue()
    treewalk.cleanup()
    circle.finalize()


if __name__ == "__main__": main()
