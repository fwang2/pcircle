#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
"""
fprof: a specialized version of parallel tree walk
designed to profile file size distribution at extreme scale.
"""

__author__ = "Feiyi Wang"
__email__ = "fwang2@ornl.gov"


# Disable auto init
# from mpi4py import rc
# rc.initialize = False
from mpi4py import MPI

from scandir import scandir
import stat
import os
import os.path
import sys
import argparse
import xattr
import numpy as np
import bisect

from task import BaseTask
from circle import Circle
from globals import G
from utils import getLogger, bytes_fmt, destpath
from dbstore import DbStore
from fdef import FileItem
from mpihelper import ThrowingArgumentParser, tally_hosts, parse_and_bcast

import utils

from _version import get_versions
__version__ = get_versions()['version']
args = None
log = getLogger(__name__)
taskloads = []
hist = [0] * (len(G.bins) + 1)
comm = MPI.COMM_WORLD


def err_and_exit(msg, code=0):
    if comm.rank == 0:
        print("\n%s" % msg)
    MPI.Finalize()
    sys.exit(0)


def gen_parser():
    parser = ThrowingArgumentParser(description="fprof - a parallel file system profiler")
    parser.add_argument("-v", "--version", action="version", version="{version}".format(version=__version__))
    parser.add_argument("--loglevel", default="ERROR", help="log level")
    parser.add_argument("path", nargs='+', default=".", help="path")
    parser.add_argument("-i", "--interval", type=int, default=10, help="interval")
    parser.add_argument("--perfile", action="store_true", help="Save perfile file size")
    return parser


def incr_local_histogram(fsz):
    """ incremental histogram  """
    global hist
    idx = bisect.bisect_right(G.bins, fsz) # (left, right)
    hist[idx] += 1


def global_histogram(treewalk):
    global hist
    hist = np.array(hist)  # switch to array format
    all_hist = comm.gather(hist)
    if comm.rank == 0:
        hist = sum(all_hist)


class ProfileWalk(BaseTask):

    def __init__(self, circle, src, perfile=True):
        BaseTask.__init__(self, circle)

        self.d = {"rank": "rank %s" % circle.rank}
        self.circle = circle
        self.src = src
        self.interval = 10  # progress report

        self.checksum = False

        # to be fixed
        self.optlist = []  # files
        self.opt_dir_list = []  # dirs

        self.sym_links = 0
        self.follow_sym_links = False

        self.outfile = None
        if perfile:
            tmpfile = os.path.join(os.getcwd(), "fprof-perfile.%s" % circle.rank)
            self.outfile = open(tmpfile, "w+")

        self.cnt_dirs = 0
        self.cnt_files = 0
        self.cnt_filesize = 0
        self.last_cnt = 0
        self.skipped = 0
        self.last_reduce_time = MPI.Wtime()

        # reduce
        self.reduce_items = 0

        self.time_started = MPI.Wtime()
        self.time_ended = None

        # flush periodically
        self.last_flush = MPI.Wtime()

    def create(self):
        if self.circle.rank == 0:
            for ele in self.src:
                self.circle.enq(ele)
            print("\nStart profiling ...")


    def process_dir(self, fitem, st):
        """ i_dir should be absolute path
        st is the stat object associated with the directory
        """
        i_dir = fitem.path
        last_report = MPI.Wtime()
        count = 0
        try:
            entries = scandir(i_dir)
        except OSError as e:
            log.warn(e, extra=self.d)
            self.skipped += 1
        else:
            for entry in entries:
                elefi = FileItem(entry.path)
                if fitem.dirname:
                    elefi.dirname = fitem.dirname
                self.circle.enq(elefi)

                count += 1
                if (MPI.Wtime() - last_report) > self.interval:
                    print("Rank %s : Scanning [%s] at %s" % (self.circle.rank, i_dir, count))
                    last_report = MPI.Wtime()
            log.info("Finish scan of [%s], count=%s" % (i_dir, count), extra=self.d)


    def process(self):
        """ process a work unit, spath, dpath refers to
            source and destination respectively """

        fitem = self.circle.deq()
        spath = fitem.path
        if spath:
            try:
                st = os.lstat(spath)
            except OSError as e:
                log.warn(e, extra=self.d)
                self.skipped += 1
                return False

            fitem.st_size = st.st_size
            self.reduce_items += 1

            if os.path.islink(spath):
                self.sym_links += 1
                # NOT TO FOLLOW SYM LINKS SHOULD BE THE DEFAULT
                return

            if stat.S_ISREG(st.st_mode):
                incr_local_histogram(st.st_size)
                if self.outfile:
                    self.outfile.write("%d\n" % st.st_size)
                    if (MPI.Wtime() - self.last_flush) > self.interval:
                        self.outfile.flush()

                self.cnt_files += 1
                self.cnt_filesize += fitem.st_size

            elif stat.S_ISDIR(st.st_mode):
                self.cnt_dirs += 1
                self.process_dir(fitem, st)
                # END OF if spath

    def tally(self, t):
        """ t is a tuple element of flist """
        if stat.S_ISDIR(t[1]):
            self.cnt_dirs += 1
        elif stat.S_ISREG(t[1]):
            self.cnt_files += 1
            self.cnt_filesize += t[2]


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
        print("Scanned files: %s, estimated processing rate: %d/s" % (buf['reduce_items'], rate))
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
            print("\nFprof epilogue:\n")
            print("\t{:<20}{:<20}".format("Directory count:", total_dirs))
            print("\t{:<20}{:<20}".format("Sym Links count:", total_symlinks))
            print("\t{:<20}{:<20}".format("File count:", total_files))
            print("\t{:<20}{:<20}".format("Skipped count:", total_skipped))
            print("\t{:<20}{:<20}".format("Total file size:", bytes_fmt(total_filesize)))
            if total_files != 0:
                print("\t{:<20}{:<20}".format("Avg file size:", bytes_fmt(total_filesize/float(total_files))))
            print("\t{:<20}{:<20}".format("Tree talk time:", utils.conv_time(self.time_ended - self.time_started)))
            print("\tFprof loads: %s" % taskloads)
            print("")

        return total_filesize

    def cleanup(self):
        if self.outfile:
            self.outfile.close()


def main():
    global comm, args
    args = parse_and_bcast(comm, gen_parser)

    try:
        G.src = utils.check_src(args.path)
    except ValueError as e:
        err_and_exit("Error: %s not accessible" % e)

    G.loglevel = args.loglevel

    hosts_cnt = tally_hosts()

    if comm.rank == 0:
        print("Running Parameters:\n")
        print("\t{:<20}{:<20}".format("fprof version:", __version__))
        print("\t{:<20}{:<20}".format("Num of hosts:", hosts_cnt))
        print("\t{:<20}{:<20}".format("Num of processes:", MPI.COMM_WORLD.Get_size()))
        print("\t{:<20}{:<20}".format("Root path:", utils.choplist(G.src)))

    circle = Circle()
    treewalk = ProfileWalk(circle, G.src, perfile=args.perfile)
    circle.begin(treewalk)

    global_histogram(treewalk)
    total = hist.sum()
    bucket_scale = 0.5
    if comm.rank == 0:
        if total == 0:
            err_and_exit("No histogram generated.\n")

        print("\nFileset histograms:\n")
        for idx, rightbound in enumerate(G.bins):
            percent = 100 * hist[idx] / float(total)
            star_count = int(bucket_scale * percent)
            print("\t{:<3}{:<15}{:<8}{:<8}{:<50}".format("< ",
                utils.bytes_fmt(rightbound), hist[idx],
                "%0.2f%%" % percent, '∎' * star_count))

        # special processing of last row

        percent = 100 * hist[-1] / float(total)
        star_count = int(bucket_scale * percent)
        print("\t{:<3}{:<15}{:<8}{:<8}{:<50}".format(">= ",
            utils.bytes_fmt(rightbound), hist[idx],
            "%0.2f%%" % percent, '∎' * star_count))
    treewalk.epilogue()
    treewalk.cleanup()
    circle.finalize()


if __name__ == "__main__":
    main()
