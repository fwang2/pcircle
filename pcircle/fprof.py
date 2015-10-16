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
import numpy as np
import bisect
import resource


from timeout import timeout, TimeoutError
from circle import Circle
from globals import G
from utils import getLogger, bytes_fmt, destpath
from mpihelper import ThrowingArgumentParser, tally_hosts, parse_and_bcast

import utils

from _version import get_versions
__version__ = get_versions()['version']
args = None
gpfs_blocks = None
log = getLogger(__name__)
taskloads = []
hist = [0] * (len(G.bins) + 1)
comm = MPI.COMM_WORLD
FSZMAX = 30000

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
    parser.add_argument("--gpfs-block-alloc", action="store_true", help="GPFS block usage analysis")
    return parser


def incr_local_histogram(fsz):
    """ incremental histogram  """
    global hist
    idx = bisect.bisect_right(G.bins, fsz) # (left, right)
    hist[idx] += 1


def gather_histogram():
    global hist
    hist = np.array(hist)  # switch to array format
    all_hist = comm.gather(hist)
    if comm.rank == 0:
        hist = sum(all_hist)


def gpfs_block_update(fsz, inodesz = 4096):
    if fsz > (inodesz - 128):
        for idx, sub in enumerate(G.gpfs_subs):
            blocks = fsz / sub
            if fsz % sub != 0:
                blocks += 1
            G.gpfs_block_cnt[idx] += blocks


def gather_gpfs_blocks():
    global gpfs_blocks
    local_blocks = np.array(G.gpfs_block_cnt)
    all_blocks = comm.gather(local_blocks)
    if comm.rank == 0:
        gpfs_blocks = sum(all_blocks)


class ProfileWalk:

    def __init__(self, circle, src, perfile=True):

        self.d = {"rank": "rank %s" % circle.rank}
        self.circle = circle
        self.src = src
        self.interval = 10  # progress report

        self.fszlst = []    # store perfile size
        self.sym_links = 0
        self.follow_sym_links = False

        self.outfile = None
        if perfile:
            tmpfile = os.path.join(os.getcwd(), "fprof-perfile.%s" % circle.rank)
            self.outfile = open(tmpfile, "w")

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

    def create(self):
        if self.circle.rank == 0:
            for ele in self.src:
                self.circle.enq(ele)
            print("\nStart profiling ...")

    def process_dir(self, path, st):
        """ i_dir should be absolute path
        st is the stat object associated with the directory
        """
        last_report = MPI.Wtime()
        count = 0
        try:
            with timeout(seconds=30):
                entries = scandir(path)
        except OSError as e:
            log.warn(e, extra=self.d)
            self.skipped += 1
        except TimeoutError as e:
            log.error("%s when scandir() on %s" % (e, path), extra=self.d)
            self.skipped += 1
        else:
            for entry in entries:
                if entry.is_symlink():
                    self.sym_links += 1
                elif entry.is_file():
                    self.circle.enq(entry.path)
                else:
                    self.circle.preq(entry.path)
                count += 1
                if (MPI.Wtime() - last_report) > self.interval:
                    print("Rank %s : Scanning [%s] at %s" % (self.circle.rank, path, count))
                    last_report = MPI.Wtime()
            log.info("Finish scan of [%s], count=%s" % (path, count), extra=self.d)

    def process(self):
        """ process a work unit, spath, dpath refers to
            source and destination respectively """

        spath = self.circle.deq()
        if spath:
            try:
                with timeout(seconds=15):
                    st = os.lstat(spath)
            except OSError as e:
                log.warn(e, extra=self.d)
                self.skipped += 1
                return False
            except TimeoutError as e:
                log.error("%s when stat() on %s" % (e, spath), extra=self.d)
                self.skipped += 1
                return

            self.reduce_items += 1

            if os.path.islink(spath):
                self.sym_links += 1
                # NOT TO FOLLOW SYM LINKS SHOULD BE THE DEFAULT
                return

            self.handle_file_or_dir(spath, st)
            del spath

    def handle_file_or_dir(self, spath, st):
        if stat.S_ISREG(st.st_mode):
            incr_local_histogram(st.st_size)
            if args.gpfs_block_alloc:
                gpfs_block_update(st.st_size)

            self.fszlst.append(st.st_size)

            if self.outfile and len(self.fszlst) >= FSZMAX:
                for ele in self.fszlst:
                    self.outfile.write("%d\n" % ele)
                self.fszlst = []

            self.cnt_files += 1
            self.cnt_filesize += st.st_size

        elif stat.S_ISDIR(st.st_mode):
            self.cnt_dirs += 1
            self.process_dir(spath, st)

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
        buf['work_qsize'] = len(self.circle.workq)
        buf['mem_snapshot'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    def reduce(self, buf1, buf2):
        buf1['cnt_dirs'] += buf2['cnt_dirs']
        buf1['cnt_files'] += buf2['cnt_files']
        buf1['cnt_filesize'] += buf2['cnt_filesize']
        buf1['reduce_items'] += buf2['reduce_items']
        buf1['work_qsize'] += buf2['work_qsize']
        buf1['mem_snapshot'] += buf2['mem_snapshot']

        return buf1

    def reduce_report(self, buf):
        # progress report
        # rate = (buf['cnt_files'] - self.last_cnt)/(MPI.Wtime() - self.last_reduce_time)
        # print("Processed objects: %s, estimated processing rate: %d/s" % (buf['cnt_files'], rate))
        # self.last_cnt = buf['cnt_files']

        rate = (buf['reduce_items'] - self.last_cnt) / (MPI.Wtime() - self.last_reduce_time)
        print("Scanned files {:,}, estimated processing rate {:d}/s, "
              "mem_snapshot {}, workq {:,}".format(buf['reduce_items'], int(rate), bytes_fmt(buf['mem_snapshot']),
                                                 buf['work_qsize']))
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
            print("\t{:<20}{:<20,}".format("Directory count:", total_dirs))
            print("\t{:<20}{:<20}".format("Sym Links count:", total_symlinks))
            print("\t{:<20}{:<20,}".format("File count:", total_files))
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
            if len(self.fszlst) > 0:
                for ele in self.fszlst:
                    self.outfile.write("%d\n" % ele)
            self.outfile.close()


def main():
    global comm, args
    args = parse_and_bcast(comm, gen_parser)

    try:
        G.src = utils.check_src2(args.path)
    except ValueError as e:
        err_and_exit("Error: %s not accessible" % e)

    G.loglevel = args.loglevel

    hosts_cnt = tally_hosts()

    if comm.rank == 0:
        print("Running Parameters:\n")
        print("\t{:<20}{:<20}".format("fprof version:", __version__))
        print("\t{:<20}{:<20}".format("Num of hosts:", hosts_cnt))
        print("\t{:<20}{:<20}".format("Num of processes:", MPI.COMM_WORLD.Get_size()))
        print("\t{:<20}{:<20}".format("Root path:", G.src))

    circle = Circle()
    treewalk = ProfileWalk(circle, G.src, perfile=args.perfile)
    circle.begin(treewalk)

    gen_histogram()

    if args.gpfs_block_alloc:
        gather_gpfs_blocks()
        if comm.rank == 0:
            print("\nGPFS Block Alloc Report:\n")
            for idx, bsz in enumerate(G.gpfs_block_size):
                totalsize = gpfs_blocks[idx] * G.gpfs_subs[idx]
                print("\tBlocksize: {:<5} SubBlocks: {:10,} Estimated Space: {:<15s}".
                      format(bsz, gpfs_blocks[idx],bytes_fmt(totalsize)))

    treewalk.epilogue()
    treewalk.cleanup()
    circle.finalize()


def gen_histogram():
    gather_histogram()
    if comm.rank == 0:
        total = hist.sum()
        bucket_scale = 0.5
        if total == 0:
            err_and_exit("No histogram generated.\n")

        print("\nFileset histograms:\n")
        msg = "\t{:<3}{:<15}{:<8}{:<8}{:<50}"

        for idx, rightbound in enumerate(G.bins):
            percent = 100 * hist[idx] / float(total)
            star_count = int(bucket_scale * percent)
            print(msg.format("< ", utils.bytes_fmt(rightbound),
                             hist[idx], "%0.2f%%" % percent, '∎' * star_count))

        # special processing of last row
        percent = 100 * hist[-1] / float(total)
        star_count = int(bucket_scale * percent)
        print(msg.format(">= ", utils.bytes_fmt(rightbound), hist[idx],
                         "%0.2f%%" % percent, '∎' * star_count))


if __name__ == "__main__":
    main()
