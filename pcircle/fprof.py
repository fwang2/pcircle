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
from collections import namedtuple
import stat
import os
import os.path
import sys
import numpy as np
import bisect
import resource
import syslog
import heapq

from timeout import timeout, TimeoutError
from circle import Circle
from globals import G, Tally
from utils import getLogger, bytes_fmt, destpath
from mpihelper import ThrowingArgumentParser, tally_hosts, parse_and_bcast

import utils
import fpipe
import lfs

from _version import get_versions
__version__ = get_versions()['version']
args = None
taskloads = []
topn = []   # track top N files
hist = [0] * (len(G.bins) + 1)

# tracking size
fsize = [0] * (len(G.bins) + 1)
DII_COUNT = 0           # data-in-inode
comm = MPI.COMM_WORLD
FSZMAX = 30000
TopFile = namedtuple("TopFile", "size, path")
EXCLUDE = set()

# shared file
stripe_out = None

def err_and_exit(msg, code=0):
    if comm.rank == 0:
        print("\n%s" % msg)
    MPI.Finalize()
    sys.exit(0)

def is_valid_exclude_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("Can't find exclude file: %s" % arg)
    else:
        return arg  # we are not returning open file handles

def gen_parser():
    parser = ThrowingArgumentParser(description="fprof - a parallel file system profiler")
    parser.add_argument("-v", "--version", action="version", version="{version}".format(version=__version__))
    parser.add_argument("--loglevel", default="ERROR", help="log level")
    parser.add_argument("path", nargs='+', default=".", help="path")
    parser.add_argument("-i", "--interval", type=int, default=10, help="interval")
    parser.add_argument("--perfile", action="store_true", help="Save perfile file size")
    parser.add_argument("--inodesz", default="4k", help="inode size, default 4k")
    parser.add_argument("--gpfs-block-alloc", action="store_true", help="GPFS block usage analysis")
    parser.add_argument("--top", type=int, default=None, help="Top N files, default is None (disabled)")
    parser.add_argument("--perprocess", action="store_true", help="Enable per-process progress report")
    parser.add_argument("--syslog", action="store_true", help="Enable syslog report")
    parser.add_argument("--profdev", action="store_true", help="Enable dev profiling")
    parser.add_argument("--item", type=int, default=3000000, help="number of items stored in memory, default: 3000000")
    parser.add_argument("--exclude", metavar="FILE",
            type=lambda x: is_valid_exclude_file(parser, x), help="A file with exclusion list")
    parser.add_argument("--lustre-stripe", action="store_true", help="Lustre stripe analysis")
    parser.add_argument("--stripe-threshold", metavar="N", default="4g", help="Lustre stripe file threshold, default is 4GB")
    parser.add_argument("--stripe-output", metavar='', default="stripe-%s.out" % utils.timestamp2(), help="stripe output file")

    # parser.add_argument("--histogram", action="store_true", help="Generate block histogram")
    return parser


def incr_local_histogram(fsz):
    """ incremental histogram  """
    global hist, fsize
    idx = bisect.bisect_left(G.bins, fsz)  # <= (inclusive)
    hist[idx] += 1
    fsize[idx] += fsz


def gather_histogram():
    global hist, fsize
    hist = np.array(hist)  # switch to array format
    fsize = np.array(fsize)
    all_hist = comm.gather(hist)
    all_fsize = comm.gather(fsize)

    if comm.rank == 0:
        hist = sum(all_hist)
        fsize = sum(all_fsize)


def update_topn(item):
    """ collect top N (as defined by args.topn) items """
    global topn
    if len(topn) >= args.top:
        heapq.heappushpop(topn, item)
    else:
        heapq.heappush(topn, item)


def gather_topfiles():
    all_topfiles = comm.gather(topn) # [ [ top list from rank x] [ top list from rank y] ]
    if comm.rank == 0:
        flat_topfiles = [item for sublist in all_topfiles for item in sublist ]
        return sorted(flat_topfiles, reverse=True)


def gpfs_block_update(fsz, inodesz=4096):
    global DII_COUNT
    if fsz > (inodesz - 128):
        for idx, sub in enumerate(G.gpfs_subs):
            blocks = fsz / sub
            if fsz % sub != 0:
                blocks += 1
            G.gpfs_block_cnt[idx] += blocks
    else:
        DII_COUNT += 1


def gather_gpfs_dii():
    """Aggregate DII count"""
    global DII_COUNT
    DII_COUNT = comm.reduce(DII_COUNT, op=MPI.SUM)


def gather_gpfs_blocks():
    local_blocks = np.array(G.gpfs_block_cnt)
    all_blocks = comm.gather(local_blocks)
    if comm.rank == 0:
        gpfs_blocks = sum(all_blocks)
    else:
        gpfs_blocks = None

    return gpfs_blocks


class ProfileWalk:

    def __init__(self, circle, src, perfile=True):

        self.logger = utils.getLogger(__name__)

        self.d = {"rank": "rank %s" % circle.rank}
        self.circle = circle
        self.src = src
        self.interval = 10  # progress report

        # hard links
        self.nlinks = 0
        self.nlinked_files = 0

        self.sym_links = 0
        self.follow_sym_links = False

        if perfile:
            tmpfile = os.path.join(os.getcwd(), "fprof-perfile.%s" % circle.rank)
            self.outfile = open(tmpfile, "w")
            self.fszlst = []    # store perfile size
        else:
            self.outfile = None

        self.cnt_dirs = 0
        self.cnt_files = 0
        self.cnt_filesize = 0
        self.last_cnt = 0
        self.skipped = 0
        self.maxfiles = 0
        self.maxfiles_dir = None
        self.devfile_cnt = 0        # track # of dev files
        self.devfile_sz = 0         # track size of dev files
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
            with timeout(seconds=10):
                entries = scandir(path)
        except OSError as e:
            self.logger.warn(e, extra=self.d)
            self.skipped += 1
        except TimeoutError as e:
            self.logger.error("%s when scandir() on %s" % (e, path), extra=self.d)
            self.skipped += 1
        else:
            for entry in entries:
                if entry.is_symlink():
                    self.sym_links += 1
                elif entry.is_file():
                    self.circle.enq(entry.path)
                elif entry.is_dir():
                    self.circle.preq(entry.path)
                else:
                    self.logger.warn("Unknown scan entry: %s" % entry.path, extra=self.d)

                count += 1
                if (MPI.Wtime() - last_report) > self.interval:
                    print("Rank %s : Scanning [%s] at %s" % (self.circle.rank, path, count))
                    last_report = MPI.Wtime()
            self.logger.info("Finish scan of [%s], count=%s" % (path, count), extra=self.d)

        if count > self.maxfiles:
            self.maxfiles = count
            self.maxfiles_dir = path


    def process(self):
        """ process a work unit, spath, dpath refers to
            source and destination respectively """

        spath = self.circle.deq()
        self.logger.debug("BEGIN process object: %s" % spath, extra=self.d)

        if spath:
            if spath in EXCLUDE:
                self.logger.warn("Skip excluded path: %s" % spath, extra=self.d)
                self.skipped += 1
                return

            try:
                with timeout(seconds=5):
                    st = os.lstat(spath)
            except OSError as e:
                self.logger.warn(e, extra=self.d)
                self.skipped += 1
                return None
            except TimeoutError as e:
                self.logger.error("%s when stat() on %s" % (e, spath), extra=self.d)
                self.skipped += 1
                return None
            except Exception as e:
                self.logger.error("Unknown: %s on %s" % (e, spath), extra=self.d)
                self.skipped += 1
                return None

            self.reduce_items += 1

            self.logger.debug("FIN lstat object: %s" % spath, extra=self.d)

            # islink() return True if it is symbolic link
            if os.path.islink(spath):
                self.sym_links += 1
                # NOT TO FOLLOW SYM LINKS SHOULD BE THE DEFAULT
                return None

            self.handle_file_or_dir(spath, st)

            self.logger.debug("END process object: %s" % spath, extra=self.d)

    def handle_file_or_dir(self, spath, st):
        if stat.S_ISREG(st.st_mode):
            incr_local_histogram(st.st_size)
            if args.gpfs_block_alloc:
                inodesz = utils.conv_unit(args.inodesz)
                gpfs_block_update(st.st_size, inodesz)

            if args.top:
                update_topn(TopFile(st.st_size, spath))

            if self.outfile:
                self.fszlst.append(st.st_size)
                if len(self.fszlst) >= FSZMAX:
                    for ele in self.fszlst:
                        self.outfile.write("%d\n" % ele)
                    self.fszlst = []

            self.cnt_files += 1
            self.cnt_filesize += st.st_size

            if args.profdev and utils.is_dev_file(spath):
                self.devfile_cnt += 1
                self.devfile_sz += st.st_size

            # check hard links
            if st.st_nlink > 1:
                self.nlinks += st.st_nlink
                self.nlinked_files += 1

            # stripe analysis
            if args.lustre_stripe and st.st_size > G.stripe_threshold:
                # path, size, stripe_count
                try:
                    with timeout(seconds=5):
                        stripe_count = lfs.lfs_get_stripe(G.lfs_bin, spath)
                except OSError as e:
                    self.logger.warn(e, extra=self.d)
                except TimeoutError as e:
                    self.logger.error("%s when lfs getstripe on %s" % (e,spath), extra=self.d)
                else:
                    if stripe_count:
                        os.write(stripe_out, "%-4s, %-10s, %s\n" % (stripe_count, st.st_size, spath))
                        Tally.spcnt += 1
                    else:
                        self.logger.error("Failed to read stripe info: %s" % spath, extra=self.d)

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
        if sys.platform == 'darwin':
            buf['mem_snapshot'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        else:
            buf['mem_snapshot'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024

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
        fmt_msg = "Scanned files: {:<12,}   Processing rate: {:<6,}/s   HWM mem: {:<12}   Work Queue: {:<12,}"
        print(fmt_msg.format(
            buf['reduce_items'],
            int(rate),
            bytes_fmt(buf['mem_snapshot']),
            buf['work_qsize']))
        self.last_cnt = buf['reduce_items']
        self.last_reduce_time = MPI.Wtime()

    def reduce_finish(self, buf):
        # get result of reduction
        pass

    def total_tally(self):
        """ TODO: refactor it to a named tuple? or object
        """
        global taskloads
        Tally.total_dirs = self.circle.comm.reduce(self.cnt_dirs, op=MPI.SUM)
        Tally.total_files = self.circle.comm.reduce(self.cnt_files, op=MPI.SUM)
        Tally.total_filesize = self.circle.comm.reduce(self.cnt_filesize, op=MPI.SUM)
        Tally.total_symlinks = self.circle.comm.reduce(self.sym_links, op=MPI.SUM)
        Tally.total_skipped = self.circle.comm.reduce(self.skipped, op=MPI.SUM)
        Tally.taskloads = self.circle.comm.gather(self.reduce_items)
        Tally.max_files = self.circle.comm.reduce(self.maxfiles, op=MPI.MAX)
        Tally.total_nlinks = self.circle.comm.reduce(self.nlinks, op=MPI.SUM)
        Tally.total_nlinked_files = self.circle.comm.reduce(self.nlinked_files, op=MPI.SUM)
        if args.profdev:
            Tally.devfile_cnt = self.circle.comm.reduce(self.devfile_cnt, op=MPI.SUM)
            Tally.devfile_sz = self.circle.comm.reduce(self.devfile_sz, op=MPI.SUM)

    def epilogue(self):
        self.total_tally()
        self.time_ended = MPI.Wtime()

        if self.circle.rank == 0:
            print("\nFprof epilogue:\n")
            fmt_msg1 = "\t{:<25}{:<20,}"    # numeric
            fmt_msg2 = "\t{:<25}{:<20}"     # string

            print(fmt_msg1.format("Directory count:", Tally.total_dirs))
            print(fmt_msg1.format("Sym links count:", Tally.total_symlinks))
            print(fmt_msg1.format("Hard linked files:", Tally.total_nlinked_files))
            print(fmt_msg1.format("File count:", Tally.total_files))
            if args.profdev:
                print(fmt_msg1.format("Dev file count:", Tally.devfile_cnt))
                print(fmt_msg2.format("Dev file size:", bytes_fmt(Tally.devfile_sz)))
            print(fmt_msg1.format("Skipped count:", Tally.total_skipped))
            print(fmt_msg2.format("Total file size:", bytes_fmt(Tally.total_filesize)))
            if Tally.total_files != 0:
                print(fmt_msg2.format("Avg file size:",
                                      bytes_fmt(Tally.total_filesize/float(Tally.total_files))))
            print(fmt_msg1.format("Max files within dir:", Tally.max_files))
            elapsed_time = self.time_ended - self.time_started
            processing_rate = int((Tally.total_files + Tally.total_dirs + Tally.total_symlinks + Tally.total_skipped) / elapsed_time)
            print(fmt_msg2.format("Tree walk time:", utils.conv_time(elapsed_time)))
            print(fmt_msg2.format("Scanning rate:", str(processing_rate) + "/s"))
            print(fmt_msg2.format("Fprof loads:", Tally.taskloads))
            print("")

            if args.syslog:
                sendto_syslog("fprof.rootpath", "%s" % ",".join(G.src))
                sendto_syslog("fprof.version", "%s" % __version__)

                sendto_syslog("fprof.dir_count", Tally.total_dirs)
                sendto_syslog("fprof.sym_count", Tally.total_symlinks)
                sendto_syslog("fprof.file_count", Tally.total_files)
                sendto_syslog("fprof.total_file_size", bytes_fmt(Tally.total_filesize))
                if Tally.total_files > 0:
                    sendto_syslog("fprof.avg_file_size", bytes_fmt(Tally.total_filesize/float(Tally.total_files)))
                sendto_syslog("fprof.walktime", utils.conv_time(elapsed_time))
                sendto_syslog("fprof.scan_rate", processing_rate)

        return Tally.total_filesize

    def cleanup(self):
        if self.outfile:  # flush the leftover
            if len(self.fszlst) > 0:
                for ele in self.fszlst:
                    self.outfile.write("%d\n" % ele)
            self.outfile.close()


def sendto_syslog(key, msg):
    """ set up ident for syslog, and convert msg to string
    """
    syslog.openlog(ident=key, facility=syslog.LOG_DEBUG)
    syslog.syslog(str(msg))
    syslog.closelog()

def process_exclude_file():
    global EXCLUDE
    with open(args.exclude, 'r') as f:
        for line in f:
            line = line.strip()
            if not line.startswith("/"):
                continue
            else:
                EXCLUDE.add(os.path.realpath(line))

def main():
    global comm, args, stripe_out

    fpipe.listen()

    args = parse_and_bcast(comm, gen_parser)

    try:
        G.src = utils.check_src2(args.path)
    except ValueError as e:
        err_and_exit("Error: %s not accessible" % e)

    G.memitem_threshold = args.item
    G.loglevel = args.loglevel
    hosts_cnt = tally_hosts()


    # Doing stripe analysis? lfs is not really bullet-proof way
    # we might need a better way of doing fstype check.

    if args.lustre_stripe:
        G.lfs_bin = lfs.check_lfs()
        G.stripe_threshold = utils.conv_unit(args.stripe_threshold)
        try:
            stripe_out = os.open(args.stripe_output, os.O_CREAT|os.O_WRONLY|os.O_APPEND)
        except:
            err_and_exit("Error: can't create stripe output: %s" % args.stripe_output)

    if args.exclude:
        process_exclude_file()

    if comm.rank == 0:
        print("Running Parameters:\n")
        print("\t{:<20}{:<20}".format("fprof version:", __version__))
        print("\t{:<20}{:<20}".format("Num of hosts:", hosts_cnt))
        print("\t{:<20}{:<20}".format("Num of processes:", MPI.COMM_WORLD.Get_size()))
        if args.lustre_stripe:
            print("\t{:<20}{:<20}".format("Stripe analysis: ", "yes"))
            print("\t{:<20}{:<20}".format("Stripe threshold: ", args.stripe_threshold))
        else:
            print("\t{:<20}{:<20}".format("Stripe analysis: ", "no"))
        print("\t{:<20}{:<20}".format("Root path:", G.src))

        if args.exclude:
            print("\nExclusions:\n")
            for ele in EXCLUDE:
                print("\t %s" % ele)

    circle = Circle()
    if args.perprocess:
        circle.report_enabled = True
    else:
        circle.reduce_enabled = True

    treewalk = ProfileWalk(circle, G.src, perfile=args.perfile)
    circle.begin(treewalk)

    # we need the total file size to calculate GPFS efficiency
    total_file_size = treewalk.epilogue()

    msg1, msg2 = gen_histogram(total_file_size)

    if comm.rank == 0 and args.syslog:
        sendto_syslog("fprof.filecount.hist", msg1)
        sendto_syslog("fprof.fsize_perc.hist", msg2)

    if args.top:
        topfiles = gather_topfiles()
        if comm.rank == 0:
            print("\nTop File Report:\n")
            # edge case: not enough files (< args.top)
            totaln = args.top if len(topfiles) > args.top else len(topfiles)
            for index, _ in enumerate(xrange(totaln)):
                size, path = topfiles[index]
                print("\t%s: %s (%s)" % (index + 1,
                                       path,
                                       utils.bytes_fmt(size)))
            print("")

    if args.gpfs_block_alloc:
        gpfs_blocks = gather_gpfs_blocks()
        gather_gpfs_dii()
        if comm.rank == 0:
            print("\nGPFS Block Alloc Report:\n")
            print("\t{:<15}{:<4}".format("inode size:", args.inodesz))
            print("\t{:<25}{:>15,}".format("DII (data-in-inode) count:", DII_COUNT))
            print("\tSubblocks: %s\n" % gpfs_blocks)
            for idx, bsz in enumerate(G.gpfs_block_size):
                gpfs_file_size = gpfs_blocks[idx] * G.gpfs_subs[idx]
                fmt_msg = "\tBlocksize: {:<6}   Estimated Space: {:<20s}   Efficiency: {:>6.2%}"
                if gpfs_file_size != 0:
                    print(fmt_msg.format(bsz, bytes_fmt(gpfs_file_size), total_file_size/float(gpfs_file_size)))
                else:
                    print(fmt_msg.format(bsz, bytes_fmt(gpfs_file_size), 0))

    treewalk.cleanup()
    circle.finalize()

    if args.lustre_stripe and stripe_out:
        os.close(stripe_out)

        sp_workload = comm.gather(Tally.spcnt)
        if comm.rank == 0:
            print("Stripe workload total: %s, distribution: %s" % (sum(sp_workload), sp_workload))

def gen_histogram(total_file_size):
    syslog_filecount_hist = ""
    syslog_fsizeperc_hist = ""
    bins_fmt = utils.bins_strs(G.bins)
    gather_histogram()
    if comm.rank == 0:
        total_num_of_files = hist.sum()
        if total_num_of_files == 0:
            err_and_exit("No histogram generated.\n")

        print("Fileset Histogram\n")

        msg = "\t{:<3}{:<15}{:<15,}{:>10}{:>15}{:>15}"
        msg2 = "\t{:<3}{:<15}{:<15}{:>10}{:>15}{:>15}"

        print(msg2.format("", "Buckets", "Num of Files", "Size",  "%(Files)", "%(Size)"))
        print("")
        for idx, rightbound in enumerate(G.bins):
            percent_files = 100 * hist[idx] / float(total_num_of_files)
            percent_size = 100 * fsize[idx] / float(total_file_size)

            print(msg.format("<= ", utils.bytes_fmt(rightbound),
                             hist[idx],
                             utils.bytes_fmt(fsize[idx]),
                             "%0.2f%%" % percent_files, "%0.2f%%" % percent_size))

            # NO BLOCK HISTOGRAM
            #
            # bucket_scale = 0.30
            # star_count = int(bucket_scale * percent)
            # print(msg.format("<= ", utils.bytes_fmt(rightbound),
            #                  hist[idx],
            #                  utils.bytes_fmt(fsize[idx]),
            #                  "%0.2f%%" % percent, '∎' * star_count))

            syslog_filecount_hist += "%s = %s, " % (bins_fmt[idx], hist[idx])
            syslog_fsizeperc_hist += "%s = %s, " % (bins_fmt[idx], percent_size)

        # special processing of last row
        percent_files = 100 * hist[-1] / float(total_num_of_files)
        percent_size = 100 * fsize[-1] / float(total_file_size)
        print(msg.format("> ", utils.bytes_fmt(rightbound),
                         hist[-1],
                         utils.bytes_fmt(fsize[-1]),
                         "%0.2f%%" % percent_files,
                         "%0.2f%%" % percent_size))

        # star_count = int(bucket_scale * percent)
        # print(msg.format("> ", utils.bytes_fmt(rightbound), hist[-1],
        #                  utils.bytes_fmt(fsize[-1]),
        #                  "%0.2f%%" % percent, '∎' * star_count))
        syslog_filecount_hist += "%s = %s" % (bins_fmt[-1], hist[-1])
        syslog_fsizeperc_hist += "%s = %s" % (bins_fmt[-1], percent_size)

    return syslog_filecount_hist, syslog_fsizeperc_hist

if __name__ == "__main__":
    main()
