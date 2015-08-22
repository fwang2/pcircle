#!/usr/bin/env python
"""
PCP provides MPI-based parallel data transfer functionality.

Author: Feiyi Wang (fwang2@ornl.gov)

Note on Logging:
    1. Define global variable "logger"
    2. Assign G.loglevel based on command line args
    3. Assign G.logfile based on command line args
    4. Assign "logger" with utils.getLogger(name, loglevel, logfile)
       Here the logfile arg can be G.logfile

    The logging will only write out ERROR messgage to console, the command line
    --loglevel is for controling the loglevel of logfile

    The logfile is per-rank, which could be problematic when it gets large.
    A better solution remains to be found.

"""
from __future__ import print_function

import time
import stat
import os
import os.path
import logging
import argparse
import hashlib
import sys
import signal
import resource
import cPickle as pickle
from collections import Counter
from lru import LRU
from threading import Thread

from mpi4py import MPI

import utils
from utils import bytes_fmt, destpath
from task import BaseTask
from verify import PVerify
from circle import Circle
from cio import readn, writen
from fwalk import FWalk
from checkpoint import Checkpoint
from fdef import FileChunk, ChunkSum
from globals import G
from dbstore import DbStore
from dbsum import DbSum
from _version import get_versions

__version__ = get_versions()['version']
del get_versions

ARGS = None
logger = None
circle = None
NUM_OF_HOSTS = 0
taskloads = []
comm = MPI.COMM_WORLD


def parse_args():
    parser = argparse.ArgumentParser(description="Parallel Data Copy",
                                     epilog="Please report issues to help@nccs.gov")
    parser.add_argument("--version", action="version", version="{version}".format(version=__version__))
    parser.add_argument("--use-store", action="store_true", help="Use persistent store")
    parser.add_argument("--loglevel", default="warn", help="log level for file, default WARN")
    parser.add_argument("--chunksize", metavar="sz", default="1m", help="chunk size (KB, MB, GB, TB), default: 1MB")
    parser.add_argument("--adaptive", action="store_true", default=True, help="Adaptive chunk size")
    parser.add_argument("--reduce-interval", metavar="seconds", type=int, default=10, help="interval, default 10s")
    parser.add_argument("--checkpoint-interval", metavar="seconds", type=int, default=360,
                        help="checkpoint interval, default: 360s")
    parser.add_argument("-c", "--checksum", action="store_true", help="verify after copy, default: off")
    parser.add_argument("-s", "--signature", action="store_true", help="aggregate checksum for signature, default: off")
    parser.add_argument("--checkpoint-id", metavar="ID", default=None, help="default: timestamp")
    parser.add_argument("--disable-preserve", action="store_true", help="no preserving meta, default: off")
    parser.add_argument("-r", "--resume", dest="rid", metavar="ID", nargs=1, help="resume ID, required in resume mode")
    parser.add_argument("-f", "--force", action="store_true", help="force overwrite")
    parser.add_argument("--pause", type=int, help="pause a delay (seconds) after copy, test only")
    parser.add_argument("--fix-opt", action="store_true", help="fix ownership, permssion, timestamp")
    parser.add_argument("src", help="copy from")
    parser.add_argument("dest", help="copy to")
    parser.add_argument("-o", "--output", default="sha1-%s.sig" % utils.timestamp2(), help="sha1 output file")

    return parser.parse_args()


def sig_handler(signal, frame):
    # catch keyboard, do nothing
    # eprint("\tUser cancelled ... cleaning up")
    sys.exit(1)


class FCP(BaseTask):
    def __init__(self, circle, src, dest,
                 treewalk=None,
                 totalsize=0,
                 hostcnt=0,
                 prune=False,
                 do_checksum=False,
                 resume=False,
                 workq=None):
        BaseTask.__init__(self, circle)
        self.circle = circle
        self.treewalk = treewalk
        self.totalsize = totalsize
        self.prune = prune
        self.workq = workq
        self.resume = resume
        self.checkpoint_file = None
        self.vvv = False
        self.src = os.path.abspath(src)
        self.srcbase = os.path.basename(src)
        self.dest = os.path.abspath(dest)

        # cache, keep the size conservative
        # TODO: we need a more portable LRU size

        if hostcnt != 0:
            max_ofile, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
            procs_per_host = self.circle.size / hostcnt
            self._read_cache_limit = ((max_ofile - 64) / procs_per_host) / 3
            self._write_cache_limit = ((max_ofile - 64) / procs_per_host) * 2 / 3

        if self._read_cache_limit <= 0 or self._write_cache_limit <= 0:
            self._read_cache_limit = 1
            self._write_cache_limit = 8

        self.rfd_cache = LRU(self._read_cache_limit)
        self.wfd_cache = LRU(self._write_cache_limit)

        self.cnt_filesize_prior = 0
        self.cnt_filesize = 0

        self.blocksize = 1024 * 1024
        self.chunksize = 1024 * 1024

        # debug
        self.d = {"rank": "rank %s" % circle.rank}
        self.wtime_started = MPI.Wtime()
        self.wtime_ended = None
        self.workcnt = 0  # this is the cnt for the enqued items
        self.reduce_items = 0  # this is the cnt for processed items
        if self.treewalk and self.vvv:
            logger.debug("treewalk files = %s" % treewalk.flist, extra=self.d)

        # fini_check
        self.fini_cnt = Counter()

        # checksum
        self.do_checksum = do_checksum
        self.chunksums = []

        # checkpointing
        self.checkpoint_interval = sys.maxsize
        self.checkpoint_last = MPI.Wtime()

        if self.circle.rank == 0:
            print("Start copying process ...")

    def set_fixed_chunksize(self, sz):
        self.chunksize = sz

    def set_adaptive_chunksize(self, totalsz):
        MB = 1024 * 1024
        TB = 1024 * 1024 * 1024 * 1024
        if totalsz < 10 * TB:
            self.chunksize = 16 * MB
        elif totalsz < 100 * TB:
            self.chunksize = 64 * MB
        elif totalsz < 512 * TB:
            self.chunksize = 128 * MB
        elif totalsz < 1024 * TB:
            self.chunksize = 256 * MB
        else:
            self.chunksize = 512 * MB

        if self.circle.rank == 0:
            print("Adaptive chunksize: %s" % bytes_fmt(self.chunksize))

    def set_checkpoint_file(self, f):
        self.checkpoint_file = f

    def cleanup(self):
        for f in self.rfd_cache.values():
            try:
                os.close(f)
            except OSError as e:
                pass

        for f in self.wfd_cache.values():
            try:
                os.close(f)
            except OSError as e:
                pass

        # remove checkpoint file
        if self.checkpoint_file and os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)

        # we need to do this because if last job didn't finish cleanly
        # the fwalk files can be found as leftovers
        # and if fcp cleanup has a chance, it should clean up that

        fwalk = "%s/fwalk.%s" % (self.circle.tempdir, self.circle.rank)
        if os.path.exists(fwalk):
            os.remove(fwalk)

    def new_fchunk(self, f):
        fchunk = FileChunk()  # default cmd = copy
        fchunk.src = f.path
        fchunk.dest = destpath(self.src, self.dest, f.path)
        return fchunk

    def enq_file(self, fi):
        """ Process a single file, represented by "fi" - file item
        It involves chunking this file and equeue all chunks. """

        chunks = fi.st_size / self.chunksize
        remaining = fi.st_size % self.chunksize

        workcnt = 0

        if fi.st_size == 0:  # empty file
            fchunk = self.new_fchunk(fi)
            fchunk.offset = 0
            fchunk.length = 0
            self.enq(fchunk)
            workcnt += 1
        else:
            for i in range(chunks):
                fchunk = self.new_fchunk(fi)
                fchunk.offset = i * self.chunksize
                fchunk.length = self.chunksize
                self.enq(fchunk)
            workcnt += chunks

        if remaining > 0:
            # send remainder
            fchunk = self.new_fchunk(fi)
            fchunk.offset = chunks * self.chunksize
            fchunk.length = remaining
            self.enq(fchunk)
            workcnt += 1

        # save work cnt
        self.workcnt += workcnt

        logger.debug("enq_file(): %s, size = %s, workcnt = %s" % (fi.path, fi.st_size, workcnt),
                     extra=self.d)

    def handle_fitem(self, fi):
        if os.path.islink(fi.path):
            dest = destpath(self.src, self.dest, fi.path)
            linkto = os.readlink(fi.path)
            try:
                os.symlink(linkto, dest)
            except Exception as e:
                logger.warn("%s, skipping sym link %s" % (e, fi.path), extra=self.d)
        elif stat.S_ISREG(fi.st_mode):
            self.enq_file(fi)  # where chunking takes place

    def create(self):
        """ Each task has one create(), which is invoked by circle ONCE.
        For FCP, each task will handle_fitem() -> enq_file()
        to process each file gathered during the treewalk stage. """

        if not G.use_store and self.workq:  # restart
            self.setq(self.workq)
            return

        if self.resume:
            return

        # construct and enable all copy operations
        # we batch operation hard-coded
        logger.info("create() starts, flist length = %s" % len(self.treewalk.flist),
                    extra=self.d)

        if G.use_store:
            while self.treewalk.flist.qsize > 0:
                fitems, _ = self.treewalk.flist.mget(G.DB_BUFSIZE)
                for fi in fitems:
                    self.handle_fitem(fi)
                self.treewalk.flist.mdel(G.DB_BUFSIZE)

            # store checkpoint
            logger.debug("dbname = %s" % self.circle.dbname)
            dirname = os.path.dirname(self.circle.dbname)
            basename = os.path.basename(self.circle.dbname)
            chkpointname = basename + ".CHECK_OK"
            self.checkpoint_file = os.path.join(dirname, chkpointname)
            with open(self.checkpoint_file, "w") as f:
                f.write("%s" % self.totalsize)

        else:  # use memory
            for fi in self.treewalk.flist:
                self.handle_fitem(fi)

            # memory-checkpoint
            if self.checkpoint_file:
                self.do_no_interrupt_checkpoint()
                self.checkpoint_last = MPI.Wtime()

    def do_open(self, k, d, flag, limit):
        """
        @param k: the file path
        @param d: dictionary of <path, file descriptor>
        @return: file descriptor
        """
        if d.has_key(k):
            return d[k]

        if len(d.keys()) >= limit:
            # over the limit
            # clean up the least used
            old_k, old_v = d.items()[-1]
            try:
                os.close(old_v)
            except OSError as e:
                logger.warn("FD for %s not valid when closing" % old_k, extra=self.d)

        fd = -1
        try:
            fd = os.open(k, flag)
        except OSError as e:
            if e.errno == 28:  # no space left
                logger.error("Critical error: %s, exit!" % e, extra=self.d)
                self.circle.exit(0)  # should abort
            else:
                logger.error("OSError({0}):{1}, skipping {2}".format(e.errno, e.strerror, k), extra=self.d)
        else:
            if fd > 0:
                d[k] = fd
        finally:
            return fd

    @staticmethod
    def do_mkdir(work):
        src = work.src
        dest = work.dest
        if not os.path.exists(dest):
            os.makedirs(dest)

    def do_copy(self, work):
        src = work.src
        dest = work.dest

        basedir = os.path.dirname(dest)
        if not os.path.exists(basedir):
            os.makedirs(basedir)

        rfd = self.do_open(src, self.rfd_cache, os.O_RDONLY, self._read_cache_limit)
        if rfd < 0:
            return False
        wfd = self.do_open(dest, self.wfd_cache, os.O_WRONLY | os.O_CREAT, self._write_cache_limit)
        if wfd < 0:
            if ARGS.force:
                try:
                    os.unlink(dest)
                except OSError as e:
                    logger.error("Failed to unlink %s, %s " % (dest, e), extra=self.d)
                    return False
                else:
                    wfd = self.do_open(dest, self.wfd_cache, os.O_WRONLY, self._write_cache_limit)
            else:
                logger.error("Failed to create output file %s" % dest, extra=self.d)
                return False

        # do the actual copy
        self.write_bytes(rfd, wfd, work)

        # update tally
        self.cnt_filesize += work.length

        if self.vvv:
            logger.debug("Transferred %s bytes from:\n\t [%s] to [%s]" %
                         (self.cnt_filesize, src, dest), extra=self.d)

        return True

    def do_no_interrupt_checkpoint(self):
        a = Thread(target=self.do_checkpoint)
        a.start()
        a.join()
        logger.debug("checkpoint: %s" % self.checkpoint_file, extra=self.d)

    def do_checkpoint(self):
        for k in self.wfd_cache.keys():
            os.close(self.wfd_cache[k])

        # clear the cache
        self.wfd_cache.clear()

        tmp_file = self.checkpoint_file + ".part"
        with open(tmp_file, "wb") as f:
            cobj = Checkpoint(self.src, self.dest, self.get_workq(), self.totalsize)
            pickle.dump(cobj, f, pickle.HIGHEST_PROTOCOL)
        # POSIX requires rename to be atomic
        os.rename(tmp_file, self.checkpoint_file)

    def process(self):
        """
        The only work is "copy"
        TODO: clean up other actions such as mkdir/fini_check
        """
        if not G.use_store:
            curtime = MPI.Wtime()
            if curtime - self.checkpoint_last > self.checkpoint_interval:
                self.do_no_interrupt_checkpoint()
                logger.info("Checkpointing done ...", extra=self.d)
                self.checkpoint_last = curtime

        work = self.deq()
        self.reduce_items += 1
        if isinstance(work, FileChunk):
            self.do_copy(work)
        else:
            logger.warn("Unknown work object: %s" % work, extra=self.d)

    def reduce_init(self, buf):
        buf['cnt_filesize'] = self.cnt_filesize

    def reduce(self, buf1, buf2):
        buf1['cnt_filesize'] += buf2['cnt_filesize']
        return buf1

    def reduce_report(self, buf):
        out = ""
        if self.totalsize != 0:
            out += "%.2f %% finished, " % (100 * float(buf['cnt_filesize']) / self.totalsize)

        out += "%s copied" % bytes_fmt(buf['cnt_filesize'])

        if self.circle.reduce_time_interval != 0:
            rate = float(buf['cnt_filesize'] - self.cnt_filesize_prior) / self.circle.reduce_time_interval
            self.cnt_filesize_prior = buf['cnt_filesize']
            out += ", estimated transfer rate: %s/s" % bytes_fmt(rate)

        print(out)

    def reduce_finish(self, buf):
        # self.reduce_report(buf)
        pass

    def epilogue(self):
        global taskloads
        self.wtime_ended = MPI.Wtime()
        taskloads = self.circle.comm.gather(self.reduce_items)
        if self.circle.rank == 0:
            if self.totalsize == 0:
                return
            tlapse = self.wtime_ended - self.wtime_started
            rate = float(self.totalsize) / tlapse
            print("\nFCP Epilogue:\n")
            print("\t{:<20}{:<20}".format("Ending at:", utils.current_time()))
            print("\t{:<20}{:<20}".format("Completed in:", utils.conv_time(tlapse)))
            print("\t{:<20}{:<20}".format("Transfer Rate:", "%s/s" % bytes_fmt(rate)))
            print("\t{:<20}{:<20}".format("FCP Loads:", "%s" % taskloads))

    def read_then_write(self, rfd, wfd, work, num_of_bytes, m):
        """ core entry point for copy action: first read then write.

        @param num_of_bytes: the exact amount of bytes we will copy
        @return: False if unsuccessful.

        """
        buf = None
        try:
            buf = readn(rfd, num_of_bytes)
        except IOError:
            self.logger.error("Failed to read %s", work.src, extra=self.d)
            return False

        try:
            writen(wfd, buf)
        except IOError:
            self.logger.error("Failed to write %s", work.dest, extra=self.d)
            return False

        if m:
            m.update(buf)

        return True

    def write_bytes(self, rfd, wfd, work):
        os.lseek(rfd, work.offset, os.SEEK_SET)
        os.lseek(wfd, work.offset, os.SEEK_SET)

        m = None
        if self.do_checksum:
            m = hashlib.sha1()

        remaining = work.length
        while remaining != 0:
            if remaining >= self.blocksize:
                self.read_then_write(rfd, wfd, work, self.blocksize, m)
                remaining -= self.blocksize
            else:
                self.read_then_write(rfd, wfd, work, remaining, m)
                remaining = 0

        if self.do_checksum:
            ck = ChunkSum(work.dest, offset=work.offset, length=work.length,
                          digest=m.hexdigest())
            self.chunksums.append(ck)


def err_and_exit(msg, code):
    if comm.rank == 0:
        print("\n%s" % msg)
    MPI.Finalize()
    sys.exit(0)


def check_dbstore_resume_condition(rid):
    global circle

    local_checkpoint_cnt = 0
    local_dbfile_cnt = 0
    db_file = "workq.%s-%s" % (rid, circle.rank)
    db_full = os.path.join(".pcircle", db_file)
    chk_file = "workq.%s-%s.CHECK_OK" % (rid, circle.rank)
    chk_full = os.path.join(".pcircle", chk_file)
    if not os.path.exists(db_full):
        err_and_exit("Resume condition not met, can't locate %s" % db_file, 0)
    else:
        local_dbfile_cnt = 1
    if not os.path.exists(chk_full):
        err_and_exit("Resume condition not met, can't locate %s" % chk_file, 0)
    else:
        local_checkpoint_cnt = 1
    total_checkpoint_cnt = circle.comm.allreduce(local_checkpoint_cnt)
    total_dbfile_cnt = circle.comm.allreduce(local_dbfile_cnt)
    if total_dbfile_cnt != 0 and total_checkpoint_cnt == total_dbfile_cnt:
        if circle.rank == 0:
            print("Resume condition ... OK\n")
    else:
        if circle.rank == 0:
            err_and_exit("Resume conditon not be met: mismatch db and check file", 0)

    return chk_full, db_full


def check_path(isrc, idest):
    """ verify and return target destination"""
    isrc = os.path.abspath(isrc)
    idest = os.path.abspath(idest)

    if os.path.exists(isrc) and os.path.isfile(isrc):
        err_and_exit("Error: source [%s] is a file, directory required" % isrc, 0)

    if os.path.exists(idest) and os.path.isfile(idest):
        err_and_exit("Error: destination [%s] is a file, directory required" % idest, 0)

    if not os.path.exists(isrc) or not os.access(isrc, os.R_OK):
        err_and_exit("source directory %s is not readable" % isrc, 0)

    if os.path.exists(idest) and not ARGS.force:
        err_and_exit("Destination [%s] exists, will not overwrite!" % idest, 0)

    # idest doesn't exits at this point
    # we check if its parent exists
    dest_parent = os.path.dirname(idest)

    if not (os.path.exists(dest_parent) and os.access(dest_parent, os.W_OK)):
        err_and_exit("Error: destination [%s] is not accessible" % dest_parent, 0)


def set_chunksize(pcp, tsz):
    if ARGS.adaptive:
        pcp.set_adaptive_chunksize(tsz)
    else:
        pcp.set_fixed_chunksize(utils.conv_unit(ARGS.chunksize))


def mem_start():
    global circle
    src = os.path.abspath(ARGS.src)
    src = os.path.realpath(src)  # the starting point can't be a sym-linked path
    dest = os.path.abspath(ARGS.dest)
    # dest = check_path(circle, src, dest)

    treewalk = FWalk(circle, src, dest, force=ARGS.force)

    circle.begin(treewalk)
    circle.finalize(reduce_interval=ARGS.reduce_interval)
    tsz = treewalk.epilogue()

    pcp = FCP(circle, src, dest, treewalk=treewalk,
              totalsize=tsz, do_checksum=ARGS.checksum, hostcnt=NUM_OF_HOSTS)

    set_chunksize(pcp, tsz)

    pcp.checkpoint_interval = ARGS.checkpoint_interval

    if ARGS.checkpoint_id:
        pcp.set_checkpoint_file(".pcp_workq.%s.%s" % (ARGS.checkpoint_id, circle.rank))
    else:
        ts = utils.timestamp()
        circle.comm.bcast(ts)
        pcp.set_checkpoint_file(".pcp_workq.%s.%s" % (ts, circle.rank))

    circle.begin(pcp)
    circle.finalize(reduce_interval=ARGS.reduce_interval)
    pcp.cleanup()

    return treewalk, pcp, tsz


def get_workq_size(workq):
    if workq is None:
        return 0
    sz = 0
    for w in workq:
        sz += w['length']
    return sz


def verify_checkpoint(chk_file, total_checkpoint_cnt):
    if total_checkpoint_cnt == 0:
        if circle.rank == 0:
            print("")
            print("Error: Can't find checkpoint file: %s" % chk_file)
            print("")

        circle.exit(0)


def mem_resume(rid):
    global circle
    dmsg = {"rank": "rank %s" % circle.rank}
    oldsz, tsz, sz = 0, 0, 0
    cobj = None
    timestamp = None
    workq = None
    src = None
    dest = None
    local_checkpoint_cnt = 0
    chk_file = ".pcp_workq.%s.%s" % (rid, circle.rank)

    if os.path.exists(chk_file):
        local_checkpoint_cnt = 1
        with open(chk_file, "rb") as f:
            try:
                cobj = pickle.load(f)
                sz = get_workq_size(cobj.workq)
                src = cobj.src
                dest = cobj.dest
                oldsz = cobj.totalsize
            except Exception as e:
                logger.error("error reading %s" % chk_file, extra=dmsg)
                circle.comm.Abort()

    logger.debug("located chkpoint %s, sz=%s, local_cnt=%s" %
                 (chk_file, sz, local_checkpoint_cnt), extra=dmsg)

    # do we have any checkpoint files?

    total_checkpoint_cnt = circle.comm.allreduce(local_checkpoint_cnt)
    logger.debug("total_checkpoint_cnt = %s" % total_checkpoint_cnt, extra=dmsg)
    verify_checkpoint(chk_file, total_checkpoint_cnt)

    # acquire total size
    tsz = circle.comm.allreduce(sz)
    if tsz == 0:
        if circle.rank == 0:
            print("Recovery size is 0 bytes, can't proceed.")
        circle.exit(0)

    if circle.rank == 0:
        print("Original size: %s" % bytes_fmt(oldsz))
        print("Recovery size: %s" % bytes_fmt(tsz))

    # second task
    pcp = FCP(circle, src, dest,
              totalsize=tsz, workq=cobj.workq,
              hostcnt=NUM_OF_HOSTS)

    set_chunksize(pcp, tsz)

    pcp.checkpoint_interval = ARGS.checkpoint_interval
    if rid:
        pcp.set_checkpoint_file(".pcp_workq.%s.%s" % (rid, circle.rank))
    else:
        ts = utils.timestamp()
        circle.comm.bcast(ts)
        pcp.set_checkpoint_file(".pcp_workq.%s.%s" % (ts, circle.rank))
    circle.begin(pcp)
    circle.finalize(reduce_interval=ARGS.reduce_interval)
    pcp.cleanup()

    return pcp, tsz


def get_oldsize(chk_file):
    totalsize = 0
    with open(chk_file) as f:
        totalsize = int(f.read())
    return totalsize


def fix_opt(treewalk):
    flist = treewalk.flist
    for f in flist:
        dpath = destpath(treewalk.src, treewalk.dest, f.path)  # f[0]
        try:
            os.lchown(dpath, f.st_uid, f.st_gid)
            os.chmod(dpath, f.st_mode)
        except OSError as e:
            logging.warn(e)


def parse_and_bcast():
    global ARGS
    parse_flags = True
    if MPI.COMM_WORLD.rank == 0:
        try:
            ARGS = parse_args()
        except argparse.ArgumentError as e:
            parse_flags = False
    parse_flags = MPI.COMM_WORLD.bcast(parse_flags)
    if parse_flags:
        ARGS = MPI.COMM_WORLD.bcast(ARGS)
    else:
        sys.exit(0)

    if MPI.COMM_WORLD.rank == 0 and ARGS.loglevel == "debug":
        print("ARGUMENT DEBUG: %s", ARGS)


def store_resume(rid):
    global circle, ARGS
    dmsg = {"rank": "rank %s" % circle.rank}

    # check and exchange old dataset size
    oldsz = 0
    chk_file, db_file = check_resume_condition(rid)
    if circle.rank == 0:
        oldsz = get_oldsize(chk_file)
    oldsz = circle.comm.bcast(oldsz)

    # check and exchange recovery size
    localsz = circle.workq.fsize
    tsz = circle.comm.allreduce(localsz)

    if circle.rank == 0:
        print("Original size: %s" % bytes_fmt(oldsz))
        print("Recovery size: %s" % bytes_fmt(tsz))

    if tsz == 0:
        if circle.rank == 0:
            print("Recovery size is 0 bytes, can't proceed.")
        circle.exit(0)

    # src, dest probably not needed here anymore.
    src = os.path.abspath(ARGS.src)
    dest = os.path.abspath(ARGS.dest)

    # resume mode, we don't check destination path
    # dest = check_path(circle, src, dest)
    # note here that we use resume flag
    pcp = FCP(circle, src, dest, resume=True,
              totalsize=tsz, do_checksum=ARGS.checksum,
              hostcnt=NUM_OF_HOSTS)

    pcp.checkpoint_file = chk_file

    set_chunksize(pcp, tsz)
    circle.begin(pcp)
    circle.finalize(cleanup=True)

    return pcp, tsz


def store_start():
    global circle
    src = os.path.abspath(ARGS.src)
    dest = os.path.abspath(ARGS.dest)
    # dest = check_path(circle, src, dest)

    treewalk = FWalk(circle, src, dest, force=ARGS.force)
    circle.begin(treewalk)
    treewalk.flushdb()

    circle.finalize(cleanup=False)
    total_sz = treewalk.epilogue()

    pcp = FCP(circle, src, dest, treewalk=treewalk,
              totalsize=total_sz, do_checksum=ARGS.checksum, hostcnt=NUM_OF_HOSTS)
    set_chunksize(pcp, total_sz)
    circle.begin(pcp)

    # cleanup the db trails
    treewalk.cleanup()
    pcp.cleanup()

    # we hold this off until last
    # since it is possible pcheck will need the database
    # as well
    # circle.finalize(cleanup=True)

    return treewalk, pcp, total_sz


def get_dbname():
    global ARGS
    name = None
    if ARGS.checkpoint_id:
        name = "workq.%s" % ARGS.checkpoint_id
    elif ARGS.rid:
        name = "workq.%s" % ARGS.rid[0]
    else:
        ts = utils.timestamp()
        MPI.COMM_WORLD.bcast(ts)
        name = "workq.%s" % ts
    return name


def tally_hosts():
    """ How many physical hosts are there?
    """
    global NUM_OF_HOSTS
    localhost = MPI.Get_processor_name()
    hosts = MPI.COMM_WORLD.gather(localhost)
    if MPI.COMM_WORLD.rank == 0:
        NUM_OF_HOSTS = len(set(hosts))
    NUM_OF_HOSTS = MPI.COMM_WORLD.bcast(NUM_OF_HOSTS)


def aggregate_checksums(localChunkSums, dbname="checksums.db"):
    signature, size = None, None

    if comm.rank == 0:
        if os.path.exists(dbname):
            os.remove(dbname)

        # init database
        db = DbSum(dbname)

        # add self
        for chksum in localChunkSums:
            db.put(chksum)

        # ask from the others
        for p in xrange(1, comm.size):
            chunksums = comm.recv(source=p)
            for chksum in chunksums:
                db.put(chksum)
    else:
        comm.send(localChunkSums, dest=0)

    comm.Barrier()

    if comm.rank == 0:
        signature = db.fsum()
        size = db.size()

    return size, signature


def gen_signature(pcp, totalsize):
    """ Generate a signature for dataset, it assumes the checksum
       option is set and done """
    if comm.rank == 0:
        print("\nAggregating checksums for a dataset signature ...\n")
    tbegin = MPI.Wtime()
    size, sig = aggregate_checksums(pcp.chunksums)
    tend = MPI.Wtime()
    if comm.rank == 0:
        print("\t{:<20}{:<20}".format("Aggregated chunks:", size))
        print("\t{:<20}{:<20}".format("Running time:", utils.conv_time(tend - tbegin)))
        print("\t{:<20}{:<20}".format("SHA1 Signature:", sig))
        with open(ARGS.output, "w") as f:
            f.write("sha1: %s\n" % sig)
            f.write("chunksize: %s\n" % pcp.chunksize)
            f.write("fcp version: %s\n" % __version__)
            f.write("src: %s\n" % pcp.src)
            f.write("destination: %s\n" % pcp.dest)
            f.write("date: %s\n" % utils.current_time())
            f.write("totoalsize: %s\n" % utils.bytes_fmt(totalsize))

        print("\t{:<20}{:<20}".format("Signature File:", ARGS.output))


def main():
    global ARGS, logger, circle

    # This might be an overkill function
    signal.signal(signal.SIGINT, sig_handler)

    treewalk, pcp, totalsize = None, None, None

    parse_and_bcast()
    tally_hosts()
    G.loglevel = ARGS.loglevel
    G.use_store = ARGS.use_store

    if ARGS.fix_opt and os.geteuid() == 0:
        G.fix_opt = True

    if ARGS.disable_preserve:
        G.preserve = False

    if ARGS.signature:
        ARGS.checksum = True

    if comm.rank == 0:
        check_path(ARGS.src, ARGS.dest)

    dbname = get_dbname()

    G.logfile = ".pcircle-%s.log" % MPI.COMM_WORLD.Get_rank()
    logger = utils.getLogger("fcp")

    if ARGS.rid:
        circle = Circle(dbname=dbname, reduce_interval=ARGS.reduce_interval, resume=True)
    else:
        circle = Circle(dbname=dbname, reduce_interval=ARGS.reduce_interval)

    if circle.rank == 0:
        print("Running Parameters:\n")
        print("\t{:<20}{:<20}".format("Starting at:", utils.current_time()))
        print("\t{:<20}{:<20}".format("FCP version:", __version__))
        print("\t{:<20}{:<20}".format("Num of Hosts:", NUM_OF_HOSTS))
        print("\t{:<20}{:<20}".format("Num of Processes:", MPI.COMM_WORLD.Get_size()))
        print("\t{:<20}{:<20}".format("Source:", os.path.abspath(ARGS.src)))
        print("\t{:<20}{:<20}".format("Destination:", os.path.abspath(ARGS.dest)))
        print("\t{:<20}{:<20}".format("Overwrite:", "%r" % ARGS.force))
        print("\t{:<20}{:<20}".format("Checksum verify:", "%r" % ARGS.checksum))
        print("\t{:<20}{:<20}".format("Dataset signature:", "%r" % ARGS.signature))
        print("\t{:<20}{:<20}".format("Stripe Preserve:", "%r" % G.preserve))

    # TODO: there are some redundant code brought in by merging
    # memory/store-based checkpoint/restart, need to be refactored
    if ARGS.rid:
        if G.use_store:
            pcp, totalsize = store_resume(ARGS.rid[0])
        else:
            treewalk, pcp, totalsize = mem_resume(ARGS.rid[0])
    else:
        if G.use_store:
            treewalk, pcp, totalsize = store_start()
        else:
            treewalk, pcp, totalsize = mem_start()

    if ARGS.pause and ARGS.checksum:
        if circle.rank == 0:
            # raw_input("\n--> Press any key to continue ...\n")
            print("Pause, resume after %s seconds ..." % ARGS.pause)
            sys.stdout.flush()
        time.sleep(ARGS.pause)
        circle.comm.Barrier()

    # third task
    if ARGS.checksum:
        pcheck = PVerify(circle, pcp, totalsize)
        circle.begin(pcheck)
        tally = pcheck.fail_tally()

        if circle.rank == 0:
            print("")
            if tally == 0:
                print("\t{:<20}{:<20}".format("Result:", "PASS"))
            else:
                print("\t{:<20}{:<20}".format("Result:", "FAILED"))

        comm.Barrier()

        if ARGS.signature:
            gen_signature(pcp, totalsize)

    if ARGS.fix_opt and treewalk and os.geteuid() == 0:
        print("\nFixing ownership and permissions ...")
        fix_opt(treewalk)

    if treewalk:
        treewalk.cleanup()

    if pcp:
        pcp.epilogue()
        pcp.cleanup()

    # if circle:
    #     circle.finalize(cleanup=True)
    # TODO: a close file error can happen when circle.finalize()
    #
    if isinstance(circle.workq, DbStore):
        circle.workq.cleanup()



if __name__ == "__main__":
    main()
