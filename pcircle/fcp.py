#!/usr/bin/env python
"""
PCP provides MPI-based parallel data transfer functionality.

Author: Feiyi Wang (fwang2@ornl.gov)

"""
from __future__ import print_function

import time
import stat
import os
import shutil
import os.path
import hashlib
import sys
import signal
import resource
import sqlite3
import math
import cPickle as pickle
from collections import Counter
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
from globals import Tally as T
from dbstore import DbStore
from dbsum import MemSum
from fsum import export_checksum2
from fdef import FileItem
from _version import get_versions
from mpihelper import ThrowingArgumentParser, parse_and_bcast
from bfsignature import BFsignature

__version__ = get_versions()['version']
del get_versions

args = None
circle = None
treewalk = None
fcp = None
num_of_hosts = 0
taskloads = []
comm = MPI.COMM_WORLD
dmsg = {"rank": "rank %s" % comm.rank}
log = utils.getLogger(__name__)


def err_and_exit(msg, code=0):
    if comm.rank == 0:
        print("\n%s\n" % msg)
    MPI.Finalize()
    sys.exit(0)


def gen_parser():
    parser = ThrowingArgumentParser(description="Parallel Data Copy",
                                     epilog="Please report issues to fwang2@ornl.gov")
    parser.add_argument("--version", action="version", version="{version}".format(version=__version__))
    parser.add_argument("-v", "--verbosity", action="count", help="increase verbosity")
    parser.add_argument("--loglevel", default="error", help="log level, default ERROR")
    parser.add_argument("--chunksize", metavar="sz", default="1m", help="chunk size (KB, MB, GB, TB), default: 1MB")
    parser.add_argument("--adaptive", action="store_true", default=True, help="Adaptive chunk size")
    parser.add_argument("--reduce-interval", metavar="s", type=int, default=10, help="interval, default 10s")
    parser.add_argument("--no-fixopt", action="store_true", help="skip fixing ownership, permssion, timestamp")
    parser.add_argument("--verify", action="store_true", help="verify after copy, default: off")
    parser.add_argument("-s", "--signature", action="store_true", help="aggregate checksum for signature, default: off")
    parser.add_argument("-p", "--preserve", action="store_true", help="Preserving meta, default: off")
    # using bloom filter for signature genearation, all chunksums info not available at root process anymore
    parser.add_argument("-o", "--output", metavar='', default="sha1-%s.sig" % utils.timestamp2(), help="sha1 output file")
    parser.add_argument("-f", "--force", action="store_true", help="force overwrite")
    parser.add_argument("-t", "--cptime", metavar="s", type=int, default=3600, help="checkpoint interval, default: 1hr")
    parser.add_argument("-i", "--cpid", metavar="ID", default=None, help="checkpoint file id, default: timestamp")
    parser.add_argument("-r", "--rid", dest="rid", metavar="ID", help="resume ID, required in resume mode")
    parser.add_argument("--pause", metavar="s", type=int, help="pause a delay (seconds) after copy, test only")
    parser.add_argument("--item", type=int, default=100000, help="number of items stored in memory, default: 100000")
    parser.add_argument("src", nargs='+', help="copy from")
    parser.add_argument("dest", help="copy to")

    return parser


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
                 verify=False,
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
        self.checkpoint_db = None
        self.src = src
        self.dest = os.path.abspath(dest)

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
        if self.treewalk:
            log.debug("treewalk files = %s" % treewalk.flist, extra=self.d)

        # fini_check
        self.fini_cnt = Counter()

        # verify
        self.verify = verify
        self.use_store = False
        if self.verify:
            self.chunksums_mem = []
            self.chunksums_buf = []

        # checkpointing
        self.checkpoint_interval = sys.maxsize
        self.checkpoint_last = MPI.Wtime()

        if self.circle.rank == 0:
            print("Start copying process ...")


    def set_fixed_chunksize(self, sz):
        self.chunksize = sz

    def set_adaptive_chunksize(self, totalsz):
        self.chunksize = utils.calc_chunksize(totalsz)
        if self.circle.rank == 0:
            print("Adaptive chunksize: %s" % bytes_fmt(self.chunksize))

    def cleanup(self):

        # remove checkpoint file
        if self.checkpoint_file and os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)
        if self.checkpoint_db and os.path.exists(self.checkpoint_db):
            os.remove(self.checkpoint_db)

        # remove provided checkpoint file
        if G.resume and G.chk_file and os.path.exists(G.chk_file):
            os.remove(G.chk_file)
        if G.resume and G.chk_file_db and os.path.exists(G.chk_file_db):
            os.remove(G.chk_file_db)

        # remove chunksums file
        if self.verify:
            if hasattr(self, "chunksums_db"):
                self.chunksums_db.cleanup()

        # we need to do this because if last job didn't finish cleanly
        # the fwalk files can be found as leftovers
        # and if fcp cleanup has a chance, it should clean up that
        """
        fwalk = "%s/fwalk.%s" % (G.tempdir, self.circle.rank)
        if os.path.exists(fwalk):
            os.remove(fwalk)
        """

    def new_fchunk(self, fitem):
        fchunk = FileChunk()  # default cmd = copy
        fchunk.src = fitem.path
        fchunk.dest = destpath(fitem, self.dest)
        return fchunk

    def enq_file(self, fi):
        """ Process a single file, represented by "fi" - FileItem
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

        log.debug("enq_file(): %s, size = %s, workcnt = %s" % (fi.path, fi.st_size, workcnt),
                     extra=self.d)

    def handle_fitem(self, fi):
        if os.path.islink(fi.path):
            dest = destpath(fi, self.dest)
            linkto = os.readlink(fi.path)
            try:
                os.symlink(linkto, dest)
            except Exception as e:
                log.debug("%s, skipping sym link %s" % (e, fi.path), extra=self.d)
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
        log.info("create() starts, flist length = %s" % len(self.treewalk.flist),
                    extra=self.d)

        # flist in memory
        if len(self.treewalk.flist) > 0:
            for fi in self.treewalk.flist:
                self.handle_fitem(fi)

        # flist in buf
        if len(self.treewalk.flist_buf) > 0:
            for fi in self.treewalk.flist_buf:
                self.handle_fitem(fi)

        # flist in database
        if self.treewalk.use_store:
            while self.treewalk.flist_db.qsize > 0:
                fitems, _ = self.treewalk.flist_db.mget(G.DB_BUFSIZE)
                for fi in fitems:
                    self.handle_fitem(fi)
                self.treewalk.flist_db.mdel(G.DB_BUFSIZE)

        # both memory and databse checkpoint
        if self.checkpoint_file:
            self.do_no_interrupt_checkpoint()
            self.checkpoint_last = MPI.Wtime()

        # gather total_chunks
        self.circle.comm.barrier()
        G.total_chunks = self.circle.comm.allreduce(self.workcnt, op=MPI.SUM)
        #G.total_chunks = self.circle.comm.bcast(G.total_chunks)
        #print("Total chunks: ",G.total_chunks)


    def do_open2(self, k, flag):
        """ open path 'k' with 'flags' """
        try:
            fd = os.open(k, flag)
        except OSError as e:
            if e.errno == 28:  # no space left
                log.error("Critical error: %s, exit!" % e, extra=self.d)
                self.circle.exit(0)  # should abort
            else:
                log.error("OSError({0}):{1}, skipping {2}".format(e.errno, e.strerror, k), extra=self.d)
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

        rfd = self.do_open2(src, os.O_RDONLY)
        if rfd < 0:
            return False
        wfd = self.do_open2(dest, os.O_WRONLY | os.O_CREAT)
        if wfd < 0:
            if args.force:
                try:
                    os.unlink(dest)
                except OSError as e:
                    log.error("Failed to unlink %s, %s " % (dest, e), extra=self.d)
                    return False
                else:
                    wfd = self.do_open2(dest, os.O_WRONLY)
            else:
                log.error("Failed to create output file %s" % dest, extra=self.d)
                return False

        # do the actual copy
        self.write_bytes(rfd, wfd, work)

        # update tally
        self.cnt_filesize += work.length

        if G.verbosity > 2:
            log.debug("Transferred %s bytes from:\n\t [%s] to [%s]" %
                         (self.cnt_filesize, src, dest), extra=self.d)

        return True

    def do_no_interrupt_checkpoint(self):
        a = Thread(target=self.do_checkpoint)
        a.start()
        a.join()
        if G.verbosity > 0:
            print("Checkpoint: %s" % self.checkpoint_file)

    def do_checkpoint(self):
        tmp_file = self.checkpoint_file + ".part"
        with open(tmp_file, "wb") as f:
            self.circle.workq.extend(self.circle.workq_buf)
            self.circle.workq_buf.clear()
            cobj = Checkpoint(self.src, self.dest, self.get_workq(), self.totalsize)
            pickle.dump(cobj, f, pickle.HIGHEST_PROTOCOL)
        # POSIX requires rename to be atomic
        os.rename(tmp_file, self.checkpoint_file)

        # copy workq_db database file
        if hasattr(self.circle, "workq_db") and len(self.circle.workq_db) > 0:
            self.checkpoint_db = self.checkpoint_file + ".db"
            if not G.resume:
                shutil.copy2(self.circle.dbname, self.checkpoint_db)
            else:
                # in resume mode, make a copy of current workq db file, which is provided checkpoint db file
                self.workdir = os.getcwd()
                existingCheckpoint = os.path.join(self.workdir,".pcp_workq.%s.%s.db" % (G.rid, self.circle.rank))
                shutil.copy2(existingCheckpoint,self.checkpoint_db)

    def process(self):
        """
        The only work is "copy"
        TODO: clean up other actions such as mkdir/fini_check
        """
        if not G.use_store:
            curtime = MPI.Wtime()
            if curtime - self.checkpoint_last > self.checkpoint_interval:
                self.do_no_interrupt_checkpoint()
                log.info("Checkpointing done ...", extra=self.d)
                self.checkpoint_last = curtime

        work = self.deq()
        self.reduce_items += 1
        if isinstance(work, FileChunk):
            self.do_copy(work)
        else:
            log.warn("Unknown work object: %s" % work, extra=self.d)
            err_and_exit("Not a correct workq format")

    def reduce_init(self, buf):
        buf['cnt_filesize'] = self.cnt_filesize
        if sys.platform == 'darwin':
            buf['mem_snapshot'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        else:
            buf['mem_snapshot'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024

    def reduce(self, buf1, buf2):
        buf1['cnt_filesize'] += buf2['cnt_filesize']
        buf1['mem_snapshot'] += buf2['mem_snapshot']
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

        out += ", memory usage: %s" % bytes_fmt(buf['mem_snapshot'])
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
                print("\nZero filesize detected, done.\n")
                return
            tlapse = self.wtime_ended - self.wtime_started
            rate = float(self.totalsize) / tlapse
            print("\nFCP Epilogue:\n")
            print("\t{:<20}{:<20}".format("Ending at:", utils.current_time()))
            print("\t{:<20}{:<20}".format("Completed in:", utils.conv_time(tlapse)))
            print("\t{:<20}{:<20}".format("Transfer Rate:", "%s/s" % bytes_fmt(rate)))
            print("\t{:<20}{:<20}".format("Use store chunksums:", "%s" % self.use_store))
            print("\t{:<20}{:<20}".format("Use store workq:", "%s" % self.circle.use_store))
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
        if self.verify:
            m = hashlib.sha1()

        remaining = work.length
        while remaining != 0:
            if remaining >= self.blocksize:
                self.read_then_write(rfd, wfd, work, self.blocksize, m)
                remaining -= self.blocksize
            else:
                self.read_then_write(rfd, wfd, work, remaining, m)
                remaining = 0

        if self.verify:
            # use src path here
            ck = ChunkSum(work.dest, offset=work.offset, length=work.length,
                          digest=m.hexdigest())

            if len(self.chunksums_mem) < G.memitem_threshold:
                self.chunksums_mem.append(ck)
            else:
                self.chunksums_buf.append(ck)
                if len(self.chunksums_buf) == G.DB_BUFSIZE:
                    if self.use_store == False:
                        self.workdir = os.getcwd()
                        self.chunksums_dbname = "%s/chunksums.%s" % (G.tempdir, self.circle.rank)
                        self.chunksums_db = DbStore(dbname=self.chunksums_dbname)
                        self.use_store = True
                    self.chunksums_db.mput(self.chunksums_buf)
                    del self.chunksums_buf[:]


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
        err_and_exit("Resume conditon not be met: mismatch db and check file", 0)

    return chk_full, db_full


def check_source_and_target(isrc, idest):
    """ verify and return target destination, isrc is iterable, idest is not.
    """
    checked_src = []
    checked_dup = set()

    is_dest_exist, is_dest_dir, is_dest_file, is_dest_parent_ok = False, False, False, False

    idest = os.path.abspath(idest)

    if os.path.exists(idest):
        if not os.access(idest, os.W_OK):
            err_and_exit("Destination is not accessible", 0)
        is_dest_exist = True
        if os.path.isfile(idest):
            is_dest_file = True
        elif os.path.isdir(idest):
            is_dest_dir = True
    else:
        # idest doesn't exits at this point
        # we check if its parent exists
        dest_parent = os.path.dirname(idest)

        if not (os.path.exists(dest_parent) and os.access(dest_parent, os.W_OK)):
            err_and_exit("Error: destination [%s] is not accessible" % dest_parent, 0)
        is_dest_parent_ok = True

    for ele in isrc:
        elepath = os.path.abspath(ele)
        elefi = FileItem(elepath)
        elefi.dirname = os.path.dirname(elepath) # save dirname for proper dest construction
        elebase = os.path.basename(elepath)
        if elebase in checked_dup:
            err_and_exit("Error: source name conflict detected: [%s]" % elepath)
        checked_dup.add(elebase)

        if os.path.exists(elepath) and os.access(elepath, os.R_OK):
            checked_src.append(elefi)
        else:
            err_and_exit("Error: source [%s] doesn't exist or not accessible." % ele, 0)

    if len(checked_src) == 0:
        err_and_exit("Error, no valid input", 0)
    elif len(checked_src) == 1 and os.path.isfile(checked_src[0].path):
        if is_dest_exist:
            if is_dest_file and args.force:
                try:
                    os.remove(idest)
                except OSError as e:
                    err_and_exit("Error: can't overwrite %s" % idest, 0)
                else:
                    G.copytype = 'file2file'
            elif is_dest_dir:
                G.copytype = "file2dir"
        elif is_dest_parent_ok:
            G.copytype = 'file2file'
        else:
            err_and_exit("Error: can't detect correct copy type!", 0)
    elif len(checked_src) == 1 and not is_dest_exist:
        G.copytype = "dir2dir"
    elif len(checked_src) == 1 and is_dest_dir:
        if not args.force:
            err_and_exit("Error: destination [%s] exists, will not overwrite!" % idest)
        else:
            G.copytype = "dir2dir"
    else:
        # multiple sources, destination must be directory

        if not os.path.exists(idest):
            err_and_exit("Error: target directory %s doesn't exist!" % idest)

        if os.path.exists(idest) and os.path.isfile(idest):
            err_and_exit("Error: destination [%s] is a file, directory required" % idest, 0)

        # if is_dest_exist and not (args.force or args.rid):
        #     err_and_exit("Destination [%s] exists, will not overwrite!" % idest, 0)

        G.copytype = "file2dir"

    return checked_src, idest

def set_chunksize(pcp, tsz):
    if args.adaptive:
        pcp.set_adaptive_chunksize(tsz)
    else:
        pcp.set_fixed_chunksize(utils.conv_unit(args.chunksize))


def prep_recovery():
    """ Prepare for checkpoint recovery, return recovered workq """
    global args, circle

    oldsz, tsz, sz = 0, 0, 0
    sz_db = 0
    cobj = None
    local_checkpoint_cnt = 0
    chk_file = ".pcp_workq.%s.%s" % (args.rid, circle.rank)
    chk_file_db = ".pcp_workq.%s.%s.db" % (args.rid, circle.rank)
    G.chk_file = chk_file
    G.chk_file_db = chk_file_db

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
                log.error("error reading %s" % chk_file, extra=dmsg)
                circle.comm.Abort()

    if os.path.exists(chk_file_db):
        qsize_db = 0
        local_checkpoint_cnt = 1
        conn = sqlite3.connect(chk_file_db)
        cur = conn.cursor()
        try:
            cur.execute("SELECT * FROM checkpoint")
            qsize_db, sz_db = cur.fetchone()
        except sqlite3.OperationalError as e:
            pass

    log.debug("located chkpoint %s, sz=%s, local_cnt=%s" %
                 (chk_file, sz, local_checkpoint_cnt), extra=dmsg)

    total_checkpoint_cnt = circle.comm.allreduce(local_checkpoint_cnt)
    log.debug("total_checkpoint_cnt = %s" % total_checkpoint_cnt, extra=dmsg)
    verify_checkpoint(chk_file, total_checkpoint_cnt)

    # acquire total size
    total_sz_mem = circle.comm.allreduce(sz)
    total_sz_db = circle.comm.allreduce(sz_db)
    T.total_filesize = total_sz_mem + total_sz_db
    if T.total_filesize == 0:
        if circle.rank == 0:
            print("\nRecovery size is 0 bytes, can't proceed.")
        circle.exit(0)

    if circle.rank == 0:
        print("\nResume copy\n")
        print("\t{:<20}{:<20}".format("Original size:", bytes_fmt(oldsz)))
        print("\t{:<20}{:<20}".format("Recovery size:", bytes_fmt(T.total_filesize)))
        print("")

    return cobj.workq


def fcp_start():
    global circle, fcp, treewalk

    workq = None  # if fresh start, workq is None

    if not args.rid: # if not in recovery
        treewalk = FWalk(circle, G.src, G.dest, force=args.force)
        circle.begin(treewalk)
        circle.finalize()
        treewalk.epilogue()
    else:  # okay, let's do checkpoint recovery
        workq = prep_recovery()

    circle = Circle(dbname="fcp")
    fcp = FCP(circle, G.src, G.dest,
              treewalk=treewalk,
              totalsize=T.total_filesize,
              verify=args.verify,
              workq=workq,
              hostcnt=num_of_hosts)

    set_chunksize(fcp, T.total_filesize)
    fcp.checkpoint_interval = args.cptime
    fcp.checkpoint_file = ".pcp_workq.%s.%s" % (args.cpid, circle.rank)

    circle.begin(fcp)
    circle.finalize()
    fcp.epilogue()

def get_workq_size(workq):
    """ workq is a list of FileChunks, we iterate each and summarize the size,
    which amounts to work to be done """
    if workq is None:
        return 0
    sz = 0
    for w in workq:
        sz += w.length
    return sz


def verify_checkpoint(chk_file, total_checkpoint_cnt):
    if total_checkpoint_cnt == 0:
        if circle.rank == 0:
            print("")
            print("Error: Can't find checkpoint file: %s" % chk_file)
            print("")

        circle.exit(0)


def get_oldsize(chk_file):
    totalsize = 0
    with open(chk_file) as f:
        totalsize = int(f.read())
    return totalsize


def do_fix_opt(optlist):
    """ f is file/dir path """
    for ele in optlist:
        fi, st = ele
        try:
            if not stat.S_ISLNK(st.st_mode):
                if G.am_root:
                    os.lchown(fi, st.st_uid, st.st_gid)
                os.chmod(fi, st.st_mode)
        except OSError as e:
            log.warn("fix-opt: lchown() or chmod(): %s" % e, extra=dmsg)


def fix_opt(treewalk):
    do_fix_opt(treewalk.optlist)
    treewalk.opt_dir_list.sort(reverse=True)
    do_fix_opt(treewalk.opt_dir_list)


#
# def store_resume(rid):
#     global circle, args
#
#     # check and exchange old dataset size
#     oldsz = 0
#     chk_file, db_file = check_resume_condition(rid)
#     if circle.rank == 0:
#         oldsz = get_oldsize(chk_file)
#     oldsz = circle.comm.bcast(oldsz)
#
#     # check and exchange recovery size
#     localsz = circle.workq.fsize
#     tsz = circle.comm.allreduce(localsz)
#
#     if circle.rank == 0:
#         print("Original size: %s" % bytes_fmt(oldsz))
#         print("Recovery size: %s" % bytes_fmt(tsz))
#
#     if tsz == 0:
#         if circle.rank == 0:
#             print("Recovery size is 0 bytes, can't proceed.")
#         circle.exit(0)
#
#     # src, dest probably not needed here anymore.
#     src = os.path.abspath(args.src)
#     dest = os.path.abspath(args.dest)
#
#     # resume mode, we don't check destination path
#     # dest = check_path(circle, src, dest)
#     # note here that we use resume flag
#     pcp = FCP(circle, src, dest, resume=True,
#               totalsize=tsz, do_checksum=args.checksum,
#               hostcnt=num_of_hosts)
#
#     pcp.checkpoint_file = chk_file
#
#     set_chunksize(pcp, tsz)
#     circle.begin(pcp)
#     circle.finalize(cleanup=True)
#
#     return pcp, tsz
#
#
# def store_start():
#     global circle, treewalk, fcp
#     src = os.path.abspath(args.src)
#     dest = os.path.abspath(args.dest)
#     # dest = check_path(circle, src, dest)
#
#     treewalk = FWalk(circle, src, dest, force=args.force)
#     circle.begin(treewalk)
#     treewalk.flushdb()
#
#     circle.finalize(cleanup=False)
#     T.total_filesize = treewalk.epilogue()
#
#     fcp = FCP(circle, src, dest, treewalk=treewalk,
#               totalsize=T.total_filesize, do_checksum=args.checksum, hostcnt=num_of_hosts)
#     set_chunksize(fcp, T.total_filesize)
#     circle.begin(fcp)
#
#     # cleanup the db trails
#     if treewalk:
#         treewalk.cleanup()
#
#     if fcp:
#         fcp.cleanup()

def get_workq_name():
    global args
    name = None
    if args.cpid:
        name = "workq.%s" % args.cpid
    elif args.rid:
        name = "workq.%s" % args.rid[0]
    else:
        ts = utils.timestamp()
        MPI.COMM_WORLD.bcast(ts)
        name = "workq.%s" % ts
    return name

def tally_hosts():
    """ How many physical hosts are there?
    """
    global num_of_hosts
    localhost = MPI.Get_processor_name()
    hosts = MPI.COMM_WORLD.gather(localhost)
    if MPI.COMM_WORLD.rank == 0:
        num_of_hosts = len(set(hosts))
    num_of_hosts = MPI.COMM_WORLD.bcast(num_of_hosts)


def aggregate_checksums(bfsign):
    signature, size, chunksums = None, None, None

    if comm.rank > 0:
        comm.send(bfsign.bitarray, dest=0)
    else:
        for p in xrange(1, comm.size):
            other_bitarray = comm.recv(source=p)
            bfsign.or_bf(other_bitarray)

    comm.Barrier()

    if comm.rank == 0:
        signature = bfsign.gen_signature()

    return signature


def gen_signature(bfsign, totalsize):
    """ Generate a signature for dataset, it assumes the checksum
       option is set and done """
    if comm.rank == 0:
        print("\nAggregating dataset signature ...\n")
    tbegin = MPI.Wtime()
    sig = aggregate_checksums(bfsign)
    tend = MPI.Wtime()
    if comm.rank == 0:
        #print("\t{:<20}{:<20}".format("Aggregated chunks:", size))
        print("\t{:<20}{:<20}".format("Running time:", utils.conv_time(tend - tbegin)))
        print("\t{:<20}{:<20}".format("SHA1 Signature:", sig))
        with open(args.output, "w") as f:
            f.write("sha1: %s\n" % sig)
            f.write("chunksize: %s\n" % fcp.chunksize)
            f.write("fcp version: %s\n" % __version__)
            f.write("src: %s\n" % fcp.src)
            f.write("destination: %s\n" % fcp.dest)
            f.write("date: %s\n" % utils.current_time())
            f.write("totoalsize: %s\n" % utils.bytes_fmt(totalsize))
        #print("\t{:<20}{:<20}".format("Signature File:", export_checksum2(chunksums, args.output)))

def main():
    global args, log, circle, fcp, treewalk
    # This might be an overkill function
    signal.signal(signal.SIGINT, sig_handler)
    args = parse_and_bcast(comm, gen_parser)
    tally_hosts()
    G.loglevel = args.loglevel
    G.fix_opt = False if args.no_fixopt else True
    G.preserve = args.preserve
    G.resume = True if args.cpid else False
    G.reduce_interval = args.reduce_interval
    G.verbosity = args.verbosity
    G.am_root = True if os.geteuid() == 0 else False
    G.memitem_threshold = args.item

    if args.signature:  # with signature implies doing verify as well
        args.verify = True

    if args.rid:
        G.resume = True
        args.force = True
        G.rid = args.rid
        args.signature = False # when recovery, no signature

    if not args.cpid:
        ts = utils.timestamp()
        args.cpid = MPI.COMM_WORLD.bcast(ts)

    G.tempdir = os.path.join(os.getcwd(),(".pcircle" + args.cpid))
    if not os.path.exists(G.tempdir):
        try:
            os.mkdir(G.tempdir)
        except OSError:
            pass

    G.src, G.dest = check_source_and_target(args.src, args.dest)
    dbname = get_workq_name()

    circle = Circle(dbname="fwalk")
    #circle.dbname = dbname

    if circle.rank == 0:
        print("Running Parameters:\n")
        print("\t{:<25}{:<20}".format("Starting at:", utils.current_time()))
        print("\t{:<25}{:<20}".format("FCP version:", __version__))
        print("\t{:<25}{:<20}".format("Source:", utils.choplist(G.src)))
        print("\t{:<25}{:<20}".format("Destination:", os.path.abspath(args.dest)))
        print("\t{:<25}{:<10}{:5}{:<25}{:<10}".format("Num of Hosts:", num_of_hosts, "|",
            "Num of Processes:", comm.size))
        print("\t{:<25}{:<10}{:5}{:<25}{:<10}".format("Overwrite:", "%r" % args.force, "|",
            "Copy Verification:", "%r" % args.verify))
        print("\t{:<25}{:<10}{:5}{:<25}{:<10}".format("Dataset signature:", "%r" % args.signature, "|",
            "Stripe Preserve:", "%r" % G.preserve))
        print("\t{:<25}{:<10}{:5}{:<25}{:<10}".format("Checkpoint interval:", "%s" % utils.conv_time(args.cptime), "|",
            "Checkpoint ID:", "%s" % args.cpid))

        print("\t{:<25}{:<10}".format("Items in memory:", "%r" % G.memitem_threshold))
        #
        if args.verbosity > 0:
            print("\t{:<25}{:<20}".format("Copy Mode:", G.copytype))

    fcp_start()

    if args.pause and args.verify:
        if circle.rank == 0:
            # raw_input("\n--> Press any key to continue ...\n")
            print("Pause, resume after %s seconds ..." % args.pause)
            sys.stdout.flush()
        time.sleep(args.pause)
        circle.comm.Barrier()

    # do checksum verification
    if args.verify:
        circle = Circle(dbname="verify")
        pcheck = PVerify(circle, fcp, G.total_chunks, T.total_filesize, args.signature)
        circle.begin(pcheck)
        circle.finalize()
        tally = pcheck.fail_tally()
        tally = comm.bcast(tally)
        if circle.rank == 0:
            print("")
            if tally == 0:
                print("\t{:<20}{:<20}".format("Verify result:", "PASS"))
            else:
                print("\t{:<20}{:<20}".format("Verify result:", "FAILED"))

        comm.Barrier()

        if args.signature and tally == 0:
            gen_signature(pcheck.bfsign, T.total_filesize)

    # fix permission
    comm.Barrier()
    if G.fix_opt and treewalk:
        if comm.rank == 0:
            print("\nFixing ownership and permissions ...")
        fix_opt(treewalk)

    if treewalk:
        treewalk.cleanup()
    if fcp:
        fcp.cleanup()
    #if circle:
    #    circle.finalize(cleanup=True)
    comm.Barrier()
    if comm.rank == 0:
        try:
            os.rmdir(G.tempdir)
        except:
            pass

    # TODO: a close file error can happen when circle.finalize()
    #
    #if isinstance(circle.workq, DbStore):
    #    circle.workq.cleanup()

if __name__ == "__main__":
    main()
