#!/usr/bin/env python
"""
PCP provides MPI-based parallel data transfer functionality.

Author:
    Feiyi Wang (fwang2@ornl.gov)

"""
from __future__ import print_function

from mpi4py import MPI
import stat
import os
import os.path
import logging
import argparse
import utils
import hashlib
import sys
import signal
import resource
import cPickle as pickle

from collections import Counter, defaultdict
from utils import bytes_fmt, destpath
from lru import LRU
from threading import Thread

from task import BaseTask
from verify import PVerify
from circle import Circle
from cio import readn, writen
from fwalk import FWalk
from checkpoint import Checkpoint
from _version import get_versions

__version__ = get_versions()['version']
del get_versions

ARGS = None
logger = logging.getLogger("fcp")
circle = None
NUM_OF_HOSTS = 0

def parse_args():

    parser = argparse.ArgumentParser(description="Parallel Data Copy",
                epilog="Please report issues to help@nccs.gov")
    parser.add_argument("-v", "--version", action="version", version="{version}".format(version=__version__))

    parser.add_argument("--loglevel", default="ERROR", help="log level, default ERROR")
    parser.add_argument("--chunksize", metavar="sz", default="1m", help="chunk size (KB, MB, GB, TB), default: 1MB")
    parser.add_argument("--adaptive", action="store_true", default=True, help="Adaptive chunk size")
    parser.add_argument("--reduce-interval", metavar="seconds", type=int, default=10, help="interval, default 10s")
    parser.add_argument("--checkpoint-interval", metavar="seconds", type=int, default=360, help="checkpoint interval, default: 360s")
    parser.add_argument("-c", "--checksum", action="store_true", help="verify after copy, default: off")

    parser.add_argument("--checkpoint-id", metavar="ID", default=None, help="default: timestamp")
    parser.add_argument("-p", "--preserve", action="store_true", help="preserve meta, default: off")
    parser.add_argument("-r", "--resume", dest="rid", metavar="ID", nargs=1, help="resume ID, required in resume mode")

    parser.add_argument("--pause", action="store_true", help="pause after copy, test only")

    parser.add_argument("src", help="copy from")
    parser.add_argument("dest", help="copy to")

    return parser.parse_args()

def sig_handler(signal, frame):
    # catch keyboard, do nothing
    # eprint("\tUser cancelled ... cleaning up")
    sys.exit(1)

class FCP(BaseTask):
    def __init__(self, circle, src, dest,
                 treewalk = None,
                 totalsize=0,
                 hostcnt=0,
                 prune=False,
                 do_checksum=False,
                 workq=None):
        BaseTask.__init__(self, circle)
        self.circle = circle
        self.treewalk = treewalk
        self.totalsize = totalsize
        self.prune = prune
        self.workq = workq

        self.checkpoint_file = None

        self.src = os.path.abspath(src)
        self.srcbase = os.path.basename(src)
        self.dest = os.path.abspath(dest)

        # cache, keep the size conservative
        # TODO: we need a more portable LRU size

        if hostcnt != 0:
            max_ofile, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
            procs_per_host = self.circle.size / hostcnt
            self._read_cache_limit = ((max_ofile - 64)/procs_per_host)/3
            self._write_cache_limit = ((max_ofile - 64)/procs_per_host)*2/3

        if self._read_cache_limit <= 0 or self._write_cache_limit <= 0:
            self._read_cache_limit = 1
            self._write_cache_limit = 8


        self.rfd_cache = LRU(self._read_cache_limit)
        self.wfd_cache = LRU(self._write_cache_limit)

        self.cnt_filesize_prior = 0
        self.cnt_filesize = 0

        self.blocksize = 1024*1024
        self.chunksize = 1024*1024


        # debug
        self.d = {"rank": "rank %s" % circle.rank}
        self.wtime_started = MPI.Wtime()
        self.wtime_ended = None
        self.workcnt = 0
        if self.treewalk:
            logger.debug("treewalk files = %s" % treewalk.flist, extra=self.d)

        # fini_check
        self.fini_cnt = Counter()

        # checksum
        self.do_checksum = do_checksum
        self.checksum = defaultdict(list)

        # checkpointing
        self.checkpoint_interval = sys.maxsize
        self.checkpoint_last = MPI.Wtime()

        if self.circle.rank == 0:
            print("Start copying process ...")

    def set_chunksize(self, sz):
        self.chunksize = sz

    def set_adaptive_chunksize(self, totalsz):
        MB = 1024*1024
        TB = 1024*1024*1024*1024
        if totalsz < 10*TB:
            self.chunksize = MB
        elif totalsz < 100*TB:
            self.chunksize = 32*MB
        elif totalsz < 512*TB:
            self.chunksize = 128*MB
        elif totalsz < 1024*TB:
            self.chunksize = 256*MB
        else:
            self.chunksize = 512*MB

        print("Adaptive chunksize: %s" %  bytes_fmt(self.chunksize))

    def set_checkpoint_interval(self, interval):
        self.checkpoint_interval = interval

    def set_checkpoint_file(self, f):
        self.checkpoint_file = f

    def cleanup(self):
        for f in self.rfd_cache.values():
            os.close(f)

        for f in self.wfd_cache.values():
            os.close(f)

        # remove checkpoint file
        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)

    def enq_dir(self, f):
        """ Deprecated, should not be in use anymore """
        d = {}
        d['cmd'] = "mkdir"
        d['src'] = f[0]
        d['dest'] = self.dest + "/" + self.srcbase + "/" + os.path.relpath(f[0], start=self.src)
        self.enq(d)

    def enq_file(self, f):
        '''
        f[0] path f[1] mode f[2] size - we enq all in one shot
        CMD = copy src  dest  off_start  last_chunk
        '''
        chunks    = f[2] / self.chunksize
        remaining = f[2] % self.chunksize

        d = {}
        d['cmd'] = 'copy'
        d['src'] = f[0]
        d['dest'] = destpath(self.src, self.dest, f[0])

        workcnt = 0

        if f[2] == 0:
            # empty file
            d['off_start'] = 0
            d['length'] = 0
            self.enq(d)
            logger.debug("%s" % d, extra=self.d)
            workcnt += 1
        else:
            for i in range(chunks):
                d['off_start'] = i * self.chunksize
                d['length'] = self.chunksize
                self.enq(d)
                logger.debug("%s" % d, extra=self.d)
            workcnt += chunks

        if remaining > 0:
            # send remainder
            d['off_start'] = chunks * self.chunksize
            d['length' ] = remaining
            self.enq(d)
            logger.debug("%s" % d, extra=self.d)
            workcnt += 1

        # tally work cnt
        self.workcnt += workcnt

        # --------------------------------------
        # TODO: make finish token for this file
        # --------------------------------------
        # t = {'cmd' : 'fini_check', 'workcnt' : workcnt, 'src' : f[0], 'dest': d['dest'] }
        # self.enq(t)


    def create(self):
        if self.workq:  # restart
            self.setq(self.workq)
            return

        # construct and enable all copy operations
        for f in self.treewalk.flist:
            if os.path.islink(f[0]):
                dest = destpath(self.src, self.dest, f[0])
                linkto = os.readlink(f[0])
                os.symlink(linkto, dest)
            elif stat.S_ISREG(f[1]):
                self.enq_file(f)

        # right after this, we do first checkpoint
        if self.checkpoint_file:
            self.do_no_interrupt_checkpoint()
            self.checkpoint_last = MPI.Wtime()

    def do_open(self, k, d, flag, limit):
        """
        :param k: key
        :param d: dict
        :return: file handle
        """
        if d.has_key(k):
            return d[k]

        if len(d.keys()) < limit:
            fd = os.open(k, flag)
            if fd > 0: d[k] = fd
            return fd
        else:
            # over the limit
            # clean up the least used
            old_k, old_v = d.items()[-1]
            logger.debug("Closing fd for %s" % old_k, extra=self.d)
            os.close(old_v)

            fd = os.open(k, flag)
            if fd > 0: d[k] = fd
            return fd

    def do_mkdir(self, work):
        src = work['src']
        dest = work['dest']
        if not os.path.exists(dest):
            os.makedirs(dest)

    def do_copy(self, work):
        src = work['src']
        dest = work['dest']

        basedir = os.path.dirname(dest)
        if not os.path.exists(basedir):
            os.makedirs(basedir)

        rfd = self.do_open(src, self.rfd_cache, os.O_RDONLY, self._read_cache_limit)
        wfd = self.do_open(dest, self.wfd_cache, os.O_WRONLY | os.O_CREAT, self._write_cache_limit)
        if wfd < 0:
            if ARGS.force:
                os.unlink(dest)
                wfd = self.do_open(dest, self.wfd_cache, os.O_WRONLY)
            else:
                logger.error("Failed to create output file %s" % dest, extra=self.d)
                return

        # do the actual copy
        self.write_bytes(rfd, wfd, work)

        # update tally
        self.cnt_filesize += work['length']
        logger.debug("transfer bytes %s" % self.cnt_filesize, extra=self.d)

        #self.fini_cnt[src] += 1
        #logger.debug("Inc workcnt for %s (workcnt=%s)" % (src, self.fini_cnt[src]), extra=self.d)

    def do_fini_check(self, work):
        src = work['src']
        dest = work['dest']
        workcnt = work['workcnt']

        mycnt = self.fini_cnt[src]
        if workcnt == mycnt:
            # all job finished
            # we should have cached this before
            rfd = self.rfd_cache[src]
            wfd = self.wfd_cache[dest]
            os.close(rfd)
            os.close(wfd)
            del self.rfd_cache[src]
            del self.wfd_cache[dest]
            logger.debug("Finish done: %s" % src, extra=self.d)
        elif mycnt < workcnt:
            # worked some, but not yet done
            work['workcnt'] -= mycnt
            self.enq(work)
            logger.debug("Finish enq: %s (workcnt=%s) " % (src, work['workcnt']), extra=self.d)
        # either way, clear the cnt
        del self.fini_cnt[src]


    def do_no_interrupt_checkpoint(self):
        a = Thread(target=self.do_checkpoint)
        a.start()
        a.join()
        logger.debug("checkpoint: %s" % self.checkpoint_file, extra=self.d )

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
        The only work is "copy", TODO: clean up other actions such as mkdir/fini_check
        """
        curtime = MPI.Wtime()
        if curtime - self.checkpoint_last > self.checkpoint_interval:
            self.do_no_interrupt_checkpoint()
            self.checkpoint_last = curtime

        work = self.deq()
        if work['cmd'] == 'copy':
            self.do_copy(work)
        elif work['cmd'] == 'mkdir':
            # self.do_mkdir(work)
            pass
        elif work['cmd'] == 'fini_check':
            # self.do_fini_check(work)
            pass
        else:
            logger.error("Unknown command %s" % work['cmd'], extra=self.d)
            self.abort()

    def reduce_init(self, buf):
        buf['cnt_filesize'] = self.cnt_filesize


    def reduce(self, buf1, buf2):
        buf1['cnt_filesize'] += buf2['cnt_filesize']
        return buf1

    def reduce_report(self, buf):
        out = ""
        if self.totalsize != 0:
            out += "%.2f %% finished, " % (100* float(buf['cnt_filesize']) / self.totalsize)

        out += "%s copied" % bytes_fmt(buf['cnt_filesize'])

        if self.circle.reduce_time_interval != 0:
            rate = float(buf['cnt_filesize'] - self.cnt_filesize_prior) / self.circle.reduce_time_interval
            self.cnt_filesize_prior = buf['cnt_filesize']
            out += ", estimated transfer rate: %s/s" % bytes_fmt(rate)

        print(out)

    def reduce_finish(self, buf):
        #self.reduce_report(buf)
        pass

    def epilogue(self):
        self.wtime_ended = MPI.Wtime()
        if self.circle.rank == 0:
            print("")
            if self.totalsize == 0: return
            time = self.wtime_ended - self.wtime_started
            rate = float(self.totalsize)/time
            print("Copy Job Completed In: %.2f seconds" % (time))
            print("Average Transfer Rate: %s/s\n" % bytes_fmt(rate))

    def read_then_write(self, rfd, wfd, work, num_of_bytes, m):
        buf = None
        try:
            buf = readn(rfd, num_of_bytes)
        except IOError:
            self.circle.Abort("Failed to read %s", work['src'], extra=self.d)

        try:
            writen(wfd, buf)
        except IOError:
            self.circle.Abort("Failed to write %s", work['dest'], extra=self.d)

        if m:
            m.update(buf)


    def write_bytes(self, rfd, wfd, work):
        os.lseek(rfd, work['off_start'], os.SEEK_SET)
        os.lseek(wfd, work['off_start'], os.SEEK_SET)

        m = None
        if self.do_checksum:
            m = hashlib.sha1()

        remaining = work['length']
        while remaining != 0:
            if remaining >= self.blocksize:
                self.read_then_write(rfd, wfd, work, self.blocksize, m)
                remaining -= self.blocksize
            else:
                self.read_then_write(rfd, wfd, work, remaining, m)
                remaining = 0

        # are we doing checksum?
        if self.do_checksum:
            self.checksum[work['dest']].append((work['off_start'], work['length'], m.hex_digest(), work['src']))


def err_and_exit(msg, code):
    if circle.rank == 0:
        print(msg)
    circle.exit(0)

def check_path(circ, isrc, idest):
    """ verify and return target destination"""

    if not os.path.exists(isrc) or not os.access(isrc, os.R_OK):
        err_and_exit("source directory %s is not readable" % isrc, 0)

    if os.path.exists(idest):
        err_and_exit("Destination [%s] exists, will not overwrite!" % idest, 0)

    # idest doesn't exits at this point
    # we check if its parent exists

    dest_parent = os.path.dirname(idest)

    if os.path.exists(dest_parent) and os.access(dest_parent, os.W_OK):
        return idest
    else:
        err_and_exit("Error: destination [%s] is not accessible" % dest_parent, 0)

    # should not come to this point
    raise

def set_adaptive_chunksize(pcp, tsz):

    if ARGS.adaptive:
        pcp.set_adaptive_chunksize(tsz)
    else:
        pcp.set_chunksize(utils.conv_unit(ARGS.chunksize))

def main_start():
    global circle
    src = os.path.abspath(ARGS.src)
    dest = os.path.abspath(ARGS.dest)
    dest = check_path(circle, src, dest)

    treewalk = FWalk(circle, src, dest, preserve = ARGS.preserve)
    treewalk.set_loglevel(ARGS.loglevel)
    circle.begin(treewalk)
    circle.finalize(reduce_interval=ARGS.reduce_interval)
    tsz = treewalk.epilogue()

    pcp = FCP(circle, src, dest, treewalk = treewalk,
              totalsize=tsz, do_checksum=ARGS.checksum, hostcnt=NUM_OF_HOSTS)

    set_adaptive_chunksize(pcp, tsz)

    pcp.set_checkpoint_interval(ARGS.checkpoint_interval)

    if ARGS.checkpoint_id:
        pcp.set_checkpoint_file(".pcp_workq.%s.%s" % (ARGS.checkpoint_id, circle.rank))
    else:
        ts = utils.timestamp()
        circle.comm.bcast(ts)
        pcp.set_checkpoint_file(".pcp_workq.%s.%s" % (ts, circle.rank))

    circle.begin(pcp)
    circle.finalize(reduce_interval=ARGS.reduce_interval)
    pcp.cleanup()

    return pcp, tsz

def get_workq_size(workq):
    if workq is None: return 0
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

def main_resume(rid):
    global circle
    dmsg = {"rank": "rank %s" % circle.rank}
    oldsz = 0; tsz = 0; sz = 0
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

            except:
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
              totalsize=tsz, checksum=ARGS.checksum,
              workq = cobj.workq,
              hostcnt = NUM_OF_HOSTS)

    set_adaptive_chunksize(pcp, tsz)

    pcp.set_checkpoint_interval(ARGS.checkpoint_interval)
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



def main():

    global ARGS, logger, circle, NUM_OF_HOSTS
    signal.signal(signal.SIGINT, sig_handler)
    parse_flags = True

    if MPI.COMM_WORLD.rank == 0:
        try:
            ARGS = parse_args()
        except:
            parse_flags = False

    parse_flags = MPI.COMM_WORLD.bcast(parse_flags)

    if parse_flags:
        ARGS = MPI.COMM_WORLD.bcast(ARGS)
    else:
        sys.exit(0)

    localhost = MPI.Get_processor_name()
    hosts = MPI.COMM_WORLD.gather(localhost)
    if MPI.COMM_WORLD.rank == 0:
        NUM_OF_HOSTS = len(set(hosts))
    NUM_OF_HOSTS = MPI.COMM_WORLD.bcast(NUM_OF_HOSTS)

    circle = Circle(reduce_interval=ARGS.reduce_interval)
    circle.setLevel(logging.ERROR)
    logger = utils.logging_init(logger, ARGS.loglevel)

    pcp = None
    totalsize = None

    if ARGS.rid:
        pcp, totalsize = main_resume(ARGS.rid[0])
    else:
        pcp, totalsize = main_start()

    # pause

    if ARGS.pause and ARGS.checksum:
        if circle.rank == 0:
            raw_input("\n--> Press any key to continue ...\n")
            sys.stdout.flush()

        circle.comm.Barrier()

    # third task
    if ARGS.checksum:
        pcheck = PVerify(circle, pcp, totalsize)
        pcheck.setLevel(ARGS.loglevel)
        circle.begin(pcheck)
        circle.finalize()

        tally = pcheck.fail_tally()

        if circle.rank == 0:
            print("")
            if tally == 0:
                print("Verification passed!")
            else:
                print("Verification failed")
                print("Note that checksum errors can't be corrected by checkpoint/resume!")

    pcp.epilogue()

if __name__ == "__main__": main()

