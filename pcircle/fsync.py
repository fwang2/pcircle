__author__ = 'f7b'

import os
from mpi4py import MPI
from pcircle.task import BaseTask
from pcircle.utils import getLogger
from globals import G
from utils import bytes_fmt
import rsync

logger = getLogger(__name__)
comm = MPI.COMM_WORLD
dmsg = {"rank": "rank %s" % comm.rank}

class SyncFile:
    def __init__(self, src, dest, size):
        self.src = src
        self.dest = dest
        self.size = size

    def __repr__(self):
        return "SyncFile: " + ",".join(self.src, self.dest)


class FileSync(BaseTask):
    def __init__(self, circle, syncfiles, size):
        BaseTask.__init__(self, circle)
        self.circle = circle
        self.syncfiles = syncfiles
        self.syncfiles_sz = size
        self.totalsize = 0

        # reduce helper variables
        self.reduce_items = 0
        self.last_cnt = 0
        self.last_reduce_time = MPI.Wtime()
        self.cnt_filesize = 0
        self.cnt_filesize_prior = 0
        self.logger = getLogger(__name__)
        self.d = {"rank": "rank %s" % circle.rank}

    def create(self):
        for ele in self.syncfiles:
            self.circle.enq(ele)

    def process(self):
        sf = self.circle.deq()
        save_to = sf.dest + ".patched"
        with open(sf.src, "rb") as fsrc, open(sf.dest, "rb") as fdst, \
                open(save_to, "wb") as fdst_patched:
            signature = rsync.blockchecksums(fdst)
            delta = rsync.rsyncdelta(fsrc, signature)
            rsync.patchstream(fdst, fdst_patched, delta) # dest, delta, newfile
            self.cnt_filesize += sf.size
            if G.verbosity > 0:
                print("Sync copy: %s" % sf.src)

        self.reduce_items += 1
        try:
            os.rename(save_to, sf.dest)
        except OSError as e:
            self.logger.error("Failed to rename: %s to %s" % (save_to, sf.dest))

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
        # get result of reduction
        pass
