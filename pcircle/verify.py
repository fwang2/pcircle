from task import BaseTask
import logging
from utils import bytes_fmt
from fdef import ChunkSum
import hashlib
from mpi4py import MPI
import utils
from lru import LRU

class PVerify(BaseTask):
    def __init__(self, circle, pcp, totalsize=0):
        BaseTask.__init__(self, circle)
        self.circle = circle
        self.pcp = pcp
        self.totalsize = totalsize

        # cache
        self.fd_cache = LRU(512)

        # failed
        self.failed = {}

        self.failcnt = 0

        # debug
        self.d = {"rank": "rank %s" % circle.rank}

        # reduce
        self.vsize = 0

        self.logger = utils.getLogger(__name__)

        if self.circle.rank == 0:
            print("\nStart verification process ...")

    def create(self):
        self.logger.debug("Chunk count: %s" % len(self.pcp.checksum), extra=self.d)
        for ck in self.pcp.checksum:
            self.enq(ck)


    def process(self):
        chunk = self.deq()
        self.logger.debug("process: %s" % chunk, extra = self.d)

        # fd = None
        # if chunk.filename in self.fd_cache:
        #     fd = self.fd_cache[chunk.filename]
        #
        # if not fd:
        #     # need to open for read
        #     try:
        #         fd = open(chunk.filename, "rb")
        #     except IOError as e:
        #         self.logger.error(e, extra=self.d)
        #         self.failcnt += 1
        #         return
        #
        #     self.fd_cache[chunk.filename] = fd

        try:
            fd = open(chunk.filename, "rb")
        except IOError as e:
            self.logger.error(e, extra=self.d)
            self.failcnt += 1
            return

        fd.seek(chunk.offset)
        digest = hashlib.sha1(fd.read(chunk.length)).hexdigest()
        if digest != chunk.digest:
            self.logger.error("Verification failed for %s \n src-digest: %s\n dst-digest: %s \n"
                         % (chunk.filename, chunk.digest, digest), extra=self.d)
            if chunk.filename not in self.failed:
                self.failed[chunk.filename] = chunk.digest
                self.failcnt += 1

        self.vsize += chunk.length

    def fail_tally(self):
        total_fails = self.circle.comm.reduce(self.failcnt, op=MPI.SUM)
        return total_fails


    def reduce_init(self, buf):
        buf['vsize'] = self.vsize

    def reduce_report(self, buf):
        out = ""
        if self.totalsize != 0:
            out += "%.2f %% verified, " % (100 * float(buf['vsize'])/self.totalsize)

        out += "%s bytes done" % bytes_fmt(buf['vsize'])
        print(out)

    def reduce_finish(self, buf):
        #self.reduce_report(buf)
        pass

    def reduce(self, buf1, buf2):
        buf1['vsize'] += buf2['vsize']
        return buf1


