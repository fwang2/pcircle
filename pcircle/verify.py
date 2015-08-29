from task import BaseTask
from utils import bytes_fmt
import hashlib
from mpi4py import MPI
import utils
from lru import LRU


class PVerify(BaseTask):
    def __init__(self, circle, fcp, totalsize=0):
        BaseTask.__init__(self, circle)
        self.circle = circle
        self.fcp = fcp
        self.totalsize = totalsize

        # cache
        self.fd_cache = LRU(512)

        # failed
        self.failed = {}

        self.failcnt = 0

        # debug
        self.d = {"rank": "rank %s" % circle.rank}
        self.logger = utils.getLogger(__name__)

        # reduce
        self.vsize = 0

        assert len(circle.workq) == 0

        if self.circle.rank == 0:
            print("\nChecksum verification ...")

    def create(self):
        self.logger.info("Chunk count: %s" % len(self.fcp.chunksums), extra=self.d)
        for ck in self.fcp.chunksums:
            self.enq(ck)

    def process(self):
        chunk = self.deq()

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
        except AttributeError as e:
            self.logger.error(e, extra=self.d)
            self.logger.error(chunk, extra=self.d)
            self.failcnt += 1
            #self.circle.Abort(1)
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
            out += "%.2f %% verified, " % (100 * float(buf['vsize']) / self.totalsize)

        out += "%s bytes done" % bytes_fmt(buf['vsize'])
        print(out)

    def reduce_finish(self, buf):
        # self.reduce_report(buf)
        pass

    def reduce(self, buf1, buf2):
        buf1['vsize'] += buf2['vsize']
        return buf1
