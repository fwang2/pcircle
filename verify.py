from task import BaseTask
import logging
from utils import logging_init, bytes_fmt
import hashlib
from mpi4py import MPI

logger = logging.getLogger("pcheck")

class PVerify(BaseTask):
    def __init__(self, circle, pcp, totalsize=0):
        global logger
        BaseTask.__init__(self, circle)
        self.circle = circle
        self.pcp = pcp
        self.totalsize = totalsize

        # cache
        self.fd_cache = {}

        # failed
        self.failed = {}

        self.failcnt = 0

        # debug
        self.d = {"rank": "rank %s" % circle.rank}

        # reduce
        self.vsize = 0

        if self.circle.rank == 0:
            print("Start verification process ...")

    def create(self):
        logger.debug("Chunk count: %s" % len(self.pcp.checksum), extra=self.d)
        for k, v in self.pcp.checksum.iteritems():
            for chunk in v:
                self.enq([k, chunk])


    def process(self):
        work = self.deq()
        src = work[0]
        chunk = work[1] # offset, length, digest
        logger.debug("process: %s -> %s" % (src, chunk), extra = self.d)

        fd = None
        if src in self.fd_cache:
            fd = self.fd_cache[src]

        if not fd:
            # need to open for read
            try:
                fd = open(src, "rb")
            except IOError as e:
                logger.error(e)
                self.failcnt += 1
                return

            self.fd_cache[src] = fd

        fd.seek(chunk[0])
        digest = hashlib.md5(fd.read(chunk[1])).hexdigest()
        if digest != chunk[2]:
            logger.error("Verification failed for %s \n src-digest: %s\n dst-digest: %s \n"
                         % (src, chunk[2], digest), extra=self.d)
            if src not in self.failed:
                self.failed[src] = chunk[3]
                self.failcnt += 1

        self.vsize += chunk[1]

    def fail_tally(self):
        total_fails = self.circle.comm.reduce(self.failcnt, op=MPI.SUM)
        return total_fails

    def setLevel(self, level):
        global logger
        logging_init(logger, level)


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


