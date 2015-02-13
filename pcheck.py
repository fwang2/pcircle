from task import BaseTask
import logging
import utils
import hashlib
from mpi4py import MPI

logger = logging.getLogger("pcheck")

class PCheck(BaseTask):
    def __init__(self, circle, pcp):
        global logger
        BaseTask.__init__(self, circle)
        self.circle = circle
        self.pcp = pcp

        # cache
        self.fd_cache = {}

        # failed
        self.failed = {}

        self.failcnt = 0

        # debug
        self.d = {"rank": "rank %s" % circle.rank}

    def create(self):
        for k, v in self.pcp.checksum.iteritems():
            for chunk in v:
                self.enq( [k, chunk] )
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
            fd = open(src, "rb")
            self.fd_cache[src] = fd

        fd.seek(chunk[0])
        digest = hashlib.md5(fd.read(chunk[1])).hexdigest()
        if digest != chunk[2]:
            logger.error("Verification failed for %s \n src-digest: %s\n dst-digest: %s \n"
                         % (src, chunk[2], digest), extra=self.d)
            if src not in self.failed:
                self.failed[src] = chunk[3]
                self.failcnt += 1


    def fail_tally(self):
        total_fails = self.circle.comm.reduce(self.failcnt, op=MPI.SUM)
        return total_fails

    def setLevel(self, level):
        global logger
        utils.logging_init(logger, level)


    def reduce_init(self):
        pass

    def reduce_finish(self):
        pass

    def reduce(self):
        pass


