from __future__ import print_function
from mpi4py import MPI
from globals import TAG, G
import logging

logger = logging.getLogger("app")


def setup_logging(level):
    global logger
    fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.setLevel(level)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)
    logger.propagate = False


class Circle:
    comm = MPI.COMM_WORLD.Dup()
    abort = False

    def __init__(self, task, split = "equal"):
        self.task = task
        self.size = Circle.comm.Get_size()
        self.rank = Circle.comm.Get_rank()
        self.requestors = []
        self.queue = []

        self.processed = 0

        # token management

        self.token_src = None
        self.token_dest = None
        self.token_color = None
        self.token_proc = None

        #
        self.reduce_outstanding = False
        self.request_outstanding = False
        self.token_send_req = MPI.REQUEST_NULL

        #
        self.barrier_started = False

    def begin(self):

        if self.rank == 0:
            self.task.create()

        # work until terminate
        self.loop()

    def enqueue(self):
        pass

    def dequeue(self):
        pass


    def check_reduce(self):
        pass

    def barrier_start(self):
        self.barrier_started = True

    def barrier_test(self):

        # barrier has not been started
        if not self.barrier_started:
            return False

        # FIXME
        # do we really need async barrier?
        Circle.comm.Barrier()

    def bcase_abort(self):
        Circle.abort = True
        buf = G.ABORT
        for i in range(self.size):
            if (i != self.rank):
                Circle.comm.Send(buf, i, tag = TAG.WORK_REQUEST)
                logger.warn("abort message sent to %s" % i)

    def loop(self):
        while True:
            self.check_request()
            #
            self.check_reduce()

            # if I have no work, request work from others
            self.request_work()

            # if I have work, and no abort signal, process one
            if len(self.queue) != 0:
                self.task.process(self.queue.pop(0))
                self.processed += 1
            else:
                status = self.check_for_term();
                if status == G.TERMINATE:
                    break;
        #
        # We got here because
        # (1) all processes finish the work
        # (2) abort
        #
        while True:
            if not (self.reduce_outstanding or self.request_outstanding) and \
                self.token_send_req == MPI.REQUEST_NULL:
                self.barrier_start()

            # break the loop when non-blocking barrier completes
            if self.barrier_test():
                break;

            # send no work message for any work request that comes in
            self.workreq_check()

            # clean up any outstanding reduction
            self.reduce_check()

            # recv any incoming work reply messages
            self.request_work()

            # check and recv any incoming token
            self.token_check()

            # if we have an outstanding token, check if it has been recv'ed
            # FIXME
            if self.token_send_req != MPI.REQUEST_NULL:
                self.token_send_req.Test()

    def workreq_check(self):
        buf = None
        self.requestors = []

        while True:
            status = MPI.Status()
            ret = Circle.comm.Iprobe(MPI.ANY_SOURCE, TAG.WORK_REQUEST, status)

            if not ret: break

            # we have work request message
            rank = status.Get_source()
            Circle.comm.Recv([buf, MPI.INT], rank, TAG.WORK_REQUEST, status)
            if buf == TAG.ABORT:
                Circle.abort = True
                logger.info("Abort request recv'ed")
                return
            else:
                # add rank to requesters
                self.requestors.append(rank)

        if len(self.requestors) != 0:
            if len(self.queue) == 0:
                for rank in self.requestors:
                    self.send_no_work(rank)
            else:
                self.send_work_to_many()


    def send_no_work(self, rank):
        """ send no work reply to someone requesting work"""

        buf = TAG.ABORT if Circle.abort else 0
        Circle.comm.Isend(buf, dest = rank, tag = TAG.WORK_REPLY)

    def send_work_to_many(self):
        if self.split == "equal":
            self.spread_counts()
        else:
            # TODO self.split == "random":
            raise NotImplementedError


        logger.debug("Done with servicing requests.")

    def send_work(self, dest, count):

        # for termination detection
        if (dest < self.rank) or (dest == self.token_src):
            self.token_proc = G.BLACK

        Circle.comm.Send(self.queue[0:count], dest, TAG.WORK_REPLY)

        # remove work items
        del self.queue[0:count]

    def request_work(self):
        pass


