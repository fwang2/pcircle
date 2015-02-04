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

        # token management

        self.token_src = None
        self.token_dest = None
        self.token_color = None
        self.token_proc = None

    def begin(self):

        if self.rank == 0:
            self.task.create()

        # work until terminate
        self.loop()

    def enqueue(self):
        pass

    def dequeue(self):
        pass


    def loop(self):
        while True:
            self.workreq_check()


    def check_work_request(self):
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
        if dest < self.rank or dest == self.token_src:
            self.token_proc = G.BLACK

        Circle.comm.Send(self.queue[0:count], dest, TAG.WORK_REPLY)

        # remove work items
        del self.queue[0:count]

    def request_work(self):
        pass


