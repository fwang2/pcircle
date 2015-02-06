from __future__ import print_function
from mpi4py import MPI
from globals import T, G
import logging
import random

logger = logging.getLogger("circle")


def setup_logging(level):
    global logger
    fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.setLevel(level)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)
    logger.propagate = False

class Circle:

    def __init__(self, name="Circle Work Comm",  split = "equal"):
        random.seed()  # use system time to seed
        self.comm = MPI.COMM_WORLD.Dup()
        self.comm.Set_name(name)
        self.size = self.comm.Get_size()
        self.rank = self.comm.Get_rank()
        self.token_init()

        self.reduce_outstanding = False
        self.request_outstanding = False
        self.task = None
        self.abort = False
        self.requestors = []
        self.workq = []
        self.next_proc = None # rank of next process to request work

        # counters
        self.work_processed = [0] * self.size
        self.work_request_sent = [0] * self.size
        self.work_request_received = [0] * self.size
        self.processed = 0

        # barriers
        self.barrier_started = False

    def register(self, task):

        self.task = task

    def next_proc(self):
        """ Note next proc could return rank of itself """
        return random.randint(0, self.size)

    def token_init(self):

        self.token_src = (self.rank - 1 + self.size) % self.size
        self.token_dest = (self.rank + 1 + self.size) % self.size
        self.token_color = G.BLACK
        self.token_proc = G.WHITE
        self.token_is_local = False
        if self.rank == 0:
            self.token_is_local = True
        self.token_send_req = MPI.REQUEST_NULL

    def begin(self):
        """ entry point to work """

        if self.rank == 0:
            self.task.create()

        # work until terminate
        self.loop()

        # check point?
        if self.abort:
            self.checkpoint()

    def token_recv(self):
        # verify we don't have a local token
        if self.token_is_local:
            raise RuntimeError("token error")

        # this won't block as token is waiting
        self.comm.recv(self.token_color, self.token_src,
            T.TOKEN)

        # record token is local
        self.token_is_local = True

        # if we have a token outstanding, at this point
        # we should have received the reply (even if we
        # sent the token to ourself, we just replied above
        # so the send should now complete
        #
        # FIXME?
        # if self.token_send_req != MPI.PROC_NULL:

        # now set our state
        if self.token_proc == G.BLACK and self.token_color == G.BLACK:
            self.token_proc = G.WHITE

        # check for terminate condition
        terminate = False

        if self.rank == 0 and self.token_color == G.WHITE:
            # if rank 0 receive a white token
            logger.debug("Master detected termination")
            terminate = True
        elif self.token_color == G.TERMINATE:
            terminate = True

        # forward termination token if we have one
        if terminate:
            # send terminate token, don't bother
            # if we the last rank
            self.token_color = G.TERMINATE
            if self.rank < self.size -1:
                self.token_send()

            # set our state to terminate
            self.token_proc = G.TERMINATE

    def token_check(self):
        status = MPI.Status()
        flag = self.comm.Iprobe(self.token_src, T.TOKEN, status)
        if flag: self.token_recv()


    def checkpoint(self):
        pass

    def enq(self, work):
        self.workq.append(work)

    def deq(self):
        if len(self.workq) > 0:
            return self.workq.pop(0)
        else:
            return None


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
        self.comm.Barrier()

    def bcase_abort(self):
        self.abort = True
        buf = G.ABORT
        for i in range(self.size):
            if (i != self.rank):
                self.comm.send(buf, i, tag = T.WORK_REQUEST)
                logger.warn("abort message sent to %s" % i)

    def loop(self):
        while True:
            self.check_request()
            self.check_reduce()

            # if I have no work, request work from others
            self.request_work()

            # if I have work, and no abort signal, process one
            if len(self.workq) != 0:
                self.task.process()
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

    def check_for_term(self):
        if self.token_proc == G.TERMINATE:
            return G.TERMINATE

        if self.token_is_local:
            # we have token
            if self.rank == 0:
                # rank 0 start with white token
                self.token_color = G.WHITE
            elif self.token_proc == G.BLACK:
                # others turn the token black
                # if they are in black state
                self.token_color = G.BLACK
            # send the token
            self.send_token()

    def send_token(self):
        # don't send if abort
        if self.abort: return

        self.comm.issend(self.token_color,
            self.token_dest, tag = T.TOKEN)

        # now we don't have the token
        self.token_is_local = False

    def check_request(self):
        buf = None
        while True:
            status = MPI.Status()
            ret = self.comm.Iprobe(MPI.ANY_SOURCE, T.WORK_REQUEST, status)

            if not ret: break

            # we have work request message
            rank = status.Get_source()
            self.comm.recv(buf, rank, T.WORK_REQUEST, status)
            if buf == T.ABORT:
                self.abort = True
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

        buf = G.ABORT if self.abort else 0
        self.comm.Isend(buf, dest = rank, tag = T.WORK_REPLY)

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

        self.comm.send(self.queue[0:count], dest, T.WORK_REPLY)

        # remove work items
        del self.queue[0:count]

    def request_work(self):
        pass

    def finalize(self):
        """ clean up """
        pass

    def reduce(self, count):
        pass


