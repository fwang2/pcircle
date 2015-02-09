from __future__ import print_function
from mpi4py import MPI
from globals import T, G
import logging
import random

logger = logging.getLogger("circle")

class Token:
    pass

class Circle:

    # Keep init() and reset() in sync ... it is a pain, I know

    def __init__(self, name="Circle Work Comm",  split = "equal",
                 reduce_interval=10):

        random.seed()  # use system time to seed
        logging_init()

        self.comm = MPI.COMM_WORLD.Dup()
        self.comm.Set_name(name)
        self.size = self.comm.Get_size()
        self.rank = self.comm.Get_rank()
        self.token_init()

        self.split = split
        self.task = None
        self.abort = False
        self.requestors = []
        self.workq = []

        # counters
        self.work_requested = 0
        self.work_processed = 0
        self.work_request_received = 0
        self.workreq_outstanding = False
        self.workreq_rank = None

        # manage reduction
        self.reduce_enabled = True
        self.reduce_time_last = MPI.Wtime()
        self.reduce_time_interval = reduce_interval
        self.reduce_outstanding = False
        self.reduce_replies = 0

        # debug
        self.d = {"rank" : "rank %s" % self.rank}


    def next_proc(self):
        """ Note next proc could return rank of itself """
        if self.size == 1:
            return MPI.PROC_NULL
        else:
            return random.randint(0, self.size-1)


    def token_status(self):
        return "rank: %s, token_src: %s, token_dest: %s, token_color: %s, token_proc: %s" % \
            (self.rank, self.token_src, self.token_dest, G.str[self.token_color], G.str[self.token_proc])

    def token_init(self):

        self.token_src = (self.rank - 1 + self.size) % self.size
        self.token_dest = (self.rank + 1 + self.size) % self.size
        self.token_color = G.NONE
        self.token_proc = G.WHITE
        self.token_is_local = False
        if self.rank == 0:
            self.token_is_local = True
            self.token_color = G.WHITE
            self.token_proc  = G.WHITE


        self.token_send_req = MPI.REQUEST_NULL


    def workq_info(self):
        s =  "rank %s has %s items in work queue\n" % (self.rank, len(self.workq))
        for w in self.workq:
            s = s + "\t %s" % w
        return s

    def begin(self, task):
        """ entry point to work """

        self.task = task

        if self.rank == 0:
            self.task.create()

        # work until terminate
        self.loop()


    def enq(self, work):
        logger.debug("enq: %s" % work, extra=self.d)
        if work is not None:
            self.workq.append(work)
        else:
            logger.warn("enq work item is None")

    def deq(self):
        logger.debug("deq: %s" % self.workq[0], extra=self.d)
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
        """ central loop to finish the work """
        while True:

            # check for and service requests
            self.workreq_check()

            if self.reduce_enabled:
                self.reduce_check()

            if len(self.workq) == 0:
                self.request_work()

            # if I have work, and no abort signal, process one
            if len(self.workq) > 0 and not self.abort:
                self.task.process()
                self.work_processed += 1

            else:
                status = self.check_for_term();
                if status == G.TERMINATE:
                    break;
        #
        # We got here because
        # (1) all processes finish the work
        # (2) abort
        #
        # we now clean up
        self.workreq_check(cleanup=True)
        self.request_work(cleanup=True)
        self.token_check()
        if self.token_send_req != MPI.REQUEST_NULL:
            logger.warn("Have outstanding token req", extra = self.d)
        self.comm.Barrier()
        if self.rank == 0:
            logger.debug("All process in sync now", extra=self.d)

    def cleanup(self):
        while True:
            if not (self.reduce_outstanding or self.request_outstanding) and \
                self.token_send_req == MPI.REQUEST_NULL:
                self.barrier_start()

            # break the loop when non-blocking barrier completes
            if self.barrier_test():
                break

            # send no work message for any work request that comes in
            self.request_check()

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

        if self.size == 1:
            self.token_proc = G.TERMINATE
            return G.TERMINATE

        if self.token_is_local:
            # we have no work, but we have token
            if self.rank == 0:
                # rank 0 start with white token
                self.token_color = G.WHITE
            elif self.token_proc == G.BLACK:
                # others turn the token black
                # if they are in black state
                self.token_color = G.BLACK

            self.token_issend()

            # flip process color from black to white
            self.token_proc = G.WHITE
        else:
            # we have no work, but we don't have the token
            self.token_check()

        # return current status
        return self.token_proc

    def workreq_check(self, cleanup=False):
        logger.debug("in workreq_check(): %s" % self.token_status(), extra=self.d)
        while True:
            st = MPI.Status()
            ret = self.comm.Iprobe(source = MPI.ANY_SOURCE, tag = T.WORK_REQUEST, status = st)
            if not ret: break
            # we have work request message
            rank = st.Get_source()
            buf = self.comm.recv(source = rank, tag = T.WORK_REQUEST, status = st)
            if buf == G.ABORT:
                self.abort = True
                logger.warn("Abort request recv'ed")
                return False
            else:
                logger.debug("receive work request from requestor [%s]"  % rank, extra=self.d)
                # add rank to requesters
                self.requestors.append(rank)

        if len(self.requestors) == 0:
            return False
        else:
            logger.debug("have %s requesters, with %s work in queue" %
                         (len(self.requestors), self.workq), extra=self.d)
            # have work requesters
            if len(self.workq) == 0 or cleanup:
                for rank in self.requestors:
                    self.send_no_work(rank)
            else:
                # we do have work
                self.send_work_to_many()

            self.requestors = []

    def send_no_work(self, rank):
        """ send no work reply to someone requesting work"""

        buf = G.ABORT if self.abort else G.ZERO
        self.comm.send(buf, dest = rank, tag = T.WORK_REPLY)

    def spread_counts(self, rcount, wcount):
        """ Given wcount work items and rcount requesters
            spread it evenly among all requesters
        """

        base = wcount / rcount
        extra = wcount - (base * rcount)
        sizes = [base] * rcount
        for i in range(extra):
            sizes[i] += 1
        return sizes

    def send_work_to_many(self):
        rcount = len(self.requestors)
        wcount = len(self.workq)
        sizes = None
        if self.split == "equal":
            sizes = self.spread_counts(rcount, wcount)
        else:
            raise NotImplementedError

        logger.debug("requester count: %s, work count: %s, spread: %s" %
                     (rcount, wcount, sizes), extra=self.d)
        for idx, dest in enumerate(self.requestors):
            self.send_work(dest, sizes[idx])


    def send_work(self, dest, count):
        """
        @dest   - the rank of requester
        @count  - the number of work to send
        """
        # for termination detection
        if (dest < self.rank) or (dest == self.token_src):
            self.token_proc = G.BLACK

        # first message, send # of work items
        self.comm.send(count, dest, T.WORK_REPLY)

        # second message, actual work items
        self.comm.send(self.workq[0:count], dest, T.WORK_REPLY)
        logger.debug("%s work items sent to rank %s" % (count, dest), extra=self.d)

        # remove work items
        del self.workq[0:count]

    def request_work(self, cleanup = False):
        # check if we have request outstanding
        if self.workreq_outstanding:

            st = MPI.Status()
            reply = self.comm.Iprobe(source = self.work_requested_rank,
                                     tag = T.WORK_REPLY, status = st)
            if reply:
                self.work_receive(self.work_requested_rank)
                # flip flag to indicate we no longer waiting for reply
                self.workreq_outstanding = False

        elif not cleanup:
            # send request
            dest = self.next_proc()
            if dest == self.rank or dest == MPI.PROC_NULL:
                # have no one to ask, we are done
                return
            buf = G.ABORT if self.abort else G.MSG
            # blocking send
            self.comm.send(buf, dest, T.WORK_REQUEST)
            self.workreq_outstanding = True
            self.work_requested_rank = dest

    def work_receive(self, rank):
        """ when incoming work reply detected """

        # first message, check normal or abort
        buf = self.comm.recv(source = rank, tag = T.WORK_REPLY)
        logger.debug("receive work from rank %s, first msg, work count: %s"
                    % (rank, buf), extra=self.d)

        if buf == G.ABORT:
            logger.debug("receive abort signal")
            self.abort = True
            return
        elif buf == G.ZERO:
            logger.debug("receive zero signal", extra=self.d)
            # no follow up message, return
            return

        # second message, the actual work itmes
        buf = self.comm.recv(source = rank, tag = T.WORK_REPLY)
        logger.debug("receive work from rank %s, second msg:  %s"
                    % (rank, buf), extra=self.d)
        if buf is None:
            raise RuntimeError("work reply of 2nd message is None")
        else:
            self.workq.extend(buf)

    def finalize(self):
        """ clean up """
        pass


    def token_recv(self):
        # verify we don't have a local token
        if self.token_is_local:
            raise RuntimeError("token_is_local True")

        # this won't block as token is waiting
        buf = self.comm.recv(source = self.token_src, tag = T.TOKEN)
        if buf is None:
            raise RuntimeError("token color is None")
        self.token_color = buf

        # record token is local
        self.token_is_local = True

        # if we have a token outstanding, at this point
        # we should have received the reply (even if we
        # sent the token to ourself, we just replied above
        # so the send should now complete
        #
        if self.token_send_req != MPI.PROC_NULL:
            pass


        # now set our state
        if self.token_proc == G.BLACK:
            self.token_color = G.BLACK
            self.token_proc = G.WHITE

        # check for terminate condition
        terminate = False

        if self.rank == 0 and self.token_color == G.WHITE:
            # if rank 0 receive a white token
            logger.debug("Master detected termination", extra=self.d)
            terminate = True
        elif self.token_color == G.TERMINATE:
            terminate = True

        # forward termination token if we have one
        if terminate:
            # send terminate token, don't bother
            # if we the last rank
            self.token_color = G.TERMINATE
            if self.rank < self.size -1:
                self.token_issend()

            # set our state to terminate
            self.token_proc = G.TERMINATE

    def token_check(self):
        """
        check for token, and receive it if arrived
        """

        status = MPI.Status()
        flag = self.comm.Iprobe(self.token_src, T.TOKEN, status)
        if flag:
            self.token_recv()

    def token_issend(self):

        if self.abort: return

        logger.debug("token send: token_color = %s" % G.str[self.token_color], extra=self.d)

        self.comm.send(self.token_color,
            self.token_dest, tag = T.TOKEN)

        # now we don't have the token
        self.token_is_local = False

    def reduce(self, count):
        pass

    def reduce_check(self, cleanup=False):
        """
        initiate and progress a reduce operation at specified interval,
        ensure progress of reduction in background, stop reduction if cleanup flag is True
        """
        pass


    def set_loglevel(self, level):
        global logger
        logger.setLevel(level)

def logging_init(level=logging.INFO):

    global logger
    fmt = logging.Formatter(G.simple_fmt)
    logger.setLevel(level)
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)
    logger.propagate = False

