from __future__ import print_function
from mpi4py import MPI
from globals import T, G
from copy import copy
import logging
import random

logger = logging.getLogger("circle")

class Token:
    pass

class Circle:

    # Keep init() and reset() in sync ... it is a pain, I know

    def __init__(self, name="Circle Work Comm",  split = "equal",
                 reduce_interval=10, k=2):

        random.seed()  # use system time to seed
        logging_init()

        self.comm = MPI.COMM_WORLD.Dup()
        self.comm.Set_name(name)
        self.size = self.comm.Get_size()
        self.rank = self.comm.Get_rank()

        # debug
        self.d = {"rank" : "rank %s" % self.rank}


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
        self.reduce_buf = [0] * 3   # work items, work bytes

        # barriers
        self.barrier_started = False
        self.barrier_up = False    # flag to indicate barrier sent to parent
        self.barrier_replies = 0

        # token
        self.token_init()

        # tree
        self.tree_init(k)

    def token_init(self):

        self.token_src = (self.rank - 1 + self.size) % self.size
        self.token_dest = (self.rank + 1 + self.size) % self.size
        self.token_color = G.BLACK
        self.token_proc = G.WHITE
        self.token_is_local = False
        if self.rank == 0:
            self.token_is_local = True
            self.token_color = G.WHITE
            self.token_proc  = G.WHITE
        self.token_send_req = MPI.REQUEST_NULL


    def tree_init(self, k):
        self.k = k
        self.parent_rank = MPI.PROC_NULL
        self.child_ranks = []  # [MPI.PROC_NULL] * k is too much C
        self.children = 0
        # compute rank of parent if we have one
        if self.rank > 0:
            self.parent_rank = (self.rank - 1) / k

        # identify ranks of what would be leftmost and rightmost children
        left = self.rank * k + 1
        right = self.rank * k + k

        # if we have at least one child
        # compute number of children and list of child ranks
        if left < self.size:
            # adjust right child in case we don't have a full set of k
            if right >= self.size:
                right = self.size - 1;
            # compute number of children and the list
            self.children = right - left + 1

            for i in range(self.children):
                self.child_ranks.append( left + i)

        logger.debug("parent: %s, children: %s" % (self.parent_rank, self.child_ranks),
                        extra=self.d)

    def next_proc(self):
        """ Note next proc could return rank of itself """
        if self.size == 1:
            return MPI.PROC_NULL
        else:
            return random.randint(0, self.size-1)


    def token_status(self):
        return "rank: %s, token_src: %s, token_dest: %s, token_color: %s, token_proc: %s" % \
            (self.rank, self.token_src, self.token_dest, G.str[self.token_color], G.str[self.token_proc])

    def workq_info(self):
        s =  "has %s items in work queue\n" % len(self.workq)
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

        if self.rank == 0:
            logger.debug("Loop finished, cleaning up ... ", extra=self.d)

        self.cleanup()

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


    def barrier_start(self):
        self.barrier_started = True

    def barrier_test(self):
        if not self.barrier_start:
            return False

        # check if we have received message from all children
        if self.barrier_replies < self.children:
            # still waiting for barries from children
            st = MPI.Status()
            flag = self.comm.Iprobe(MPI.ANY_SOURCE, T.BARRIER, st)
            if flag:
                child = st.Get_source()
                self.comm.recv(source = child, tag = T.BARRIER)
                self.barrier_replies += 1

        # if we have not sent a message to our parent, and we
        # have received a message from all of our children (or we have no children)
        # send a message to our parent
        if not self.barrier_up and self.barrier_replies == self.children:
            if self.parent_rank != MPI.PROC_NULL:
                self.comm.send(None, self.parent_rank, T.BARRIER)

            # transition to state where we're waiting for parent
            # to notify us that the barrier is complete
            self.barrier_up = True

        # wait for message to come back down from parent to mark end of barrier
        complete = False
        if self.barrier_up:
            if self.parent_rank != MPI.PROC_NULL:
                # check for message from parent
                flag = self.comm.Iprobe(self.parent_rank, T.BARRIER)
                if flag:
                    self.comm.recv(source = self.parent_rank, tag = T.BARRIER)
                    # mark barrier as complete
                    complete = True
            else:
                # we have no parent, we must be root
                # so mark the barrier complete
                complete = True

        # barrier is complete, send messages to children if any and return true
        if complete:
            for child in self.child_ranks:
                self.comm.send(dest=child, tag = T.BARRIER)

            # reset state for another barrier
            self.barrier_started = False
            self.barrier_up = False
            self.barrier_replies = 0
            return True

        # barrier still not complete
        return False


    def bcast_abort(self):
        self.abort = True
        buf = G.ABORT
        for i in range(self.size):
            if (i != self.rank):
                self.comm.send(buf, dest = i, tag = T.WORK_REQUEST)
                logger.warn("abort message sent to %s" % i, extra=self.d)


    def cleanup(self):
        while True:
            # start non-block barrier if we have no outstanding items
            if not self.reduce_outstanding and not self.workreq_outstanding and \
                self.token_send_req == MPI.REQUEST_NULL:
                self.barrier_start()

            # break the loop when non-blocking barrier completes
            if self.barrier_test():
                break

            # send no work message for any work request that comes in
            self.workreq_check(cleanup = True)

            # clean up any outstanding reduction
            if self.reduce_enabled:
                self.reduce_check(cleanup = True)

            # recv any incoming work reply messages
            self.request_work(cleanup = True)

            # check and recv any incoming token
            self.token_check()

            # if we have an outstanding token, check if it has been recv'ed
            # I don't think this is needed as there seem no side effect
            if self.token_send_req != MPI.REQUEST_NULL:
                if self.token_send_req.Test():
                    self.token_send_req = MPI.REQUEST_NULL

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
        while True:
            st = MPI.Status()
            ret = self.comm.Iprobe(source = MPI.ANY_SOURCE, tag = T.WORK_REQUEST, status = st)
            if not ret: break
            # we have work request message
            rank = st.Get_source()
            buf = self.comm.recv(source = rank, tag = T.WORK_REQUEST, status = st)
            if buf == G.ABORT:
                logger.warn("Abort request from rank %s" % rank, extra=self.d)
                self.abort = True
                self.send_no_work(rank)
                return
            else:
                logger.debug("receive work request from requestor [%s]"  % rank, extra=self.d)
                # add rank to requesters
                self.requestors.append(rank)

        if len(self.requestors) == 0:
            return
        else:
            logger.debug("have %s requesters, with work items in queue: %s" %
                         (len(self.requestors), self.workq), extra=self.d)
            # have work requesters
            if len(self.workq) == 0 or cleanup:
                for rank in self.requestors:
                    self.send_no_work(rank)
            else:
                # we do have work
                self.send_work_to_many()

            self.requestors = []

    def spread_counts(self, rcount, wcount):
        """ Given wcount work items and rcount requesters
            spread it evenly among all requesters
        """

        base = wcount / (rcount + 1)            # leave self a base number of works
        extra = wcount - base * (rcount + 1)
        if extra > rcount: extra = rcount       # take fewer, no big deal
        sizes = [base] * rcount
        for i in range(extra):
            sizes[i] += 1
        return sizes

    def send_no_work(self, rank):
        """ send no work reply to someone requesting work"""

        buf = { G.KEY: G.ABORT } if self.abort else { G.KEY: G.ZERO }
        r = self.comm.isend(buf, dest = rank, tag = T.WORK_REPLY)
        r.wait()

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


    def send_work(self, rank, count):
        """
        @dest   - the rank of requester
        @count  - the number of work to send
        """
        # for termination detection
        if (rank < self.rank) or (rank == self.token_src):
            self.token_proc = G.BLACK

        buf = { G.KEY: count,
                G.VAL: self.workq[0:count] }

        self.comm.send(buf, dest = rank, tag = T.WORK_REPLY)
        logger.debug("%s work items sent to rank %s" % (count, rank), extra=self.d)

        # remove work items
        del self.workq[0:count]


    def request_work(self, cleanup = False):
        # check if we have request outstanding
        if self.workreq_outstanding:
            logger.debug("has req outstanding, dest = %s" % self.work_requested_rank,
                         extra = self.d)
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
            logger.debug("send work request to %s : %s" % (dest, G.str[buf]),
                         extra = self.d)
            self.comm.send(buf, dest, T.WORK_REQUEST)
            self.workreq_outstanding = True
            self.work_requested_rank = dest


    def work_receive(self, rank):
        """ when incoming work reply detected """

        buf = self.comm.recv(source = rank, tag = T.WORK_REPLY)

        if buf[G.KEY] == G.ABORT:
            logger.debug("receive abort signal", extra=self.d)
            self.abort = True
            return
        elif buf[G.KEY] == G.ZERO:
            logger.debug("receive no work signal", extra=self.d)
            return
        else:
            assert type(buf[G.VAL]) == list
            self.workq.extend(buf[G.VAL])

    def finalize(self):
        """ clean up """
        pass


    def token_recv(self):
        # verify we don't have a local token
        if self.token_is_local:
            raise RuntimeError("token_is_local True")

        # this won't block as token is waiting
        buf = self.comm.recv(source = self.token_src, tag = T.TOKEN)

        # record token is local
        self.token_is_local = True

        # if we have a token outstanding, at this point
        # we should have received the reply (even if we
        # sent the token to ourself, we just replied above
        # so the send should now complete
        #
        if self.token_send_req != MPI.PROC_NULL:
            self.token_send_req.Wait()

        # now send is complete, we can overwrite
        self.token_color = buf

        # now set our state
        # a black machine receives a black token, set machine color as white
        if self.token_proc == G.BLACK and self.token_color == G.BLACK:
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

        self.token_send_req = self.comm.issend(self.token_color,
            self.token_dest, tag = T.TOKEN)

        # now we don't have the token
        self.token_is_local = False

    def reduce_init(self, x):
        self.reduce_items = x

    def reduce(self, buf):
        # copy data from user buffer
        # do I really need to copy?
        self.reduce_buf = copy(buf)


    def reduce_check(self, cleanup=False):
        """
        initiate and progress a reduce operation at specified interval,
        ensure progress of reduction in background, stop reduction if cleanup flag is True
        """

        # if we have outstanding reduce, check message from children
        # otherwise, check whether we should start new reduce

        if self.reduce_outstanding:
            for child in self.child_ranks:
                flag = self.comm.Iprobe(child, T.REDUCE)
                if flag:
                    # receive message from child
                    # 0st element is G.MSG_VALID or not
                    # 1st element is number of completed work items
                    # 2nd element is number of bytes of user data
                    inbuf = self.comm.recv(source = child, tag = T.REDUCE)
                    self.reduce_replies += 1

                    logger.debug("client data from %s: %s" %
                                 (child, inbuf), extra=self.d)

                    if inbuf[0] == G.MSG_INVALID:
                        self.reduce_buf[0] = False
                    else:
                        self.reduce_buf[0] = True
                        self.reduce_buf[1] += inbuf[1]
                        self.reduce_buf[2] += inbuf[2]

                        # invoke user's callback to reduce user data
                        if hasattr(self.task, "reduce"):
                            self.task.reduce(self.reduce_buf, inbuf)


            # check if we have gotten replies from all children
            if self.reduce_replies == self.children:
                # all children replied
                # add our own contents to reduce buffer
                self.reduce_buf[1] += self.work_processed

                # send message to parent if we have one
                if self.parent_rank != MPI.PROC_NULL:
                    self.comm.send(self.reduce_buf, self.parent_rank, T.REDUCE)
                else:
                    # we are the root, print results if we have valid data
                    if self.reduce_buf[0]:
                        logger.info("Object processed: %s" % self.reduce_buf[1], extra=self.d)

                    # invoke callback on root to deliver final results (?)
                    if hasattr(self.task, "reduce_finish"):
                        self.task.reduce_finish(self.reduce_buf)

                # disable flag to indicate we got what we want
                self.reduce_outstanding = False
        else:
            # we don't have an outstanding reduction
            # determine if a new reduce should be started
            # only bother checking if we think it is about time or
            # we are in cleanup mode

            start_reduce = False

            time_now = MPI.Wtime()
            time_next = self.reduce_time_last + self.reduce_time_interval
            if time_now >= time_next or cleanup:
                if self.parent_rank == MPI.PROC_NULL:
                    # we are root, kick it off
                    start_reduce = True
                else:
                    # we are not root, check if parent sent us a message
                    flag = self.comm.Iprobe(self.parent_rank, T.REDUCE)
                    if flag:
                        # receive message from parent and set flag to start reduce
                        self.comm.recv(source=self.parent_rank, tag = T.REDUCE)
                        start_reduce = True

            # it is critical that we don't start a reduce if we are in cleanup
            # phase because we may have already started the non-blocking barrier
            # just send an invalid message back to parent
            if start_reduce and cleanup:
                # avoid starting a reduce
                start_reduce = False
                # if we have parent, send invalid msg
                if self.parent_rank != MPI.PROC_NULL:
                    self.reduce_buf[0] = G.MSG_INVALID
                    self.comm.send(self.reduce_buf, self.parent_rank, T.REDUCE)

            if start_reduce:
                # set flag to indicate we have a reduce outstanding
                # and initiate state for a fresh reduction

                self.reduce_time_last = time_now
                self.reduce_outstanding = True
                self.reduce_replies = 0
                self.reduce_buf[0] = G.MSG_VALID
                self.reduce_buf[1] = 0
                self.reduce_buf[2] = 0

                # invoke callback to get input data
                if hasattr(self.task, "reduce_init"):
                    self.task.reduce_init()

                # sent message to each child
                for child in self.child_ranks:
                    self.comm.send(None, child, T.REDUCE)


    def set_loglevel(self, level):
        global logger
        logger.setLevel(level)


    def debug_info(self):
        ret = "request %s, " % self.workreq_outstanding
        if self.token_send_req == MPI.REQUEST_NULL:
            token_send_req = False
        else:
            token_send_req = True
        ret = ret + "token_send_request: %s, " % token_send_req
        ret = ret + "reduce outstanding: %s, " % self.reduce_outstanding
        ret = ret + "barrier started: %s" % self.barrier_started
        return ret

    # define method alias
    finalize = __init__

def logging_init(level=logging.INFO):

    global logger
    fmt = logging.Formatter(G.simple_fmt)
    logger.setLevel(level)
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)
    logger.propagate = False

