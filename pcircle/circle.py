from __future__ import print_function
from mpi4py import MPI
from copy import copy
import random
import sys
import os
import os.path
from collections import deque
from pprint import pprint

"""

Reduce Note:

    The reduce logic is located in reduce_check().
    There are three related functions that expects from a task: be it FCP or FWALK

    (1) reduce_init(buf): this is invoked at the the starting point of a reduce operation.
    The "buf" is self.reduce_buf

    (2) reduce(buf1, buf2): buf1 is self.reduce_buf; buf2 is from one of the child input.
    Usually, each child will report a number, which represent the amount of task it has performed
    during this period. self.reduce_buf simply accumulate it - non-descreasing.

    In that sense, the buffer (a free form dictionary) is not really needed. A simple return of
    of integer number might make more sense. Each child will simply return a number each time reduce()
    is called, and that number represent the amount of works it has done over this period. Once reduce()
    is called, the number is reset to zero.

    Inside the circle, self.reduce_buf -> let's name it as self.reduce_work is a number.

        for each child:
            self.reduce_work += self.task.reduce()

        if I have parent:
            send self.reduce_work upward
            self.reduce_work = 0

        if I am root:
            self.reduce_report()


    (3) reduce_finish(): I don't see this is in use today.

"""

from pcircle.globals import T, G
from pcircle.dbstore import DbStore
from pcircle.utils import getLogger
from pcircle.token import Token

DB_BUFSIZE = 10000


class Circle:
    def __init__(self, name="Circle", split="equal", k=2, dbname=None, resume=False):

        random.seed()  # use system time to seed
        self.comm = MPI.COMM_WORLD
        self.comm.Set_name(name)
        self.size = self.comm.Get_size()
        self.rank = self.comm.Get_rank()
        self.d = {"rank": "rank %s" % self.rank}
        self.logger = getLogger(__name__)


        self.useStore = G.use_store
        self.split = split
        self.dbname = dbname
        self.resume = resume
        self.reduce_time_interval = G.reduce_interval

        self.task = None
        self.abort = False
        self.requestors = []

        """
        # workq buffer
        if self.useStore:
            self.workq_buf = deque()
        """
        # counters
        self.work_requested = 0
        self.work_processed = 0
        self.work_request_received = 0
        self.workreq_outstanding = False
        self.workreq_rank = None

        # reduction
        self.reduce_enabled = True
        self.reduce_time_last = MPI.Wtime()
        self.reduce_outstanding = False
        self.reduce_replies = 0
        self.reduce_buf = {}
        self.reduce_status = None

        # barriers
        self.barrier_started = False
        self.barrier_up = False  # flag to indicate barrier sent to parent
        self.barrier_replies = 0

        self.workdir = os.getcwd()
        self.tempdir = os.path.join(self.workdir, ".pcircle")
        if not os.path.exists(self.tempdir):
            try:
                os.mkdir(self.tempdir)
            except OSError:
                pass

        # token
        self.token = Token(self)

        # tree init
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
                right = self.size - 1
            # compute number of children and the list
            self.children = right - left + 1

            for i in range(self.children):
                self.child_ranks.append(left + i)

        self.logger.debug("parent: %s, children: %s" % (self.parent_rank, self.child_ranks),
                          extra=self.d)

        # workq init
        # TODO: compare list vs. deque
        """
        if G.use_store:
            self.workq_init(dbname, resume)
        else:
            self.workq = []
        """
        self.use_store = False
        self.workq = []
	self.workq_buf = []
        self.workq_init(dbname, resume)

        self.logger.debug("Circle initialized", extra=self.d)

    def finalize(self, cleanup=True):
        #if cleanup and G.use_store:
        self.workq_db.cleanup()

    def workq_init(self, dbname=None, resume=False):

        # NOTE: the db filename and its rank is seprated with "-"
        # we rely on this to separate, so the filename itself (within our control)
        # should not use dash ... the default is to use "." for sepration
        # Yes, this is very fragile, hopefully we will fix this later

        if dbname is None:
            self.dbname = os.path.join(self.tempdir, "workq-%s" % self.rank)
        else:
            self.dbname = os.path.join(self.tempdir, "%s-%s" % (dbname, self.rank))

        self.workq_db = DbStore(self.dbname, resume=resume)

    # after task(fcp) creation, push works in workq_buf into workq_db
    def push_remaining_buf(self):
        if len(self.workq_buf) > 0:
            self.workq_db.mput(self.workq_buf)
            del self.workq_buf[:]

    def next_proc(self):
        """ Note next proc could return rank of itself """
        if self.size == 1:
            return MPI.PROC_NULL
        else:
            return random.randint(0, self.size - 1)

    def workq_info(self):
        s = "has %s items in work queue\n" % (len(self.workq) + len(self.workq_buf) + len(self.workq_db))
        return s

    def qsize(self):
        return (len(self.workq) + len(self.workq_buf) + len(self.workq_db))

    def begin(self, task):
        """ entry point to work """

        self.task = task
        self.task.create()
        self.comm.barrier()
        self.loop()

        if self.rank == 0:
            self.logger.debug("Loop finished, cleaning up ... ", extra=self.d)
        self.cleanup()
        self.comm.barrier()

        if self.qsize() != 0:
            pprint("Rank %s workq.len = %s" % (self.rank, self.qsize()))
            pprint(self.__dict__)
            sys.stdout.flush()
            self.comm.Abort(1)

    def loop(self):
        """ central loop to finish the work """
        while True:

            # check for and service requests
            self.workreq_check()

            if self.reduce_enabled:
                self.reduce_check()

            if self.qsize() == 0:
                self.request_work()

            # if I have work, and no abort signal, process one
            if self.qsize() > 0 and not self.abort:
                self.task.process()
                self.work_processed += 1
            else:
                status = self.token.check_for_term()
                if status == G.TERMINATE:
                    break

    def enq(self, work):
        if work is None:
            self.logger.warn("enq work item is None", extra=self.d)
            return
        if len(self.workq) < G.memitem_threshold:
            self.workq.append(work)
            return
        else:
            self.use_store = True
            self.workq_buf.append(work)
            if len(self.workq_buf) == G.DB_BUFSIZE:
                self.workq_db.mput(self.workq_buf)
                del self.workq_buf[:]

    def preq(self, work):
        self.workq.insert(0, work)

    def setq(self, q):
        self.workq = q

    def deq(self):
        # deque a work starting from workq, then from workq_buf, then from workq_db
        if len(self.workq) > 0:
            return self.workq.pop()
	elif len(self.workq_buf) > 0:
            return self.workq_buf.pop()
        elif len(self.workq_db) > 0:
            #read a batch of works into memory
            self.workq, _ = self.workq_db.mget(G.memitem_threshold)
            self.workq_db.mdel(G.memitem_threshold)
            return self.workq.pop()
        else:
            return None

    def barrier_start(self):
        self.barrier_started = True

    def barrier_test(self):
        if not self.barrier_started:
            return False

        # check if we have received message from all children
        if self.barrier_replies < self.children:
            # still waiting for barries from children
            st = MPI.Status()
            flag = self.comm.Iprobe(MPI.ANY_SOURCE, T.BARRIER, st)
            if flag:
                child = st.Get_source()
                self.comm.recv(source=child, tag=T.BARRIER)
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
                    self.comm.recv(source=self.parent_rank, tag=T.BARRIER)
                    # mark barrier as complete
                    complete = True
            else:
                # we have no parent, we must be root
                # so mark the barrier complete
                complete = True

        # barrier is complete, send messages to children if any and return true
        if complete:
            for child in self.child_ranks:
                self.comm.send(None, dest=child, tag=T.BARRIER)

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
                self.comm.send(buf, dest=i, tag=T.WORK_REQUEST)
                self.logger.warn("abort message sent to %s" % i, extra=self.d)

    def cleanup(self):
        while True:
            # start non-block barrier if we have no outstanding items
            if not self.reduce_outstanding and \
                    not self.workreq_outstanding and \
                    self.token.send_req == MPI.REQUEST_NULL:
                self.barrier_start()

            # break the loop when non-blocking barrier completes
            if self.barrier_test():
                break

            # send no work message for any work request that comes in
            self.workreq_check(cleanup=True)

            # clean up any outstanding reduction
            if self.reduce_enabled:
                self.reduce_check(cleanup=True)

            # recv any incoming work reply messages
            self.request_work(cleanup=True)

            # check and recv any incoming token
            self.token.check_and_recv()

            # if we have an outstanding token, check if it has been recv'ed
            # I don't think this is needed as there seem no side effect
            if self.token.send_req != MPI.REQUEST_NULL:
                if self.token.send_req.Test():
                    self.token.send_req = MPI.REQUEST_NULL


    def workreq_check(self, cleanup=False):
        """ for any process that sends work request message:
                add the process to the requester list
            if my work queue is not empty:
                distribute the work evenly
            else:
                send "no work" message to each requester
            reset the requester list to empty
        """
        while True:
            st = MPI.Status()
            ret = self.comm.Iprobe(source=MPI.ANY_SOURCE, tag=T.WORK_REQUEST, status=st)
            if not ret:  # no work request, break out the loop
                break
            # we have work request message
            rank = st.Get_source()
            buf = self.comm.recv(source=rank, tag=T.WORK_REQUEST, status=st)
            if buf == G.ABORT:
                self.logger.warn("Abort request from rank %s" % rank, extra=self.d)
                self.abort = True
                self.send_no_work(rank)
                return
            else:
                self.logger.debug("receive work request from requestor [%s]" % rank, extra=self.d)
                # add rank to requesters
                self.requestors.append(rank)

        # out of while loop

        if not self.requestors:
            return
        else:
            # first combine workq and work_buf, both of them are in memory
            if len(self.workq_buf) > 0:
                self.workq.extend(self.workq_buf)
                del self.workq_buf[:]

            # if in-memory workq is empty, get a batch of works from database
            if len(self.workq) == 0 and len(self.workq_db) > 0:
                self.workq, _  = self.workq_db.mget(G.memitem_threshold)
                self.workq_db.mdel(G.memitem_threshold)

            self.logger.debug("have %s requesters, with %s work items in queue" %
                              (len(self.requestors), len(self.workq)), extra=self.d)
            # have work requesters
            if self.qsize() == 0 or cleanup:
                for rank in self.requestors:
                    self.send_no_work(rank)
            else:
                # we do have work
                self.send_work_to_many()

            self.requestors = []

    def spread_counts(self, rcount, wcount):
        """
        @rcount: # of requestors
        @wcount: # of work items
        @return: spread it evenly among all requesters

        case 1: wcount == rcount:
                    base = 0
                    extra = wcount
                    each requestor get 1
        case 2: wcount < rcount:
                    base = 0
                    extra = wcount
                    first "wcount" requester get 1
        case 3: wcount > rcount:
                    is it possible?
        """

        if self.split != "equal":
            raise NotImplementedError
        base = wcount / (rcount + 1)  # leave self a base number of works
        extra = wcount - base * (rcount + 1)
        assert extra <= rcount
        sizes = [base] * rcount
        for i in xrange(extra):
            sizes[i] += 1
        return sizes

    def send_no_work(self, rank):
        """ send no work reply to someone requesting work"""

        buf = {G.KEY: G.ABORT} if self.abort else {G.KEY: G.ZERO}
        r = self.comm.isend(buf, dest=rank, tag=T.WORK_REPLY)
        r.wait()
        self.logger.debug("Send no work reply to %s" % rank, extra=self.d)

    def send_work_to_many(self):
        rcount = len(self.requestors)
        wcount = len(self.workq)
        sizes = self.spread_counts(rcount, wcount)

        self.logger.debug("requester count: %s, work count: %s, spread: %s" %
                          (rcount, wcount, sizes), extra=self.d)
        for idx, dest in enumerate(self.requestors):
            self.send_work(dest, sizes[idx])

    def send_work(self, rank, witems):
        """
        @dest   - the rank of requester
        @count  - the number of work to send
        """
        if witems <= 0:
            self.send_no_work(rank)
            return

        # for termination detection
        if (rank < self.rank) or (rank == self.token.src):
            self.token.proc = G.BLACK

        buf = None

        # based on if it is memory or store-based
        # we have different ways of constructing buf
        #if self.useStore:
        #   objs, size = self.workq.mget(witems)
        #    buf = {G.KEY: witems, G.VAL: objs}
        #else:
        buf = {G.KEY: witems, G.VAL: self.workq[0:witems]}

        self.comm.send(buf, dest=rank, tag=T.WORK_REPLY)
        self.logger.debug("%s work items sent to rank %s" % (witems, rank), extra=self.d)

        # remove (witems) of work items
        # for DbStotre, all we need is a number, not the actual objects
        # for KVStore, we do need the object list for its key value
        # the "size" is a bit awkward use - we know the size after we do mget()
        # however, it is not readily available when we do mdel(), so we keep
        # previous data and pass it back in to save us some time.
        #

        #if self.useStore:
        #   self.workq.mdel(witems, size)
        #else:
        del self.workq[0:witems]

    def request_work(self, cleanup=False):
        if self.workreq_outstanding:
            st = MPI.Status()
            reply = self.comm.Iprobe(source=self.work_requested_rank,
                                     tag=T.WORK_REPLY, status=st)
            if reply:
                self.work_receive(self.work_requested_rank)
                # flip flag to indicate we no longer waiting for reply
                self.workreq_outstanding = False
                # else:
                #    self.logger.debug("has req outstanding, dest = %s, no reply" %
                #                 self.work_requested_rank, extra = self.d)

        elif not cleanup:
            # send request
            dest = self.next_proc()
            if dest == self.rank or dest == MPI.PROC_NULL:
                # have no one to ask, we are done
                return
            buf = G.ABORT if self.abort else G.MSG
            # blocking send
            self.logger.debug("send work request to rank %s : %s" % (dest, G.str[buf]),
                              extra=self.d)
            self.comm.send(buf, dest, T.WORK_REQUEST)
            self.workreq_outstanding = True
            self.work_requested_rank = dest

    def work_receive(self, rank):
        """ when incoming work reply detected """

        buf = self.comm.recv(source=rank, tag=T.WORK_REPLY)

        if buf[G.KEY] == G.ABORT:
            self.logger.debug("receive abort signal", extra=self.d)
            self.abort = True
            return
        elif buf[G.KEY] == G.ZERO:
            self.logger.debug("receive no work signal", extra=self.d)
            return
        else:
            assert type(buf[G.VAL]) == list
            self.workq.extend(buf[G.VAL])

    def reduce(self, buf):
        # copy data from user buffer
        self.reduce_buf = copy(buf)

    def reduce_check(self, cleanup=False):
        """
        initiate and progress a reduce operation at specified interval,
        ensure progress of reduction in background, stop reduction if cleanup flag is True
        """

        if self.reduce_outstanding:
            # if we have outstanding reduce, check message from children
            # otherwise, check whether we should start new reduce

            for child in self.child_ranks:
                if self.comm.Iprobe(source=child, tag=T.REDUCE):
                    # receive message from child
                    # 'status' element is G.MSG_VALID or not
                    # the rest is opaque
                    inbuf = self.comm.recv(source=child, tag=T.REDUCE)
                    self.reduce_replies += 1

                    self.logger.debug("client data from %s: %s" %
                                      (child, inbuf), extra=self.d)

                    if inbuf['status'] == G.MSG_INVALID:
                        self.reduce_status = False
                    else:
                        self.reduce_status = True
                        # invoke user's callback to reduce user data
                        if hasattr(self.task, "reduce"):
                            self.task.reduce(self.reduce_buf, inbuf)

            # check if we have gotten replies from all children
            if self.reduce_replies == self.children:
                # all children replied
                # add our own contents to reduce buffer

                # send message to parent if we have one
                if self.parent_rank != MPI.PROC_NULL:
                    self.comm.send(self.reduce_buf, self.parent_rank, T.REDUCE)
                else:
                    # we are the root, print results if we have valid data
                    if self.reduce_status and hasattr(self.task, "reduce_report"):
                        self.task.reduce_report(self.reduce_buf)

                    # invoke callback on root to deliver final results
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
                elif self.comm.Iprobe(source=self.parent_rank, tag=T.REDUCE):
                    # we are not root, check if parent sent us a message
                    # receive message from parent and set flag to start reduce
                    self.comm.recv(source=self.parent_rank, tag=T.REDUCE)
                    start_reduce = True

            # it is critical that we don't start a reduce if we are in cleanup
            # phase because we may have already started the non-blocking barrier
            # just send an invalid message back to parent
            if start_reduce and cleanup:
                # avoid starting a reduce
                start_reduce = False
                # if we have parent, send invalid msg
                if self.parent_rank != MPI.PROC_NULL:
                    self.reduce_status = G.MSG_INVALID
                    self.comm.send(self.reduce_buf, self.parent_rank, T.REDUCE)

            if start_reduce:
                # set flag to indicate we have a reduce outstanding
                # and initiate state for a fresh reduction

                self.reduce_time_last = time_now
                self.reduce_outstanding = True
                self.reduce_replies = 0
                self.reduce_status = G.MSG_VALID
                self.reduce_buf['status'] = G.MSG_VALID

                # invoke callback to get input data
                if hasattr(self.task, "reduce_init"):
                    self.task.reduce_init(self.reduce_buf)

                # sent message to each child
                for child in self.child_ranks:
                    self.comm.send(None, child, T.REDUCE)

    @staticmethod
    def exit(code):
        MPI.Finalize()
        sys.exit(code)
