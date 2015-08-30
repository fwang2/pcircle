__author__ = 'f7b'

from mpi4py import MPI
from globals import G, T
from utils import getLogger


def colorstr(c):
    if c == G.BLACK:
        return "black"
    elif c == G.WHITE:
        return "white"
    elif c == G.TERMINATE:
        return "terminate"
    else:
        return "unknown"


class Token:

    def __init__(self, circle):
        """ The Token protocol
        src: which rank send token to me
        dest: which rank to send token to next
        color: current token color
        proc: current color of process (black, white, terminate)
        send_req: request associating with pending receive
        """
        self.circle = circle

        self.rank = circle.rank
        self.size = circle.size
        self.comm = circle.comm
        self.src = (self.rank - 1 + self.size) % self.size
        self.dest = (self.rank + 1 + self.size) % self.size
        self.color = G.BLACK
        self.proc = G.WHITE
        self.is_local = False
        if self.rank == 0:
            self.is_local = True
            self.color = G.WHITE
            self.proc = G.WHITE
        self.send_req = MPI.REQUEST_NULL

        self.d = {"rank": "rank %s" % self.rank}
        self.logger = getLogger(__name__)

    def check_and_recv(self):
        """ check for token, and receive it if arrived """

        status = MPI.Status()
        flag = self.comm.Iprobe(self.src, T.TOKEN, status)
        if flag:
            self.recv()

    def issend(self):
        """ send token -- it's important that we use issend here,
        because this way the send won't complete until a matching
        receive has been posted, which means as long as the send
        is pending, the message is still on the wire """

        if self.circle.abort:
            return

        self.logger.debug("token send to rank %s: token_color = %s" %
                          (self.dest, colorstr(self.color)), extra=self.d)

        self.send_req = self.comm.issend(self.color, self.dest, tag=T.TOKEN)

        # now we don't have the token
        self.is_local = False

    def recv(self):
        # verify we don't have a local token
        if self.is_local:
            raise RuntimeError("token_is_local True")

        # this won't block as token is waiting
        buf = self.comm.recv(source=self.src, tag=T.TOKEN)

        # record token is local
        self.is_local = True

        # if we have a token outstanding, at this point
        # we should have received the reply (even if we
        # sent the token to ourself, we just replied above
        # so the send should now complete
        #
        if self.send_req != MPI.PROC_NULL:
            self.send_req.Wait()

        # now send is complete, we can overwrite
        self.color = buf

        # now set our state
        # a black machine receives a black token, set machine color as white
        if self.proc == G.BLACK and self.color == G.BLACK:
            self.proc = G.WHITE

        # check for terminate condition
        terminate = False

        if self.rank == 0 and self.color == G.WHITE:
            # if rank 0 receive a white token
            terminate = True
        elif self.color == G.TERMINATE:
            terminate = True

        # forward termination token if we have one
        if terminate:
            # send terminate token, don't bother
            # if we the last rank
            self.color = G.TERMINATE
            if self.rank < self.size - 1:
                self.issend()

            # set our state to terminate
            self.proc = G.TERMINATE

    def check_for_term(self):
        if self.proc == G.TERMINATE:
            return G.TERMINATE

        if self.size == 1:
            self.proc = G.TERMINATE
            return G.TERMINATE

        if self.is_local:
            # we have no work, but we have token
            if self.rank == 0:
                # rank 0 start with white token
                self.color = G.WHITE
            elif self.proc == G.BLACK:
                # others turn the token black
                # if they are in black state
                self.color = G.BLACK

            self.issend()
            # flip process color from black to white
            self.proc = G.WHITE
        else:
            # we have no work, but we don't have the token
            self.check_and_recv()

        # return machine token state
        return self.proc

    def __repr__(self):
        buf = "Token@Rank %s:" % self.rank
        buf += "token.proc: %s / " % colorstr(self.proc)
        buf += "token.color: %s / " % colorstr(self.color)
        buf += "token.local: %r / " % self.is_local
        if self.send_req == MPI.REQUEST_NULL:
            buf += "send_req: NULL"
        else:
            buf += "send_req: Not NULL"
        return buf
