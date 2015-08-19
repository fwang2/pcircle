class T:
    WORK_REQUEST = 1
    WORK_REPLY = 2
    REDUCE = 3
    BARRIER = 4
    TOKEN = 7


class G:
    ZERO = 0
    ABORT = -1
    WHITE = 50
    BLACK = 51
    NONE = -99
    TERMINATE = -100
    MSG = 99
    MSG_VALID = True
    MSG_INVALID = False

    fmt1 = '%(asctime)s - %(levelname)s - %(rank)s:%(filename)s:%(lineno)d - %(message)s'
    fmt2 = '%(asctime)s - %(rank)s:%(filename)s:%(lineno)d - %(message)s'
    bare_fmt = '%(name)s - %(levelname)s - %(message)s'
    mpi_fmt = '%(name)s - %(levelname)s - %(rank)s - %(message)s'
    bare_fmt2 = '%(message)s'

    str = {WHITE: "white", BLACK: "black", NONE: "not set", TERMINATE: "terminate",
           ABORT: "abort", MSG: "message"}

    KEY = "key"
    VAL = "val"
    logger = None
    logfile = None
    loglevel = "warn"
    use_store = False
    fix_opt = False

    DB_BUFSIZE = 10000
