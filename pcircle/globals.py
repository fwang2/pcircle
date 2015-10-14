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
    preserve = False
    DB_BUFSIZE = 10000
    totalsize = 0
    src = None
    dest = None
    args_src = None
    args_dest = None
    resume = None
    reduce_interval = 10
    verbosity = 0
    am_root = False
    copytype = 'dir2dir'

    b4k = 4 * 1024
    b64k = 64 * 1024
    b512k = 512 * 1024
    b1m = 1024 *1024
    b4m = 4 * b1m
    b16m = 16 * b1m
    b512m = 512 * b1m
    b1g = 2 * b512m
    b100g = 100 * b1g
    b512g = 512 * b1g
    b1tb = 1024 * b1g
    bins = [b4k, b64k,b512k, b1m, b4m, b16m, b512m, b1g, b512g, b1tb]
