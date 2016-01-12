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
    b8k = 8 * 1024
    b16k = 16 * 1024
    b32k = 32 * 1024
    b64k = 64 * 1024
    b256k = 256 * 1024
    b512k = 512 * 1024
    b1m = 1024 * 1024
    b4m = 4 * b1m
    b8m = 8 * b1m
    b16m = 16 * b1m
    b128m = 128 * b1m
    b256m = 256 * b1m
    b512m = 512 * b1m
    b1g = 1024 * b1m
    b100g = 100 * b1g
    b256g = 256 * b1g
    b512g = 512 * b1g
    b1tb = 1024 * b1g
    bins = [b4k, b8k, b16k, b32k, b64k, b256k, b512k, b1m, b4m, b16m, b512m, b1g, b100g, b256g, b512g, b1tb]

    # 17 bins, the last bin is special
    bins_fmt = ["B1_000k_004k", "B1_004k_008k", "B1_008k_016k", "B1_016k_032k", "B1_032k_064k", "B1_064k_256k",
                "B1_256k_512k", "B1_512k_001m",
                "B2_001m_004m", "B2_m004_016m", "B2_016m_512m", "B2_512m_001g",
                "B3_001g_100g", "B3_100g_256g", "B3_256g_512g",
                "B4_512g_001t",
                "B5_001t_up"]

    # GPFS
    gpfs_block_size = ("256k", "512k", "b1m", "b4m", "b8m")
    gpfs_block_cnt = [0, 0, 0, 0, 0]
    gpfs_subs = (b256k/32, b512k/32, b1m/32, b4m/32, b8m/32)

