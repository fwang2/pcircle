class T:
    WORK_REQUEST    = 1
    WORK_REPLY      = 2
    REDUCE          = 3
    BARRIER         = 4
    MSG_VALID       = 5
    MSG_INVALID     = 6
    TOKEN           = 7

class G:

    ZERO            = 0
    WHITE           = 50
    BLACK           = 51
    ABORT           = 52
    TERMINATE       = 53
    NORMAL          = 99

    detail_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    simple_fmt = '%(name)s - %(message)s'
    bare_fmt   = '%(message)s'


