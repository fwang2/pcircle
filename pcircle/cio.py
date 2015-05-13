__author__ = 'f7b'

import time
import os

MAX_TRIES = 5
SLEEP = 0.1

def readn(fd, size):
    tries = 0
    ret = ''
    while len(ret) < size:
        buf = os.read(fd, size - len(ret))
        if len(buf) > 0:
            tries = MAX_TRIES
            ret += buf
        elif not buf:
            # EOF
            return ret
        else:
            tries -= 1
            if tries < 0:
                raise IOError

            time.sleep(SLEEP)

    return ret

def writen(fd, buf):
    size = len(buf)
    n = 0
    tries = 0
    while n < size:
        rc = os.write(fd, buf[n:])
        if rc > 0:
            n += rc
            tries = MAX_TRIES
        else:
            tries -= 1
            if tries < 0:
                raise IOError
            time.sleep(SLEEP)
    return n