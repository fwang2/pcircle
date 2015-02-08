#!/usr/bin/env python
from __future__ import print_function
import time
import hashlib

class S:
    sz_1k = 1024
    sz_1m = 1048576
    sz_1g = 1073741824

def timing(f):
    def wrap(*args):
        time1 = time.time()
        ret = f(*args)
        time2 = time.time()
        print("\t%s function took %0.4f seconds" %
              (f.func_name, time2-time1))
        return ret
    return wrap

def read_in_chunks(file_object, chunk_size = S.sz_1k):
    """
    lazy function to read a file piece by piece
    """
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data

@timing
def run(fn, method):
    func = getattr(hashlib, method)
    h = func()
    with open(fn) as f:
        for piece in read_in_chunks(f):
            h.update(piece)

if __name__ == "__main__":
    methods=["sha1","md5"]
    for m in methods:
        print(m + ":")
        run("test.mp4", m)
