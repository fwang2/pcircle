#!/usr/bin/env python
from __future__ import print_function
import stat
import os
import os.path
import logging
import argparse
import utils
import hashlib
import sys
import signal
import cPickle as pickle

from task import BaseTask
from pcheck import PCheck
from circle import Circle
from globals import G
from mpi4py import MPI
from pwalk import PWalk
from checkpoint import Checkpoint
from pcp import PCP
from collections import Counter, defaultdict
from utils import bytes_fmt, destpath
from lru import LRU


ARGS = None
logger = logging.getLogger("pcp-restart")
circle = None

def parse_args():
    parser = argparse.ArgumentParser(description="A MPI-based Parallel Copy Restart Tool")
    parser.add_argument("--loglevel", default="ERROR", help="log level")
    parser.add_argument("-i", "--reduce-interval", type=int, default=5, help="reduce interval")
    parser.add_argument("--checkpoint-interval", type=int, default=60, help="checkpoint interval")
    parser.add_argument("-c", "--checksum", action="store_true", help="verify")
    parser.add_argument("-p", "--preserve", action="store_true", help="preserve meta info")
    parser.add_argument("--checkpoint", type=int, default=0, help="checkpoint setting")
    parser.add_argument("rid", help="restart id")

    return parser.parse_args()

def sig_handler(signal, frame):
    # catch keyboard, do nothing
    # eprint("\tUser cancelled ... cleaning up")
    sys.exit(1)


def get_workq_size(workq):
    if workq is None: return 0
    sz = 0
    for w in workq:
      sz += w['length']
    return sz


def verify_checkpoint(total_checkpoint_cnt):
    if total_checkpoint_cnt == 0:
        if circle.rank == 0:
            print("Error: Can't find checkpoint file.")
            print("")

        circle.exit(0)

def main():

    global ARGS, logger, circle
    signal.signal(signal.SIGINT, sig_handler)
    ARGS = parse_args()

    circle = Circle(reduce_interval=ARGS.reduce_interval)
    circle.setLevel(logging.ERROR)
    logger = utils.logging_init(logger, ARGS.loglevel)
    dmsg = {"rank": "rank %s" % circle.rank}

    pcp = None
    pcheck = None
    oldsz = 0; tsz = 0; sz = 0
    cobj = None
    timestamp = None
    workq = None
    src = None
    dest = None
    local_checkpoint_cnt = 0
    chk_file = ".pcp_workq.%s.%s" % (ARGS.rid, circle.rank)

    if os.path.exists(chk_file):
        local_checkpoint_cnt = 1
        with open(chk_file, "rb") as f:
            try:
                cobj = pickle.load(f)
                sz = get_workq_size(cobj.workq)
                src = cobj.src
                dest = cobj.dest
                oldsz = cobj.totalsize

            except:
                logger.error("error reading %s" % chk_file, extra=dmsg)
                circle.comm.Abort()

    logger.debug("located chkpoint %s, sz=%s, local_cnt=%s" %
                 (chk_file, sz, local_checkpoint_cnt), extra=dmsg)

    # do we have any checkpoint files?

    total_checkpoint_cnt = circle.comm.allreduce(local_checkpoint_cnt)
    logger.debug("total_checkpoint_cnt = %s" % total_checkpoint_cnt, extra=dmsg)
    verify_checkpoint(total_checkpoint_cnt)


    # acquire total size
    tsz = circle.comm.allreduce(sz)
    if tsz == 0:
        if circle.rank == 0:
            print("Recovery size is 0 bytes, can't proceed.")
        circle.exit(0)

    if circle.rank == 0:
        print("Original size: %s" % bytes_fmt(oldsz))
        print("Recovery size: %s" % bytes_fmt(tsz))


    # second task
    pcp = PCP(circle, src, dest,
              totalsize=tsz, checksum=ARGS.checksum,
              workq = cobj.workq)
    pcp.set_checkpoint_interval(ARGS.checkpoint_interval)
    if ARGS.rid:
        pcp.set_checkpoint_file(".pcp_workq.%s.%s" % (ARGS.rid, circle.rank))
    else:
        ts = utils.timestamp()
        circle.comm.bcast(ts)
        pcp.set_checkpoint_file(".pcp_workq.%s.%s" % (ts, circle.rank))
    circle.begin(pcp)
    circle.finalize(reduce_interval=ARGS.reduce_interval)
    pcp.cleanup()


    # third task
    if ARGS.checksum:
        pcheck = PCheck(circle, pcp, tsz)
        pcheck.setLevel(ARGS.loglevel)
        circle.begin(pcheck)
        circle.finalize()

        tally = pcheck.fail_tally()

        if circle.rank == 0:
            print("")
            if tally == 0:
                print("Verification passed!")
            else:
                print("Verification failed")

    pcp.epilogue()

if __name__ == "__main__": main()

