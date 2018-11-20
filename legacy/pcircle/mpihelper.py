__author__ = 'f7b'

import argparse
import sys
from mpi4py import MPI


def tally_hosts():
    """ How many physical hosts are there? """
    localhost = MPI.Get_processor_name()
    hosts = MPI.COMM_WORLD.gather(localhost)
    if hosts:
        hostcnt = len(set(hosts))
    else:
        hostcnt = 0
    hostcnt = MPI.COMM_WORLD.bcast(hostcnt)
    return hostcnt


class ArgumentParserError(Exception):
    """ change default error handling behavior of argparse
    we need to catch the error so MPI can gracefully exit
    """
    pass


class ThrowingArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentParserError(message)


def parse_and_bcast(comm, func_get_parser):
    """ Using argparse in a MPI setting
    either quit cleanly on error of parsing, or return args. This is a collective call"""
    args = None
    parse_flags = True
    if comm.rank == 0:
        parser = func_get_parser()
        try:
            args = parser.parse_args()
        except ArgumentParserError as e:
            parse_flags = False
            parser.print_usage()
            print(e)

    parse_flags = comm.bcast(parse_flags)
    if parse_flags:
        args = comm.bcast(args)
    else:
        sys.exit(0)

    if comm.rank == 0 and args.loglevel == "debug":
        print("ARGUMENT DEBUG: %s", args)

    return args
