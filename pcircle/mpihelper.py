__author__ = 'f7b'

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
