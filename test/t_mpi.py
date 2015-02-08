#!/usr/bin/env python
from mpi4py import MPI

comm = MPI.COMM_WORLD.Dup()

rank = comm.Get_rank()
size = comm.Get_size()

if rank == 0:
    buf = 4
    comm.send(buf, 1, tag = 11)
else:
    buf = comm.recv(source=0, tag=11)
    print "recv = ", buf

