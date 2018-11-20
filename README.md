

# P2

This is a different take on large-scale parallel operations: an implementation
of master-slave pattern. Differing from the more fancy peer to peer, work
stealing pattern as seen in [pcirlce](http://github.com/olcf/pcircle),
master/slave is dead simple conceptually: master distributes the work,
slaves/works does the work as assigned.

The biggest advantage of this pattern is: simplicity and predictability.
Specifically to the file system environment, since each process goes on its own
to gather next work items (more efficient and balanced workload distribution),
it is fairly difficult to checkpoint the work. In contrast, it is relatively
easy to achieve that in the master/slave pattern.


# Build

The main dependency:
* mpi
* protobuf

The reason for the later is this implementation are not registering new MPI
datatype, instead, it is relying protobuf to serialize everything into a byte
stream. In short, to build:


    mkdir build
    cd build
    cmake ..
    make

# Run

    mpirun -np 8 ffind --pattern ">250k" \
        --delete --print /path/to/your/directory

This is to search and delete every file that is larger than 250KB. `--delete` is
pretty destructive, use it with caution. You should do a few test runs without this
option. The main purpose is to aid the HPC Ops to handle large-scale file system
management.

For now the pattern is mainly file size, following the syntax of GNU `find`. I
may add more pattern as needed.

















