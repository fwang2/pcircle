## Concepts

The ubiquitous MPI environment in HPC cluster + Work Stealing Pattern +
Distributed Termination Detection = Efficient and Scalable Parallel Solution.

`pcircle` contains a suite of file system tools that we are
developing at OLCF to take advantage of highly scalable parallel file system
such as Lustre and GPFS. Early tests show very promising scaling properties. However,
it is still in active development, please use it at your own risk. For bug report and feedbacks, 
please post it here at https://github.com/ORNL-TechInt/pcircle/issues. 


## Parallel Data Transfer

A typical use of parallel copy:

    mpirun -H host1,host2,host3,host4 -np 16 fcp \
        /path/of/source \
        /path/of/destination

Notable features:

- `--preserve`: to preserve extended attribute information (e.g. Lustre
  striping information), the default is off, there are performance
  implications if this option is enabled.

- `--checksum`: to verify through parallel checksumming. This
  involves re-read all the files back from destination, therefore the
  performance penalty applies.

- `--checkpoint-interval`: specify checkpoint interval in seconds. For very
  large data transfer (TiBs or PiBs), frequent checkpoint will have
  performance impact as well.


- `--checkpoint-d ID`: we use timestamp as default checkpoint ID, which is
  required for later resume. You can supply a custom string value for this.

- `--resume`: to resume from previous transfer (resume ID must match previous
  checkpoint ID)

## Parallel Checksumming

A typical use of parallel checksumming:

    mpirun -H host1,host2 -np 8 fsum \
        /path/to/dataset

Differing from typical checksumming, the `fsum` handles a directory and compute
one signature for the whole dataset. Since we parallelize the compute process,
it is both scalable and high performance. On a 8 node cluster, we observed as
high as 6.25 GB/s average processing rate. This is a likely a I/O bound
process instead of compute-bound.


## Dependencies

`pcircle` is largely a Python implementation, but we don't rule out possible
and judicious integration of C/C++ code for performance gains. Currently, it
has the following dependencies:

- `mpi4py` - wraps the MPI library
- `xattr` - wraps the libattr library
- `cffi` - python interface to `libffi`
- `lru-dict` - wrap a small C-based LRU cache

## Installation

`xattr` dependency requires `cffi`, which depends on `libffi`, which is
notoriously difficult to install right.

- On Mac, you might have to manually do:

        brew install pkg-config libffi
        PKG_CONFIG_PATH=/usr/local/opt/libffi/lib/pkgconfig pip install cffi

- On Redhat:

        sudo yum install openmpi-devel
        sudo yum install libffi-devel
      
- Then, the rest of setup is pretty much Python standard:

        python setup.py install


## Virtualenv

If you have **setuptools** and **virtualenv** packages, then

        make deploy

should grab all dependencies and produce you a **container**, which isolate you from the default
installation:

        source ~/app-pcircle/bin/activate
        fcp -h

For this command to work, you do need a working `libffi` as well.
        

## Scalability and Performance

There has no detailed study on performance, CPU and memory usage yet. It
varies based on number of files, size distribution, the transfer cluster size
etc. It also depends on how well striped of the source file, and if the
bandwidth if balanced between source and destination. 

The scaling limits most likely come from one of the worst case scenario:
tens or hundreds of millions small files. In this case, we have no choice by
to treat each file as a single chunk. 




## Help

Please contact Feiyi Wang AT fwang2@ornl.gov

