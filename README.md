# PCircle


## About

The ubiquitous MPI environment in HPC cluster + Work Stealing Pattern +
Distributed Termination Detection = Efficient and Scalable Parallel Solution.

`pcircle` contains a suite of file system tools that we are developing at OLCF
to take advantage of highly scalable parallel file system such as Lustre and
GPFS. Early tests show very promising scaling properties. However, it is still
in active development, please use it at your own risk. For bug report and
feedbacks, please post it here at https://github.com/olcf/pcircle/issues.



## Quick Start

To jumpstart and do a quick test run on MacOS:

    $ brew install pkg-config libffi openmpi python
    $ pip2 install virtualenv
    $ virtualenv pcircle
    $ source ~/pcircle/bin/activate
    $ (pcircle) pip2 install git+https://github.com/olcf/pcircle@dev

To run a simple test:

    $ mpirun -np 4 fprof ~

This also shows the core dependencies of pcircle: `python`, `libffi`, and `openmpi`. For Linux alike, we need their dev rpms. For example:

        sudo yum install openmpi-devel
        sudo yum install libffi-devel


## Manpage

Note: this is a bit out of date, `-h` shows current options:

* Parallel Data Copy: [fcp.8](https://rawgit.com/olcf/pcircle/master/man/fcp.8.html)
* Parallel Checksumming: [fsum.8](https://rawgit.com/olcf/pcircle/master/man/fsum.8.html)
* Parallel Profiler: [fprof.8](https://rawgit.com/olcf/pcircle/master/man/fprof.8.html)

## Publications:

- ["FCP: A Fast and Scalable Data Copy Tool for High Performance Parallel File Systems"](docs/cug15-fcp.pdf), by F. Wang, V.G.V. Larrea, D. Leverman, S. Oral, at CUG'2015.

- ["A Bloom Filter BAsed Scalable Data Integrity Check Tool for Large-scale Dataset"](docs/pdsw16-fsum.pdf), by S. Xiong, F. Wang, and Q. Cao, at PDSW'2016.

- ["Diving into Petascale Production File Systems through Large Scale Profiling and Analysis"](docs/pdsw17-fprof.pdf), by F. Wang, H. Sim, C. Harr and S. Oral, at PDSW'2017.

## Authors and Contributors

- Feiyi Wang | Oak Ridge National Laboratory | fwang2@ornl.gov
- Sisi Xiong | University of Tennessee (Now at Microsoft Corp.)
- Cameron Harr | Lawrence Livermore National Laboratory | harr1@llnl.gov
- Sarp Oral | Oak Ridge National Laboratory | oralhs@ornl.gov


