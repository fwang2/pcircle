# pcircle 

## Overview

The ubiquitous MPI environment in HPC cluster + Work Stealing Pattern +
Distributed Termination Detection = Efficient and Scalable Parallel Solution.

`pcircle` contains a suite of file system tools that we are developing at OLCF
to take advantage of highly scalable parallel file system such as Lustre and
GPFS. Early tests show very promising scaling properties. However, it is still
in active development, please use it at your own risk. For bug report and
feedbacks, please post it here at https://github.com/olcf/pcircle/issues.


## Prerequisites:

* cmake3 (this is available through [EPEL Repo](https://fedoraproject.org/wiki/EPEL). Once it is enabled, `yum install cmake3` should do it.

* A working C++11 compiler. Notes: RHEL 7 shipped gcc 4.8.5 which doesn't have complete support for C++11. In particular, its `regex` library is hopelessly broken. You can compile and generate a binary, but it will just core dump when you run it. This unfortrunately requires us to move to 4.9+ or bring third party library `boost` into play. One alternative solution is to enable [Redhat developer toolset 6](https://www.softwarecollections.org/en/scls/rhscl/devtoolset-6/) or [Redhat developer toolset 7](https://www.softwarecollections.org/en/scls/rhscl/devtoolset-7/). Once this is enabled, you can bring new compiler into environment by:

    source /opt/rh/devtoolset-7/enable

Another alternative is to use `spack` software package environment, which is what DOE/ECP recommended path forward. See below for more information.

* A working MPI dev environment. For RHEL7, that means:
      
      sudo yum install mpi-devel
      module load mpi 



## Build from source: (RHEL/CentOS 7)

      git clone http://github.com/olcf/pcircle
      cd pcircle; mkdir build; cd build; cmake3 ..


## Build (Spark)

TBD


## Example Runs


    mpirun -np 8 ffind --pattern ">250k" \
        --delete --print /path/to/your/directory

This is to search and delete every file that is larger than 250KB. `--delete` is
pretty destructive, use it with caution. You should do a few test runs without this
option. The main purpose is to aid the HPC Ops to handle large-scale file system
management.

For now the pattern is mainly file size, following the syntax of GNU `find`. I
may add more pattern as needed.

















