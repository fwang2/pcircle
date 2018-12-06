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

* cmake3 This is available through [Redhat EPEL Repo](https://fedoraproject.org/wiki/EPEL). Once it is enabled, `yum install cmake3`.

* A working C++11 compiler. 
* A working MPI dev environment. For RHEL7, that means:
      
      sudo yum install mpi-devel
      module load mpi 

## Build from source: (RHEL/CentOS 7)

      git clone http://github.com/olcf/pcircle
      
      cd pcircle; ./buildme.sh      

This should build the `fprof` binary by default in the `build` directory.


## Example Runs:

      mpirun -np 8 fprof /path/to/directory
















