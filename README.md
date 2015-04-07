## Concepts


The ubiquitous MPI environment in HPC cluster + Work Stealing Pattern +
Distributed Termination Detection = Efficient and Scalable Parallel Solution.

`pcircle` software package contains a suite of file system tools that we are
developing at OLCF to take advantage of highly scalable parallel file system
such as Lustre. Early tests show very promising scaling properties. However,
it is still in active development, please use it at your own risk. Any
feedbacks and bug report are appreciated. 


## Features

A typical use of parallel copy:

    mpirun -H host1,host2,host3,host4 -np 16 fcp /path/of/source
        /path/of/destination

Notable features:

- `--preserve`: to preserve extended attribute information (e.g. Lustre
  striping information), the default is off, there are performance
  implications if this option is enabled.

- `--checksum`: to verify through parallel checksumming. This
  involves re-read all the files back from destination, therefore the
  performance penalty.

- `--checkpoint-interval`: specify checkpoint interval in seconds. For very
  large data transfer (TiBs or PiBs), frequent checkpoint will have
  performance impact as well.


- `--checkpoint-d ID`: we use timestamp as default checkpoint ID, which is
  required for later resume. You can supply a custom string value for this.

- `--resume`: to resume from previous transfer (resume ID must match previous
  checkpoint ID)



## Installation

`xattr` dependency requires `cffi`, which depends on `libffi`, which is
notoriously difficult to install right.

### On Mac, you might have to manually do:

    brew install pkg-config libffi
    PKG_CONFIG_PATH=/usr/local/opt/libffi/lib/pkgconfig pip install cffi

### On Redhat:

    sudo yum install openmpi-devel
    sudo yum install libffi-devel
  
Then, the rest of setup is pretty much Python standard:

    python setup.py install

## Help

Please contact Feiyi Wang AT fwang2@ornl.gov

