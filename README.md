## Overview

TBD

## Usage

A typical use of parallel copy:

    mpirun -H host1,host2,host3,host4 -np 8 pcp /path/of/source
        /path/of/destination

Other notable features:

- `--preserve`: to preserve extended attribute information
- `--checksum`: to verify through parallel checksumming


## Installation

`xattr` dependency requires `cffi`, which depends on `libffi`, which is
notoriously difficult to install right.

On Mac, you might have to manually do:

    brew install pkg-config libffi
    PKG_CONFIG_PATH=/usr/local/opt/libffi/lib/pkgconfig pip install cffi

On Redhat:

    sudo yum install openmpi-devel
    sudo yum install libffi-devel
  
Then, the rest of setup is pretty much Python standard:

    python setup.py install

