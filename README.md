## Usage

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

