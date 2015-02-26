
`xattr` dependency requires `cffi`, which depends on `libffi`, which is
notoriously difficult to install right.

On Mac, you might have to manually do:

    brew install pkg-config libffi
    PKG_CONFIG_PATH=/usr/local/opt/libffi/lib/pkgconfig pip install cffi


Then, the rest of setup is pretty standard:

    python setup.py install

