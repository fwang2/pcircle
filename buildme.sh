#!/bin/bash
mkdir -p build
cd build
if [ -f /bin/cmake3 ]; then
    cmake3 ..
else
    cmake ..
fi

make

echo ""
echo "Binary is written in ./build"
echo ""
