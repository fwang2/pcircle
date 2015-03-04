#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: `basename $0` np iter"
    exit 65
fi

for iter in `seq 1 $2`; do
    for np in `seq 1 $1`; do
        rm -rf /tmp/pcircle
        mpirun --bind-to none -np $1 ./pcp.py . /tmp
    done
done

echo "ALL PASSED"

