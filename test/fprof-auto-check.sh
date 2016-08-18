#!/bin/bash

## sanity check using Judy directory
## this run is usually performed on rhea
## with fprof in the path

TARGET="$HOME/Judy_Oct"
VALUE="99,906"
ITER=200
if [ ! -d $TARGET ]; then
    echo "Test directory [$TARGET] doesn't exist"
    exit 1
fi

    
for i in `seq 1 $ITER`; do
    np=`expr $i % 15`
    np=`expr $np + 1`
    echo "Run $i, np=$np"
    mpirun --mca mpi_warn_on_fork 0  -np ${np} fprof $TARGET > tmp.out
    fcount=`cat tmp.out  | grep "File count:" | awk '{print $3}'`
    if [ "$fcount" != "$VALUE" ]; then
        echo "FAILED at run $i: expected=$VALUE, real=$fcount"
        exit 1
    fi
done

echo "$ITER runs, PASS"



