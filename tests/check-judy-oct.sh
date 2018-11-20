#!/bin/bash
SRCDIR="$HOME/Judy_Oct"
ITER=1000
PRCS=16
VALUE="99,937"
for i in `seq 1 $ITER`; do
    for j in `seq 1 $PROCS`; do
        echo -n "Running iter $i of $ITER ... "
        mpirun --oversubscribe -np $j ../build/fprof $SRCDIR > tmp.out
        fcount=`cat tmp.out | grep "Scanned a total of" | awk '{print $5}'`
        if [ "$fcount" != "$VALUE" ]; then
            echo "FAILED at run $i, expected=$VALUE, got=$fcount"
            exit 1
        fi
        echo "PASS"
    done
done

echo "All Done"
