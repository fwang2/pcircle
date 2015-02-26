#!/bin/bash
if [ ! -n "$1" ]; then
    echo "Usage: `basename $0` num_of_procs"
    exit 1
fi

src=/lustre/atlas1/stf008/scratch/fwang2/one-mill
dst=/lustre/atlas2/stf008/scratch/fwang2/dcp/

mpirun -H dtn-sch04,dtn-sch05,dtn-sch06,dtn-sch07,dtn-sch08,dtn-sch09,dtn-sch10 -np $1 dcp $src $dst
