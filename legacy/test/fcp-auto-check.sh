#!/bin/bash
#PBS -A STF008
#PBS -l nodes=2,walltime=23:00:00
#PBS -j oe
#PBS -N fcp-auto-dtn
[[ "$PBS_JOBID" ]] || PBS_JOBID=$(date +%s)

ITER=1
SRCDIR=$1
DSTDIR=/lustre/atlas2/stf008/scratch/fwang2/FCP_auto_$PBS_JOBID
module load git openmpi libffi/3.2.1 python
cd /ccs/techint/home/fwang2/pcircle
git pull
source $HOME/dev-pcircle/bin/activate
mkdir -p $HOME/FCP_auto_output
for i in `seq 1 $ITER`; do
    cd $HOME/FCP_auto_output
    mpirun --mca mpi_warn_on_fork 0 -np 8 fcp $SRCDIR $DSTDIR -fcp --fix-opt
    rm -rf $DSTDIR
done

deactivate

