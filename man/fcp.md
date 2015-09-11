fcp(8) - A Scalable Parallel Data Copy Tool
================================================================

## SYNOPSIS

    fcp [--version] [-c] [-s] [-p] [-f] src_dir dest_dir
    mpirun -np 8 fcp ...

## DESCRIPTION

**fcp** is a program designed to do large-scale parallel data transfer from
source directory to destination directory across _locally_ mounted file
systems. It is not designed for wide area data transfer such as ftp, bbcp, or
globus-ftp. In that sense, it is closer to **cp**. One crucial difference
from regular **cp**, is that the required arguments of source and destination
must be directories. **fcp** does do sanity check and will fail if these
conditions can not be met. In the most general case, **fcp** works in two stages:
first it analyzes the workload by parallel walking the tree; second it
parallelize the job of data copy. The following options are available:

* `-c`, `--checksum`:
  Perform checksum verification after the copy. 

* `-s`, `--signature`:
  Generate a single signature for the entire dataset. `-c` is a prerequisite
  for this to work, otherwise, this option will be ignored.

* `-p`, `--preserve`:
  This option will preserve the metadata attributes. In the case of Lustre,
  the striping information is kept.

* `-f`, `--force`:
  With this option, **fcp** will overwrite the destination. The default is
  off.

* `--reduce-interval`:
  **fcp** by default will provide progress report. This option controls the
  frequency. The default is 10 seconds.

* `--chunksize sz`:
  **fcp** will break up large files into pieces to increase parallelism. This
  option can force upon a certain size. By default, **fcp** adaptively set the
  chunk size based on the overall size of the workload.



## PERFORMANCE CONSIDERATIONS

**fcp** performance is subject to the bandwidth and conditions of source file
system, storage area network, and destination file system, as well as number
of process and nodes involved in the transfer. More processes per node is not
necessarily better due to metadata performance and potential contentions. A
rule of thumb is to match or halve the number of physical cores per transfer
node. 

Both checksum and dataset signature options have performance implications. It
requires **fcp** to calculate the checksums of each chunk/file for both source
and destination. It also involves read-back from destination. The bookkeepings
increases the memory usage as well. Therefore, for large scale data transfer,
a _fat_ node is often needed.


## AUTHOR

Feiyi Wang (fwang2@ornl.gov)




