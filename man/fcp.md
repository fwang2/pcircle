fcp(8) - A Scalable Parallel Data Copy Tool
================================================================

## SYNOPSIS

    fcp [--version] [-c] [-s] [-p] [-f] src_dir dest_dir
    mpirun -np 8 fcp ...

## DESCRIPTION

**fcp** is a program designed to do large-scale parallel data transfer from
a source directory to a destination directory across _locally_ mounted file
systems. It is not for wide area data transfers such as ftp, bbcp, or
globus-ftp. In that sense, it is closer to **cp**. One crucial difference
from regular **cp**, is that **fcp** requires the source and
destination to be directories.  **fcp** will fail if these conditions are not
met.  In the most general case, **fcp** works in two stages: first it analyzes
the workload by walking the tree in parallel; and then it parallelizes the data
copy operation.  **fcp** supports the following options:


* `-p`, `--preserve`:
  Preserve metadata attributes. In the case of Lustre,
  the striping information is kept.

* `-f`, `--force`:
  Overwrite the destination directory. The default is
  off.

* `--verify`:
  Perform checksum-based verification after the copy. 

* `-s`, `--signature`:
  Generate a single sha1 signature for the entire dataset. This option also 
  implies `--verify` for post-copy verification.


* `--chunksize sz`:
   **fcp** will break up large files into pieces to increase parallelism. By
   default, **fcp** adaptively sets the chunk size based on the overall size of
   the workload. Use this option to specify a particular chunk size in KB, MB. 
   For example, `--chunksize 128MB`.

* `--reduce-interval`:
  Controls progress report frequency. The default is 10 seconds.



## PERFORMANCE CONSIDERATIONS


**fcp** performance is subject to the bandwidth and conditions of the source file
system, the storage area network, the destination file system, and
the number of processes and nodes involved in the transfer. Using more
processes per node does not necessarily result in better performance due to an
increase in the number of metadata operations as well as additional contention
generated from a larger number of processes. A rule of thumb is to match or
halve the number of physical cores per transfer node.

Both post copy verification (`--verify`) and dataset signature (`--signature`)
options have performance implications. When turned on, **fcp** calculates the
checksums of each chunk/file for both source and destination, in addition to
reading back from destination. This increases both the amount of bookkeeping
and memory usage. Therefore, for large scale data transfers, a large memory
node is recommended.


## AUTHOR

Feiyi Wang (fwang2@ornl.gov)




