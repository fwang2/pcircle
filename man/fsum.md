fsum(8) - A Scalable Parallel Checksum Tool
================================================================

## SYNOPSIS
    fsum file1 file2 ...
    fsum dir 
    
    mpirun -np 8 fsum ...


## DESCRIPTION

**fsum** is a program designed to do large scale data checksumming. Compared to
conventional checksumming utilities such as **md5sum**, there are two major
differences: (1) it is parallel; (2) it is dataset-based instead of file-based.
**fsum** supports the following options:


* `--output filename`:
  Rename signature file. By default, fsum generates a signature file using the
  current time stamp.

* `--chunksize sz`:
   **fsum** will break up large files into pieces to increase parallelism. By
   default, **fsum** adaptively sets the chunk size based on the overall size of
   the workload. Use this option to specify a particular chunk size in KB, MB. 
   For example: `--chunksize 128MB`.

* `--reduce-interval`:
  Controls progress report frequency. The default is 10 seconds.


* `----export-block-signatures`:
  Control whether the signature file contains checksums of each data block. By
  default, only the aggregated checksum is saved.


## PERFORMANCE/RESOURCE CONSIDERATIONS

The final step of aggregating and sorting block checksums is not parallelized.
The reduction is performed on a single node and requires a large memory
footprint as the number of files increases.


## AUTHOR

Feiyi Wang (fwang2@ornl.gov)




