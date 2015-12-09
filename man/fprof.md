fprof(8) - File System Profiler
================================================================

## SYNOPSIS
    fprof target_directory
    
    mpirun -np 8 fprof ...


## DESCRIPTION

**fprof** is a lightweight profiler designed to provide a set of useful
statistical characterization of the target file system at extreme scale. A
sample report on ORNL Atlas file system is as the following:

    Fileset histograms:

            <  4.00 KiB       19888046           17.08%    ∎∎∎∎∎∎∎∎
            <  8.00 KiB       5708765             4.90%    ∎∎
            <  16.00 KiB      4376361             3.76%    ∎
            <  32.00 KiB      5366888             4.61%    ∎∎
            <  64.00 KiB      7061464             6.07%    ∎∎∎
            <  256.00 KiB     13388304           11.50%    ∎∎∎∎∎
            <  512.00 KiB     8406809             7.22%    ∎∎∎
            <  1.00 MiB       3652496             3.14%    ∎
            <  4.00 MiB       13817394           11.87%    ∎∎∎∎∎
            <  16.00 MiB      13088330           11.24%    ∎∎∎∎∎
            <  512.00 MiB     18992735           16.32%    ∎∎∎∎∎∎∎∎
            <  1.00 GiB       1918471             1.65%
            <  100.00 GiB     740765              0.64%
            <  256.00 GiB     2779                0.00%
            <  512.00 GiB     630                 0.00%
            <  1.00 TiB       699                 0.00%
            >  1.00 TiB       458                 0.00%

    Fprof epilogue:

            Directory count:         35,945,074
            Sym Links count:         1,050,485
            File count:              116,411,394
            Skipped count:           364
            Total file size:         8160.08 TiB
            Avg file size:           73.50 MiB
            Max files within dir:    1,003,319


**fprof** supports two other options:

* `--perfile`             
   Save perfile file size for more analysis

* `--gpfs-block-alloc`    
   GPFS block usage analysis. This is to aid the transition from Lustre-based
   file system to GPFS-based file system and reports the GPFS block usage
   characterization.



## AUTHOR

Feiyi Wang (fwang2@ornl.gov)




