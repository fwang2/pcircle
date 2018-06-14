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


**fprof** supports many other options:

 `--cpr`             
   Estimate compression saving

 `--cpr-per-file`             
   Print compression saving for each file

 `--dii`             
   Enable data-in-inode (dii)

 `--dirprof`             
   Enable directory count profiling

 `--dirbins <INT[ INT INT...]>`             
   Directory bins for histogram, list each bin in order
   e.g. --dirbins 100 1000 100000 ...

 `--exclude <FILE>`             
   A file with exclusion list

 `--gpfs-block-alloc`    
   GPFS block usage analysis. This is to aid the transition from Lustre-based
   file system to GPFS-based file system and reports the GPFS block usage
   characterization.

 `-h, --help`                
   Print out the help menu

 `-i, --interval <INTERVAL>`             
   Interval 

 `-inodesz <INODESZ>`             
   Inode size, default 4k

 `--item <ITEM>`             
   Number of items stored in memory, default: 3000000

 `--loglevel <LOGLEVEL>`             
   Log Level

 `--lustre-stripe`             
   Lustre stripe analysis

 `--perfile`             
   Save perfile file size for more analysis

 `--perprocess`             
   Enable per-process progress report

 `--profdev`             
   Enable dev profiling

 `--progress`             
   Enable periodoic progress report

 `--sparse`             
   Print out detected spare files

 `--stripe-output`             
   Stripe output file

 `--stripe-threshold <N>`             
   Lustre stripe file size threshold above which to analyze, default is 4GB

 `--syslog`             
   Enable syslog report

 `--topn-dirs <TOPN_DIRS>`             
   Print out the top N largest directories

 `--topn-files <TOPN_FILES>`             
   Print out the top N largest files

 `-v`             
   Verbose mode

 `--version`             
   Show program version number and exit


An sample output of `--gpfs-block-alloc` is as the following, against a
dataset with mostly small files. The report shows that if GPFS blocksize is
8M, then the 1GB dataset will inflate 10x to 10.39GB, with a poor space
efficiency of 10%. **However**, this trend should not generalize to other file
systems. We observe that for file system with many large files, this space
inefficiency issue can be largely neglected.

    GPFS Block Alloc Report:

            Subblocks: [162887  90033  64419  45701  42551]

            Blocksize: 256k     Estimated Space: 1.24 GiB               Efficiency:    86%
            Blocksize: 512k     Estimated Space: 1.37 GiB               Efficiency:    78%
            Blocksize: b1m      Estimated Space: 1.97 GiB               Efficiency:    55%
            Blocksize: b4m      Estimated Space: 5.58 GiB               Efficiency:    19%
            Blocksize: b8m      Estimated Space: 10.39 GiB              Efficiency:    10%



## AUTHOR

Feiyi Wang (fwang2@ornl.gov), 
Cameron Harr (charr@llnl.gov)
