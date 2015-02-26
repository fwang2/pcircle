#!/bin/bash
src=/lustre/atlas1/stf008/scratch/fwang2/xattr
rm -rf $src

mkdir -p $src
lfs setstripe -c 1 $src

mkdir -p $src/t1
lfs setstripe -c 2 $src/t1

mkdir -p $src/t2
lfs setstripe -c 3 $src/t2

echo "aaa" > $src/t1/a.txt
echo "bbb" > $src/t2/b.txt

echo "src ..."
lfs getstripe $src
lfs getstripe $src/t1/a.txt

dst=/lustre/atlas2/stf008/scratch/fwang2
rm -rf $dst/xattr
$HOME/pcircle/pcp.py -p $src $dst

echo "dest ..."
lfs getstripe $dst/xattr
lfs getstripe $dst/xattr/t1/a.txt


