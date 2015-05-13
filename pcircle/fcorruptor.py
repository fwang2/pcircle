#!/usr/bin/env python

from __future__ import print_function

import os
import sys
import random


def main():
    if len(sys.argv) < 3 or not sys.argv[2]:
        print('''
        Usage: corrupt.py filename magic_string [offset]

        magic_string is what you want to write to the file
        it can not be empty and will be randomly placed \n\n''')

        sys.exit(1)

    size = 0
    offset = 0

    try:
        if len(sys.argv) == 4:
            offset = int(sys.argv[3])
    except Exception as e:
        print(e)
        sys.exit(1)

    try:
        size = os.stat(sys.argv[1]).st_size
    except Exception as e:
        print(e)
        sys.exit(1)

    try:
        with open(sys.argv[1], "rb+") as f:
            if offset == 0:
                offset = random.randint(0, size)
            f.seek(offset)
            f.write(sys.argv[2])

        print("Corrupted file at offset: %s\n" % offset)
    except Exception as e:
        print(e)
        sys.exit(1)

if __name__ == '__main__': main()
