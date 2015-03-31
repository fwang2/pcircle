#!/usr/bin/env python

from __future__ import print_function

import os
import sys
import random

if len(sys.argv) != 3 and not sys.argv[2]:
    print('''
    Usage: corrupt.py filename magic_string

    magic_string is what you want to write to the file
    it can not be empty and will be randomly placed \n\n''')

    sys.exit(1)

size = 0
index = 0
try:
    size = os.stat(sys.argv[1]).st_size
except Exception as e:
    print(e)
    sys.exit(1)

with open(sys.argv[1], "rb+") as f:
    index = random.randint(0, size)
    f.seek(index)
    f.write(sys.argv[2])

print("Corrupted file offset: %s\n" % index)

