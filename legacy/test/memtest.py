from __future__ import print_function

__author__ = 'f7b'


import random
import string
import sys
import resource
from pcircle.utils import bytes_fmt

NUM_FILES = 10**7

if len(sys.argv) !=2:
    print("memtest [v1|v2]")
    sys.exit(0)

if sys.argv[1] == 'v0':
    from pcircle.fdef import FileItem0 as FileItem
elif sys.argv[1] == 'v1':
    from pcircle.fdef import FileItem1 as FileItem
else:
    print("Wrong version")
    sys.exit(1)

def randstr(size=32):

    size = random.randint(16, 32)
    return "/the/common/path/we/have/" + \
           ''.join(random.choice(string.ascii_letters) for i in range(size))

mem_init = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print('Creating {:,} FileItem instances'.format(NUM_FILES))
alist = [FileItem(randstr()) for i in xrange(NUM_FILES)]
mem_final = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

print('Initial RAM usage: {:14}'.format(bytes_fmt(mem_init)))
print('  Final RAM usage: {:14}'.format(bytes_fmt(mem_final)))
print('  Delta RAM usage: {:14}'.format(bytes_fmt(mem_final - mem_init)))

