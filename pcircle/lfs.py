"""
wrapper for lfs command line
"""

# -*- coding: utf-8 -*-
from __future__ import print_function
import subprocess
from distutils import spawn
from pcircle.globals import  G


def check_lfs():
    # python 3.3 code introduce shutils.which()
    # but for 2.7 code, we will reply on distutils
    lfs_path = spawn.find_executable("lfs")
    if lfs_path:
        G.fs_lustre = True
    else:
        G.fs_lustre = False

    return lfs_path


def lfs_get_stripe(lfs, path):
    ''' return stripe count given a path'''
    args = [lfs, 'getstripe', '--stripe-count', path]
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if stderr:
        raise IOError(stderr)
    elif stdout:
        # we parse the output, which is in the form of "4\n"
        return int(stdout.strip())
    else:
        raise IOError("Unable to get stripe info on " + path)
