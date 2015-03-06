
import setuptools
import os, subprocess, re
from distutils.core import Command
from distutils.command.sdist import sdist as _sdist



# The version code is adapted from:
# https://github.com/warner/python-ecdsa/blob/9e21c3388cc98ba90877a1e4dbc2aaf66c67d365/setup.py


import sys

VERSION_PY = """
__version__ = '%s'
"""

requires = [
        'mpi4py',
        'lru-dict',
        'xattr']
if sys.version_info[:3] < (2,7,0):
    raise RuntimeError("This application requires Python 2.7+")

details="""
More details on the package
"""
def update_version_py():
    if not os.path.isdir(".git"):
        print "This does not appear to be a Git repository."
        return
    try:
        p = subprocess.Popen(["git", "describe",
                "--tags", "--always"],
                stdout=subprocess.PIPE)
    except EnvironmentError:
        print "unable to run git, leaving _version.py alone"
        return

    stdout = p.communicate()[0]
    if p.returncode != 0:
        print "unable to run git, leaving ecdsa/_version.py alone"
        return

    # we use tags like "pcircle-v0.3", so strip the prefix
    assert stdout.startswith("pcircle-v")
    ver = stdout[len("pcircle-v"):].strip()
    f = open("_version.py", "w")
    f.write(VERSION_PY % ver)
    f.close()
    print "set _version.py to '%s'" % ver


def get_version():
    try:
        f = open("_version.py")
    except EnvironmentError:
        return None

    for line in f.readlines():
        mo = re.match("__version__ = '([^']+)'", line)
        if mo:
            ver = mo.group(1)
            return ver

    return None

class Version(Command):

    description = "update _version.py from Git repo"
    user_options = []
    boolean_options = []

    def initialize_options(self):
        pass
    def finalize_options(self):
        pass

    def run(self):
        update_version_py()
        print "Version is now", get_version()

class sdist(_sdist):
    def run(self):
        update_version_py()
        self.distribution.metadata.version = get_version()
        return _sdist.run(self)


setuptools.setup(name='pcircle',
    description="A parallel file system tool suite",
    url="http://github.com/fwang2/pcircle",
    license="Apache",
    version=get_version(),
    author='Feiyi Wang',
    author_email='fwang2@ornl.gov',
    py_modules=['globals', 'utils', 'task', 'pcheck', 'pwalk', 'pcp', 'circle', 'prestart',
                'checkpoint', '_version'],
    data_files=[],
    entry_points={
        'console_scripts': [
            'pcp=pcp:main',
            'pwalk=pwalk:main',
            'prestart=prestart:main'
        ]
    },
   classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: System Administrators',
            'Topic :: System :: Monitoring',
            'Programming Language :: Python :: 2.7',
      ],
    install_requires = requires,
    long_description = details,
    cmdclass={ "version": Version, "sdist": sdist }
)


