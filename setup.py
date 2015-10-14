"""
    To update versionner:

    pip install versioneer

    cd pcircle-repo
    versioneer install

    This will bring versioneer.py etc to the latest in YOUR source tree.

    Now commit everything.

    make # generate tar ball, and try "make deploy" to verify

"""
import setuptools
import versioneer
import sys

requires = [
    'cffi==1.2.1',
    'mpi4py==1.3.1',
    'lru-dict==1.1.1',
    'xattr==0.7.8',
    'scandir==1.1',
    'numpy==1.9.2']

if (3, 0, 0) < sys.version_info[:3] < (2, 7, 0):
    raise RuntimeError("This application requires Python 2.7.x")

details = """
More details on the package
"""

setuptools.setup(name='pcircle',
                 description="A parallel file system tool suite",
                 url="http://github.com/ORNL-TechInt/pcircle",
                 license="Apache",
                 author='Feiyi Wang',
                 author_email='fwang2@ornl.gov',
                 packages=['pcircle'],
                 data_files=[],
                 entry_points={
                     'console_scripts': [
                         'fcp=pcircle.fcp:main',
                         'fwalk=pcircle.fwalk:main',
                         'fsum=pcircle.fsum:main',
                         'fcorruptor=pcircle.fcorruptor:main',
                         'fdiff=pcircle.fdiff:main',
                         'fgen=pcircle.fgen:main',
                         'fprof=pcircle.fprof.main'
                     ]
                 },
                 classifiers=[
                     'Development Status :: 3 - Beta',
                     'Intended Audience :: System Administrators',
                     'Topic :: System :: Monitoring',
                     'Programming Language :: Python :: 2.7',
                 ],
                 install_requires=requires,
                 long_description=details,
                 version=versioneer.get_version(),
                 cmdclass=versioneer.get_cmdclass(),
                 )
