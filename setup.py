import setuptools
import versioneer
import sys

versioneer.VCS = 'git'
versioneer.versionfile_source = '_version.py'
versioneer.versionfile_build = '_version.py'
versioneer.tag_prefix = 'pcircle-v'
versioneer.parentdir_prefix = 'pcircle-v'


requires = [
        'mpi4py',
        'lru-dict',
        'xattr']
if sys.version_info[:3] < (2,7,0):
    raise RuntimeError("This application requires Python 2.7+")

details="""
More details on the package
"""


setuptools.setup(name='pcircle',
    description="A parallel file system tool suite",
    url="http://github.com/fwang2/pcircle",
    license="Apache",
    version=versioneer.get_version(),
    author='Feiyi Wang',
    author_email='fwang2@ornl.gov',
    py_modules=['globals', 'utils', 'task', 'verify', 'fwalk', 'fcp', 'circle', 'frestart',
                'checkpoint', '_version', 'cio', 'versioneer', 'checksum'],
    data_files=[],
    entry_points={
        'console_scripts': [
            'fcp=fcp:main',
            'fwalk=fwalk:main',
            'frestart=frestart:main',
            'checksum=checksum:main'
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
    cmdclass=versioneer.get_cmdclass()
)


