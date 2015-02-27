import setuptools

import sys

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
    version='0.1a1',
    author='Feiyi Wang',
    author_email='fwang2@ornl.gov',
    py_modules=['globals', 'utils', 'task', 'pcheck', 'pwalk', 'pcp', 'circle'],
    data_files=[],
    entry_points={
        'console_scripts': [
            'pcp=pcp:main',
            'pwalk=pwalk:main'
        ]
    },
   classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: System Administrators',
            'Topic :: System :: Monitoring',
            'Programming Language :: Python :: 2.7',
      ],
    install_requires = requires,
    long_description=details
      )


