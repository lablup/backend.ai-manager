# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

setup(
    name='sorna-manager',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version='0.5.0',
    description='Sorna Manager',
    long_description='',
    url='https://github.com/lablup/sorna-manager',
    author='Lablup Inc.',
    author_email='joongi@lablup.com',
    license='LGPLv3',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Environment :: No Input/Output (Daemon)',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development',
    ],

    packages=['sorna.manager', 'sorna.gateway', 'sorna.monitor'],
    namespace_packages=['sorna'],

    install_requires=['ConfigArgParse', 'namedlist', 'coloredlogs',
                      'pyzmq', 'aiozmq', 'aiohttp', 'aioredis', 'asyncpg',
                      'python-dateutil', 'simplejson', 'uvloop'],
    extras_require={
        'dev': [],
        'test': ['pytest', 'pytest-mock'],
    },
    data_files=[],
)
