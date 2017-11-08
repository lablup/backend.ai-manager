from setuptools import setup
from pathlib import Path
import ai.backend.manager


requires = [
    'aioconsole>=0.1.3',
    'aiodataloader',
    'aiohttp~=2.3.0',
    'aiopg~=0.13.0',
    'aioredis~=0.3.3',
    'aiotools>=0.5.0',
    'aiozmq>=0.7',
    'alembic~=0.9.2',
    'coloredlogs>=5.2',
    'ConfigArgParse',
    'graphene>=2.0.dev',
    'iso8601',  # required by graphene
    'namedlist',
    'psycopg2~=2.7.0',
    'python-dateutil>=2.5',
    'pytz',
    'PyYAML',
    'pyzmq>=16.0',
    'simplejson',
    'SQLAlchemy~=1.1.14',
    'uvloop>=0.8',
    'backend.ai-common~=1.0.3',
]
build_requires = [
    'wheel',
    'twine',
]
test_requires = [
    'pytest>=3.1',
    'pytest-asyncio',
    'pytest-aiohttp',
    'pytest-cov',
    'pytest-mock',
    'codecov',
    'flake8',
]
dev_requires = build_requires + test_requires + [
    'pytest-sugar',
]
ci_requires = []
monitor_requires = [
    'datadog>=0.16.0',
    'raven>=6.1',
]


setup(
    name='backend.ai-manager',
    version=ai.backend.manager.__version__,
    description='Backend.AI Manager',
    long_description=Path('README.rst').read_text(),
    url='https://github.com/lablup/backend.ai-manager',
    author='Lablup Inc.',
    author_email='joongi@lablup.com',
    license='LGPLv3',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: ' +
            'GNU Lesser General Public License v3 or later (LGPLv3+)',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Environment :: No Input/Output (Daemon)',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development',
    ],
    packages=[
        'ai.backend.manager',
        'ai.backend.manager.models',
        'ai.backend.manager.cli',
        'ai.backend.gateway',
    ],
    python_requires='>=3.6',
    install_requires=requires,
    extras_require={
        'build': build_requires,
        'test': test_requires,
        'dev': dev_requires,
        'ci': ci_requires,
        'monitor': monitor_requires,
    },
    data_files=[],
)
