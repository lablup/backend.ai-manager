from setuptools import setup
from pathlib import Path
import re


def read_src_version():
    p = (Path(__file__).parent / 'ai' / 'backend' / 'manager' / '__init__.py')
    src = p.read_text()
    m = re.search(r"^__version__\s*=\s*'([^']+)'", src, re.M)
    return m.group(1)


requires = [
    'aioconsole>=0.1.3',
    'aiodataloader',
    'aiohttp>=3.0.0',
    'aiojobs>=0.1',
    'aiopg~=0.13.0',
    'aioredis~=1.0.0',
    'aiotools>=0.6.0',
    'aiozmq>=0.7',
    'alembic~=0.9.2',
    'coloredlogs>=5.2',
    'ConfigArgParse==0.12',
    'dataclasses; python_version<"3.7"',
    'graphene~=2.0.1',
    'iso8601',  # required by graphene
    'namedlist',
    'psycopg2-binary>=2.7.0',
    'python-dateutil>=2.5',
    'python-snappy~=0.5.1',
    'pytz',
    'PyYAML',
    'pyzmq>=16.0',
    'SQLAlchemy~=1.1.14',
    'uvloop~=0.8.0',
    'backend.ai-common~=1.3.0',
]
build_requires = [
    'wheel',
    'twine',
]
test_requires = [
    'aiodocker',
    'pytest>=3.4.0',
    'pytest-asyncio>=0.8.0',
    'pytest-aiohttp',
    'pytest-cov',
    'pytest-mock',
    'codecov',
    'flake8',
]
dev_requires = build_requires + test_requires + [
    'pytest-sugar>=0.9.1',
]
ci_requires = []
monitor_requires = [
    'datadog>=0.16.0',
    'raven>=6.1',
]


setup(
    name='backend.ai-manager',
    version=read_src_version(),
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
    dependency_links=[
        'git+https://github.com/achimnol/aiohttp@dynamic-subapp-prefix#egg=aiohttp',
    ],
    data_files=[],
)
