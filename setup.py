from setuptools import setup
import sys
try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except (IOError, ImportError):
    long_description = ""


requires = [
    'ConfigArgParse',
    'coloredlogs>=5.2',
    'pyzmq>=16.0',
    'aiozmq>=0.7',
    'aiohttp~=2.2.0',
    'aioredis~=0.2.8',
    'namedlist',
    'SQLAlchemy',
    'asyncpgsa~=0.10.0',
    'python-dateutil>=2.5',
    'simplejson',
    'uvloop>=0.8',
    'backend.ai-common~=0.9.0',
]
build_requires = [
    'pypandoc',
    'wheel',
    'twine',
]
test_requires = [
    'pytest>=3.1',
    'pytest-mock',
    'pytest-aiohttp',
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

sys.path.insert(0, '.')
import backend.ai.manager


setup(
    name='backend.ai-manager',
    version=backend.ai.manager.__version__,
    description='Backend.Ai Manager',
    long_description=long_description,
    url='https://github.com/lablup/backend.ai-manager',
    author='Lablup Inc.',
    author_email='joongi@lablup.com',
    license='LGPLv3',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)',
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

    packages=['backend.ai.manager', 'backend.ai.gateway', 'backend.ai.monitor'],
    namespace_packages=['backend.ai'],

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
