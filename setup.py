from setuptools import setup
from pathlib import Path
import pip
import re

here = Path(__file__).resolve().parent


try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except (IOError, ImportError):
    long_description = ""

requires = []
links = []
requirements = pip.req.parse_requirements(
    'requirements.txt', session=pip.download.PipSession()
)
for item in requirements:
    if getattr(item, 'url', None):  # older pip has url
        links.append(str(item.url))
    if getattr(item, 'link', None):  # newer pip has link
        links.append(str(item.link))
    if item.req:
        requires.append(str(item.req))  # always the package name


def _get_version():
    root_src = (here / 'sorna' / 'manager' / '__init__.py').read_text()
    try:
        version = re.findall(r"^__version__ = '([^']+)'\r?$", root_src, re.M)[0]
    except IndexError:
        raise RuntimeError('Unable to determine myself version.')
    return version


setup(
    name='sorna-manager',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version=_get_version(),
    description='Sorna Manager',
    long_description=long_description,
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
        'Programming Language :: Python :: 3.6',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Environment :: No Input/Output (Daemon)',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development',
    ],

    packages=['sorna.manager', 'sorna.gateway', 'sorna.monitor'],
    namespace_packages=['sorna'],

    python_requires='>=3.6',
    install_requires=requires,
    dependency_links=links,
    extras_require={
        'dev': [],
        'test': ['pytest', 'pytest-mock'],
        'datadog': ['datadog'],
    },
    data_files=[],
)
