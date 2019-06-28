# setup.py
# Author: Thomas MINIER - MIT License 2017-2018
from setuptools import setup, find_packages

__version__ = "2.0.1"

# dependencies required for the HDT backend
HDT_DEPS = [
    'pybind11==2.2.4',
    'hdt==2.0'
]

console_scripts = [
    'sage = sage.cli.start_server:start_sage_server'
]

with open('README.rst') as file:
    long_description = file.read()

with open('requirements.txt') as file:
    install_requires = file.read().splitlines()

setup(
    name="sage-engine",
    version=__version__,
    author="Thomas Minier",
    author_email="thomas.minier@univ-nantes.fr",
    url="https://github.com/sage-org/sage-engine",
    description="Sage: a SPARQL query engine for public Linked Data providers",
    long_description=long_description,
    keywords=["rdf", "sparql", "query engine"],
    license="MIT",
    install_requires=install_requires,
    include_package_data=True,
    zip_safe=False,
    packages=find_packages(exclude=["tests", "tests.*"]),
    # extras dependencies for the native backends
    extras_require={
        'hdt': HDT_DEPS
    },
    entry_points={
        'console_scripts': console_scripts
    }
)
