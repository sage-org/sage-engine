# setup.py
# Author: Thomas MINIER - MIT License 2017-2018
from setuptools import setup, find_packages
from subprocess import run, SubprocessError
from os import getcwd
from sys import exit


def install_web_deps():
    """Install Sage web interface dependencies using npm"""
    path = "{}/sage/http_server/static".format(getcwd())
    run(["npm", "install", "--production"], cwd=path)


__version__ = "2.0.0"

# dependencies required for the HDT backend
HDT_DEPS = [
    'pybind11==2.2.1',
    'hdt==1.1.0'
]

console_scripts = [
    'sage = sage.http_server.cli:cli_sage'
]

with open('README.rst') as file:
    long_description = file.read()

with open('requirements.txt') as file:
    install_requires = file.read().splitlines()

try:
    print('Installing Sage Web interface dependencies using npm...')
    install_web_deps()
    print('Sage Web interface successfully installed')
except SubprocessError as e:
    print('Error: cannot install Sage Web interface successfully installed')
    print('Error: {}'.format(e))
    exit(1)

setup(
    name="sage",
    version=__version__,
    author="Thomas Minier",
    author_email="thomas.minier@univ-nantes.fr",
    url="https://github.com/sage-org/sage-engine",
    description="Sage: a stable, responsive and unrestricted SPARQL query server",
    long_description=long_description,
    keywords=["rdf", "sparql", "query engine"],
    license="MIT",
    install_requires=install_requires,
    include_package_data=True,
    zip_safe=False,
    packages=find_packages(exclude=["tests", "tests.*"]),
    # extras dependencies for the native backends (HDT, PostgreSQL and Cassandra)
    extras_require={
        'hdt': HDT_DEPS
    },
    entry_points={
        'console_scripts': console_scripts
    }
)
