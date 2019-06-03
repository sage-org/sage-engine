# setup.py
# Author: Thomas MINIER - MIT License 2017-2018
from setuptools import setup, find_packages
from subprocess import run, SubprocessError
from setuptools.command.develop import develop
from setuptools.command.install import install
from os import getcwd
from sys import exit


def install_web_deps():
    """Install Sage web interface dependencies using npm"""
    try:
        print('Installing Sage Web interface dependencies using npm...')
        path = "{}/sage/http_server/static".format(getcwd())
        run(["npm", "install", "--production"], cwd=path)
        print('Sage Web interface successfully installed')
    except SubprocessError as e:
        print('Error: cannot install Sage Web interface successfully installed')
        print('Error: {}'.format(e))
        exit(1)


class PostDevelopCommand(develop):
    """Post-installation for development mode."""

    def run(self):
        install_web_deps()
        develop.run(self)


class PostInstallCommand(install):
    """Post-installation for installation mode."""

    def run(self):
        install_web_deps()
        print("moo")
        install.run(self)


__version__ = "2.0.1"

# dependencies required for the HDT backend
HDT_DEPS = [
    'pybind11==2.2.4',
    'hdt==2.0'
]

console_scripts = [
    'sage = sage.http_server.cli:cli_sage'
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
    cmdclass={
        'develop': PostDevelopCommand,
        'install': PostInstallCommand,
    },
    # extras dependencies for the native backends
    extras_require={
        'hdt': HDT_DEPS
    },
    entry_points={
        'console_scripts': console_scripts
    }
)
