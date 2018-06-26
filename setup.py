# setup.py
# Author: Thomas MINIER - MIT License 2017-2018
from setuptools import setup, find_packages
from subprocess import run
from os import getcwd
from sys import exit


def install_web_deps():
    """Install Sage web interface dependencies using npm"""
    path = "{}/http_server/static".format(getcwd())
    run(["npm", "install", "--production"], cwd=path)


__version__ = "1.0.0"

console_scripts = [
    'sage = http_server.cli:cli_sage'
]

install_requires = [
    "Flask==0.12.2",
    "hdt==1.1.0",
    "marshmallow==2.15.0",
    "PyYAML==3.12",
    "rdflib==4.2.2",
    "requests==2.18.4",
    "gunicorn==19.7.1",
    "flask-cors==3.0.3",
    "protobuf==3.5.2"
]

with open('README.rst') as file:
    long_description = file.read()

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
    url="https://github.com/Callidon/sage-engine",
    description="...",
    long_description=long_description,
    keywords=["rdf", "sparql", "query engine"],
    license="MIT",
    install_requires=install_requires,
    include_package_data=True,
    zip_safe=False,
    packages=find_packages(exclude=["tests", "tests.*"]),
    entry_points={
        'console_scripts': console_scripts
    }
)
