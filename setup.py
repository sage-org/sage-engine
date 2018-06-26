# setup.py
# Author: Thomas MINIER - MIT License 2017-2018
from setuptools import setup
from subprocess import call

__version__ = "1.0.0"

console_scripts = [
    'sage = http_server.cli:cli_sage'
]

packages = [
    "database",
    "http_server",
    "query_engine"
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

# run npm install before
call(["make", "install-web"])

setup(
    name="sage-engine",
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
    packages=packages,
    entry_points={
        'console_scripts': console_scripts
    }
)
