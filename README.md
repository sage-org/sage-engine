# Sage: a SPARQL query engine for public Linked Data providers
[![Build Status](https://travis-ci.com/sage-org/sage-engine.svg?branch=master)](https://travis-ci.com/sage-org/sage-engine) [![PyPI version](https://badge.fury.io/py/sage-engine.svg)](https://badge.fury.io/py/sage-engine) [![Docs](https://img.shields.io/badge/docs-passing-brightgreen)](https://sage-org.github.io/sage-engine/)

[Read the online documentation](https://sage-org.github.io/sage-engine/)

SaGe is a SPARQL query engine for public Linked Data providers that implements *Web preemption*. The SPARQL engine includes a smart Sage client
and a Sage SPARQL query server hosting RDF datasets (hosted using [HDT](http://www.rdfhdt.org/)).
This repository contains the **Python implementation of the SaGe SPARQL query server**.

SPARQL queries are suspended by the web server after a fixed quantum of time and resumed upon client request. Using Web preemption, Sage ensures stable response times for query execution and completeness of results under high load.

The complete approach and experimental results are available in a Research paper accepted at The Web Conference 2019, [available here](https://hal.archives-ouvertes.fr/hal-02017155/document). *Thomas Minier, Hala Skaf-Molli and Pascal Molli. "SaGe: Web Preemption for Public SPARQL Query services" in Proceedings of the 2019 World Wide Web Conference (WWW'19), San Francisco, USA, May 13-17, 2019*.

We appreciate your feedback/comments/questions to be sent to our [mailing list](mailto:sage@univ-nantes.fr) or [our issue tracker on github](https://github.com/sage-org/sage-engine/issues).

# Table of contents

* [Installation](#installation)
* [Getting started](#getting-started)
  * [Server configuration](#server-configuration)
  * [PostgreSQL configuration](#postgresql-configuration)
  * [Data ingestion](#data-ingestion)
  * [Starting the server](#starting-the-server)
* [Sage Docker image](#sage-docker-image)
* [Command line utilities](#command-line-utilities)
* [Documentation](#documentation)

# Installation

Installation in a [virtualenv](https://virtualenv.pypa.io/en/stable/) is **strongly advised!**

Requirements:
* Python 3.7 (*or higher*)
* [pip](https://pip.pypa.io/en/stable/)
* **gcc/clang** with **c++11 support**
* **Python Development headers**
> You should have the `Python.h` header available on your system.   
> For example, for Python 3.6, install the `python3.6-dev` package on Debian/Ubuntu systems.

## Installation using pip

The core engine of the SaGe SPARQL query server with [HDT](http://www.rdfhdt.org/) as a backend can be installed as follows:
```bash
pip install sage-engine[hdt,postgres,hbase]
```
The SaGe query engine uses various **backends** to load RDF datasets.
The various backends available are installed as extras dependencies. The above command install both the HDT, the PostgreSQL and the HBase backends.

## Manual Installation using poetry

The SaGe SPARQL query server can also be manually installed using the [poetry](https://github.com/sdispater/poetry) dependency manager.
```bash
git clone https://github.com/sage-org/sage-engine
cd sage-engine
poetry install --extras "hdt postgres hbase"
```
As with pip, the various SaGe backends are installed as extras dependencies, using the  `--extras` flag.

# Getting started

## Server configuration

A SaGe server is configured using a configuration file in [YAML syntax](http://yaml.org/).
You will find below a minimal working example of such a configuration file.
Full examples are available [in the `config_examples/` directory](https://github.com/sage-org/sage-engine/blob/master/config_examples/example.yaml)

```yaml
name: SaGe Test server
maintainer: Chuck Norris
quota: 75
max_results: 2000
graphs:
-
  name: dbpedia
  uri: http://example.org/dbpedia
  description: DBPedia
  backend: hdt-file
  file: datasets/dbpedia.2016.hdt
```

The `quota` and `max_results` fields are used to set the maximum time quantum and the maximum number of results
allowed per request, respectively.

Each entry in the `graphs` field declare a RDF dataset with a name, description, backend and options specific to this backend.
Different backends are available:
- the `hdt-file` backend allows a SaGe server to load RDF datasets from [HDT files](http://www.rdfhdt.org/). SaGe uses [pyHDT](https://github.com/Callidon/pyHDT) to load and query HDT files.
- the `postgres` backend allows a SaGe server to create, query and update RDF datasets stored in [PostgreSQL](https://www.postgresql.org/). Each dataset is stored in a single table composed of 3 columns; S (subject), P (predicate) and O (object). Tables are created with B-Tree indexes on SPO, POS and OSP. SaGe uses [psycopg2](https://pypi.org/project/psycopg2/) to interact with PostgreSQL.
- the `postgres-catalog` backend uses a different schema than `postgres` to store datasets. Triples terms are mapped to unique identifiers and a dictionary table that is common to all datasets is used to map RDF terms with their identifiers. This schema allows to reduce the space required to store datasets.
- the `sqlite` backend allows a SaGe server to create, query and update RDF datasets stored in [SQLite](https://docs.python.org/3/library/sqlite3.html). Datasets are stored using the same schema as the `postgres` backend.
- the `sqlite-catalog` is another backend for SQLite that uses a dictionary based schema as the `postgres-catalog` backend.
- the `hbase` backend allows a SaGe server to create, query and update RDF datasets stored in [HBase](https://hbase.apache.org/). To have a sorted access on dataset triples, triples are inserted three times in three different tables using SPO, POS and OSP as triples keys. SaGe uses [happybase](https://happybase.readthedocs.io/en/latest/) to interact with HBase.

## PostgreSQL configuration

This section is optional and can be skipped if you don't use one of the PostgreSQL backends.

To ensure stable performance when using PostgreSQL with SaGe, PostgreSQL needs to be configured. Open the file `postgresql.conf` in the PostgreSQL main directory and apply the following changes in the *Planner Method Configuration* section:
- Uncomment all enable_XYZ options
- Set *enable_indexscan*, *enable_indexonlyscan* and *enable_nestloop* to **on**
- Set all the other enable_XYZ options to **off**

These changes force the PostgreSQL query optimizer to generate the desired query plan for the SaGe resume queries.

## Data ingestion

Different executables are available to load a RDF file depending on the backend you want to use.

To load a dataset from a HDT file, just declare a new dataset in your configuration file using the `hdt-file` backend.

To load a N-Triples file using one of the `postgres`, `postgres-catalog`, `hbase`, `sqlite` and `sqlite-catalog` backends, first declare a new dataset in your configuration file. For example, to load the file `my_dataset.nt` using the `sqlite` backend, we start by declaring a new dataset named `my_dataset` in our configuration file `my_config.yaml`.

```yaml
quota: 75
max_results: 10000
graphs:
-
  name: my_dataset
  uri: http://example.org/my_dataset
  backend: sqlite
  database: sage-sqlite.db
```

For each backend, an example that illustrate how to declare a new dataset is available in the [`config_examples/`](https://github.com/sage-org/sage-engine/blob/master/config_examples/example.yaml) directory.

To load a file into a dataset declared using one of the `SQLite` backends, use the following commands:

```bash
# Create the required SQLite tables to store the dataset
sage-sqlite-init --no-index my_config.yaml my_dataset
# Insert the RDF triples in SQLite
sage-sqlite-put my_dataset.nt my_config.yaml my_dataset
# Create the SPO, OSP and POS indexes
sage-sqlite-index my_config.yaml my_dataset_name
```

To load a file into a dataset declared using one of the `PostgreSQL` backends, use the following commands:

```bash
# Create the required PostgreSQL tables to store the dataset
sage-postgres-init --no-index my_config.yaml my_dataset
# Insert the RDF triples in PostgreSQL
sage-postgres-put my_dataset.nt my_config.yaml my_dataset
# Create the SPO, OSP and POS indexes
sage-postgres-index my_config.yaml my_dataset_name
```

To load a file into a dataset declared using the `hbase` backend, use the following commands:

```bash
# Create the required HBase tables to store the dataset
sage-hbase-init my_config.yaml my_dataset
# Insert the RDF triples in HBase
sage-hbase-put my_dataset.nt my_config.yaml my_dataset
```

## Starting the server

The `sage` executable, installed alongside the SaGe server, allows to easily start a SaGe server from a configuration file using [Gunicorn](http://gunicorn.org/), a Python WSGI HTTP Server.

```bash
# launch Sage server with 4 workers on port 8000
sage my_config.yaml -w 4 -p 8000
```

The full usage of the `sage` executable is detailed below:
```
Usage: sage [OPTIONS] CONFIG

  Launch the Sage server using the CONFIG configuration file

Options:
  -p, --port INTEGER              The port to bind  [default: 8000]
  -w, --workers INTEGER           The number of server workers  [default: 4]
  --log-level [debug|info|warning|error]
                                  The granularity of log outputs  [default:
                                  info]
  --help                          Show this message and exit.
```

# SaGe Docker image

The Sage server is also available through a [Docker image](https://hub.docker.com/r/callidon/sage/).
In order to use it, do not forget to [mount in the container](https://docs.docker.com/storage/volumes/) the directory that contains you configuration file and your datasets.

```bash
docker pull callidon/sage
docker run -v path/to/config-file:/opt/data/ -p 8000:8000 callidon/sage sage /opt/data/config.yaml -w 4 -p 8000
```

# Documentation

To generate the documentation, navigate in the `docs` directory and generate the documentation

```bash
cd docs/
make html
open build/html/index.html
```

Copyright 2017-2019 - [GDD Team](https://sites.google.com/site/gddlina/), [LS2N](https://www.ls2n.fr/?lang=en), [University of Nantes](http://www.univ-nantes.fr/)
