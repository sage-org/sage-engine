# Sage: a SPARQL query engine for public Linked Data providers
[![Build Status](https://travis-ci.com/sage-org/sage-engine.svg?branch=master)](https://travis-ci.com/sage-org/sage-engine)

Python implementation of SaGe, a stable, responsive and unrestricted SPARQL query server.

# Table of contents

* [Installation](#installation)
* [Getting started](#getting-started)
  * [Installation](#installation)
  * [Starting the server](#starting-the-server)
* [Sage Docker image](#sage-docker-image)
* [Command line utilities](#command-line-utilities)
* [Documentation](#documentation)

# Installation

Installation in a [virtualenv](https://virtualenv.pypa.io/en/stable/) is **strongly advised!**

Requirements:
* [git](https://git-scm.com/)
* [pip](https://pip.pypa.io/en/stable/)
* [npm](https://nodejs.org/en/) (shipped with Node.js on most systems)
* **gcc/clang** with **c++11 support**
* **Python Development headers**
> You should have the `Python.h` header available on your system.   
> For example, for Python 3.6, install the `python3.6-dev` package on Debian/Ubuntu systems.

## Installing SaGe with the HDT backend

The core engine of the SaGe SPARQL query server with [HDT](http://www.rdfhdt.org/) as a backend can be installed as follows:

```bash
git clone https://github.com/sage-org/sage-engine
cd sage-engine/
pip install -r requirements.txt
pip install -e .[hdt]
```

# Getting started

## Server configuration

A Sage server is configured using a configuration file in [YAML syntax](http://yaml.org/).
You will find below a minimal working example of such configuration file.
A full example is available [in the `config_examples/` directory](https://github.com/sage-org/sage-engine/blob/master/config_examples/example.yaml)

```yaml
name: SaGe Test server
maintainer: Chuck Norris
quota: 75
max_results: 2000
datasets:
-
  name: dbpedia
  description: DBPedia
  backend: hdt-file
  file: datasets/dbpedia.2016.hdt
```

The `quota` and `max_results` fields are used to set the maximum time quantum and the maximum number of results
allowed per request, respectively.

Each entry in the `datasets` field declare a RDF dataset with a name, description, backend and options specific to this backend.
Currently, **only** the `hdt-file` backend is supported, which allow a Sage server to load RDF datasets from [HDT files](http://www.rdfhdt.org/). Sage uses [pyHDT](https://github.com/Callidon/pyHDT) to load and query HDT files.

## Starting the server

The `sage` executable, installed alongside the Sage server, allows to easily start a Sage server from a configuration file using [Gunicorn](http://gunicorn.org/), a Python WSGI HTTP Server.

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

# Sage Docker image

The Sage server is also available through a [Docker image](https://hub.docker.com/r/callidon/sage/).
In order to use it, do not forget to [mount in the container](https://docs.docker.com/storage/volumes/) the directory that contains you configuration file and your datasets.

```bash
docker pull callidon/sage
docker run -v path/to/config-file:/opt/data/ -p 8000:8000 callidon/sage sage /opt/data/config.yaml -w 4 -p 8000
```

# Command line utilities

The SaGe server providers several command line utilities, alongside the `sage` command used to start the server.

## `sage-query`: send SPARQL queries through HTTP requests
```
Usage: sage-query [OPTIONS] ENTRYPOINT DEFAULT_GRAPH_URI

  Send a SPARQL query to a SaGe server hosted at ENTRYPOINT, with
  DEFAULT_GRAPH_URI as the default RDF Graph. It does not act as a Smart
  client, so only queries supported by the server will be evaluated.

  Example usage: sage-query http://sage.univ-nantes.fr/sparql
  http://sage.univ-nantes.fr/sparql/dbpedia-2016-04 -q "SELECT * WHERE { ?s
  ?p ?o }"

Options:
  -q, --query TEXT     SPARQL query to execute (passed in command-line)
  -f, --file TEXT      File containing a SPARQL query to execute
  --format [json|xml]  Format of the results set, formatted according to W3C
                       SPARQL standards.
  -l, --limit INTEGER  Maximum number of solutions bindings to fetch, similar
                       to the SPARQL LIMIT modifier.
  --help               Show this message and exit.
```

## `sage-postgre-init`: Initialize a PostgreSQL dataset with Sage
```
Usage: sage-postgre-init [OPTIONS] CONFIG DATASET_NAME

  Initialize the RDF dataset DATASET_NAME with a PostgreSQL backend,
  described in the configuration file CONFIG.

Options:
  --index / --no-index  Enable/disable indexing of SQL tables. The indexes can
                        be created separately using the command sage-postgre-
                        index
  --help                Show this message and exit.
```

## `sage-postgre-put`: Efficiently insert RDF data into a Sage-PostgreSQL dataset
```
Usage: sage-postgre-put [OPTIONS] RDF_FILE CONFIG DATASET_NAME

  Inert RDF triples from file RDF_FILE into the RDF dataset DATASET_NAME,
  described in the configuration file CONFIG. The dataset must use the
  PostgreSQL backend.

Options:
  -f, --format [nt|ttl|hdt]       Format of the input file. Supported: nt
                                  (N-triples), ttl (Turtle) and hdt (HDT).
                                  [default: nt]
  -b, --block_size INTEGER        Block size used for the bulk loading
                                  [default: 100]
  -c, --commit_threshold INTEGER  Commit after sending this number of RDF
                                  triples  [default: 500000]
  --help                          Show this message and exit.
```

## `sage-postgre-index`: (Re)generate indexes to speed-up query processing with PostgreSQL
```
Usage: sage-postgre-index [OPTIONS] CONFIG DATASET_NAME

  Create the additional B-tree indexes on the RDF dataset DATASET_NAME,
  described in the configuration file CONFIG. The dataset must use the
  PostgreSQL backend.

Options:
  --help  Show this message and exit.
```

# Documentation

To generate the documentation, you must install the following dependencies

```bash
pip install sphinx sphinx_rtd_theme sphinxcontrib-httpdomain
```

Then, navigate in the `docs` directory and generate the documentation

```bash
cd docs/
make html
open build/html/index.html
```

Copyright 2017-2018 - [GDD Team](https://sites.google.com/site/gddlina/), [LS2N](https://www.ls2n.fr/?lang=en), [University of Nantes](http://www.univ-nantes.fr/)
