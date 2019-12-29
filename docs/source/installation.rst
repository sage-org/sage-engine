Installation
============

Installation in a `virtualenv <https://virtualenv.pypa.io/en/stable/>`__
is **strongly advised!**

| Requirements: \* `git <https://git-scm.com/>`__ \*
  `pip <https://pip.pypa.io/en/stable/>`__ \*
  `npm <https://nodejs.org/en/>`__ (shipped with Node.js on most
  systems) \* **gcc/clang** with **c++11 support** \* **Python
  Development headers** > You should have the ``Python.h`` header
  available on your system.
| > For example, for Python 3.6, install the ``python3.6-dev`` package
  on Debian/Ubuntu systems.

Installing SaGe with the HDT backend
------------------------------------

The core engine of the SaGe SPARQL query server with [HDT](http://www.rdfhdt.org/) as a backend can be installed as follows:

.. code:: bash

    git clone https://github.com/sage-org/sage-engine
    cd sage-engine/
    pip install -r requirements.txt
    pip install -e .[hdt]

Installing SaGe with the PostgreSQL backend
--------------------------------------------

The SaGe SPARQL query server can also be installed with a PostgreSQL backend](http://www.rdfhdt.org/).

.. code:: bash

    pip install -e .[postgres]

Getting started
===============

Server configuration
--------------------

A Sage server is configured using a configuration file in `YAML
syntax <http://yaml.org/>`__.

.. code:: yaml

    name: SaGe Test server
    maintainer: Chuck Norris
    quota: 75
    maxResults: 500
    graphs:
    -
      name: dbpedia-2016
      uri: http://example.org#dbpedia-2016
      description: DBPedia v2016
      backend: hdt-file
      file: graphs/dbpedia.2016.hdt
    -
      name: geonames
      uri: http://example.org#geonames
      description: Geonames graph
      backend: hdt-file
      file: graphs/geonames.hdt

The ``quota`` and ``maxResults`` fields are used to set the maximum
query execution time and the maximum nuber of results per request,
respectively.

Each entry in the ``graphs`` field declare a RDF graph with a name, URI,
description, backend and options specific to this backend. Currently,
only the ``hdt-file`` and ``postgres`` backends are supported.

Launch a Sage server
--------------------

The ``sage`` executable , installed alongside the Sage server, allows to
easily start a sage server from a configuration file, using
`Gunicorn <http://gunicorn.org/>`__, a Python WSGI HTTP Server.

.. code:: bash

    # launch Sage server with 4 workers on port 8000
    sage data/watdiv_config.yaml -w 4 -p 8000

Sage Docker image
=================

The Sage server is also available through a `Docker
image <https://hub.docker.com/r/callidon/sage/>`__

.. code:: bash

    docker pull callidon/sage
    docker run -p 8000:8000 callidon/sage sage config.yaml -w 4 -p 8000
