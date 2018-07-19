SaGe server
===========

Python implementation of SaGe, a preemptive query engine for Basic Graph
pattern evaluation.

Table of contents
=================

-  `Installation <#installation>`__
-  `Getting started <#getting-started>`__
-  `Sage Docker image <#sage-docker-image>`__
-  `Documentation <#documentation>`__

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

.. code:: bash

    git clone https://github.com/Callidon/sage-bgp
    cd sage-engine/
    pip install -r requirements.txt
    python setup.py install

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
    datasets:
    -
      name: dbpedia-2016
      description: DBPedia v2016
      backend: hdt-file
      file: datasets/dbpedia.2016.hdt
    -
      name: geonames
      description: Geonames dataset
      backend: hdt-file
      file: datasets/geonames.hdt

The ``quota`` and ``maxResults`` fields are used to set the maximum
query execution time and the maximum nuber of results per request,
respectively.

Each entry in the ``datasets`` field declare a RDF dataset with a name,
description, backend and options specific to this backend. Currently,
only the ``hdt-file`` backend is supported, which allow a Sage server to
load RDF datasets from `HDT files <http://www.rdfhdt.org/>`__. Sage uses
`pyHDT <https://github.com/Callidon/pyHDT>`__ to load an query HDT
files.

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

Documentation
=============

To generate the documentation, you must install the following
dependencies

.. code:: bash

    pip install sphinx sphinx_rtd_theme sphinxcontrib-httpdomain

Then, navigate in the ``docs`` directory and generate the documentation

.. code:: bash

    cd docs/
    make html
    open build/html/index.html
