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
