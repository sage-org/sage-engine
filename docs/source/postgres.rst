.. _postgres-cli:

Sage and PostgreSQL
====================

The SaGe server providers several command line utilities
for initializing and manipulating RDF graphs hosted by the PostgreSQL backends.

Initializing a PostgreSQL graph
-------------------------------

.. code:: bash

    Usage: sage-postgres-init [OPTIONS] CONFIG GRAPH_NAME

      Initialize the RDF dataset GRAPH_NAME with the postgres or postgres-mvcc
      backends, as described in the configuration file CONFIG.

    Options:
      --index / --no-index  Enable/disable indexing of SQL tables. The indexes can
                            be created separately using the command sage-postgre-index
      --help                Show this message and exit.


Insert RDF triples into PostgreSQL
----------------------------------

.. code:: bash

  Usage: sage-postgres-put [OPTIONS] RDF_FILE CONFIG GRAPH_NAME

    Insert RDF triples from file RDF_FILE into the RDF graph GRAPH_NAME,
    described in the configuration file CONFIG. The graph must use the
    postgres or postgres-mvcc backends.

  Options:
    -f, --format [nt|ttl|hdt]       Format of the input file. Supported: nt
                                    (N-triples), ttl (Turtle) and hdt (HDT).
                                    [default: nt]
    -b, --block_size INTEGER        Block size used for the bulk loading
                                    [default: 100]
    -c, --commit_threshold INTEGER  Commit after sending this number of RDF
                                    triples  [default: 500000]
    --help                          Show this message and exit.

(Re)generate indexes to speed-up query processing
-------------------------------------------------

.. code:: bash

  Usage: sage-postgres-index [OPTIONS] CONFIG GRAPh_NAME

    Create the additional B-tree indexes on the RDF graph GRAPH_NAME,
    described in the configuration file CONFIG. The graph must use the
    postgres or postgres-mvcc backends.

  Options:
    --help  Show this message and exit.

