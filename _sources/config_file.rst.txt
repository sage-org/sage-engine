Configuring the SaGe server
===========================

Configuration reference
-----------------------

SaGe configuration files are written in YAML syntax,
according to the following format.

.. code:: yaml

  # The name of the server
  name: SaGe Example Server

  # The Server maintainer
  maintainer: Tim Berners-Lee

  # The public URL used to access the server
  public_url: http://server-url.com

  # Time quantum used by the server
  # Use 'inf' to allow an infinite quantum per query (Warning: it may break SaGe properties)
  quota: 75

  # (Optional) Maximum number of results fetched by HTTP request
  # Defaults to 2000. Use 'inf' to disable the limitations.
  max_results: 2000

  # RDF Graphs hosted by the server
  graphs:
  -
    # The name of the RDF Graph
    name: dbpedia
    # The URI of the RDF Graph in the RDF dataset
    uri: http://example.org/dbpedia
    # The description of the RDF Graph
    description: The DBpedia dataset
    # Type of backend (an example here with the HDT backend)
    backend: hdt-file
    file: ./dbpedia.hdt
    # Example queries that can be executed using this dataset
    queries:
      - name: "Every RDF triples"
        value: |
          SELECT * WHERE {
            ?s ?p ?o.
          }
      - name: "Airport located in Italy"
        value: |
          prefix dbo: <http://dbpedia.org/ontology/>
          prefix dbp: <http://dbpedia.org/property/>
          prefix dbr: <http://dbpedia.org/resource/>
          SELECT DISTINCT ?entity WHERE {
            ?entity a dbo:Airport;
              dbp:cityServed dbr:Italy.
          }

  -
    name: geonames
    uri: http://example.org/geonames
    description: The Geonames dataset
    backend: hdt-file
    file: ./geonames.hdt


HDT backend configuration
--------------------------

The `hdt-file` backend allows to query HDT files.

The following option must be set with this backend
  * **file** (str): Absolute path to the HDT file

The following options are optionals
  * **mapped** (bool): True maps the HDT file on disk (faster), False loads everything in memory.
  * **indexed** (bool: True if the HDT must be loaded with indexes, False otherwise. The SaGe server will looks for indexes in the same directory as the original HDT files. If they are missing, they will be automatically re-built from the data (Warning: this process way be expensive for large HDT files).

PostgreSQL backend configuration
--------------------------------

The `postgres` and `postgres-mvcc` backend allows to store
and query RDF data using a PostgreSQL database.
The `postgres` backend stores RDF triples using a simple triple-store layout,
while the `postgres-mvcc` backend rely on a multi-version concurrency control
protocol to ensure consitent reads in presence of concurrent updates.

For initializing a PostgreSQL database to be used with these backend,
please refers to the :ref:`postgres-cli` chapter.

With both backends, the following options must be set
  * **table_name** (str): Name of the SQL table containing RDF data.
  * **dbname** (str): the database name.
  * **user** (str): user name used to authenticate.
  * **password** (str): password used to authenticate.

With both backends, the following options are optionals
  * **host** (str): database host address (defaults to UNIX socket if not provided).
  * **port** (int: connection port number (defaults to 5432 if not provided).
  * **fetch_size** (int): The number of SQL rows/RDF triples to fetch per batch (defaults to 2000).
