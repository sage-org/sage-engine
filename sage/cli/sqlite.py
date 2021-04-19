# postgres.py
# Author: Thomas MINIER - MIT License 2017-2019
import click
import sqlite3
import coloredlogs
import logging
import time
import pylru

import sage.cli.sqlite_utils as sqlite_utils

from sage.cli.utils import load_graph, get_nb_triples
from sage.cli.parsers import ParserFactory

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def connect_sqlite(graph):
    if 'database' not in graph:
        print("Error: a valid SQlite dataset must be declared with a field 'database'")
        return None
    database = graph['database']
    return sqlite3.connect(database)


@click.command()
@click.argument("config")
@click.argument("graph_name")
@click.option('--index/--no-index', default=True,
    help="Enable/disable indexing of SQL tables. The indexes can be created separately using the command sage-postgres-index")
def init_sqlite(config, graph_name, index):
    """Initialize the RDF graph GRAPH_NAME with a SQlite backend, described in the configuration file CONFIG."""
    # load graph from config file
    graph, backend = load_graph(config, graph_name, logger, backends=['sqlite', 'sqlite-catalog'])

    # init SQlite connection
    logger.info("Connecting to the SQlite server...")
    connection = connect_sqlite(graph)
    connection.isolation_level = None
    if connection is None:
        logger.error('Failed to establish a connection with SQlite')
        exit(1)
    logger.info("Connected to the SQlite server")

    # create a cursor to interact with the database
    cursor = connection.cursor()

    # start a transaction
    cursor.execute("BEGIN TRANSACTION")

    # create the main SQL tables
    logger.info("Creating SQlite tables...")
    create_table_queries = sqlite_utils.get_create_tables_queries(graph_name, backend)
    for query in create_table_queries:
        cursor.execute(query)
    logger.info("SQlite tables successfully created")

    # create the additional indexes on OSP and POS
    if index:
        logger.info("Creating additional B-tree indexes...")
        create_indexes_queries = sqlite_utils.get_create_indexes_queries(graph_name, backend)
        for query in create_indexes_queries:
            cursor.execute(query)
        logger.info("Additional B-tree indexes successfully created")
    else:
        logger.info("Skipping additional indexes creation on user-demand")

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    cursor.execute("COMMIT")
    cursor.close()
    connection.close()
    logger.info(f"Sage SQlite model for graph '{graph_name}' successfully initialized")


@click.command()
@click.argument("config")
@click.argument("graph_name")
def index_sqlite(config, graph_name):
    """Create the additional B-tree indexes on the RDF graph GRAPH_NAME, described in the configuration file CONFIG."""
    # load graph from config file
    graph, backend = load_graph(config, graph_name, logger, backends=['sqlite', 'sqlite-catalog'])

    # init SQlite connection
    logger.info("Connecting to the SQlite server...")
    connection = connect_sqlite(graph)
    connection.isolation_level = None
    if connection is None:
        logger.error('Failed to establish a connection with SQlite')
        exit(1)
    logger.info("Connected to the SQlite server")

    # create a cursor to interact with the database
    cursor = connection.cursor()

    # start a transaction
    cursor.execute("BEGIN TRANSACTION")

    # create indexes
    start = time.time()
    logger.info("Creating additional B-tree indexes...")
    create_indexes_queries = sqlite_utils.get_create_indexes_queries(graph_name, backend)
    for query in create_indexes_queries:
        cursor.execute(query)
    stop = time.time()
    logger.info(f"Additional B-tree indexes successfully created in {stop - start}s")

    # rebuild table statistics
    logger.info("Rebuilding table statistics...")
    start = time.time()
    cursor.execute(sqlite_utils.get_analyze_query(graph_name))
    logger.info(f"Table statistics successfully rebuilt in {time.time() - start}s")

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    cursor.execute("COMMIT")
    cursor.close()
    connection.close()
    logger.info(f"Sage SQlite model for graph '{graph_name}' successfully initialized")


def insert_bucket(cursor, bucket, graph_name, backend, block_size, cache):
    if backend == 'sqlite':
        insert_query = sqlite_utils.get_insert_into_query(graph_name)
        cursor.executemany(insert_query, bucket)
    elif backend == 'sqlite-catalog':
        # Insert terms into the catalog
        insert_query = sqlite_utils.get_insert_into_catalog_query()
        values = dict()
        cached_identifiers = dict()
        for (s, p, o) in bucket:
            if s in cache:
                cached_identifiers[s] = cache[s]
            else:
                values[s] = 0
            if p in cache:
                cached_identifiers[p] = cache[p]
            else:
                values[p] = 0
            if o in cache:
                cached_identifiers[o] = cache[o]
            else:
                values[o] = 0
        values = [ [term] for term in list(values.keys()) ]
        cursor.executemany(insert_query, values)
        # Insert triples where terms are replaced by their identifier
        insert_query = sqlite_utils.get_insert_into_query(graph_name)
        select_id_query = sqlite_utils.get_select_identifier_query()
        values = list()
        for (s, p, o) in bucket:
            if s in cached_identifiers:
                subject_id = cached_identifiers[s]
            else:
                subject_id = cursor.execute(select_id_query, [s]).fetchone()[0]
            if p in cached_identifiers:
                predicate_id = cached_identifiers[p]
            else:
                predicate_id = cursor.execute(select_id_query, [p]).fetchone()[0]
            if o in cached_identifiers:
                object_id = cached_identifiers[o]
            else:
                object_id = cursor.execute(select_id_query, [o]).fetchone()[0]
            cache[s] = subject_id
            cache[p] = predicate_id
            cache[o] = object_id
            values.append((subject_id, predicate_id, object_id))
        cursor.executemany(insert_query, values)
    else:
        raise Exception(f'Unknown backend for SQlite: {backend}')


@click.command()
@click.argument("rdf_file")
@click.argument("config")
@click.argument("graph_name")
@click.option("-f", "--format", type=click.Choice(["nt", "hdt"]),
    default="nt", show_default=True,
    help="Format of the input file. Supported: nt (N-triples) and hdt (HDT).")
@click.option("--block-size", type=int,
    default=100, show_default=True,
    help="Block size used for the bulk loading")
@click.option("--commit-threshold", type=int,
    default=500000, show_default=True,
    help="Commit after sending this number of RDF triples")
@click.option("--cache-size", type=int,
    default=300, show_default=True,
    help="Store terms identifier when using the catalog schema to improve loading performance")
def put_sqlite(config, graph_name, rdf_file, format, block_size, commit_threshold, cache_size):
    """Insert RDF triples from file RDF_FILE into the RDF graph GRAPH_NAME, described in the configuration file CONFIG."""
    # load graph from config file
    graph, backend = load_graph(config, graph_name, logger, backends=['sqlite', 'sqlite-catalog'])

    # init SQlite connection
    logger.info("Connecting to the SQlite server...")
    connection = connect_sqlite(graph)
    connection.isolation_level = None
    if connection is None:
        logger.error('Failed to establish a connection with SQlite')
        exit(1)
    logger.info("Connected to the SQlite server")

    # create a cursor to interact with the database
    cursor = connection.cursor()

    # start a transaction
    cursor.execute("BEGIN TRANSACTION")

    logger.info("Reading RDF source file...")
    nb_triples = get_nb_triples(rdf_file, format)
    logger.info(f"Found ~{nb_triples} RDF triples to ingest.")

    start = time.time()
    to_commit = 0
    inserted = 0
    dropped = 0

    cache = pylru.lrucache(cache_size)

    with click.progressbar(length=nb_triples, label=f"Inserting RDF triples 0/{nb_triples} - {dropped} triples dropped.") as bar:

        def on_bucket(bucket):
            nonlocal to_commit, inserted, dropped
            insert_bucket(cursor, bucket, graph_name, backend, block_size, cache)
            to_commit = to_commit + len(bucket)
            if to_commit >= commit_threshold:
                connection.commit()
                to_commit = 0
            inserted = inserted + len(bucket)
            bar.label = f"Inserting RDF triples {inserted}/{nb_triples} - {dropped} triples dropped."
            bar.update(len(bucket))

        def on_error(error):
            nonlocal dropped, inserted
            dropped = dropped + 1
            bar.label = f"Inserting RDF triples {inserted}/{nb_triples} - {dropped} triples dropped."
            bar.update(0)

        def on_complete():
            nonlocal start
            logger.info(f"Triples ingestion successfully completed in {time.time() - start}s")
            logger.info("Rebuilding table statistics...")
            start = time.time()
            cursor.execute(sqlite_utils.get_analyze_query(graph_name))
            logger.info(f"Table statistics successfully rebuilt in {time.time() - start}s")
            logger.info("Committing and cleaning up...")
            cursor.execute("COMMIT")
            cursor.close()
            connection.close()
            logger.info(f"RDF data from file '{rdf_file}' successfully inserted into RDF graph '{graph_name}'")

        logger.info("Starting RDF triples ingestion...")
        parser = ParserFactory.create_parser(format, block_size)
        parser.on_bucket = on_bucket
        parser.on_error = on_error
        parser.on_complete = on_complete
        parser.parsefile(rdf_file)
