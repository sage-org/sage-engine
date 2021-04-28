# postgres.py
# Author: Thomas MINIER - MIT License 2017-2019
import coloredlogs
import logging
import time
import click
import psycopg2
import pylru

import sage.cli.postgres_utils as psql_utils

from psycopg2.extras import execute_values
from sage.cli.utils import load_graph, get_nb_triples
from sage.cli.parsers import ParserFactory

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def connect_postgres(graph):
    """Try to connect to a PostgreSQL server"""
    if 'dbname' not in graph or 'user' not in graph or 'password' not in graph:
        logger.error("Error: a valid PostgreSQL graph must be declared with fields 'dbname', 'user' and 'password'")
        return None
    dbname = graph['dbname']
    user = graph['user']
    password = graph['password']
    host = graph['host'] if 'host' in graph else ''
    port = int(graph['port']) if 'port' in graph else 5432
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)


@click.command()
@click.argument("config")
@click.argument("graph_name")
@click.option('--index/--no-index', default=True,
    help="Enable/disable indexing of SQL tables. The indexes can be created separately using the command sage-postgres-index")
def init_postgres(config, graph_name, index):
    """Initialize the RDF graph GRAPH_NAME with a PostgreSQL backend, described in the configuration file CONFIG."""
    # load graph from config file
    graph, backend = load_graph(config, graph_name, logger, backends=['postgres', 'postgres-mvcc', 'postgres-catalog'])

    # init postgre connection
    logger.info("Connecting to the PostgreSQL server...")
    connection = connect_postgres(graph)
    if connection is None:
        logger.error('Failed to establish a connection with PostgreSQL')
        exit(1)
    logger.info("Connected to the PostgreSQL server")

    # turn off autocommit
    connection.autocommit = False

    # create a cursor to interact with the database
    cursor = connection.cursor()

    # create the main SQL tables
    logger.info("Creating PostgreSQL tables...")
    create_table_queries = psql_utils.get_create_tables_queries(graph_name, backend)
    for query in create_table_queries:
        cursor.execute(query)
    logger.info("PostgreSQL tables successfully created")

    # create the additional indexes on OSP and POS
    if index:
        logger.info("Creating additional B-tree indexes...")
        create_indexes_queries = psql_utils.get_create_indexes_queries(graph_name, backend)
        for query in create_indexes_queries:
            cursor.execute(query)
        logger.info("Additional B-tree indexes successfully created")
    else:
        logger.info("Skipping additional indexes creation on user-demand")

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    connection.commit()
    cursor.close()
    connection.close()
    logger.info(f"Sage PostgreSQL model for graph '{graph_name}' successfully initialized")


@click.command()
@click.argument("config")
@click.argument("graph_name")
def index_postgres(config, graph_name):
    """Create the additional B-tree indexes on the RDF graph GRAPH_NAME, described in the configuration file CONFIG."""
    # load graph from config file
    graph, backend = load_graph(config, graph_name, logger, backends=['postgres', 'postgres-mvcc', 'postgres-catalog'])

    # init PostgreSQL connection
    logger.info("Connecting to the PostgreSQL server...")
    connection = connect_postgres(graph)
    if connection is None:
        logger.error('Failed to establish a connection with PostgreSQL')
        exit(1)
    logger.info("Connected to the PostgreSQL server")

    # turn off autocommit
    connection.autocommit = False

    # create a cursor to interact with the database
    cursor = connection.cursor()

    # create indexes
    start = time.time()
    logger.info("Creating additional B-tree indexes...")
    create_indexes_queries = psql_utils.get_create_indexes_queries(graph_name, backend)
    for query in create_indexes_queries:
        cursor.execute(query)
    stop = time.time()
    logger.info(f"Additional B-tree indexes successfully created in {stop - start}s")

    # rebuild table statistics
    logger.info("Rebuilding table statistics...")
    start = time.time()
    cursor.execute(psql_utils.get_analyze_query(graph_name))
    logger.info(f"Table statistics successfully rebuilt in {time.time() - start}s")

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    connection.commit()
    cursor.close()
    connection.close()
    logger.info(f"Sage PostgreSQL model for graph '{graph_name}' successfully initialized")


def insert_bucket(cursor, bucket, graph_name, backend, block_size, cache):
    if backend == 'postgres' or backend == 'postgres-mvcc':
        insert_query = psql_utils.get_insert_into_query(graph_name)
        execute_values(cursor, insert_query, bucket, page_size=block_size)
    elif backend == 'postgres-catalog':
        # Insert terms into the catalog
        insert_query = psql_utils.get_insert_into_catalog_query()
        values = list()
        terms_index = dict()
        cached_identifiers = dict()
        for (s, p, o) in bucket:
            if s in cache:
                cached_identifiers[s] = cache[s]
            elif s not in terms_index:
                terms_index[s] = len(values)
                values.append([s])
            if p in cache:
                cached_identifiers[p] = cache[p]
            elif p not in terms_index:
                terms_index[p] = len(values)
                values.append([p])
            if o in cache:
                cached_identifiers[o] = cache[o]
            elif o not in terms_index:
                terms_index[o] = len(values)
                values.append([o])
        terms_identifier = execute_values(cursor, insert_query, values, page_size=block_size, fetch=True)
        # Insert triples where terms are replaced by their identifier
        insert_query = psql_utils.get_insert_into_query(graph_name)
        values = list()
        for (s, p, o) in bucket:
            if s in cached_identifiers:
                subject_id = cached_identifiers[s]
            else:
                subject_id = terms_identifier[terms_index[s]]
            if p in cached_identifiers:
                predicate_id = cached_identifiers[p]
            else:
                predicate_id = terms_identifier[terms_index[p]]
            if o in cached_identifiers:
                object_id = cached_identifiers[o]
            else:
                object_id = terms_identifier[terms_index[o]]
            cache[s] = subject_id
            cache[p] = predicate_id
            cache[o] = object_id
            values.append((subject_id, predicate_id, object_id))
        execute_values(cursor, insert_query, values, page_size=block_size)
    else:
        raise Exception(f'Unknown backend for PostgreSQL: {backend}')


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
def put_postgres(config, graph_name, rdf_file, format, block_size, commit_threshold, cache_size):
    """Insert RDF triples from file RDF_FILE into the RDF graph GRAPH_NAME, described in the configuration file CONFIG. The graph must use the PostgreSQL or PostgreSQL-MVCC backend."""
    # load graph from config file
    graph, backend = load_graph(config, graph_name, logger, backends=['postgres', 'postgres-mvcc', 'postgres-catalog'])

    # init PostgreSQL connection
    logger.info("Connecting to the PostgreSQL server...")
    connection = connect_postgres(graph)
    if connection is None:
        logger.error('Failed to establish a connection with PostgreSQL')
        exit(1)
    logger.info("Connected to the PostgreSQL server")

    # turn off autocommit
    connection.autocommit = False

    # create a cursor to interact with the database
    cursor = connection.cursor()

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
            if to_commit >= commit_threshold and ignore_errors:
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
            cursor.execute(psql_utils.get_analyze_query(graph_name))
            logger.info(f"Table statistics successfully rebuilt in {time.time() - start}s")
            logger.info("Committing and cleaning up...")
            connection.commit()
            cursor.close()
            connection.close()
            logger.info(f"RDF data from file '{rdf_file}' successfully inserted into RDF graph '{graph_name}'")

        logger.info("Starting RDF triples ingestion...")
        parser = ParserFactory.create_parser(format, block_size)
        parser.on_bucket = on_bucket
        parser.on_error = on_error
        parser.on_complete = on_complete
        parser.parsefile(rdf_file)
