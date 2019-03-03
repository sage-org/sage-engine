# postgre.py
# Author: Thomas MINIER - MIT License 2017-2019
import sage.cli.postgre_utils as p_utils
from sage.cli.utils import load_dataset, get_rdf_reader
import argparse
import psycopg2
from psycopg2.extras import execute_values
import coloredlogs
import logging
from math import modf
from time import time


def connect_postgre(dataset):
    """Try to connect to a PostGre server"""
    if 'dbname' not in dataset or 'user' not in dataset or 'password' not in dataset:
        print("Error: a valid PostGre dataset must be declared with fields 'dbname', 'user' and 'password'")
        return None
    dbname = dataset['dbname']
    user = dataset['user']
    password = dataset['password']
    host = dataset['host'] if 'host' in dataset else ''
    port = int(dataset['port']) if 'port' in dataset else 5432
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)


def init_postgre():
    """Initialize a PostgreSQL table in order to be used as a RDF dataset by Sage"""
    # install logger
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description='Initialize a PostGre table to be used as a backend by a Sage server')
    parser.add_argument('config', metavar='config', help='Path to the configuration file used')
    parser.add_argument('dataset', metavar='dataset', help='Name of the dataset to initialize (as declared in the configuration file provided)')
    args = parser.parse_args()
    # load dataset from config file
    dataset = load_dataset(args.config, args.dataset, 'postgre', logger)

    # init postgre connection
    connection = connect_postgre(dataset)
    if connection is None:
        exit(1)
    # turn off autocommit
    connection.autocommit = False

    # create all SQL queries used to init the dataset, using the dataset name
    table_name = dataset['name']
    create_table_query = p_utils.get_postgre_create_table(table_name)
    create_indexes_queries = p_utils.get_postgre_create_indexes(table_name)
    create_cursors_queries = p_utils.get_postgre_functions(table_name)

    cursor = connection.cursor()
    # create the main SQL table
    logger.info("Creating SQL table {}...".format(table_name))
    cursor.execute(create_table_query)
    logger.info("SPARQL table {} successfully created".format(table_name))

    # load additional data
    # TODO

    # create the additional inexes on OSP and POS
    logger.info("Creating additional B-tree indexes...")
    for q in create_indexes_queries:
        cursor.execute(q)
    logger.info("Additional B-tree indexes successfully created")

    # create the cursor functions used to perform index scans
    logger.info("Creating utility PL-SQL functions...")
    for q in create_cursors_queries:
        cursor.execute(q)
    logger.info("Utility PL-SQL functions successfully created")

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    connection.commit()
    cursor.close()
    connection.close()
    logger.info("Sage PostgreSQL model for table {} successfully initialized".format(table_name))


def put_postgre():
    """Insert RDF triples into a PostgreSQL Sage dataset"""
    # install logger
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description='Insert RDF triples into a PostgreSQL dataset')
    parser.add_argument('config', metavar='config', help='Path to the configuration file used')
    parser.add_argument('dataset', metavar='dataset', help='Name of the dataset to initialize (as declared in the configuration file provided)')
    parser.add_argument('source', metavar='source', help='File containing RDF triples')
    parser.add_argument('-f', '--format', dest='rdf_format', help='Format of the input file. Supported: nt (N-triples), ttl (Turtle) and hdt (HDT). Default to nt', default='nt')
    parser.add_argument('-b', '--block_size', dest='block_size', help='Block size used for the bulk loading. Defaults to 100', type=int, default=100)
    args = parser.parse_args()

    # load dataset from config file
    dataset = load_dataset(args.config, args.dataset, 'postgre', logger)

    # init postgre connection
    logger.info("Connecting to PostgreSQL server...")
    connection = connect_postgre(dataset)
    logger.info("Connected to PostgreSQL server")
    if connection is None:
        exit(1)
    # turn off autocommit
    connection.autocommit = False

    # compute SQL table name and the bulk load SQL query
    table_name = dataset['name']
    insert_into_query = p_utils.get_postgre_insert_into(table_name)

    logger.info("Reading RDF source file...")
    iterator, nb_triples = get_rdf_reader(args.source, format=args.rdf_format)
    logger.info("RDF source file loaded. ~{} RDF triples to ingest.".format(nb_triples))

    logger.info("Starting RDF triples ingestion...")
    cursor = connection.cursor()

    # insert rdf triples
    bucket = list()
    cpt = 0
    prev_progress = 0.0
    start = time()
    # insert by bucket
    for triple in iterator:
        cpt += 1
        bucket.append(triple)
        # bucket read to be inserted
        if len(bucket) >= args.block_size:
            # bulk load the bucket of RDF triples
            execute_values(cursor, insert_into_query, bucket, page_size=args.block_size)
            bucket = list()
            # update and display progress
            frac_part, progress = modf(cpt / nb_triples * 100)
            if prev_progress < progress:
                prev_progress = progress
                logger.info("Progression: {}% ({}/{} RDF triples ingested)".format(progress, cpt, nb_triples))
    # finish the last non-empty bucket
    if len(bucket) > 0:
        execute_values(cursor, insert_into_query, bucket, page_size=args.block_size)
    end = time()
    logger.info("RDF triples ingestion successfully completed in {}s".format(end - start))

    # run an ANALYZE query to rebuild statistics
    logger.info("Rebuilding table statistics...")
    start = time()
    cursor.execute("ANALYZE {}".format(table_name))
    end = time()
    logger.info("Table statistics successfully rebuilt in {}s".format(end - time))

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    connection.commit()
    cursor.close()
    connection.close()
    logger.info("RDF data from file '{}' successfully inserted into RDF dataset '{}'".format(args.source, table_name))
