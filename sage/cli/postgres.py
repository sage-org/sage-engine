# postgres.py
# Author: Thomas MINIER - MIT License 2017-2019
import sage.cli.postgres_utils as p_utils
from sage.cli.utils import load_dataset, get_rdf_reader
from sage.database.postgres.utils import predicate_to_id
import click
import psycopg2
from psycopg2.extras import execute_values
import coloredlogs
import logging
from time import time
import datetime


def bucketify(iterable, bucket_size):
    """Group items from an iterable by buckets"""
    bucket = list()
    for s, p, o in iterable:
        # try to encode the predicate (if it is a general predicate)
        # p = predicate_to_id(p)
        bucket.append((s, p, o))
        if len(bucket) >= bucket_size:
            yield bucket
            bucket = list()
    if len(bucket) > 0:
        yield bucket


def connect_postgres(dataset):
    """Try to connect to a PostgreSQL server"""
    if 'dbname' not in dataset or 'user' not in dataset or 'password' not in dataset:
        print("Error: a valid PostgreSQL dataset must be declared with fields 'dbname', 'user' and 'password'")
        return None
    dbname = dataset['dbname']
    user = dataset['user']
    password = dataset['password']
    host = dataset['host'] if 'host' in dataset else ''
    port = int(dataset['port']) if 'port' in dataset else 5432
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)


@click.command()
@click.argument("config")
@click.argument("dataset_name")
@click.option('--index/--no-index', default=True, help="Enable/disable indexing of SQL tables. The indexes can be created separately using the command sage-postgre-index")
def init_postgres(config, dataset_name, index):
    """
        Initialize the RDF dataset DATASET_NAME with a PostgreSQL/PostgreSQL-MVCC backend, described in the configuration file CONFIG.
    """
    # install logger
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    # load dataset from config file
    dataset, kind = load_dataset(config, dataset_name, logger, backends=['postgres', 'postgres-mvcc'])
    enable_mvcc = kind == 'postgres-mvcc'

    # init postgre connection
    connection = connect_postgres(dataset)
    if connection is None:
        exit(1)
    # turn off autocommit
    connection.autocommit = False

    # create all SQL queries used to init the dataset, using the dataset name
    table_name = dataset['name']
    create_table_query = p_utils.get_postgres_create_table(table_name, enable_mvcc=enable_mvcc)
    create_indexes_queries = p_utils.get_postgres_create_indexes(table_name, enable_mvcc=enable_mvcc)

    cursor = connection.cursor()
    # create the main SQL table
    logger.info("Creating SQL table {}...".format(table_name))
    cursor.execute(create_table_query)
    logger.info("SPARQL table {} successfully created".format(table_name))

    # create the additional inexes on OSP and POS
    if index:
        logger.info("Creating additional B-tree indexes...")
        for q in create_indexes_queries:
            cursor.execute(q)
        logger.info("Additional B-tree indexes successfully created")
    else:
        logger.info("Skipping additional indexes creation on user-demand")

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    connection.commit()
    cursor.close()
    connection.close()
    logger.info("Sage PostgreSQL model for table {} successfully initialized".format(table_name))


@click.command()
@click.argument("config")
@click.argument("dataset_name")
def index_postgres(config, dataset_name):
    """
        Create the additional B-tree indexes on the RDF dataset DATASET_NAME, described in the configuration file CONFIG. The dataset must use the PostgreSQL or PostgreSQL-MVCC backend.
    """
    # install logger
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    # load dataset from config file
    dataset, kind = load_dataset(config, dataset_name, logger, backends=['postgres', 'postgres-mvcc'])
    enable_mvcc = kind == 'postgres-mvcc'

    # init PostgreSQL connection
    connection = connect_postgres(dataset)
    if connection is None:
        exit(1)
    # turn off autocommit
    connection.autocommit = False
    # create all SQL queries used to init the dataset, using the dataset name
    table_name = dataset['name']
    create_indexes_queries = p_utils.get_postgres_create_indexes(table_name, enable_mvcc=enable_mvcc)

    # create indexes
    cursor = connection.cursor()
    start = time()
    logger.info("Creating additional B-tree indexes...")
    for q in create_indexes_queries:
        cursor.execute(q)
    stop = time()
    logger.info("Additional B-tree indexes successfully created in {}s".format(stop - start))

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    connection.commit()
    cursor.close()
    connection.close()
    logger.info("Sage PostgreSQL model for table {} successfully initialized".format(table_name))


@click.command()
@click.argument("rdf_file")
@click.argument("config")
@click.argument("dataset_name")
@click.option("-f", "--format", type=click.Choice(["nt", "ttl", "hdt"]),
              default="nt", show_default=True, help="Format of the input file. Supported: nt (N-triples), ttl (Turtle) and hdt (HDT).")
@click.option("-b", "--block_size", type=int, default=100, show_default=True,
              help="Block size used for the bulk loading")
@click.option("-c", "--commit_threshold", type=int, default=500000, show_default=True,
              help="Commit after sending this number of RDF triples")
def put_postgres(config, dataset_name, rdf_file, format, block_size, commit_threshold):
    """
        Insert RDF triples from file RDF_FILE into the RDF dataset DATASET_NAME, described in the configuration file CONFIG. The dataset must use the PostgreSQL or PostgreSQL-MVCC backend.
    """
    # install logger
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    # load dataset from config file
    dataset, kind = load_dataset(config, dataset_name, logger, backends=['postgres', 'postgres-mvcc'])
    enable_mvcc = kind == 'postgres-mvcc'

    # init PostgreSQL connection
    logger.info("Connecting to PostgreSQL server...")
    connection = connect_postgres(dataset)
    logger.info("Connected to PostgreSQL server")
    if connection is None:
        exit(1)
    # turn off autocommit
    connection.autocommit = False

    # compute SQL table name and the bulk load SQL query
    table_name = dataset['name']
    insert_into_query = p_utils.get_postgres_insert_into(table_name, enable_mvcc=enable_mvcc)

    logger.info("Reading RDF source file...")
    iterator, nb_triples = get_rdf_reader(rdf_file, format=format)
    logger.info("RDF source file loaded. Found ~{} RDF triples to ingest.".format(nb_triples))

    logger.info("Starting RDF triples ingestion...")
    cursor = connection.cursor()

    # insert rdf triples
    start = time()
    to_commit = 0
    # insert by bucket (and show a progress bar)
    with click.progressbar(length=nb_triples,
                           label="Inserting RDF triples".format(nb_triples)) as bar:
        for bucket in bucketify(iterator, block_size):
            to_commit += len(bucket)
            # bulk load the bucket of RDF triples, then update progress bar
            execute_values(cursor, insert_into_query, bucket, page_size=block_size)
            bar.update(len(bucket))
            # commit if above threshold
            if to_commit >= commit_threshold:
                # logger.info("Commit threshold reached. Committing all changes...")
                connection.commit()
                # logger.info("All changes were successfully committed.")
                to_commit = 0
    end = time()
    logger.info("RDF triples ingestion successfully completed in {}s".format(end - start))

    # run an ANALYZE query to rebuild statistics
    logger.info("Rebuilding table statistics...")
    start = time()
    cursor.execute("ANALYZE {}".format(table_name))
    end = time()
    logger.info("Table statistics successfully rebuilt in {}s".format(end - start))

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    connection.commit()
    cursor.close()
    connection.close()
    logger.info("RDF data from file '{}' successfully inserted into RDF dataset '{}'".format(rdf_file, table_name))
