# postgre.py
# Author: Thomas MINIER - MIT License 2017-2019
import argparse
import psycopg2
import sage.cli.postgre_utils as p_utils
import coloredlogs
import logging
from yaml import load
from os.path import isfile
from sys import exit


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
    # install logger
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description='Initialize a PostGre table to be used as a backend by a Sage server')
    parser.add_argument('config', metavar='config', help='Path to the configuration file used')
    parser.add_argument('dataset', metavar='dataset', help='Name of the dataset to initialize (as declared in the configuration file provided)')
    args = parser.parse_args()
    # load config file and extract dataset infos
    if isfile(args.config):
        config_file = load(open(args.config))
        if 'datasets' not in config_file:
            logger.error("No RDF datasets declared in the configuration provided")
            exit(1)
        datasets = config_file['datasets']
        dataset = None
        for d in datasets:
            if d['name'] == args.dataset and d['backend'] == 'postgre':
                dataset = d
                break
        if dataset is None:
            logger.error("No PostgreSQL-compatible RDF dataset named '{}' declared in the configuration provided".format(args.dataset))
            exit(1)
        # init postgre connection
        connection = connect_postgre(dataset)
        if connection is None:
            exit(1)
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
    else:
        logger.error("Invalid configuration file supplied '{}'".format(args.config))
        exit(1)
