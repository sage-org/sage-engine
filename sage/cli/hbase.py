# cli.py
# Author: Thomas MINIER - MIT License 2019
import happybase
import click
import coloredlogs
import logging

from sys import exit
from os.path import isfile
from yaml import load
from hdt import HDTDocument
from time import time
from sage.database.hbase.utils import build_row_key
from sage.cli.utils import load_graph, get_nb_triples
from sage.cli.parsers import ParserFactory, ParseError

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


@click.command()
@click.argument("config")
@click.argument("graph_name")
def init_hbase(config, graph_name):
    """
        Initialize the RDF Graph DATASET_NAME with a Apache HBase backend, described in the configuration file CONFIG.
    """
    # load graph from config file
    graph, kind = load_graph(config, graph_name, logger, backends=['hbase'])

    thrift_port = graph['thrift_port'] if 'thrift_port' in graph else 9090

    logger.info("Connexion to the HBase server...")
    connection = happybase.Connection(graph['thrift_host'], protocol="compact", transport="framed", port=thrift_port, table_prefix=graph_name)
    logger.info("Connected to the HBase server !")

    # create HBase tables
    families = {'rdf': dict()}
    logger.info("Creating HBase tables for RDF Graph '{}'...".format(graph_name))
    connection.create_table('spo', families)
    connection.create_table('pos', families)
    connection.create_table('osp', families)
    logger.info("RDF Graph '{}' successfully created in HBase".format(graph_name))
    connection.close()


@click.command()
@click.argument("rdf_file")
@click.argument("config")
@click.argument("graph_name")
@click.option("-f", "--format", type=click.Choice(["nt", "hdt"]),
    default="nt", show_default=True,
    help="Format of the input file. Supported: nt (N-triples) and hdt (HDT).")
@click.option("-b", "--batch-size", type=int, default=1000, show_default=True,
              help="Batch size used for batch loading")
def put_hbase(rdf_file, config, graph_name, format, batch_size):
    """
        Insert RDF triples from HDT file HDT_FILE into the RDF Graph graph_name, described in the configuration file CONFIG. The dataset must use the Apache HBase backend.
    """
    # load graph from config file
    graph, kind = load_graph(config, graph_name, logger, backends=['hbase'])

    thrift_port = graph['thrift_port'] if 'thrift_port' in graph else 9090

    logger.info("Connexion to the HBase server...")
    connection = happybase.Connection(graph['thrift_host'], protocol="compact", transport="framed", port=thrift_port, table_prefix=graph_name)
    logger.info("Connected to the HBase server !")

    spo_batch = connection.table('spo').batch(batch_size=batch_size)
    pos_batch = connection.table('pos').batch(batch_size=batch_size)
    osp_batch = connection.table('osp').batch(batch_size=batch_size)

    logger.info("Reading RDF source file...")
    nb_triples = get_nb_triples(rdf_file, format)
    logger.info(f"Found ~{nb_triples} RDF triples to ingest.")

    start = time()
    inserted = 0
    dropped = 0

    with click.progressbar(length=nb_triples, label=f"Inserting RDF triples 0/{nb_triples} - {dropped} triples dropped.") as bar:

        def on_bucket(bucket):
            nonlocal inserted, dropped
            for (s, p, o) in bucket:
                columns = {
                    b'rdf:subject': s.encode('utf-8'),
                    b'rdf:predicate': p.encode('utf-8'),
                    b'rdf:object': o.encode('utf-8')
                }
                spo_key = build_row_key(s, p, o)
                pos_key = build_row_key(p, o, s)
                osp_key = build_row_key(o, s, p)
                spo_batch.put(spo_key, columns)
                pos_batch.put(pos_key, columns)
                osp_batch.put(osp_key, columns)
            inserted = inserted + len(bucket)
            bar.label = f"Inserting RDF triples {inserted}/{nb_triples} - {dropped} triples dropped."
            bar.update(len(bucket))

        def on_error(error):
            nonlocal dropped, inserted
            if isinstance(error, ParseError):
                logger.warning(error)
                dropped = dropped + 1
                bar.label = f"Inserting RDF triples {inserted}/{nb_triples} - {dropped} triples dropped."
                bar.update(0)
            else:
                logger.error(error)
                exit(1)

        def on_complete():
            nonlocal start
            # send last batch
            spo_batch.send()
            pos_batch.send()
            osp_batch.send()
            logger.info(f"RDF triples ingestion successfully completed in {time() - start}s")
            logger.info("Committing and cleaning up...")
            connection.close()
            logger.info(f"RDF data from file '{rdf_file}' successfully inserted into RDF graph '{graph_name}'")

        logger.info("Starting RDF triples ingestion...")
        parser = ParserFactory.create_parser(format, batch_size)
        parser.on_bucket = on_bucket
        parser.on_error = on_error
        parser.on_complete = on_complete
        parser.parsefile(rdf_file)
