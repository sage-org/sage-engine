# commons.py
# Author: Thomas MINIER - MIT License 2017-2019
# Author: Pascal Molli - MIT License 2017-2019

import click
import asyncio
import logging
import coloredlogs

from time import time
from math import inf
from sys import exit

# from sage.query_engine.optimizer.query_parser import parse_query
from sage.query_engine.optimizer.parser import Parser
from sage.query_engine.optimizer.optimizer import Optimizer
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.database.core.yaml_config import load_config


# install the logger
coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


async def execute(iterator: PreemptableIterator, limit: int):
    nb_solutions = 0
    start_time = time()
    while nb_solutions <= limit:
        solution = await iterator.next()
        if solution is None:
            break
        nb_solutions += 1
        print(solution)
    elapsed_time = time() - start_time
    print(f'Number of solution mappings: {nb_solutions} - execution time: {elapsed_time}sec')


@click.command()
@click.argument("config_file")
@click.argument("default_graph_uri")
@click.option(
    "-q", "--query", type=click.STRING, default=None,
    help="SPARQL query to execute (passed in command-line)"
)
@click.option(
    "-f", "--file", type=click.STRING, default=None,
    help="File containing a SPARQL query to execute"
)
@click.option(
    "-l", "--limit", type=click.INT, default=None,
    help="Maximum number of solutions bindings to fetch, similar to the SPARQL LIMIT modifier."
)
def sage_query_debug(config_file, default_graph_uri, query, file, limit):
    """
        debug a SPARQL query on an embedded Sage Server.

        Example usage: sage-query config.yaml http://example.org/swdf-postgres -f queries/spo.sparql
    """
    # assert that we have a query to evaluate
    if query is None and file is None:
        print("Error: you must specificy a query to execute, either with --query or --file. See sage-query --help for more informations.")
        exit(1)

    logging.basicConfig(level=logging.DEBUG)

    if limit is None:
        limit = inf

    # load query from file if required
    if file is not None:
        with open(file) as query_file:
            query = query_file.read()

    dataset = load_config(config_file)
    if dataset is None:
        print("config file {config_file} not found")
        exit(1)
    graph = dataset.get_graph(default_graph_uri)
    if graph is None:
        print(f"RDF Graph  not found: {default_graph_uri}")
        exit(1)

    logical_plan = Parser.parse(query)
    iterator, cardinalities = Optimizer.get_default(dataset).optimize(
        logical_plan, dataset, default_graph_uri
    )
    # iterator, cards = parse_query(query, dataset, default_graph_uri)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(execute(iterator, limit))
    loop.close()


if __name__ == '__main__':
    sage_query_debug()
