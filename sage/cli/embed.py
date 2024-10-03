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
import sys

# from sage.query_engine.optimizer.query_parser import parse_query
from sage.query_engine.optimizer.parser import Parser
from sage.query_engine.optimizer.optimizer import Optimizer
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.database.core.yaml_config import load_config
from sage.http_server.server import execute_query

from sage.database.saved_plan.saved_plan_manager import SavedPlanManager
from sage.database.saved_plan.stateless_manager import StatelessManager
from sage.database.core.dataset import Dataset

# install the logger
coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

async def execute_loop(query, default_graph_uri, dataset, saved_plan_manager,limit,timeout):
    start_time = time()
    next_size=0
    nb_solutions = 0
    nb_quantum = 1
    bindings,next_page, stats= await execute_query(query,default_graph_uri,None,dataset,saved_plan_manager)
    nb_solutions += len(bindings)
    next_size = sys.getsizeof(next_page)
    print("bindings",bindings)
    while next_page is not None and nb_solutions < limit and time() - start_time < timeout:
        bindings,next_page, stats= await execute_query(query,default_graph_uri,next_page,dataset,saved_plan_manager)
        print("bindings",bindings)
        nb_solutions += len(bindings)
        nb_quantum += 1
        next_size += sys.getsizeof(next_page)
    elapsed_time = time() - start_time
    print(f'nb_quantum: {nb_quantum}, nb_results: {nb_solutions}, next_size: {next_size/1024} kb, execution time: {elapsed_time} sec')

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
@click.option(
    "-t", "--timeout", type=click.INT, default=None,
    help="Stop execution after timeout in seconds."
)

def sage_exec(config_file, default_graph_uri, query, file, limit,timeout):
    """
        run a SPARQL query on an embedded Sage Server.

        Example usage: sage-exec config.yaml http://example.org/swdf-postgres -f queries/spo.sparql
    """
    # assert that we have a query to evaluate
    if query is None and file is None:
        print("Error: you must specificy a query to execute, either with --query or --file. See sage-query --help for more informations.")
        exit(1)

    logging.basicConfig(level=logging.DEBUG)

    if limit is None:
        limit = inf

    if timeout is None:
        timeout = inf

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

    saved_plan_manager=StatelessManager()

    asyncio.run(execute_loop(query, default_graph_uri, dataset, saved_plan_manager,limit,timeout)) 

if __name__ == '__main__':
    sage_exec()
