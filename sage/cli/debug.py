# commons.py
# Author: Thomas MINIER - MIT License 2017-2019
# Author: Pascal Molli - MIT License 2017-2019

# from sage.query_engine.optimizer.query_parser import parse_query
from sage.query_engine.optimizer.parser import Parser
from sage.query_engine.optimizer.optimizer import Optimizer
from sage.database.core.yaml_config import load_config
from sage.query_engine.sage_engine import SageEngine

import click
from math import inf
from sys import exit
import asyncio

import coloredlogs
import logging
from time import time

# install logger
coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


#    (results, saved, done, _) = await engine.execute(scan, 10e7)
async def execute(engine, iterator, limit):
    nb_solutions = 0
    start_time = time()
    while iterator.has_next() and nb_solutions <= limit:
        value = await iterator.next()
        nb_solutions += 1
        print(value)
    elapsed_time = time() - start_time
    print(f'Number of solution mappings: {nb_solutions} - execution time: {elapsed_time}sec')

    # try:
    #     (results, saved, done, _) = await engine.execute(iterator, 10e7)
    #     for r in results:
    #         print(str(r))
    # except StopAsyncIteration:
    #     pass


@click.command()
@click.argument("config_file")
@click.argument("default_graph_uri")
@click.option("-q", "--query", type=str, default=None, help="SPARQL query to execute (passed in command-line)")
@click.option("-f", "--file", type=str, default=None, help="File containing a SPARQL query to execute")
@click.option("-l", "--limit", type=int, default=None, help="Maximum number of solutions bindings to fetch, similar to the SPARQL LIMIT modifier.")
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
    engine = SageEngine()
    context = {'quantum': 1000000, 'max_results': 1000000}
    from time import time
    context['start_timestamp'] = time()
    logical_plan = Parser.parse(query)
    iterator = Optimizer.get_default(context).optimize(
        logical_plan, dataset, default_graph_uri, context
    )
    # iterator, cards = parse_query(query, dataset, default_graph_uri, context)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(execute(engine, iterator, limit))
    loop.close()


if __name__ == '__main__':
    sage_query_debug()
