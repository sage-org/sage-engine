import click
import asyncio

from time import time
from sys import exit

from sage.query_engine.optimizer.parser import Parser
from sage.query_engine.optimizer.optimizer import Optimizer
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.sage_engine import SageEngine
from sage.database.core.yaml_config import load_config


async def execute(pipeline: PreemptableIterator, print_solutions: bool = False):
    context = {}
    context.setdefault('quota', 1000000000)
    engine = SageEngine()
    start_time = time()
    solutions, is_done, abort_reason = await engine.execute(pipeline, context=context)
    elapsed_time = time() - start_time

    if print_solutions:
        print(solutions)

    print(f'Query completed in {elapsed_time}s with {len(solutions)} solutions')


@click.command()
@click.argument("config_file")
@click.argument("default_graph_uri")
@click.option(
    "-q", "--query", type=click.STRING, default=None,
    help="SPARQL query to execute (passed in command-line)")
@click.option(
    "-f", "--file", type=click.STRING, default=None,
    help="File containing a SPARQL query to execute")
@click.option(
    "--print-solutions/--ignore-solutions", default=False,
    help="True to print the solutions, False otherwise")
def sage_query_debug(config_file, default_graph_uri, query, file, print_solutions):
    """
        debug a SPARQL query on an embedded Sage Server.

        Example usage: sage-query config.yaml http://example.org/swdf-postgres -f queries/spo.sparql
    """
    # assert that we have a query to evaluate
    if query is None and file is None:
        print("Error: you must specificy a query to execute, either with --query or --file. See sage-query --help for more informations.")
        exit(1)

    # load query from file if required
    if file is not None:
        with open(file) as query_file:
            query = query_file.read()

    dataset = load_config(config_file)
    if dataset is None:
        print(f"config file {config_file} not found")
        exit(1)
    graph = dataset.get_graph(default_graph_uri)
    if graph is None:
        print(f"RDF Graph {default_graph_uri} not found")
        exit(1)

    logical_plan = Parser.parse(query)
    pipeline, cardinalities = Optimizer.get_default(dataset).optimize(
        logical_plan, dataset, default_graph_uri)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(execute(pipeline, print_solutions=print_solutions))
    loop.close()


if __name__ == '__main__':
    sage_query_debug()
