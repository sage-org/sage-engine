import click
import asyncio
import os

from time import time
from sys import exit
from typing import Dict, Any

from sage.database.core.dataset import Dataset
from sage.query_engine.optimizer.parser import Parser
from sage.query_engine.optimizer.optimizer import Optimizer
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.sage_engine import SageEngine


async def execute(
    pipeline: PreemptableIterator, limit: int = 10,
    context: Dict[str, Any] = {}
) -> None:
    engine = SageEngine()

    start_time = time()
    solutions, is_done, abort_reason = await engine.execute(pipeline, context=context)
    elapsed_time = time() - start_time

    if limit > 0:
        print(solutions[:limit])
    print(f"Query completed in {elapsed_time}s with {len(solutions)} solutions")


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
    "--limit", type=click.INT, default=10,
    help="Print the first k solutions")
def sage_query_debug(config_file, default_graph_uri, query, file, limit):
    """
    Debug a SPARQL query on an embedded Sage Server.

    Example
    -------
    >>> sage-query config.yaml http://example.org/swdf-postgres -f queries/spo.sparql
    """
    if query is None and file is None:
        print(
            "Error: you must specificy a query to execute, either with "
            "--query or --file. See sage-query --help for more informations.")
        exit(1)
    if file is not None:
        with open(file) as query_file:
            query = query_file.read()

    os.environ["SAGE_CONFIG_FILE"] = config_file
    dataset = Dataset()

    if not dataset.has_graph(default_graph_uri):
        print(f"RDF Graph {default_graph_uri} not found")
        exit(1)

    context = {"default_graph_uri": default_graph_uri, "quota": 1000000000}

    logical_plan = Parser.parse(query)
    pipeline, cardinalities = Optimizer.get_default().optimize(logical_plan, context=context)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(execute(pipeline, context=context, limit=limit))
    loop.close()


if __name__ == "__main__":
    sage_query_debug()
