import click
import os

from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate
from rdflib.plugins.sparql.parser import parseQuery, parseUpdate
from rdflib.plugins.sparql.algebra import pprintAlgebra
from rdflib.plugins.sparql.parserutils import prettify_parsetree

from sage.database.core.dataset import Dataset
from sage.query_engine.optimizer.parser import Parser
from sage.query_engine.optimizer.optimizer import Optimizer


@click.command()
@click.argument("config_file")
@click.argument("graph_uri")
@click.option(
    "-q", "--query", type=click.STRING, default=None,
    help="SPARQL query to execute (passed in command-line)")
@click.option(
    "-f", "--file", type=click.STRING, default=None,
    help="File containing a SPARQL query to execute")
@click.option(
    "-i", "--indentnb", default=1, help="pretty print indent value")
def explain(
    config_file, graph_uri, query, file, indentnb
) -> None:
    if query is None and file is None:
        print(
            "Error: you must specificy a query to execute, either with "
            "--query or --file. See sage-query --help for more information.")
        exit(1)
    if file is not None:
        with open(file) as query_file:
            query = query_file.read()

    os.environ["SAGE_CONFIG_FILE"] = config_file
    dataset = Dataset()

    if not dataset.has_graph(graph_uri):
        print(f"RDF Graph not found: {graph_uri}")
        exit(1)

    print(f"{'-' * 20}\nQuery\n{'-' * 20}")
    print(query)

    try:
        pq = parseUpdate(query)
    except Exception:
        pq = parseQuery(query)

    print(f"{'-' * 20}\nParsed Query\n{'-' * 20}")
    print(prettify_parsetree(pq))

    try:
        tq = translateUpdate(pq)
    except Exception:
        tq = translateQuery(pq)

    print(f"{'-' * 20}\nAlgebra\n{'-' * 20}")
    print(pprintAlgebra(tq))

    context = {"default_graph_uri": graph_uri, "early_pruning": True}
    logical_plan = Parser.parse(query)
    iterator = Optimizer.get_default().optimize(logical_plan, context=context)

    print(f"{'-' * 20}\nIterator pipeline\n{'-' * 20}")
    print(iterator.explain())

    print(f"{'-' * 20}\nOptimized query\n{'-' * 20}")
    print(iterator.stringify())


if __name__ == "__main__":
    explain()
