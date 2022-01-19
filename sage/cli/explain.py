#!/usr/bin/python
import click
import pprint

from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate
from rdflib.plugins.sparql.parser import parseQuery, parseUpdate
from rdflib.plugins.sparql.algebra import pprintAlgebra
from rdflib.plugins.sparql.parserutils import prettify_parsetree

from sage.database.core.yaml_config import load_config
from sage.query_engine.optimizer.parser import Parser
from sage.query_engine.optimizer.optimizer import Optimizer
from sage.query_engine.optimizer.physical.visitors.query_plan_stringifier import QueryPlanStringifier
# from sage.query_engine.optimizer.query_parser import parse_query

# be sure to load what i beleive ;)
# print(inspect.getfile(register_custom_function))
# print(inspect.getfile(parseQuery))

# seems that register custom function not present in rdflib 4.2.2
# only on very last version > 4.5
# from rdflib.plugins.sparql.operators import register_custom_function
# def rowid(x,y,z):
#    return Literal("%s %s %s" % (x, y,z), datatype=XSD.string)
#    SAGE = Namespace('http://example.org/')
#    print(SAGE.rowid)
#    register_custom_function(SAGE.rowid, rowid)


# async def execute(iterator):
#     try:
#         while iterator.has_next():
#             value = await iterator.next()
#             # discard null values
#             if value is not None:
#                 print(value)
#     except StopAsyncIteration:
# #        print("stop")
#         pass


@click.command()
@click.argument("config_file")
@click.argument("graph_uri")
@click.option(
    "-q", "--query", type=click.STRING, default=None,
    help="SPARQL query to execute (passed in command-line)"
)
@click.option(
    "-f", "--file", type=click.STRING, default=None,
    help="File containing a SPARQL query to execute"
)
@click.option(
    "-u", "--update", is_flag=True, help="explain a SPARQL update query"
)
@click.option(
    "-p", "--parse", is_flag=True, help="print the query parse tree"
)
@click.option(
    "-i", "--indentnb", default=1, help="pretty print indent value"
)
@click.option(
    "-o", "--output", type=click.STRING, default=None,
    help="File containing a SPARQL query to execute"
)
def explain(
    query, file, config_file, graph_uri, indentnb, update, parse, output
):
    if query is None and file is None:
        print("Error: you must specificy a query to execute, either with --query or --file. See sage-query --help for more informations.")
        exit(1)

    if file is not None:
        with open(file) as query_file:
            query = query_file.read()

    dataset = load_config(config_file)
    if dataset is None:
        print(f"config file {config_file} not found")
        exit(1)

    graph = dataset.get_graph(graph_uri)
    if graph is None:
        print(f"RDF Graph  not found: {graph_uri}")
        exit(1)

    pp = pprint.PrettyPrinter(indent=indentnb)

    if query is None:
        exit(1)

    print("------------")
    print("Query")
    print("------------")
    print(query)

    if update:
        pq = parseUpdate(query)
    else:
        pq = parseQuery(query)

    if pq is None:
        exit(1)

    if parse:
        print("------------")
        print("Parsed Query")
        print("------------")
        print(prettify_parsetree(pq))

    if update:
        tq = translateUpdate(pq)
    else:
        tq = translateQuery(pq)

    print("------------")
    print("Algebra")
    print("------------")
    print(pprintAlgebra(tq))

    logical_plan = Parser.parse(query)
    iterator, cardinalities = Optimizer.get_default(dataset).optimize(
        logical_plan, dataset, graph_uri
    )
    # iterator, cards = parse_query(query, dataset, graph_uri)

    print("-----------------")
    print("Iterator pipeline")
    print("-----------------")
    print(iterator.explain(step=2))
    if output is not None:
        with open(output, 'w') as outfile:
            outfile.write(QueryPlanStringifier().visit(iterator))

    print("-----------------")
    print("Optimized query")
    print("-----------------")
    print(QueryPlanStringifier().visit(iterator))

    print("-----------------")
    print("Cardinalities")
    print("-----------------")
    pp.pprint(cardinalities)

    # if you want to run it call sage-query !
    # print("-----------------")
    # print("Results")
    # print("-----------------")
    #
    # client=TestClient(run_app(config_file))
    #
    # # next_link = None
    # # response = post_sparql(client, query, next_link, graph_uri)
    # # response=response.json()
    # # print(json.dumps(response,indent=4))
    # # print("next:"+str(response['next']))
    #
    #
    # nbResults = 0
    # nbCalls = 0
    # hasNext = True
    # next_link = None
    # while hasNext:
    #     response = post_sparql(client, query, next_link, graph_uri)
    #     response = response.json()
    #     nbResults += len(response['bindings'])
    #     hasNext = response['hasNext']
    #     next_link = response['next']
    #     nbCalls += 1
    #
    #     print(json.dumps(response['bindings'],indent=4))


#    loop = asyncio.get_event_loop()
#    loop.run_until_complete(execute(iterator))
#    loop.close()

if __name__ == '__main__':
    explain()
