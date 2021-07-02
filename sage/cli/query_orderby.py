# commons.py
# Author: Thomas MINIER - MIT License 2017-2019
import logging
import coloredlogs
import click
import requests
from json import dumps
from math import inf
from sys import exit
import re
from time import time
from json import dumps
from statistics import mean
from pathlib import Path

from rdflib.plugins.sparql.parser import parseQuery, parseUpdate
from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate
from rdflib.plugins.sparql.algebra import pprintAlgebra

from sage.query_engine.protobuf.iterators_pb2 import RootTree
from sage.http_server.utils import decode_saved_plan, encode_saved_plan
from sage.query_engine.iterators.loader import load
from sage.http_server.utils import decode_saved_plan, encode_saved_plan
from sage.query_engine.protobuf.iterators_pb2 import (RootTree,SavedProjectionIterator,SavedOneTopkIterator, SavedLimitIterator, SavedScanIterator)
from sage.query_engine.protobuf.utils import pyDict_to_protoDict
from sage.query_engine.iterators.filter import to_rdflib_term

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


@click.command()
@click.argument("entrypoint")
@click.argument("default_graph_uri")
@click.option("-q", "--query", type=str, default=None, help="SPARQL query to execute (passed in command-line)")
@click.option("-f", "--file", type=str, default=None, help="File containing a SPARQL query to execute")
@click.option("--format", type=click.Choice(["json", "xml"]), default="json", help="Format of the results set, formatted according to W3C SPARQL standards.")
@click.option("--measures", type=str, default=None,
    help="The file in which query execution statistics will be stored.")
@click.option("--limit", type=int, default=10,
    help="Limit of a of SPARQL query (overwrite existing limit if it exists).")
@click.option("--output", type=str, default=None,
    help="The file in which the query result will be stored.")
def sage_query_orderby(entrypoint, default_graph_uri, query, file, format,measures,limit,output):
    """
        Send a SPARQL query to a SaGe server hosted at ENTRYPOINT, with DEFAULT_GRAPH_URI as the default RDF Graph. It does not act as a Smart client, so only queries supported by the server will be evaluated.

        Example usage: sage-query http://sage.univ-nantes.fr/sparql http://sage.univ-nantes.fr/sparql/dbpedia-2016-04 -q "SELECT * WHERE { ?s ?p ?o }"
    """
    # assert that we have a query to evaluate
    if query is None and file is None:
        print("Error: you must specificy a query to execute, either with --query or --file. See sage-query --help for more informations.")
        exit(1)

    # load query from file if required
    query_name=""
    if file is not None:
        with open(file) as query_file:
            query = query_file.read()
            query_name=Path(file).stem


    orderclause=""
    length=0
    engine="orderbyone"

    # limit x -> limit 0
    m = re.search('(.*)order by(.*) limit (.*)', query,re.DOTALL)
    orderclause=m.group(2)
    length=int(m.group(3))
    query=f'{m.group(1)} order by {m.group(2)} limit 0'
    logger.info(f"query rewritten as:{query}")

    if limit is not None:
        length=limit

    compiled_expr = parseQuery(f"SELECT * WHERE {{?s ?p ?o}} order by {orderclause}")
    compiled_expr = translateQuery(compiled_expr)
    expr = compiled_expr.algebra.p.p.expr

    # prepare query headers
    headers = {
        "accept": "text/html",
        "content-type": "application/json",
        "next": None
    }
    # TODO support xml
    # if format == "xml":
    #     headers["Accept"] = "application/sparql-results+xml"

    payload = {
        "query": query,
        "defaultGraph": default_graph_uri
    }

    nb_calls = 0
    results = list()
    nb_results = 0
    execution_time = 0
    loading_times = list()
    resume_times = list()


    has_next = True
    count = 0
    topk=[]
    while has_next:
        start_time = time()
        response = requests.post(entrypoint, headers=headers, data=dumps(payload))
        execution_time += time() - start_time
        nb_calls += 1

        json_response = response.json()
        has_next = json_response['next']
        payload['next'] = json_response['next']

        #results.extend(json_response["bindings"])
        nb_results += len(json_response["bindings"])
        loading_times.append(json_response["stats"]["import"])
        resume_times.append(json_response["stats"]["export"])


        ## recomping topk
        for bindings in json_response['bindings']:
         topk.append(bindings)

        for e in reversed(expr):
            reverse = bool(e.order and e.order == 'DESC')
            topk = sorted(topk, key=lambda x: to_rdflib_term(x['?'+e.expr]),reverse=reverse)
        if len(topk)>length:
            logger.info(f"cutting from {len(topk)} to {length}")
            del topk[length:]

        if has_next is not None and len(topk)>0 :
            #updating the OneTopkIterator in the saved_plan
            plan = decode_saved_plan(json_response["next"])
            root = RootTree()
            root.ParseFromString(plan)
            ob=root.limit_source.proj_source.onetopk_source
            logger.info(f"topk set to:{topk[-1]}")
            pyDict_to_protoDict(topk[-1], ob.topk)
            payload['next']=encode_saved_plan(root)

    logger.info(f'nbres:{len(topk)},res:{topk}')

    if output is not None:
        with open(output, 'w') as output_file:
            output_file.write(dumps(topk))

    if measures is not None:
        with open(measures, 'w') as measures_file:
            avg_loading_time = mean(loading_times)
            avg_resume_time = mean(resume_times)
            measures_file.write(f'{query_name},{engine},{limit},{execution_time},{nb_calls},{nb_results},{avg_loading_time},{avg_resume_time}')
    logger.info(f'Query complete in {execution_time}s with {nb_calls} HTTP calls. {nb_results} retreived mappings !')

if __name__ == "__main__":
    sage_query_orderby()
