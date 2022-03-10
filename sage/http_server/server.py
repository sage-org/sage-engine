# server.py
# Author: Thomas MINIER - MIT License 2017-2020
import logging

import sage.http_server.responses as responses

from traceback import format_exc
from uvloop import EventLoopPolicy
from asyncio import set_event_loop_policy
from os import environ
from sys import setrecursionlimit
from time import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlunparse
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response, StreamingResponse

from sage.database.core.dataset import Dataset
from sage.database.core.yaml_config import load_config
from sage.database.descriptors import VoidDescriptor, many_void
from sage.query_engine.optimizer.parser import Parser
from sage.query_engine.optimizer.optimizer import Optimizer
from sage.query_engine.optimizer.physical.visitors.query_plan_stringifier import QueryPlanStringifier
from sage.database.saved_plan.saved_plan_manager import SavedPlanManager
from sage.database.saved_plan.stateless_manager import StatelessManager
from sage.database.saved_plan.statefull_manager import StatefullManager
from sage.query_engine.sage_engine import SageEngine


class SagePostQuery(BaseModel):
    """Data model for the body of POST SPARQL queries"""
    query: str = Field(..., description="The SPARQL query to execute.")
    defaultGraph: str = Field(..., description="The URI of the default RDF graph queried.")
    next: str = Field(None, description="(Optional) A next link used to resume query execution from a saved state.")
    forceOrder: bool = Field(False, description="True to fix the join ordering, False otherwise.")


def choose_void_format(mimetypes):
    if "text/turtle" in mimetypes:
        return "turtle", "text/turtle"
    elif "application/xml" in mimetypes:
        return "xml", "application/xml"
    elif "application/n-quads" in mimetypes:
        return "nquads", "application/n-quads"
    elif "application/trig" in mimetypes:
        return "trig", "application/trig"
    elif "application/json" in mimetypes or "application/json+ld" in mimetypes:
        return "json-ld", "application/json"
    return "ntriples", "application/n-triples"


async def execute_query(
    query: str, default_graph_uri: str, next_link: Optional[str],
    dataset: Dataset, saved_plan_manager: SavedPlanManager
) -> Tuple[List[Dict[str, str]], Optional[str], Dict[str, str]]:
    """Execute a query using the SageEngine and returns the appropriate HTTP response.

    Any failure will results in a rollback/abort on the current query execution.

    Args:
      * query: SPARQL query to execute.
      * default_graph_uri: URI of the default RDF graph to use.
      * next_link: URI to a saved plan. Can be `None` if query execution should starts from the beginning.
      * dataset: RDF dataset on which the query is executed.
      * saved_plan_manager: The saved plan manager to save and restore SPARQL query execution plans.

    Returns:
      A tuple (`bindings`, `next_page`, `stats`) where:
      * `bindings` is a list of query results.
      * `next_page` is a link to saved query execution state. Sets to `None` if query execution completed during the time quantum.
      * `stats` are statistics about query execution.

    Throws: Any exception that have occured during query execution.
    """
    graph = None
    try:
        if not dataset.has_graph(default_graph_uri):
            raise HTTPException(status_code=404, detail=f"RDF Graph {default_graph_uri} not found on the server.")
        graph = dataset.get_graph(default_graph_uri)

        optimizer = Optimizer.get_default(dataset)

        # decode next_link or build the query execution plan
        cardinalities = dict()
        loadin_start = time()
        if next_link is not None:
            plan = saved_plan_manager.get_plan(next_link, dataset)
            saved_plan_manager.delete_plan(next_link)
        else:
            start_timestamp = datetime.now()
            logical_plan = Parser.parse(query)
            plan, cardinalities = optimizer.optimize(logical_plan, dataset, default_graph_uri, as_of=start_timestamp)
        loading_time = (time() - loadin_start) * 1000
        logging.info(f'loading time: {loading_time}ms')

        # execute the query
        engine = SageEngine()
        coverage_before = optimizer.coverage(plan)
        bindings, is_done, abort_reason = await engine.execute(
            plan, quota=graph.quota, max_results=graph.max_results)
        coverage_after = 1.0 if is_done else optimizer.coverage(plan)

        # commit or abort (if necessary)
        if abort_reason is not None:
            graph.abort()
            raise HTTPException(status_code=500, detail=f"The SPARQL query has been aborted for the following reason: '{abort_reason}'")
        graph.commit()

        # encode the saved plan if the query execution is not done
        export_start = time()
        if (not is_done) and (abort_reason is None):
            next_page = saved_plan_manager.save_plan(plan)
        else:
            next_page = None
        export_time = (time() - export_start) * 1000
        logging.info(f'export time: {export_time}ms')

        # compute statistics about the query execution
        stats = {
            "cardinalities": cardinalities,
            "import": loading_time,
            "export": export_time,
            "metrics": {
                "progression": coverage_after,
                "coverage": coverage_after - coverage_before,
                "cost": optimizer.cost(plan),
                "cardinality": optimizer.cardinality(plan)}}
        logging.info(stats['metrics'])

        return (bindings, next_page, stats)
    except Exception as err:
        # abort all ongoing transactions, then forward the exception
        logging.error(format_exc())
        if graph is not None:
            graph.abort()
        raise err


async def explain_query(
    query: str, default_graph_uri: str, next_link: Optional[str],
    dataset: Dataset
) -> str:
    optimizer = Optimizer.get_default(dataset)
    if next_link is not None:
        plan = StatelessManager().get_plan(next_link, dataset)
    else:
        logical_plan = Parser.parse(query)
        plan, cardinalities = optimizer.optimize(logical_plan, dataset, default_graph_uri)
    return JSONResponse({
        "query": QueryPlanStringifier().visit(plan),
        "cost": optimizer.cost(plan),
        "cardinality": optimizer.cardinality(plan)})


def create_response(mimetypes: List[str], bindings: List[Dict[str, str]], next_page: Optional[str], stats: dict, skol_url: str) -> Response:
    """Create an HTTP response for the results of SPARQL query execution.

    Args:
      * mimetypes: mimetypes from the input HTTP request.
      * bindings: list of query results.
      * next_link: Link to a SaGe saved plan. Use `None` if there is no one, i.e., the query execution has completed during the quantum.
      * stats: Statistics about query execution.
      * skol_url: URL used for the skolemization of blank nodes.

    Returns:
      An HTTP response built from the input mimetypes and the SPARQL query results.
    """
    if "application/json" in mimetypes:
        iterator = responses.raw_json_streaming(bindings, next_page, stats, skol_url)
        return StreamingResponse(iterator, media_type="application/json")
    elif "application/sparql-results+json" in mimetypes:
        iterator = responses.w3c_json_streaming(bindings, next_page, stats, skol_url)
        return StreamingResponse(iterator, media_type="application/json")
    elif "application/xml" in mimetypes or "application/sparql-results+xml" in mimetypes:
        iterator = responses.w3c_xml(bindings, next_page, stats)
        return Response(iterator, media_type="application/xml")
    return JSONResponse({"bindings": bindings, "next": next_page, "stats": stats})


def run_app(config_file: str) -> FastAPI:
    """Create the HTTP server, compatible with uvicorn/gunicorn.

    Argument: SaGe configuration file, in YAML format.

    Returns: The FastAPI HTTP application.
    """
    # enable uvloop for SPARQL query processing
    set_event_loop_policy(EventLoopPolicy())
    # set recursion depth (due to pyparsing issues)
    setrecursionlimit(3000)

    # create the HTTP server & activate CORS
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"])

    # Build the RDF dataset from the configuration file
    dataset = load_config(config_file)

    # Build the saved plan manager
    if dataset.is_stateless:
        saved_plan_manager = StatelessManager()
    else:
        saved_plan_manager = StatefullManager()

    async def execute_sparql_query(
        request: Request, query: str, default_graph_uri: str, next_link: Optional[str]
    ) -> Response:
        """Execute a SPARQL query using the Web Preemption model"""
        try:
            mimetypes = request.headers['accept'].split(",")
            server_url = urlunparse(request.url.components[0:3] + (None, None, None))
            bindings, next_page, stats = await execute_query(query, default_graph_uri, next_link, dataset, saved_plan_manager)
            response = create_response(mimetypes, bindings, next_page, stats, server_url)
            return response
        except HTTPException as err:
            raise err
        except Exception as err:
            raise HTTPException(status_code=500, detail=str(err))

    @app.get("/")
    async def root():
        return "The SaGe SPARQL query server is running!"

    @app.get("/sparql")
    async def sparql_get(
        request: Request,
        query: str = Query(..., description="The SPARQL query to execute."),
        default_graph_uri: str = Query(..., alias="default-graph-uri", description="The URI of the default RDF graph queried."),
        next_link: str = Query(None, alias="next", description="(Optional) A next link used to resume query execution from a saved state."),
        join_order: bool = Query(False, alias="join-order", description="True to fix the join ordering, False otherwise.")
    ):
        dataset.force_order = join_order
        return await execute_sparql_query(request, query, default_graph_uri, next_link)

    @app.post("/sparql")
    async def sparql_post(request: Request, item: SagePostQuery):
        dataset.force_order = item.forceOrder
        return await execute_sparql_query(request, item.query, item.defaultGraph, item.next)

    @app.post("/sparql/explain")
    async def sparql_post_explain(request: Request, item: SagePostQuery):
        dataset.force_order = item.forceOrder
        return await explain_query(item.query, item.defaultGraph, item.next, dataset)

    @app.get("/void/", description="Get the VoID description of the SaGe server")
    async def server_void(request: Request):
        """Describe all RDF datasets hosted by the Sage endpoint"""
        try:
            mimetypes = request.headers['accept'].split(",")
            url = urlunparse(request.url.components[0:3] + (None, None, None))
            if url.endswith('/'):
                url = url[0:len(url) - 1]
            void_format, res_mimetype = choose_void_format(mimetypes)
            description = many_void(url, dataset, void_format)
            return Response(description, media_type=res_mimetype)
        except Exception as err:
            logging.error(err)
            raise HTTPException(status_code=500, detail=str(err))

    @app.get("/.well-known/void/")
    async def well_known():
        """Alias for /void/"""
        return RedirectResponse(url="/void/")

    @app.get("/void/{graph_name}", description="Get the VoID description of a RDF Graph hosted by the SaGe server")
    async def graph_void(request: Request, graph_name: str = Field(..., description="Name of the RDF Graph")):
        """Get the VoID description of a RDF Graph hosted by the SaGe server"""
        graph = dataset.get_graph(graph_name)
        if graph is None:
            raise HTTPException(status_code=404, detail=f"RDF Graph {graph_name} not found on the server.")
        try:
            mimetypes = request.headers['accept'].split(",")
            url = urlunparse(request.url.components[0:3] + (None, None, None))
            if url.endswith('/'):
                url = url[0:len(url) - 1]
            descriptor = VoidDescriptor(url, graph)
            void_format, res_mimetype = choose_void_format(mimetypes)
            return Response(descriptor.describe(void_format), media_type=res_mimetype)
        except Exception as err:
            logging.error(err)
            raise HTTPException(status_code=500, detail=str(err))

    return app


if 'SAGE_CONFIG_FILE' in environ:
    config_file = environ['SAGE_CONFIG_FILE']
    app = run_app(config_file)
elif __name__ == "__main__":
    raise RuntimeError("You cannot run the script server.py as a plain script. Please use the SaGe CLI to start you own server.")
